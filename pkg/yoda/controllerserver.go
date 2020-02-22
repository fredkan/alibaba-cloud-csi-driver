/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package yoda

import (
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/yoda/lvmcs"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"time"
)

const (
	connectTimeout = 3 * time.Second
	PV_NAME_TAG    = "csi.storage.k8s.io/pv/name"
	PVC_NAME_TAG   = "csi.storage.k8s.io/pvc/name"
	PVC_NS_TAG     = "csi.storage.k8s.io/pvc/namespace"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	client kubernetes.Interface
}

// BindingInfo represents the pvc and disk/lvm mapping
type BindingInfo struct {
	// node is the name of selected node
	Node string `json:"node"`
	// path for mount point
	Disk string `json:"disk"`
	// VgName is the name of selected volume group
	VgName string `json:"vgName"`
	// Device is the name for raw block device: /dev/vdb
	Device string `json:"device"`
	// [lvm] or [disk] or [device] or [quota]
	VolumeType string `json:"volumeType"`

	// PersistentVolumeClaim is the metakey for pvc: {namespace}/{name}
	PersistentVolumeClaim string `json:"persistentVolumeClaim"`
}

// newControllerServer creates a controllerServer object
func newControllerServer(d *csicommon.CSIDriver) *controllerServer {
	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err.Error())
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		client:                  kubeClient,
	}
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Errorf("CreateVolume: invalid create volume req: %v", req)
		return nil, err
	}
	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}
	log.Infof("CreateVolume: starting to create volume with: %v", req)

	// Define variable
	// check volumeType, only support MountPoint/LVM/Device
	pvcName, pvcNameSpace, volumeType := "", "", ""
	volumeID := req.GetName()
	parameters := req.GetParameters()
	if value, ok := parameters["volumeType"]; ok {
		volumeType = value
	}
	if volumeType != LvmVolumeType && volumeType != MountPointType && volumeType != DeviceVolumeType {
		log.Errorf("CreateVolume: Create volume with error volumeType %v", parameters)
		return nil, status.Error(codes.InvalidArgument, "Yoda only support localVolume/lvm/device volume type")
	}

	// check pvc info
	if value, ok := parameters[PVC_NAME_TAG]; ok {
		pvcName = value
	}
	if value, ok := parameters[PVC_NS_TAG]; ok {
		pvcNameSpace = value
	}
	if pvcName == "" || pvcNameSpace == "" {
		log.Errorf("CreateVolume: Create Volume with error pvc info %v", parameters)
		return nil, status.Error(codes.InvalidArgument, "Create LocalVolume with empty pvc info")
	}
	nodeID := pickNodeID(req.GetAccessibilityRequirements())

	// Schedule lvm volume Info
	if volumeType == LvmVolumeType {
		vgName := ""
		if value, ok := parameters["vgName"]; ok {
			vgName = value
		}
		if vgName == "" || nodeID == "" {
			volumeInfo, err := ScheduleVolume(LvmVolumeType, pvcName, pvcNameSpace, vgName, nodeID)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "lvm schedule with error "+err.Error())
			}
			if volumeInfo.VgName == "" || volumeInfo.Node == "" {
				log.Errorf("Lvm Schedule finished, but get empty: %v", volumeInfo)
				return nil, status.Error(codes.InvalidArgument, "lvm schedule finish but vgName/Node empty")
			}
			vgName = volumeInfo.VgName
			nodeID = volumeInfo.Node
		}
		parameters["vgName"] = vgName

	} else if volumeType == MountPointType {
		volumeInfo, err := ScheduleVolume(MountPointType, pvcName, pvcNameSpace, "", nodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "local volume schedule with error "+err.Error())
		}
		if volumeInfo.Disk == "" {
			log.Errorf("LocalVolume Schedule finished, but get empty Disk: %v", volumeInfo)
			return nil, status.Error(codes.InvalidArgument, "lvm schedule finish but Disk empty")
		}
		parameters[MountPointType] = volumeInfo.Disk

	} else if volumeType == DeviceVolumeType {
		volumeInfo, err := ScheduleVolume(DeviceVolumeType, pvcName, pvcNameSpace, "", nodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "device schedule with error "+err.Error())
		}
		if volumeInfo.Device == "" {
			log.Errorf("Device Schedule finished, but get empty Device: %v", volumeInfo)
			return nil, status.Error(codes.InvalidArgument, "lvm schedule finish but Device empty")
		}
		parameters[DeviceVolumeType] = volumeInfo.Device

	} else {
		log.Errorf("CreateVolume: Create with no support type %s", volumeType)
		return nil, status.Error(codes.InvalidArgument, "Create with no support type "+volumeType)
	}

	response := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			VolumeContext: parameters,
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						TopologyNodeKey: nodeID,
					},
				},
			},
		},
	}

	log.Infof("Success create Volume: %s, NodeId: %s, Options: %v, Size: %d", volumeID, nodeID, parameters, req.GetCapacityRange().GetRequiredBytes())
	return response, nil
}

// pickNodeID selects node given topology requirement.
// if not found, empty string is returned.
func pickNodeID(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	nodeList := []string{}
	for _, topology := range requirement.GetPreferred() {
		nodeID, exists := topology.GetSegments()[TopologyNodeKey]
		if exists {
			nodeList = append(nodeList, nodeID)
		}
	}
	if len(nodeList) == 1 {
		return nodeList[0]
	}
	if len(nodeList) > 1 {
		return ""
	}
	for _, topology := range requirement.GetRequisite() {
		nodeID, exists := topology.GetSegments()[TopologyNodeKey]
		if exists {
			nodeList = append(nodeList, nodeID)
		}
	}
	if len(nodeList) != 1 {
		return ""
	}
	return nodeList[0]
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	pvObj, err := getPvObj(cs.client, volumeID)
	if err != nil {
		return nil, err
	}
	if pvObj.Spec.CSI == nil {
		return nil, errors.New("Remove Lvm Failed: volume is not csi type: " + volumeID)
	}
	volumeType := ""
	if value, ok := pvObj.Spec.CSI.VolumeAttributes["volumeType"]; ok {
		volumeType = value
	}

	if volumeType == LvmVolumeType {
		nodeName, vgName, err := getLvmSpec(cs.client, volumeID)
		if err != nil {
			return nil, err
		}
		if nodeName != "" {
			addr, err := getLvmdAddr(cs.client, nodeName)
			if err != nil {
				return nil, err
			}
			conn, err := lvmcs.NewLVMConnection(addr, connectTimeout)
			defer conn.Close()
			if err != nil {
				log.Errorf("DeleteVolume: New lvm %s Connection error: %s", req.GetVolumeId(), err.Error())
				return nil, err
			}

			if _, err := conn.GetLvm(ctx, vgName, volumeID); err == nil {
				if err := conn.DeleteLvm(ctx, vgName, volumeID); err != nil {
					log.Errorf("DeleteVolume: Remove lvm for %s with error: %s", req.GetVolumeId(), err.Error())
					return nil, errors.New("Remove Lvm Failed: " + err.Error())
				}
			} else {
				log.Errorf("DeleteVolume: Get lvm for %s with error: %s", req.GetVolumeId(), err.Error())
				return nil, err
			}
		}
	} else if volumeType == MountPointType {
		log.Infof("DeleteVolume: default to delete MountPoint volume type volume...")
	} else if volumeType == DeviceVolumeType {
		log.Infof("DeleteVolume: default to delete Device volume type volume...")
	}

	log.Infof("DeleteVolume: Successfully deleting volume: %s as type: %s", req.GetVolumeId(), volumeType)
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Infof("ControllerUnpublishVolume is called, do nothing by now: %s", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Infof("ControllerPublishVolume is called, do nothing by now: %s", req.VolumeId)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	log.Infof("ControllerExpandVolume::: %v", req)
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: volSizeBytes, NodeExpansionRequired: true}, nil
}
