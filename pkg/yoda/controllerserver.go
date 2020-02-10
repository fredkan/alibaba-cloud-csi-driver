/*
Copyright 2019 The Kubernetes Authors.

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
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils/lvmd"
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
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	client kubernetes.Interface
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
		log.Infof("invalid create volume req: %v", req)
		return nil, err
	}
	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	volumeID := req.GetName()
	response := &csi.CreateVolumeResponse{}

	parameters := req.GetParameters()
	volumeType := LvmVolumeType
	if value, ok := parameters["volumeType"]; ok {
		volumeType = value
	}
	if volumeType != LvmVolumeType && volumeType != LocalVolumeType {
		return nil, status.Error(codes.InvalidArgument, "Yoda only support localVolume/lvm type")
	}
	if volumeType == LvmVolumeType {
		vgName := ""
		vgAppend := true
		if value, ok := parameters["vgName"]; ok {
			volumeType = value
			vgAppend = false
		}

		// Get nodeID if pvc in topology mode.
		nodeID := pickNodeID(req.GetAccessibilityRequirements())
		// Todo:
		nodeID, vgName = GetLvmNodeAndVgName(nodeID, vgName)

		volContext := req.GetParameters()
		if vgAppend {
			volContext["vgName"] = vgName
		}

		response = &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: volContext,
				AccessibleTopology: []*csi.Topology{
					{
						Segments: map[string]string{
							TopologyNodeKey: nodeID,
						},
					},
				},
			},
		}
		// Todo: spec local volume type
	} else if volumeType == LocalVolumeType {

	}

	log.Infof("Success create Volume: %s, Size: %d", volumeID, req.GetCapacityRange().GetRequiredBytes())
	return response, nil
}

func GetLvmNodeAndVgName(nodeId, vgName string) (string, string) {
	if nodeId == "" {

	}
	if vgName == "" {

	}
	return nodeId, vgName
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

			conn, err := lvmd.NewLVMConnection(addr, connectTimeout)
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
	} else if volumeType == LocalVolumeType {

	} else {

	}

	log.Infof("DeleteVolume: Successfully deleting volume: %s", req.GetVolumeId())
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
