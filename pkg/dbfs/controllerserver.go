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

package dbfs

import (
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/dbfs"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

// resourcemode is selected by: subpath/filesystem
const (
	MBSize         = 1024 * 1024
	DRIVER         = "driver"
	SERVER         = "server"
	MODE           = "mode"
	ModeType       = "modeType"
	VolumeAs       = "volumeAs"
	PATH           = "path"
	ProtocolType   = "protocolType"
	FileSystemType = "fileSystemType"
	ZoneID         = "zoneId"
	ZoneIDTag      = "zone-id"

	TopologyZoneKey = "topology." + driverName + "/zone"
)

// controller server try to create/delete volumes
type controllerServer struct {
	dbfsClient *dbfs.Client
	region     string
	kubeClient kubernetes.Interface
	*csicommon.DefaultControllerServer
	recorder record.EventRecorder
}

// Alibaba Cloud dbfs volume parameters
type dbfsOptions struct {
	Category string
	FsName   string
	RegionId string
	ZoneId   string
	Size     int
}

// used by check pvc is processed
var pvcProcessSuccess = map[string]*csi.Volume{}
var storageClassServerPos = map[string]int{}
var pvcFileSystemIDMap = map[string]string{}
var pvcMountTargetMap = map[string]string{}

// NewControllerServer is to create controller server
func NewControllerServer(d *csicommon.CSIDriver, client *dbfs.Client, region string) csi.ControllerServer {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("NewControllerServer: Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("NewControllerServer: Failed to create client: %v", err)
	}

	c := &controllerServer{
		dbfsClient:              client,
		region:                  region,
		kubeClient:              clientset,
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		recorder:                utils.NewEventRecorder(),
	}
	return c
}

func (cs *controllerServer) createEvent(objectRef *v1.ObjectReference, eventType string, reason string, err string) {
	cs.recorder.Event(objectRef, eventType, reason, err)
}

// provisioner: create/delete dbfs volume
func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	log.Infof("CreateVolume: Starting NFS CreateVolume, %s, %v", req.Name, req)

	// step1: check pvc is created or not.
	if value, ok := pvcProcessSuccess[req.Name]; ok && value != nil {
		log.Infof("CreateVolume: Nfs Volume %s has Created Already: %v", req.Name, value)
		return &csi.CreateVolumeResponse{Volume: value}, nil
	}

	// parse nfs parameters
	pvName := req.Name
	// get dbfs information
	dbfsOpts, err := cs.getDbfsVolumeOptions(req)
	if err != nil {
		log.Errorf("CreateVolume: error parameters from input: %v, with error: %v", req.Name, err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid parameters from input: %v, with error: %v", req.Name, err)
	}

	snapshotID := ""
	volumeSource := req.GetVolumeContentSource()
	if volumeSource != nil {
		if _, ok := volumeSource.GetType().(*csi.VolumeContentSource_Snapshot); !ok {
			return nil, status.Error(codes.InvalidArgument, "CreateVolume: unsupported volumeContentSource type")
		}
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, status.Error(codes.InvalidArgument, "CreateVolume: get empty snapshot from volumeContentSource")
		}
		snapshotID = sourceSnapshot.GetSnapshotId()
	}

	createDbfsRequest := dbfs.CreateCreateDbfsRequest()
	createDbfsRequest.Category = dbfsOpts.Category
	createDbfsRequest.FsName = dbfsOpts.FsName
	createDbfsRequest.ZoneId = dbfsOpts.ZoneId
	createDbfsRequest.RegionId = dbfsOpts.RegionId
	createDbfsRequest.SizeG = requests.NewInteger(dbfsOpts.Size)
	createDbfsRequest.ClientToken = req.Name
	createDbfsRequest.Domain = "dbfs.cn-hangzhou.aliyuncs.com"


	GlobalConfigVar.DbfsClient = updateDbfsClient(GlobalConfigVar.DbfsClient)
	response, err := GlobalConfigVar.DbfsClient.CreateDbfs(createDbfsRequest)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid parameters from input: %v, with error: %v", req.Name, err)
	}

	var src *csi.VolumeContentSource
	if snapshotID != "" {
		src = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapshotID,
				},
			},
		}
	}

	tmpVol := &csi.Volume{
		VolumeId:      response.FsId,
		CapacityBytes: int64(req.GetCapacityRange().GetRequiredBytes()),
		AccessibleTopology: []*csi.Topology{
			{
				Segments: map[string]string{
					TopologyZoneKey: dbfsOpts.ZoneId,
				},
			},
		},
		ContentSource: src,
	}

	pvcProcessSuccess[pvName] = tmpVol
	log.Infof("Provision Successful: %s, with PV: %v", req.Name, tmpVol)
	return &csi.CreateVolumeResponse{Volume: tmpVol}, nil
}

// call dbfs api to delete disk
func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Infof("DeleteVolume: Starting deleting volume %s", req.GetVolumeId())

	deleteDbfsRequest := dbfs.CreateDeleteDbfsRequest()
	deleteDbfsRequest.FsId = req.VolumeId
	deleteDbfsRequest.RegionId = GlobalConfigVar.Region
	deleteDbfsRequest.Domain = "dbfs.cn-hangzhou.aliyuncs.com"


	_, err := GlobalConfigVar.DbfsClient.DeleteDbfs(deleteDbfsRequest)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "DeleteVolume: invalid delete volume req: %v", req)
	}

	// remove the pvc process mapping if exist
	if _, ok := pvcProcessSuccess[req.VolumeId]; ok {
		delete(pvcProcessSuccess, req.VolumeId)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) getDbfsVolumeOptions(req *csi.CreateVolumeRequest) (*dbfsOptions, error) {
	var ok bool
	dbfsOpts := &dbfsOptions{}
	volOptions := req.GetParameters()

	if dbfsOpts.Category, ok = volOptions["Category"]; !ok {
		dbfsOpts.Category = "cloud_essd"
	}
	dbfsOpts.RegionId = GlobalConfigVar.Region

	if dbfsOpts.ZoneId, ok = volOptions["zoneId"]; !ok {
		dbfsOpts.ZoneId = GetMetaData("zone-id")
	}

	dbfsOpts.FsName = req.Name
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	requestGB := int((volSizeBytes + 1024*1024*1024 - 1) / (1024 * 1024 * 1024))
	dbfsOpts.Size = requestGB

	return dbfsOpts, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Infof("ControllerUnpublishVolume is called, do nothing by now")
	detachDbfsRequest := dbfs.CreateDetachDbfsRequest()
	detachDbfsRequest.RegionId = GlobalConfigVar.Region
	detachDbfsRequest.FsId = req.VolumeId
	detachDbfsRequest.Domain = "dbfs.cn-hangzhou.aliyuncs.com"
	_, err := GlobalConfigVar.DbfsClient.DetachDbfs(detachDbfsRequest)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume missing VolumeId/NodeId in request")
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Infof("ControllerPublishVolume is called, do nothing by now")

	attachDbfsRequest := dbfs.CreateAttachDbfsRequest()
	attachDbfsRequest.RegionId = GlobalConfigVar.Region
	attachDbfsRequest.ECSInstanceId = req.NodeId
	attachDbfsRequest.FsId = req.VolumeId
	attachDbfsRequest.Domain = "dbfs.cn-hangzhou.aliyuncs.com"
	_, err := GlobalConfigVar.DbfsClient.AttachDbfs(attachDbfsRequest)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume missing VolumeId/NodeId in request")
	}

	return &csi.ControllerPublishVolumeResponse{}, nil
}

//
func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	log.Infof("CreateSnapshot is called, do nothing now")
	return &csi.CreateSnapshotResponse{}, nil
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	log.Infof("DeleteSnapshot is called, do nothing now")
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest,
) (*csi.ControllerExpandVolumeResponse, error) {
	log.Infof("ControllerExpandVolume is called, do nothing now")
	return &csi.ControllerExpandVolumeResponse{}, nil
}
