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

package oss

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	k8smount "k8s.io/kubernetes/pkg/util/mount"
	"os"
)

const (
	driverName = "ossplugin.csi.alibabacloud.com"
)

var (
	version    = "1.0.0"
	masterURL  string
	kubeconfig string
	// GlobalConfigVar Global Config
	GlobalConfigVar GlobalConfig
)

// GlobalConfig save global values for plugin
type GlobalConfig struct {
	OssTagEnable bool
	MetricEnable bool
	RunTimeClass string
	OssClient    *oss.Client
	RegionID     string
}

// OSS the OSS object
type OSS struct {
	driver     *csicommon.CSIDriver
	endpoint   string
	idServer   *csicommon.DefaultIdentityServer
	nodeServer *nodeServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

// NewDriver init oss type of csi driver
func NewDriver(nodeID, endpoint string) *OSS {
	log.Infof("Driver: %v version: %v", driverName, version)

	d := &OSS{}
	d.endpoint = endpoint

	if nodeID == "" {
		nodeID = GetMetaData(InstanceID)
		log.Infof("Use node id : %s", nodeID)
	}
	csiDriver := csicommon.NewCSIDriver(driverName, version, nodeID)
	csiDriver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER})
	csiDriver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_UNKNOWN})

	d.driver = csiDriver

	// Global Configs Set
	GlobalConfigSet()
	return d
}

// newNodeServer init oss type of csi nodeServer
func newNodeServer(d *OSS) *nodeServer {
	return &nodeServer{
		k8smounter:        k8smount.New(""),
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.driver),
	}
}

// Run start a newNodeServer
func (d *OSS) Run() {
	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(d.endpoint,
		csicommon.NewDefaultIdentityServer(d.driver),
		nil,
		//csicommon.NewDefaultControllerServer(d.driver),
		newNodeServer(d))
	s.Wait()
}

// GlobalConfigSet set global config
func GlobalConfigSet() {
	// Global Configs Set
	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err.Error())
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	configMapName := "csi-plugin"
	ossTagEnable := true

	configMap, err := kubeClient.CoreV1().ConfigMaps("kube-system").Get(configMapName, metav1.GetOptions{})
	if err != nil {
		log.Infof("Not found configmap named as csi-plugin under kube-system, with error: %v", err)
	} else {
		if value, ok := configMap.Data["oss-tag-enable"]; ok {
			if value == "enable" || value == "yes" || value == "true" {
				log.Infof("Oss Tag is enabled by configMap(%s).", value)
				ossTagEnable = true
			}
		}
	}

	ossTagConf := os.Getenv(OssTagByPlugin)
	if ossTagConf == "true" || ossTagConf == "yes" {
		ossTagEnable = true
	} else if ossTagConf == "false" || ossTagConf == "no" {
		ossTagEnable = false
	}

	regionID, _ := utils.GetMetaData(RegionTag)
	ossEndpoint := getOssEndpoint("vpc", regionID)
	ossClient, _ := newOSSClient("", "", "", ossEndpoint)

	GlobalConfigVar.OssTagEnable = ossTagEnable
	GlobalConfigVar.OssClient = ossClient
	GlobalConfigVar.RegionID = regionID

	log.Infof("GlobalConfigVar values: %+v", GlobalConfigVar)
}
