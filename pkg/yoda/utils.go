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
	"fmt"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/lvm/lvmd"
	"io/ioutil"
	"net"
	"net/http"
	"os/exec"
	"strings"

	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// MetadataURL is metadata server url
	MetadataURL = "http://100.100.100.200/latest/meta-data/"
	// InstanceID is the instance id tag
	InstanceID = "instance-id"
	// RegionIDTag is the region id tag
	RegionIDTag = "region-id"
)

// ErrParse is an error that is returned when parse operation fails
var ErrParse = errors.New("Cannot parse output of blkid")

// GetMetaData get host regionid, zoneid
func GetMetaData(resource string) string {
	resp, err := http.Get(MetadataURL + resource)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(body)
}

func formatDevice(devicePath, fstype string) error {
	output, err := exec.Command("mkfs", "-t", fstype, devicePath).CombinedOutput()
	if err != nil {
		return errors.New("FormatDevice error: " + string(output))
	}
	return nil
}

func checkFSType(devicePath string) (string, error) {
	// We use `file -bsL` to determine whether any filesystem type is detected.
	// If a filesystem is detected (ie., the output is not "data", we use
	// `blkid` to determine what the filesystem is. We use `blkid` as `file`
	// has inconvenient output.
	// We do *not* use `lsblk` as that requires udev to be up-to-date which
	// is often not the case when a device is erased using `dd`.
	output, err := exec.Command("file", "-bsL", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(output)) == "data" {
		return "", nil
	}
	output, err = exec.Command("blkid", "-c", "/dev/null", "-o", "export", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), "=")
		if len(fields) != 2 {
			return "", ErrParse
		}
		if fields[0] == "TYPE" {
			return fields[1], nil
		}
	}
	return "", ErrParse
}

func isVgExist(vgName string) (bool, error) {
	vgCmd := fmt.Sprintf("%s vgdisplay %s | grep 'VG Name' | grep %s | grep -v grep | wc -l", NsenterCmd, vgName, vgName)
	vgline, err := utils.Run(vgCmd)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(vgline) == "1" {
		return true, nil
	}
	return false, nil
}

func getLvmSpec(client kubernetes.Interface, volumeID string) (string, string, error) {
	pv, err := getPvObj(client, volumeID)
	if err != nil {
		log.Errorf("Get Lvm Spec for volume %s, error with %v", volumeID, err)
		return "", "", err
	}
	if pv.Spec.NodeAffinity == nil {
		log.Errorf("Get Lvm Spec for volume %s, with nil nodeAffinity", volumeID)
		return "", "", nil
	}
	if pv.Spec.NodeAffinity.Required == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		log.Errorf("Get Lvm Spec for volume %s, with nil Required", volumeID)
		return "", "", nil
	}
	if len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions) == 0 {
		log.Errorf("Get Lvm Spec for volume %s, with nil MatchExpressions", volumeID)
		return "", "", nil
	}
	key := pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Key
	if key != TopologyNodeKey {
		log.Errorf("Get Lvm Spec for volume %s, with key %s", volumeID, key)
		return "", "", nil
	}
	nodes := pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values
	if len(nodes) == 0 {
		log.Errorf("Get Lvm Spec for volume %s, with empty nodes", volumeID)
		return "", "", nil
	}

	if _, ok := pv.Spec.CSI.VolumeAttributes["vgName"]; !ok {
		log.Errorf("Get Lvm Spec for volume %s, with empty vgName", volumeID)
		return "", "", errors.New("vgName not exist for " + volumeID)
	}

	log.Infof("Get Lvm Spec for volume %s, with VgName %s, Node %s", volumeID, pv.Spec.CSI.VolumeAttributes["vgName"], nodes[0])
	return nodes[0], pv.Spec.CSI.VolumeAttributes["vgName"], nil
}

func getPvObj(client kubernetes.Interface, volumeID string) (*v1.PersistentVolume, error) {
	return client.CoreV1().PersistentVolumes().Get(volumeID, metav1.GetOptions{})
}

func getLvmdAddr(client kubernetes.Interface, node string) (string, error) {
	ip, err := GetNodeIP(client, node)
	if err != nil {
		return "", err
	}
	return ip.String() + ":" + lvmd.GetLvmdPort(), nil
}

// GetNodeIP get node address
func GetNodeIP(client kubernetes.Interface, nodeID string) (net.IP, error) {
	node, err := client.CoreV1().Nodes().Get(nodeID, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	addresses := node.Status.Addresses
	addressMap := make(map[v1.NodeAddressType][]v1.NodeAddress)
	for i := range addresses {
		addressMap[addresses[i].Type] = append(addressMap[addresses[i].Type], addresses[i])
	}
	if addresses, ok := addressMap[v1.NodeInternalIP]; ok {
		return net.ParseIP(addresses[0].Address), nil
	}
	if addresses, ok := addressMap[v1.NodeExternalIP]; ok {
		return net.ParseIP(addresses[0].Address), nil
	}
	return nil, fmt.Errorf("Node IP unknown; known addresses: %v", addresses)
}
