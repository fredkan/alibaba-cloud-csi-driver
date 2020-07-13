package om

import (
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	k8smount "k8s.io/kubernetes/pkg/util/mount"
	"os"
	"path/filepath"
	"strings"
)

var (
	// FixedPodList fix pod
	FixedPodList = map[string]string{}
	// FixedSubPathPodList fix pod
	FixedSubPathPodList = map[string]string{}
	// K8sMounter mounter
	K8sMounter = k8smount.New("")
)

// FixOrphanedPodIssue Pod Like:
// Jul 10 18:55:49 kubelet: E0710 18:55:49.251132    7643 kubelet_volumes.go:154] orphaned pod "a60244b2-e6ee-4a63-b311-13f7b29ef49a"
// found, but volume paths are still present on disk : There were a total of 1 errors similar to this. Turn up verbosity to see them.
func FixOrphanedPodIssue(line string) bool {
	splitStr := strings.Split(line, "rphaned pod")
	if len(splitStr) < 2 {
		log.Warnf("OrphanPod: Error orphaned line format: %s", line)
		return false
	}
	partStr := strings.Split(splitStr[1], "\"")
	if len(partStr) < 2 {
		log.Warnf("OrphanPod: Error line format: %s", line)
		return false
	}
	orphanUID := partStr[1]
	if len(strings.Split(orphanUID, "-")) != 5 {
		log.Warnf("OrphanPod: Error Pod Uid format: %s, %s", orphanUID, line)
		return false
	}

	// break fixed orphaned pod
	if value, ok := FixedPodList[orphanUID]; ok && value == "fixed" {
		return true
	}

	// check kubernetes csi volumes
	csiPodPath := filepath.Join("/var/lib/kubelet/pods", orphanUID, "volumes/kubernetes.io~csi")
	volumes, err := ioutil.ReadDir(csiPodPath)
	if err != nil {
		log.Warnf("OrphanPod: List Volumes with error: %s, line: %s", err.Error(), line)
		return false
	}
	for _, volume := range volumes {
		volumePath := filepath.Join(csiPodPath, volume.Name())
		volumeMountPath := filepath.Join(volumePath, "mount")
		volumeJSONPath := filepath.Join(volumePath, "vol_data.json")
		if utils.IsFileExisting(volumeMountPath) {
			if err := k8smount.CleanupMountPoint(volumeMountPath, K8sMounter, false); err != nil {
				log.Errorf("OrphanPod: CleanupMountPoint %s, with Error: %s, Log: %s", volumeMountPath, err.Error(), line)
				continue
			} else {
				log.Infof("OrphanPod: Successful Remove Path(%s).", volumeMountPath)
			}
		}
		if IsFileExisting(volumeJSONPath) {
			err = os.Remove(volumeJSONPath)
			if err != nil {
				log.Errorf("OrphanPod: Remove Json File %s with error %s", volumeJSONPath, err.Error())
			} else {
				log.Infof("OrphanPod: Remove Json File %s Successful", volumeJSONPath)
			}
		}
		if empty, _ := utils.IsDirEmpty(volumePath); empty {
			err = os.Remove(volumePath)
			if err != nil {
				log.Errorf("OrphanPod: Remove Volume Path %s with error %s", volumePath, err.Error())
			} else {
				log.Infof("OrphanPod: Remove Volume Path %s Successful", volumePath)
			}
		}
	}

	if !IsFileExisting(csiPodPath) {
		FixedPodList[orphanUID] = "fixed"
	}
	if empty, _ := utils.IsDirEmpty(csiPodPath); empty {
		FixedPodList[orphanUID] = "fixed"
	}
	return true
}

// FixSubPathOrphanedPodIssue Pod Like:
// Jul 13 20:23:01 iZwz96zhxfn3iajc89zb0gZ kubelet: E0713 20:23:01.027740   60974 kubelet_volumes.go:154] orphaned pod "cbefa9e9-9572-4bc8-809b-18083b88b232" found,
// but volume subpaths are still present on disk : There were a total of 1 errors similar to this. Turn up verbosity to see them.
func FixSubPathOrphanedPodIssue(line string) bool {
	splitStr := strings.Split(line, "rphaned pod")
	if len(splitStr) < 2 {
		log.Warnf("OrphanPod: Error orphaned line subpath format: %s", line)
		return false
	}
	partStr := strings.Split(splitStr[1], "\"")
	if len(partStr) < 2 {
		log.Warnf("OrphanPod: Error line subpath format: %s", line)
		return false
	}
	orphanUID := partStr[1]
	if len(strings.Split(orphanUID, "-")) != 5 {
		log.Warnf("OrphanPod: Error Pod Uid subpath format: %s, %s", orphanUID, line)
		return false
	}

	// break fixed orphaned pod
	if value, ok := FixedSubPathPodList[orphanUID]; ok && value == "fixed" {
		return true
	}

	// check kubernetes csi volumes
	csiPodPath := filepath.Join("/var/lib/kubelet/pods", orphanUID, "volume-subpaths")
	volumes, err := ioutil.ReadDir(csiPodPath)
	if err != nil {
		log.Warnf("OrphanPod: List Sub Volumes with error: %s, line: %s", err.Error(), line)
		return false
	}

	for _, volume := range volumes {
		volumePath := filepath.Join(csiPodPath, volume.Name())
		containers, err := ioutil.ReadDir(volumePath)
		if err != nil {
			log.Warnf("OrphanPod: List Sub Volumes with error: %s, line: %s", err.Error(), line)
			return false
		}
		for _, container := range containers {
			containerPath := filepath.Join(volumePath, container.Name())
			containerPaths, err := ioutil.ReadDir(containerPath)
			if err != nil {
				log.Warnf("OrphanPod: List Sub Volumes with error: %s, line: %s", err.Error(), line)
				return false
			}
			for _, subpath := range containerPaths {
				subPathName := filepath.Join(containerPath, subpath.Name())
				if err := k8smount.CleanupMountPoint(subPathName, K8sMounter, false); err != nil {
					log.Errorf("OrphanPod: CleanupMountPoint for subpath %s, with Error: %s, Log: %s", subPathName, err.Error(), line)
					continue
				} else {
					log.Infof("OrphanPod: Successful Remove SubPath(%s).", subPathName)
				}
			}
			if empty, _ := utils.IsDirEmpty(containerPath); empty {
				if err := os.Remove(containerPath); err != nil {
					log.Errorf("OrphanPod: Remove for Container Path %s, with Error: %s", containerPath, err.Error())
				} else {
					log.Infof("OrphanPod: Successful Remove Container Path(%s).", containerPath)
				}
			}
		}
		if empty, _ := utils.IsDirEmpty(volumePath); empty {
			if err := os.Remove(volumePath); err != nil {
				log.Errorf("OrphanPod: Remove for Volume Path %s, with Error: %s", volumePath, err.Error())
			} else {
				log.Infof("OrphanPod: Successful Remove Volume Path(%s).", volumePath)
			}
		}
	}
	if empty, _ := utils.IsDirEmpty(csiPodPath); empty {
		if err := os.Remove(csiPodPath); err != nil {
			log.Errorf("OrphanPod: Remove for Root Path %s, with Error: %s", csiPodPath, err.Error())
		} else {
			log.Infof("OrphanPod: Successful Remove Root Path(%s).", csiPodPath)
		}
	}

	if !IsFileExisting(csiPodPath) {
		FixedSubPathPodList[orphanUID] = "fixed"
	}
	return true
}
