package yoda

import (
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8smount "k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/resizefs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func (ns *nodeServer) mountLvm(ctx context.Context, req *csi.NodePublishVolumeRequest) error {
	vgName := ""
	if _, ok := req.VolumeContext[VgNameTag]; ok {
		vgName = req.VolumeContext[VgNameTag]
	}
	if vgName == "" {
		return status.Error(codes.Internal, "error with input vgName is empty")
	}

	targetPath := req.TargetPath
	lvmType := LinearType
	if _, ok := req.VolumeContext[LvmTypeTag]; ok {
		lvmType = req.VolumeContext[LvmTypeTag]
	}
	fsType := DefaultFs
	if _, ok := req.VolumeContext[FsTypeTag]; ok {
		fsType = req.VolumeContext[FsTypeTag]
	}
	volumeNewCreated := false
	volumeID := req.GetVolumeId()
	devicePath := filepath.Join("/dev/", vgName, volumeID)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		volumeNewCreated = true
		err := ns.createVolume(ctx, volumeID, vgName, lvmType)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}

	isMnt, err := ns.mounter.IsMounted(targetPath)
	if err != nil {
		if _, err := os.Stat(targetPath); os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			isMnt = false
		} else {
			return status.Error(codes.Internal, err.Error())
		}
	}

	exitFSType, err := checkFSType(devicePath)
	if err != nil {
		return status.Errorf(codes.Internal, "check fs type err: %v", err)
	}
	if exitFSType == "" {
		log.Printf("The device %v has no filesystem, starting format: %v", devicePath, fsType)
		if err := formatDevice(devicePath, fsType); err != nil {
			return status.Errorf(codes.Internal, "format fstype failed: err=%v", err)
		}
	}

	if !isMnt {
		var options []string
		if req.GetReadonly() {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}
		mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
		options = append(options, mountFlags...)

		err = ns.mounter.Mount(devicePath, targetPath, fsType, options...)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		log.Infof("NodePublishVolume:: mount successful devicePath: %s, targetPath: %s, options: %v", devicePath, targetPath, options)
	}

	// xfs filesystem works on targetpath.
	if volumeNewCreated == false {
		if err := ns.resizeVolume(ctx, volumeID, vgName, targetPath); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}

func (ns *nodeServer) mountLocalVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) error {
	sourcePath := ""
	targetPath := req.TargetPath
	if value, ok := req.VolumeContext["localVolume"]; ok {
		sourcePath = value
	}
	if sourcePath == "" {
		return status.Error(codes.Internal, "Mount LocalVolume with empty source path "+req.VolumeId)
	}

	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if !notmounted {
		log.Infof("NodePublishVolume: VolumeId: %s, Local Path %s is already mounted", req.VolumeId, targetPath)
		return nil
	}

	// start to mount
	mnt := req.VolumeCapability.GetMount()
	options := append(mnt.MountFlags, "bind")
	if req.Readonly {
		options = append(options, "ro")
	}
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}
	log.Infof("NodePublishVolume: Starting mount local volume %s with flags %v and fsType %s", req.VolumeId, options, fsType)
	if err = ns.k8smounter.Mount(sourcePath, targetPath, fsType, options); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func (ns *nodeServer) mountDeviceVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) error {
	sourceDevice := ""
	targetPath := req.TargetPath
	if value, ok := req.VolumeContext["device"]; ok {
		sourceDevice = value
	}
	if sourceDevice == "" {
		return status.Error(codes.Internal, "Mount Device with empty source path "+req.VolumeId)
	}

	// Step Start to format
	mnt := req.VolumeCapability.GetMount()
	options := append(mnt.MountFlags, "shared")
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	// do format-mount or mount
	diskMounter := &k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: k8smount.NewOsExec()}
	if err := diskMounter.FormatAndMount(sourceDevice, targetPath, fsType, options); err != nil {
		log.Errorf("mountDeviceVolume: Volume: %s, Device: %s, FormatAndMount error: %s", req.VolumeId, sourceDevice, err.Error())
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (ns *nodeServer) resizeVolume(ctx context.Context, volumeID, vgName, targetPath string) error {
	pvSize, unit := ns.getPvSize(volumeID)
	devicePath := filepath.Join("/dev", vgName, volumeID)
	sizeCmd := fmt.Sprintf("%s lvdisplay %s | grep 'LV Size' | awk '{print $3}'", NsenterCmd, devicePath)
	sizeStr, err := utils.Run(sizeCmd)
	if err != nil {
		return err
	}
	if sizeStr == "" {
		return status.Error(codes.Internal, "Get lvm size error")
	}
	sizeStr = strings.Split(sizeStr, ".")[0]
	sizeInt, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 10, 64)
	if err != nil {
		return err
	}

	// if lvmsize equal/bigger than pv size, no do expand.
	if sizeInt >= pvSize {
		return nil
	}
	log.Infof("NodeExpandVolume:: volumeId: %s, devicePath: %s, from size: %d, to Size: %d%s", volumeID, devicePath, sizeInt, pvSize, unit)

	// resize lvm volume
	// lvextend -L3G /dev/vgtest/lvm-5db74864-ea6b-11e9-a442-00163e07fb69
	resizeCmd := fmt.Sprintf("%s lvextend -L%d%s %s", NsenterCmd, pvSize, unit, devicePath)
	_, err = utils.Run(resizeCmd)
	if err != nil {
		return err
	}

	// use resizer to expand volume filesystem
	realExec := k8smount.NewOsExec()
	resizer := resizefs.NewResizeFs(&k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: realExec})
	ok, err := resizer.Resize(devicePath, targetPath)
	if err != nil {
		log.Errorf("NodeExpandVolume:: Resize Error, volumeId: %s, devicePath: %s, volumePath: %s, err: %s", volumeID, devicePath, targetPath, err.Error())
		return err
	}
	if !ok {
		log.Errorf("NodeExpandVolume:: Resize failed, volumeId: %s, devicePath: %s, volumePath: %s", volumeID, devicePath, targetPath)
		return status.Error(codes.Internal, "Fail to resize volume fs")
	}
	log.Infof("NodeExpandVolume:: resizefs successful volumeId: %s, devicePath: %s, volumePath: %s", volumeID, devicePath, targetPath)
	return nil
}

func (ns *nodeServer) getPvSize(volumeID string) (int64, string) {
	pv, err := ns.client.CoreV1().PersistentVolumes().Get(volumeID, metav1.GetOptions{})
	if err != nil {
		log.Errorf("lvcreate: fail to get pv, err: %v", err)
		return 0, ""
	}
	pvQuantity := pv.Spec.Capacity["storage"]
	pvSize := pvQuantity.Value()
	pvSizeGB := pvSize / (1024 * 1024 * 1024)

	if pvSizeGB == 0 {
		pvSizeMB := pvSize / (1024 * 1024)
		return pvSizeMB, "m"
	}
	return pvSizeGB, "g"
}

// create lvm volume
func (ns *nodeServer) createVolume(ctx context.Context, volumeID, vgName, lvmType string) error {
	pvSize, unit := ns.getPvSize(volumeID)

	pvNumber := 0
	var err error

	// check vg exist
	ckCmd := fmt.Sprintf("%s vgck %s", NsenterCmd, vgName)
	_, err = utils.Run(ckCmd)
	if err != nil {
		log.Errorf("createVolume:: VG is not exist: %s", vgName)
		return err
	}

	// Create lvm volume
	if lvmType == StripingType {
		cmd := fmt.Sprintf("%s lvcreate -i %d -n %s -L %d%s %s", NsenterCmd, pvNumber, volumeID, pvSize, unit, vgName)
		_, err = utils.Run(cmd)
		if err != nil {
			return err
		}
		log.Infof("Successful Create Striping LVM volume: %s, Size: %d%s, vgName: %s, striped number: %d", volumeID, pvSize, unit, vgName, pvNumber)
	} else if lvmType == LinearType {
		cmd := fmt.Sprintf("%s lvcreate -n %s -L %d%s %s", NsenterCmd, volumeID, pvSize, unit, vgName)
		_, err = utils.Run(cmd)
		if err != nil {
			return err
		}
		log.Infof("Successful Create Linear LVM volume: %s, Size: %d%s, vgName: %s", volumeID, pvSize, unit, vgName)
	}
	return nil
}
