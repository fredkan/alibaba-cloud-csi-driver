package lvmcs

import (
	"github.com/wavezhang/k8s-csi-lvm/pkg/lvmd"
	"testing"
)
import "golang.org/x/net/context"

func TestGetLV(t *testing.T) {

	ctx := context.Background()
	address := "0.0.0.0:1736"
	vgName := "volumegroup1"
	volumeID := "lvm-85997175-44f6-11ea-bfb5-00163e005b53"

	conn, err := lvmd.NewLVMConnection(address, 30)
	if err != nil {

	}
	if out, err := conn.GetLV(ctx, vgName, volumeID); err != nil {
		t.Fatal("GetLV error: ", err.Error())
	} else {
		t.Log("GetLV Pass: ", out)
	}
}
