package disk

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

const (
	EBSD_PORT         = "1737"
	BDF_DISK_INFO_URL = "/api/v1/bdfdiskinfo"
)

type BdfInfo struct {
	BDF        string `json:"bdf"`
	Device     string `json:"device"`
	DiskID     string `json:"diskID"`
	InstanceID string `json:"instanceID"`
	NodeID     string `json:"nodeID"`
	Error      string `json:"error"`
}

func QueryServerStart() {
	log.Infof("String BDF query server ...")

	http.HandleFunc(BDF_DISK_INFO_URL, bdfHandler)
	err := http.ListenAndServe(":"+GetEBSDPort(), nil)

	log.Errorf("BDF server stopped with error: %s", err.Error())
}

func GetEBSDPort() string {
	port := EBSD_PORT
	if value := os.Getenv("EBSD_PORT"); value != "" {
		port = value
	}
	return port
}

func bdfHandler(w http.ResponseWriter, req *http.Request) {
	bdfResponse := []BdfInfo{}
	if !IsVFNode() {
		io.WriteString(w, "ignore: not bdf node")
		return
	}
	bodyByte, err := ioutil.ReadAll(req.Body)
	if err != nil {
		io.WriteString(w, "error: "+err.Error())
		return
	}

	log.Infof("BDF handler with Body: %s", string(bodyByte))
	request := []string{}
	err = json.Unmarshal(bodyByte, &request)
	if err != nil {
		io.WriteString(w, "error: "+err.Error())
		return
	}
	bdfResponse = buildBdfInfoResponse(request)
	infoBytes, err := json.Marshal(&bdfResponse)
	if err != nil {
		io.WriteString(w, "error: "+err.Error())
		return
	}
	io.WriteString(w, string(infoBytes))
	log.Infof("BDF handler response success: %s", string(infoBytes))
}

func buildBdfInfoResponse(diskIDs []string) []BdfInfo {
	bdfResponse := []BdfInfo{}
	for _, diskID := range diskIDs {
		if diskID == "allvolumedisk" {
			// this value means reply all device info
		} else {
			device, bdf, err := getBdfDeviceByID(diskID)
			info := BdfInfo{}
			info.NodeID = GlobalConfigVar.NodeID
			info.Device = device
			info.DiskID = diskID
			info.BDF = bdf
			info.InstanceID = ""
			bdfResponse = append(bdfResponse, info)
			if err != nil {
				log.Warnf("buildBdfInfoResponse error with %s", err.Error())
			}
		}
	}
	return bdfResponse
}

func getBdfDeviceByID(diskID string) (string, string, error) {
	bdf, err := findBdf(diskID)
	if err != nil {
		return "", "", err
	}
	if bdf == "" {
		return "", "", nil
	}
	device, err := GetDeviceByBdf(bdf)
	return device, bdf, nil
}
