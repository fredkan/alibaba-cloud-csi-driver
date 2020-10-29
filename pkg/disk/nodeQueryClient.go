package disk

import (
	"encoding/json"
	"errors"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
)

func bdfInfoQuery(nodeIP string, diskIDs []string) ([]BdfInfo, error) {
	bdfInfoResponse := []BdfInfo{}
	url := "http://" + nodeIP + ":" + GetEBSDPort() + BDF_DISK_INFO_URL
	reqBytes, err := json.Marshal(&diskIDs)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(url, "application/x-www-form-urlencoded", strings.NewReader(string(reqBytes)))
	if err != nil {
		log.Errorf("bdfInfoQuery get error: %s", err.Error())
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("bdfInfoQuery read response error: %s", err.Error())
		return nil, err
	}
	log.Infof("BDF Query: Get response %s", string(body))

	if strings.HasPrefix(string(body), "ignore:") {
		bdfInfo := BdfInfo{}
		bdfInfo.Error = "ignore"
		bdfInfoResponse = append(bdfInfoResponse, bdfInfo)
		return bdfInfoResponse, nil
	}
	if strings.HasPrefix(string(body), "error:") {
		bdfInfo := BdfInfo{}
		bdfInfo.Error = "error"
		bdfInfoResponse = append(bdfInfoResponse, bdfInfo)
		return bdfInfoResponse, nil
	}
	err = json.Unmarshal(body, &bdfInfoResponse)
	if err != nil {
		log.Errorf("bdfInfoQuery Unmarshal body error: %s", err.Error())
		return nil, err
	}
	return bdfInfoResponse, nil
}

func GetInstanceIP(instanceID string) (string, error) {
	request := ecs.CreateDescribeInstancesRequest()
	request.RegionId = GlobalConfigVar.Region
	request.InstanceIds = "[\"" + instanceID + "\"]"

	response, err := GlobalConfigVar.EcsClient.DescribeInstances(request)
	if err != nil {
		return "", err
	}
	if len(response.Instances.Instance) != 1 {
		return "", errors.New("Get Instance with error response: " + response.RequestId)
	}
	instance := response.Instances.Instance[0]
	for _, ipadd := range instance.VpcAttributes.PrivateIpAddress.IpAddress {
		return ipadd, nil
	}
	for _, ipadd := range instance.InnerIpAddress.IpAddress {
		return ipadd, nil
	}
	return "", errors.New("Net InnerIpAddress found ")
}
