package yoda

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	UrlHost = "http://yoda.service.io"
)

func ScheduleVolume(volumeType, pvcName, pvcNamespace, nodeId string) (*VolumeInfo, error) {
	volumeInfo := &VolumeInfo{}

	urlPath := fmt.Sprintf("/apis/scheduling/%s/persistentvolumeclaims/%s?nodeId=%s,volumeType=%s", pvcNamespace, pvcName, nodeId, volumeType)
	if nodeId == "" {
		urlPath = fmt.Sprintf("/apis/scheduling/%s/persistentvolumeclaims/%s?volumeType=%s", pvcNamespace, pvcName, volumeType)

	}
	url := UrlHost + urlPath

	// Request restful api
	respBody, err := DoRequest(url)
	if err != nil {
		log.Errorf("Schedule Volume with request error: %s", err.Error())
		return nil, err
	}

	// make struct
	err = json.Unmarshal([]byte(respBody), volumeInfo)
	if err != nil {
		log.Errorf("Schedule Volume with Unmarshal error: %s", err.Error())
		return nil, err
	}
	return volumeInfo, nil
}

// Http Post request
func DoRequest(url string) (string, error) {
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return "", err
	}

	// Set request
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Host", UrlHost)
	client := &http.Client{Timeout: time.Second * 10}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
