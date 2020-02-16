/*
Copyright 2020 The Kubernetes Authors.

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
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

// scheduler restful address
var (
	UrlHost = "http://yoda-scheduler-extender-service:23000"
)

const (
	SCHEDULER_HOST_TAG = "SCHEDULER_HOST"
)

// make request and get expect schedule topology
func ScheduleVolume(volumeType, pvcName, pvcNamespace, nodeId string) (*BindingInfo, error) {
	bindingInfo := &BindingInfo{}
	hostEnv := os.Getenv(SCHEDULER_HOST_TAG)
	if hostEnv != "" {
		UrlHost = hostEnv
	}

	// make request url
	urlPath := fmt.Sprintf("/apis/scheduling/%s/persistentvolumeclaims/%s?nodeName=%s&volumeType=%s", pvcNamespace, pvcName, nodeId, volumeType)
	if nodeId == "" {
		urlPath = fmt.Sprintf("/apis/scheduling/%s/persistentvolumeclaims/%s?volumeType=%s", pvcNamespace, pvcName, volumeType)
	}
	url := UrlHost + urlPath

	// Request restful api
	respBody, err := DoRequest(url)
	if err != nil {
		log.Errorf("Schedule Volume with Url(%s) get error: %s", url, err.Error())
		return nil, err
	}
	// unmarshal json result.
	err = json.Unmarshal(respBody, bindingInfo)
	if err != nil {
		log.Errorf("Schedule Volume with Url(%s) get Unmarshal error: %s, and response: %s", url, err.Error(), string(respBody))
		return nil, err
	}

	log.Infof("Schedule Volume with Url(%s) Finished, get result: %v, %v", url, bindingInfo, string(respBody))
	return bindingInfo, nil
}

// Http Post Request
func DoRequest(url string) ([]byte, error) {
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Host", UrlHost)
	client := &http.Client{Timeout: time.Second * 10}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("Get Response StatusCode %d, Response: %++v", resp.StatusCode, resp)
		return nil, errors.New(msg)
	}
	//
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}
