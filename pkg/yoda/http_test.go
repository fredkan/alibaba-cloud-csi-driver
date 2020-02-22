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
	"fmt"
	"net/http"
	"testing"
)

func TestGetDiskVolumeOptions(t *testing.T) {
	UrlHost = "http://127.0.0.1:23000"
	http.HandleFunc("/apis/scheduling/default/persistentvolumeclaims/pvc-lvm", lvmResp)
	go http.ListenAndServe("127.0.0.1:23000", nil)

	volumeType, pvcName, pvcNamespace, nodeId := "lvm", "pvc-lvm", "default", "node-1"
	vloInfo, err := ScheduleVolume(volumeType, pvcName, pvcNamespace, "", nodeId)
	if err != nil {
		t.Fatal("Test Fail")
	}

	t.Log("Test Pass", vloInfo)
}

func lvmResp(w http.ResponseWriter, r *http.Request) {
	respnse := `{
    "node": "node-1",
    "volumeType": "lvm",
    "vgName": "vg-1"
}`
	fmt.Fprintf(w, respnse)
}
