package dbfs

//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//
// Code generated by Alibaba Cloud SDK Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/responses"
)

// ResetDbfs invokes the dbfs.ResetDbfs API synchronously
func (client *Client) ResetDbfs(request *ResetDbfsRequest) (response *ResetDbfsResponse, err error) {
	response = CreateResetDbfsResponse()
	err = client.DoAction(request, response)
	return
}

// ResetDbfsWithChan invokes the dbfs.ResetDbfs API asynchronously
func (client *Client) ResetDbfsWithChan(request *ResetDbfsRequest) (<-chan *ResetDbfsResponse, <-chan error) {
	responseChan := make(chan *ResetDbfsResponse, 1)
	errChan := make(chan error, 1)
	err := client.AddAsyncTask(func() {
		defer close(responseChan)
		defer close(errChan)
		response, err := client.ResetDbfs(request)
		if err != nil {
			errChan <- err
		} else {
			responseChan <- response
		}
	})
	if err != nil {
		errChan <- err
		close(responseChan)
		close(errChan)
	}
	return responseChan, errChan
}

// ResetDbfsWithCallback invokes the dbfs.ResetDbfs API asynchronously
func (client *Client) ResetDbfsWithCallback(request *ResetDbfsRequest, callback func(response *ResetDbfsResponse, err error)) <-chan int {
	result := make(chan int, 1)
	err := client.AddAsyncTask(func() {
		var response *ResetDbfsResponse
		var err error
		defer close(result)
		response, err = client.ResetDbfs(request)
		callback(response, err)
		result <- 1
	})
	if err != nil {
		defer close(result)
		callback(nil, err)
		result <- 0
	}
	return result
}

// ResetDbfsRequest is the request struct for api ResetDbfs
type ResetDbfsRequest struct {
	*requests.RpcRequest
	SnapshotId  string `position:"Query" name:"SnapshotId"`
	ClientToken string `position:"Query" name:"ClientToken"`
	FsId        string `position:"Query" name:"FsId"`
}

// ResetDbfsResponse is the response struct for api ResetDbfs
type ResetDbfsResponse struct {
	*responses.BaseResponse
	RequestId string `json:"RequestId" xml:"RequestId"`
}

// CreateResetDbfsRequest creates a request to invoke ResetDbfs API
func CreateResetDbfsRequest() (request *ResetDbfsRequest) {
	request = &ResetDbfsRequest{
		RpcRequest: &requests.RpcRequest{},
	}
	request.InitWithApiInfo("DBFS", "2020-04-18", "ResetDbfs", "", "")
	request.Method = requests.POST
	return
}

// CreateResetDbfsResponse creates a response to parse from ResetDbfs response
func CreateResetDbfsResponse() (response *ResetDbfsResponse) {
	response = &ResetDbfsResponse{
		BaseResponse: &responses.BaseResponse{},
	}
	return
}
