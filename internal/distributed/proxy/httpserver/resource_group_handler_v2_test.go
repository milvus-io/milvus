// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package httpserver

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func validateRequestBodyTestCases(t *testing.T, testEngine *gin.Engine, queryTestCases []requestBodyTestCase, allowInt64 bool) {
	for i, testcase := range queryTestCases {
		t.Run(testcase.path, func(t *testing.T) {
			bodyReader := bytes.NewReader(testcase.requestBody)
			req := httptest.NewRequest(http.MethodPost, testcase.path, bodyReader)
			if allowInt64 {
				req.Header.Set(HTTPHeaderAllowInt64, "true")
			}
			w := httptest.NewRecorder()
			testEngine.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code, "case %d: ", i, string(testcase.requestBody))
			returnBody := &ReturnErrMsg{}
			err := json.Unmarshal(w.Body.Bytes(), returnBody)
			assert.Nil(t, err, "case %d: ", i)
			assert.Equal(t, testcase.errCode, returnBody.Code, "case %d: ", i, string(testcase.requestBody))
			if testcase.errCode != 0 {
				assert.Equal(t, testcase.errMsg, returnBody.Message, "case %d: ", i, string(testcase.requestBody))
			}
			fmt.Println(w.Body.String())
		})
	}
}

func TestResourceGroupHandlerV2(t *testing.T) {
	// init params
	paramtable.Init()

	// disable rate limit
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	// mock proxy
	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	mockProxy.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(&milvuspb.DescribeResourceGroupResponse{}, nil)
	mockProxy.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	mockProxy.EXPECT().ListResourceGroups(mock.Anything, mock.Anything).Return(&milvuspb.ListResourceGroupsResponse{}, nil)
	mockProxy.EXPECT().UpdateResourceGroups(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	mockProxy.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(merr.Success(), nil)

	// setup test server
	testServer := initHTTPServerV2(mockProxy, false)

	// setup test case
	testCases := make([]requestBodyTestCase, 0)

	// test case: create resource group with empty request body
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, CreateAction),
		requestBody: []byte(`{}`),
		errCode:     1802,
		errMsg:      "missing required parameters, error: Key: 'ResourceGroupReq.Name' Error:Field validation for 'Name' failed on the 'required' tag",
	})

	// test case: create resource group with name
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, CreateAction),
		requestBody: []byte(`{"name":"test"}`),
	})

	// test case: create resource group with name and config
	requestBodyInJSON := `{"name":"test","config":{"requests":{"node_num":1},"limits":{"node_num":1},"transfer_from":[{"resource_group":"__default_resource_group"}],"transfer_to":[{"resource_group":"__default_resource_group"}]}}`
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, CreateAction),
		requestBody: []byte(requestBodyInJSON),
	})

	// test case: describe resource group with empty request body
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, DescribeAction),
		requestBody: []byte(`{}`),
		errCode:     1802,
		errMsg:      "missing required parameters, error: Key: 'ResourceGroupReq.Name' Error:Field validation for 'Name' failed on the 'required' tag",
	})
	// test case: describe resource group with name
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, DescribeAction),
		requestBody: []byte(`{"name":"test"}`),
	})

	// test case: drop resource group with empty request body
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, DropAction),
		requestBody: []byte(`{}`),
		errCode:     1802,
		errMsg:      "missing required parameters, error: Key: 'ResourceGroupReq.Name' Error:Field validation for 'Name' failed on the 'required' tag",
	})
	// test case: drop resource group with name
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, DropAction),
		requestBody: []byte(`{"name":"test"}`),
	})

	// test case: list resource groups
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, ListAction),
		requestBody: []byte(`{}`),
	})

	// test case: update resource group with empty request body
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, AlterAction),
		requestBody: []byte(`{}`),
		errCode:     1802,
		errMsg:      "missing required parameters, error: Key: 'UpdateResourceGroupReq.ResourceGroups' Error:Field validation for 'ResourceGroups' failed on the 'required' tag",
	})

	// test case: update resource group with resource groups
	requestBodyInJSON = `{"resource_groups":{"test":{"requests":{"node_num":1},"limits":{"node_num":1},"transfer_from":[{"resource_group":"__default_resource_group"}],"transfer_to":[{"resource_group":"__default_resource_group"}]}}}`
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, AlterAction),
		requestBody: []byte(requestBodyInJSON),
	})

	// test case: transfer replica with empty request body
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, TransferReplicaAction),
		requestBody: []byte(`{}`),
		errCode:     1802,
		errMsg:      "missing required parameters, error: Key: 'TransferReplicaReq.SourceRgName' Error:Field validation for 'SourceRgName' failed on the 'required' tag\nKey: 'TransferReplicaReq.TargetRgName' Error:Field validation for 'TargetRgName' failed on the 'required' tag\nKey: 'TransferReplicaReq.CollectionName' Error:Field validation for 'CollectionName' failed on the 'required' tag\nKey: 'TransferReplicaReq.ReplicaNum' Error:Field validation for 'ReplicaNum' failed on the 'required' tag",
	})

	// test case: transfer replica
	requestBodyInJSON = `{"sourceRgName":"rg1","targetRgName":"rg2","collectionName":"hello_milvus","replicaNum":1}`
	testCases = append(testCases, requestBodyTestCase{
		path:        versionalV2(ResourceGroupCategory, TransferReplicaAction),
		requestBody: []byte(requestBodyInJSON),
	})

	// verify test case
	validateRequestBodyTestCases(t, testServer, testCases, false)
}
