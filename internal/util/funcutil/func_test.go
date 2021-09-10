// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package funcutil

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

type MockComponent struct {
	compState *internalpb.ComponentStates
	strResp   *milvuspb.StringResponse
	compErr   error
}

func (mc *MockComponent) SetCompState(state *internalpb.ComponentStates) {
	mc.compState = state
}

func (mc *MockComponent) SetStrResp(resp *milvuspb.StringResponse) {
	mc.strResp = resp
}

func (mc *MockComponent) Init() error {
	return nil
}

func (mc *MockComponent) Start() error {
	return nil
}

func (mc *MockComponent) Stop() error {
	return nil
}

func (mc *MockComponent) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return mc.compState, mc.compErr
}

func (mc *MockComponent) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return mc.strResp, nil
}

func (mc *MockComponent) Register() error {
	return nil
}

func buildMockComponent(code internalpb.StateCode) *MockComponent {
	mc := &MockComponent{
		compState: &internalpb.ComponentStates{
			State: &internalpb.ComponentInfo{
				NodeID:    0,
				Role:      "role",
				StateCode: code,
			},
			SubcomponentStates: nil,
			Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		},
		strResp: nil,
		compErr: nil,
	}

	return mc
}

func Test_CheckGrpcReady(t *testing.T) {
	errChan := make(chan error)

	// test errChan can receive nil after interval
	go CheckGrpcReady(context.TODO(), errChan)

	err := <-errChan
	assert.Nil(t, err)

	// test CheckGrpcReady can finish after context done
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	CheckGrpcReady(ctx, errChan)
	cancel()
}

func Test_CheckPortAvailable(t *testing.T) {
	num := 10

	for i := 0; i < num; i++ {
		port := GetAvailablePort()
		assert.Equal(t, CheckPortAvailable(port), true)
	}
}

func Test_GetLocalIP(t *testing.T) {
	ip := GetLocalIP()
	assert.NotNil(t, ip)
	assert.NotZero(t, len(ip))
}

func Test_WaitForComponentInitOrHealthy(t *testing.T) {
	mc := &MockComponent{
		compState: nil,
		strResp:   nil,
		compErr:   errors.New("error"),
	}
	err := WaitForComponentInitOrHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
	assert.NotNil(t, err)

	mc = &MockComponent{
		compState: &internalpb.ComponentStates{
			State:              nil,
			SubcomponentStates: nil,
			Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
		},
		strResp: nil,
		compErr: nil,
	}
	err = WaitForComponentInitOrHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
	assert.NotNil(t, err)

	validCodes := []internalpb.StateCode{internalpb.StateCode_Initializing, internalpb.StateCode_Healthy}
	testCodes := []internalpb.StateCode{internalpb.StateCode_Initializing, internalpb.StateCode_Healthy, internalpb.StateCode_Abnormal}
	for _, code := range testCodes {
		mc := buildMockComponent(code)
		err := WaitForComponentInitOrHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
		if SliceContain(validCodes, code) {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func Test_WaitForComponentInit(t *testing.T) {
	validCodes := []internalpb.StateCode{internalpb.StateCode_Initializing}
	testCodes := []internalpb.StateCode{internalpb.StateCode_Initializing, internalpb.StateCode_Healthy, internalpb.StateCode_Abnormal}
	for _, code := range testCodes {
		mc := buildMockComponent(code)
		err := WaitForComponentInit(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
		if SliceContain(validCodes, code) {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func Test_WaitForComponentHealthy(t *testing.T) {
	validCodes := []internalpb.StateCode{internalpb.StateCode_Healthy}
	testCodes := []internalpb.StateCode{internalpb.StateCode_Initializing, internalpb.StateCode_Healthy, internalpb.StateCode_Abnormal}
	for _, code := range testCodes {
		mc := buildMockComponent(code)
		err := WaitForComponentHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
		if SliceContain(validCodes, code) {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func Test_ParseIndexParamsMap(t *testing.T) {
	num := 10
	keys := make([]string, 0)
	values := make([]string, 0)
	params := make(map[string]string)

	for i := 0; i < num; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
		values = append(values, "value"+strconv.Itoa(i))
		params[keys[i]] = values[i]
	}

	paramsBytes, err := json.Marshal(params)
	assert.Equal(t, err, nil)
	paramsStr := string(paramsBytes)

	parsedParams, err := ParseIndexParamsMap(paramsStr)
	assert.Equal(t, err, nil)
	assert.Equal(t, parsedParams, params)

	invalidStr := "invalid string"
	_, err = ParseIndexParamsMap(invalidStr)
	assert.NotEqual(t, err, nil)
}
