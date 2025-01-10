// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package componentutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

type MockComponent struct {
	compState *milvuspb.ComponentStates
	strResp   *milvuspb.StringResponse
	compErr   error
}

func (mc *MockComponent) SetCompState(state *milvuspb.ComponentStates) {
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

func (mc *MockComponent) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return mc.compState, mc.compErr
}

func (mc *MockComponent) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return mc.strResp, nil
}

func (mc *MockComponent) Register() error {
	return nil
}

func buildMockComponent(code commonpb.StateCode) *MockComponent {
	mc := &MockComponent{
		compState: &milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
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

func Test_WaitForComponentInitOrHealthy(t *testing.T) {
	mc := &MockComponent{
		compState: nil,
		strResp:   nil,
		compErr:   errors.New("error"),
	}
	err := WaitForComponentInitOrHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
	assert.Error(t, err)

	mc = &MockComponent{
		compState: &milvuspb.ComponentStates{
			State:              nil,
			SubcomponentStates: nil,
			Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
		},
		strResp: nil,
		compErr: nil,
	}
	err = WaitForComponentInitOrHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
	assert.Error(t, err)

	validCodes := []commonpb.StateCode{commonpb.StateCode_Initializing, commonpb.StateCode_Healthy}
	testCodes := []commonpb.StateCode{commonpb.StateCode_Initializing, commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal}
	for _, code := range testCodes {
		mc := buildMockComponent(code)
		err := WaitForComponentInitOrHealthy(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
		if funcutil.SliceContain(validCodes, code) {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_WaitForComponentInit(t *testing.T) {
	validCodes := []commonpb.StateCode{commonpb.StateCode_Initializing}
	testCodes := []commonpb.StateCode{commonpb.StateCode_Initializing, commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal}
	for _, code := range testCodes {
		mc := buildMockComponent(code)
		err := WaitForComponentInit(context.TODO(), mc, "mockService", 1, 10*time.Millisecond)
		if funcutil.SliceContain(validCodes, code) {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
