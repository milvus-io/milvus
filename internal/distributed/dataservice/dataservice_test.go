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

package grpcdataserviceclient

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

type mockMaster struct {
	types.MasterService
}

func (m *mockMaster) Init() error {
	return nil
}

func (m *mockMaster) Start() error {
	return nil
}

func (m *mockMaster) Stop() error {
	return fmt.Errorf("stop error")
}

func (m *mockMaster) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			StateCode: internalpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SubcomponentStates: []*internalpb.ComponentInfo{
			{
				StateCode: internalpb.StateCode_Healthy,
			},
		},
	}, nil
}

func (m *mockMaster) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "c1",
	}, nil
}

func (m *mockMaster) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		CollectionNames: []string{},
	}, nil
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	msFactory := msgstream.NewPmsFactory()
	svr, err := NewServer(ctx, msFactory)
	assert.Nil(t, err)

	Params.Init()
	Params.Port = 1000000
	err = svr.Run()
	assert.NotNil(t, err)
	assert.EqualError(t, err, "listen tcp: address 1000000: invalid port")

	svr.newMasterServiceClient = func(s string) (types.MasterService, error) {
		return &mockMaster{}, nil
	}

	Params.Port = rand.Int()%100 + 10000
	err = svr.Run()
	assert.Nil(t, err)

	err = svr.Stop()
	assert.Nil(t, err)
}
