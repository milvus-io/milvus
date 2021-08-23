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

package grpcdatacoordclient

/*
import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

type mockRootCoord struct {
	types.RootCoord
}

func (m *mockRootCoord) Init() error {
	return nil
}

func (m *mockRootCoord) Start() error {
	return nil
}

func (m *mockRootCoord) Stop() error {
	return fmt.Errorf("stop error")
}

func (m *mockRootCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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

func (m *mockRootCoord) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "c1",
	}, nil
}

func (m *mockRootCoord) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
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
	var mockRootCoordCreator = func(ctx context.Context, metaRootPath string,
		etcdEndpoints []string) (types.RootCoord, error) {
		return &mockRootCoord{}, nil
	}
	dsServer, err := NewServer(ctx, msFactory, datacoord.SetRootCoordCreator(mockRootCoordCreator))
	assert.Nil(t, err)

	Params.Init()
	Params.Port = rand.Int()%100 + 10000
	err = dsServer.Run()
	assert.Nil(t, err)
	defer func() {
		err = dsServer.Stop()
		assert.Nil(t, err)
	}()

	t.Run("get component states", func(t *testing.T) {
		req := &internalpb.GetComponentStatesRequest{}
		rsp, err := dsServer.GetComponentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get time tick channel", func(t *testing.T) {
		req := &internalpb.GetTimeTickChannelRequest{}
		rsp, err := dsServer.GetTimeTickChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get statistics channel", func(t *testing.T) {
		req := &internalpb.GetStatisticsChannelRequest{}
		rsp, err := dsServer.GetStatisticsChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get segment info", func(t *testing.T) {
		req := &datapb.GetSegmentInfoRequest{}
		rsp, err := dsServer.GetSegmentInfo(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("flush", func(t *testing.T) {
		req := &datapb.FlushRequest{}
		rsp, err := dsServer.Flush(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("assign segment id", func(t *testing.T) {
		req := &datapb.AssignSegmentIDRequest{}
		rsp, err := dsServer.AssignSegmentID(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get segment states", func(t *testing.T) {
		req := &datapb.GetSegmentStatesRequest{}
		rsp, err := dsServer.GetSegmentStates(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get insert binlog paths", func(t *testing.T) {
		req := &datapb.GetInsertBinlogPathsRequest{}
		rsp, err := dsServer.GetInsertBinlogPaths(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get collection statistics", func(t *testing.T) {
		req := &datapb.GetCollectionStatisticsRequest{}
		rsp, err := dsServer.GetCollectionStatistics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get partition statistics", func(t *testing.T) {
		req := &datapb.GetPartitionStatisticsRequest{}
		rsp, err := dsServer.GetPartitionStatistics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run("get segment info channel", func(t *testing.T) {
		req := &datapb.GetSegmentInfoChannelRequest{}
		rsp, err := dsServer.GetSegmentInfoChannel(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, rsp.Status.ErrorCode, commonpb.ErrorCode_Success)
	})

}
*/
