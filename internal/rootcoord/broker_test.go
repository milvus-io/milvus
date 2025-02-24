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

package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestServerBroker_ReleaseCollection(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.ReleaseCollection(ctx, 1)
		assert.NoError(t, err)
	})
}

func TestServerBroker_GetSegmentInfo(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		_, err := b.GetQuerySegmentInfo(ctx, 1, []int64{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.GetQuerySegmentInfo(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidQueryCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		resp, err := b.GetQuerySegmentInfo(ctx, 1, []int64{1, 2})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestServerBroker_WatchChannels(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		defer cleanTestEnv()

		c := newTestCore(withInvalidDataCoord(), withRocksMqTtSynchronizer())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		defer cleanTestEnv()

		c := newTestCore(withFailedDataCoord(), withRocksMqTtSynchronizer())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		defer cleanTestEnv()

		c := newTestCore(withValidDataCoord(), withRocksMqTtSynchronizer())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.WatchChannels(ctx, &watchInfo{})
		assert.NoError(t, err)
	})
}

func TestServerBroker_UnwatchChannels(t *testing.T) {
	// TODO: implement
	b := newServerBroker(newTestCore())
	ctx := context.Background()
	b.UnwatchChannels(ctx, &watchInfo{})
}

func TestServerBroker_DropCollectionIndex(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1, nil)
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1, nil)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.DropCollectionIndex(ctx, 1, nil)
		assert.NoError(t, err)
	})
}

func TestServerBroker_GetSegmentIndexState(t *testing.T) {
	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		_, err := b.GetSegmentIndexState(ctx, 1, "index_name", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		b := newServerBroker(c)
		ctx := context.Background()
		_, err := b.GetSegmentIndexState(ctx, 1, "index_name", []UniqueID{1, 2})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		mockDataCoord := mocks.NewMockDataCoordClient(t)
		mockDataCoord.EXPECT().GetSegmentIndexState(mock.Anything, mock.Anything).Return(&indexpb.GetSegmentIndexStateResponse{
			Status: merr.Success(),
			States: []*indexpb.SegmentIndexState{
				{
					SegmentID:  1,
					State:      commonpb.IndexState_Finished,
					FailReason: "",
				},
			},
		}, nil)
		c.dataCoord = mockDataCoord

		b := newServerBroker(c)
		ctx := context.Background()
		states, err := b.GetSegmentIndexState(ctx, 1, "index_name", []UniqueID{1})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(states))
		assert.Equal(t, commonpb.IndexState_Finished, states[0].GetState())
	})
}

func TestServerBroker_BroadcastAlteredCollection(t *testing.T) {
	collMeta := &model.Collection{
		CollectionID: 1,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "0",
				Data: []byte("0"),
			},
		},
		Partitions: []*model.Partition{
			{
				PartitionID:               2,
				PartitionName:             "test_partition_name_1",
				PartitionCreatedTimestamp: 0,
			},
		},
	}

	t.Run("get meta fail", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("err"))
		c.meta = meta
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.BroadcastAlteredCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.Error(t, err)
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withInvalidDataCoord())
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(collMeta, nil)
		mockGetDatabase(meta)
		c.meta = meta
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.BroadcastAlteredCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.Error(t, err)
	})

	t.Run("non success error code on execute", func(t *testing.T) {
		c := newTestCore(withFailedDataCoord())
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(collMeta, nil)
		mockGetDatabase(meta)
		c.meta = meta
		b := newServerBroker(c)
		ctx := context.Background()
		err := b.BroadcastAlteredCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		c := newTestCore(withValidDataCoord())
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(collMeta, nil)
		mockGetDatabase(meta)
		c.meta = meta
		b := newServerBroker(c)
		ctx := context.Background()

		req := &milvuspb.AlterCollectionRequest{
			CollectionID: 1,
		}
		err := b.BroadcastAlteredCollection(ctx, req)
		assert.NoError(t, err)
	})
}

func TestServerBroker_GcConfirm(t *testing.T) {
	t.Run("invalid datacoord", func(t *testing.T) {
		dc := mocks.NewMockDataCoordClient(t)
		dc.On("GcConfirm",
			mock.Anything, // context.Context
			mock.Anything, // *datapb.GcConfirmRequest
			mock.Anything, // *datapb.GcConfirmRequest
		).Return(nil, errors.New("error mock GcConfirm"))
		c := newTestCore(withDataCoord(dc))
		broker := newServerBroker(c)
		assert.False(t, broker.GcConfirm(context.Background(), 100, 10000))
	})

	t.Run("non success", func(t *testing.T) {
		dc := mocks.NewMockDataCoordClient(t)
		err := errors.New("mock error")
		dc.On("GcConfirm",
			mock.Anything, // context.Context
			mock.Anything, // *datapb.GcConfirmRequest
			mock.Anything,
		).Return(
			&datapb.GcConfirmResponse{Status: merr.Status(err)},
			nil)
		c := newTestCore(withDataCoord(dc))
		broker := newServerBroker(c)
		assert.False(t, broker.GcConfirm(context.Background(), 100, 10000))
	})

	t.Run("normal case", func(t *testing.T) {
		dc := mocks.NewMockDataCoordClient(t)
		dc.On("GcConfirm",
			mock.Anything, // context.Context
			mock.Anything, // *datapb.GcConfirmRequest
			mock.Anything,
		).Return(
			&datapb.GcConfirmResponse{Status: merr.Success(), GcFinished: true},
			nil)
		c := newTestCore(withDataCoord(dc))
		broker := newServerBroker(c)
		assert.True(t, broker.GcConfirm(context.Background(), 100, 10000))
	})
}

func mockGetDatabase(meta *mockrootcoord.IMetaTable) {
	db := model.NewDatabase(1, "default", pb.DatabaseState_DatabaseCreated, nil)
	meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
		Return(db, nil).Maybe()
	meta.EXPECT().GetDatabaseByID(mock.Anything, mock.Anything, mock.Anything).
		Return(db, nil).Maybe()
}
