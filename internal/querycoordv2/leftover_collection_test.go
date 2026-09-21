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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type capturedBroadcast struct {
	resourceKeys []message.ResourceKey
	messages     []message.BroadcastMutableMessage
}

// mockLeftoverBroadcast replaces the broadcaster and the control channel for one test.
func mockLeftoverBroadcast(t *testing.T) *capturedBroadcast {
	captured := &capturedBroadcast{}
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	api := mock_broadcaster.NewMockBroadcastAPI(t)
	api.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			captured.messages = append(captured.messages, msg)
			return &types.BroadcastAppendResult{}, nil
		}).Maybe()
	api.EXPECT().Close().Return().Maybe()
	mockey.Mock(broadcast.StartBroadcastWithResourceKeys).To(
		func(ctx context.Context, resourceKeys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			captured.resourceKeys = append(captured.resourceKeys, resourceKeys...)
			return api, nil
		}).Build()
	return captured
}

func leftoverCollection(collectionID, dbID int64) *meta.Collection {
	return &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			DbID:          dbID,
			ReplicaNumber: 1,
			Status:        querypb.LoadStatus_Loaded,
		},
	}
}

func TestReleaseLeftoverLoadedCollection(t *testing.T) {
	const collectionID, dbID = int64(1001), int64(7)

	t.Run("collection not found in rootcoord is released under the shared cluster lock", func(t *testing.T) {
		mockey.PatchConvey(t.Name(), t, func() {
			captured := mockLeftoverBroadcast(t)
			mockey.Mock((*meta.CollectionManager).Exist).Return(true).Build()
			mockey.Mock((*meta.CollectionManager).GetCollection).Return(leftoverCollection(collectionID, dbID)).Build()

			broker := meta.NewMockBroker(t)
			broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(nil, merr.WrapErrCollectionNotFound(collectionID))
			s := &Server{meta: &meta.Meta{}, broker: broker}

			err := s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(context.Background(),
				&querypb.ReleaseCollectionRequest{CollectionID: collectionID})
			assert.NoError(t, err)

			assert.Equal(t, []message.ResourceKey{message.NewSharedClusterResourceKey()}, captured.resourceKeys)
			assert.Len(t, captured.messages, 1)
			header := message.MustAsBroadcastDropLoadConfigMessageV2(captured.messages[0]).Header()
			assert.Equal(t, collectionID, header.GetCollectionId())
			assert.Equal(t, dbID, header.GetDbId())
		})
	})

	t.Run("collection not found in rootcoord and not loaded is a no-op", func(t *testing.T) {
		mockey.PatchConvey(t.Name(), t, func() {
			captured := mockLeftoverBroadcast(t)
			mockey.Mock((*meta.CollectionManager).Exist).Return(false).Build()

			broker := meta.NewMockBroker(t)
			broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(nil, merr.WrapErrCollectionNotFound(collectionID))
			s := &Server{meta: &meta.Meta{}, broker: broker}

			err := s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(context.Background(),
				&querypb.ReleaseCollectionRequest{CollectionID: collectionID})
			assert.ErrorIs(t, err, errReleaseCollectionNotLoaded)
			assert.Empty(t, captured.messages)
		})
	})

	t.Run("other describe errors are returned without releasing", func(t *testing.T) {
		mockey.PatchConvey(t.Name(), t, func() {
			captured := mockLeftoverBroadcast(t)
			describeErr := merr.WrapErrServiceNotReady("mixcoord", 1, "initializing")
			broker := meta.NewMockBroker(t)
			broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(nil, describeErr)
			s := &Server{meta: &meta.Meta{}, broker: broker}

			err := s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(context.Background(),
				&querypb.ReleaseCollectionRequest{CollectionID: collectionID})
			assert.ErrorIs(t, err, merr.ErrServiceNotReady)
			assert.Empty(t, captured.messages)
		})
	})
}

func TestReleaseLeftoverLoadedCollectionsOnStart(t *testing.T) {
	mockey.PatchConvey(t.Name(), t, func() {
		// 1: exists in rootcoord; 2: leftover; 3: describe fails with a transient error.
		mockey.Mock((*meta.CollectionManager).GetAll).Return([]int64{1, 2, 3}).Build()
		broker := meta.NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 1}, nil)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(2)).Return(nil, merr.WrapErrCollectionNotFound(int64(2)))
		broker.EXPECT().DescribeCollection(mock.Anything, int64(3)).Return(nil, errors.New("rpc timeout"))

		var released []int64
		attempts := 0
		mockey.Mock((*Server).broadcastDropLoadConfigForLeftoverCollection).To(
			func(s *Server, ctx context.Context, collectionID int64) error {
				attempts++
				if attempts == 1 {
					return errors.New("broadcaster not ready")
				}
				released = append(released, collectionID)
				return nil
			}).Build()

		s := &Server{meta: &meta.Meta{}, broker: broker}
		s.releaseLeftoverLoadedCollections(context.Background())

		assert.Equal(t, []int64{2}, released)
		assert.Equal(t, 2, attempts)
	})
}
