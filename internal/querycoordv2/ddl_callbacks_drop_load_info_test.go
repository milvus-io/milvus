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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestDropLoadConfigV2AckCallbackUpdatesQViewsRuntime(t *testing.T) {
	ctx := context.Background()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog:    catalog,
		queryViewCatalog:     &fakeQueryViewCatalog{},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, runtime.loadManager.UpdateLoadConfig(ctx, buildAlterLoadConfigBroadcastResult(100)))
	require.Contains(t, runtime.loadConfigStore.Snapshot().ConfigsMap(), int64(100))
	fakeBalancer.triggers = nil

	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	s := &Server{qviewsRuntime: runtime}
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()

	require.NoError(t, s.dropLoadConfigV2AckCallback(ctx, buildDropLoadConfigBroadcastResult(100)))

	assert.NotContains(t, runtime.loadConfigStore.Snapshot().ConfigsMap(), int64(100))
	assert.Equal(t, []balancer.TriggerScope{{DirtyCollections: []int64{100}}}, fakeBalancer.triggers)
}

func buildDropLoadConfigBroadcastResult(collectionID int64) message.BroadcastResultDropLoadConfigMessageV2 {
	broadcastMsg := message.NewDropLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.DropLoadConfigMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&messagespb.DropLoadConfigMessageBody{}).
		WithBroadcast([]string{"v0", "v1"}).
		MustBuildBroadcast()
	return message.BroadcastResultDropLoadConfigMessageV2{
		Message: message.MustAsBroadcastDropLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{
			"v0": {},
			"v1": {},
		},
	}
}
