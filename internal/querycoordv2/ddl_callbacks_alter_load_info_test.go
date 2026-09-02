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
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func buildAlterLoadConfigBroadcastResult(collectionID int64, vchannels ...string) message.BroadcastResultAlterLoadConfigMessageV2 {
	if len(vchannels) == 0 {
		vchannels = []string{funcutil.GetControlChannel("test")}
	}
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			CollectionId: collectionID,
			Replicas: []*messagespb.LoadReplicaConfig{
				{ReplicaId: 1000, ResourceGroupName: "__default_resource_group"},
			},
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()

	results := make(map[string]*message.AppendResult, len(vchannels))
	for _, vchannel := range vchannels {
		results[vchannel] = &message.AppendResult{}
	}
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: results,
	}
}

func TestAlterLoadConfigV2AckCallbackUpdatesQViewsRuntime(t *testing.T) {
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

	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	s := &Server{qviewsRuntime: runtime}
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()

	require.NoError(t, s.alterLoadConfigV2AckCallback(ctx, buildAlterLoadConfigBroadcastResult(100)))

	assert.Contains(t, runtime.loadConfigStore.Snapshot().ConfigsMap(), int64(100))
	assert.Equal(t, []balancer.TriggerScope{{DirtyCollections: []int64{100}}}, fakeBalancer.triggers)
}
