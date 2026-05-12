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

package job

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type releaseJobCatalog struct {
	metastore.QueryCoordCatalog

	mu                     sync.Mutex
	replicas               map[int64]*querypb.Replica
	releaseCollectionCalls int
	releaseReplicasCalls   int
}

func newReleaseJobCatalog() *releaseJobCatalog {
	return &releaseJobCatalog{
		replicas: make(map[int64]*querypb.Replica),
	}
}

func (c *releaseJobCatalog) SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, replica := range replicas {
		c.replicas[replica.GetID()] = proto.Clone(replica).(*querypb.Replica)
	}
	return nil
}

func (c *releaseJobCatalog) ReleaseReplica(ctx context.Context, collection int64, replicas ...int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.releaseReplicasCalls++
	for _, replicaID := range replicas {
		delete(c.replicas, replicaID)
	}
	return nil
}

func (c *releaseJobCatalog) ReleaseReplicas(ctx context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.releaseReplicasCalls++
	for replicaID, replica := range c.replicas {
		if replica.GetCollectionID() == collectionID {
			delete(c.replicas, replicaID)
		}
	}
	return nil
}

func (c *releaseJobCatalog) ReleaseCollection(ctx context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.releaseCollectionCalls++
	return nil
}

func buildReleaseCollectionResult(collectionID int64) message.BroadcastResultDropLoadConfigMessageV2 {
	const controlChannel = "_ctrl_channel"
	broadcastMsg := message.NewDropLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.DropLoadConfigMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&messagespb.DropLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	return message.BroadcastResultDropLoadConfigMessageV2{
		Message: message.MustAsBroadcastDropLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{
			controlChannel: {},
		},
	}
}

func newReleaseCollectionJobMeta(t *testing.T, collectionID, replicaID int64, nodes ...int64) (*meta.Meta, *releaseJobCatalog) {
	t.Helper()

	catalog := newReleaseJobCatalog()
	m := meta.NewMeta(func() (int64, error) { return 0, nil }, catalog, session.NewNodeManager())
	err := m.CollectionManager.PutCollectionWithoutSave(context.Background(), &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			LoadType:     querypb.LoadType_LoadCollection,
			Status:       querypb.LoadStatus_Loaded,
		},
	})
	require.NoError(t, err)

	err = m.ReplicaManager.Put(context.Background(), utils.CreateTestReplica(replicaID, collectionID, nodes))
	require.NoError(t, err)
	return m, catalog
}

func TestReleaseCollectionJobRemovesReplicaAfterDistributionReleased(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1000)
	replicaID := int64(10)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, 1)
	dist := meta.NewDistributionManager(session.NewNodeManager())

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	require.NoError(t, releaseJob.Execute())
	require.Nil(t, m.GetCollection(ctx, collectionID))
	require.Empty(t, m.GetByCollection(ctx, collectionID))
	require.Equal(t, 1, catalog.releaseCollectionCalls)
	require.Equal(t, 1, catalog.releaseReplicasCalls)
}

func TestReleaseCollectionJobKeepsReplicaWhenDistributionStillExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	collectionID := int64(1001)
	replicaID := int64(11)
	nodeID := int64(1)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, nodeID)
	dist := meta.NewDistributionManager(session.NewNodeManager())
	dist.ChannelDistManager.Update(nodeID, utils.CreateTestChannel(collectionID, nodeID, 1, "channel-1"))

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	err := releaseJob.Execute()
	require.Error(t, err)
	require.Nil(t, m.GetCollection(context.Background(), collectionID))
	require.NotNil(t, m.Get(context.Background(), replicaID))
	require.Len(t, m.GetByCollection(context.Background(), collectionID), 1)
	require.Equal(t, 1, catalog.releaseCollectionCalls)
	require.Equal(t, 0, catalog.releaseReplicasCalls)
}

func TestReleaseCollectionJobFinalizesReplicaCleanupOnRetry(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1002)
	replicaID := int64(12)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, 1)
	require.NoError(t, m.CollectionManager.RemoveCollection(ctx, collectionID))
	dist := meta.NewDistributionManager(session.NewNodeManager())

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	require.NoError(t, releaseJob.Execute())
	require.Nil(t, m.GetCollection(ctx, collectionID))
	require.Empty(t, m.GetByCollection(ctx, collectionID))
	require.Equal(t, 1, catalog.releaseReplicasCalls)
}
