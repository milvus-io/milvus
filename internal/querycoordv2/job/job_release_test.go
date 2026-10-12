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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type releaseJobCatalog struct {
	metastore.QueryCoordCatalog

	mu                     sync.Mutex
	replicas               map[int64]*querypb.Replica
	releaseCollectionCalls int
	releaseReplicasCalls   int
	releaseCollectionErr   error
	releaseReplicasErr     error
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
	if c.releaseReplicasErr != nil {
		return c.releaseReplicasErr
	}
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
	return c.releaseCollectionErr
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
	err := m.PutCollectionWithoutSave(context.Background(), &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			LoadType:     querypb.LoadType_LoadCollection,
			Status:       querypb.LoadStatus_Loaded,
		},
	})
	require.NoError(t, err)

	err = m.Put(context.Background(), utils.CreateTestReplica(replicaID, collectionID, nodes))
	require.NoError(t, err)
	return m, catalog
}

func newReleaseJobDeps(t *testing.T, m *meta.Meta, dist *meta.DistributionManager) (*observers.TargetObserver, *checkers.CheckerController, proxyutil.ProxyClientManagerInterface) {
	t.Helper()

	targetMgr := meta.NewMockTargetManager(t)
	targetMgr.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true).Maybe()
	targetMgr.EXPECT().IsCurrentTargetExist(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, mock.Anything, mock.Anything).Return(map[string]*meta.DmChannel{}).Maybe()
	targetMgr.EXPECT().UpdateCollectionNextTarget(mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().RemoveCollection(mock.Anything, mock.Anything).Return().Maybe()
	nodeMgr := session.NewNodeManager()

	targetObserver := observers.NewTargetObserver(m, targetMgr, dist, nil, nil, nodeMgr)
	targetObserver.Start()
	t.Cleanup(targetObserver.Stop)

	assign.InitGlobalAssignPolicyFactory(nil, nodeMgr, dist, m, targetMgr)
	checkerController := checkers.NewCheckerController(m, dist, targetMgr, nodeMgr, nil, nil)
	proxyManager := proxyutil.NewProxyClientManager(nil)
	return targetObserver, checkerController, proxyManager
}

func TestReleaseCollectionJobRemovesReplicaAfterDistributionReleased(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1000)
	replicaID := int64(10)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, 1)
	dist := meta.NewDistributionManager(session.NewNodeManager())
	targetObserver, checkerController, proxyManager := newReleaseJobDeps(t, m, dist)

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
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
	targetObserver, checkerController, proxyManager := newReleaseJobDeps(t, m, dist)

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
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
	targetObserver, checkerController, proxyManager := newReleaseJobDeps(t, m, dist)

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
	)

	require.NoError(t, releaseJob.Execute())
	require.Nil(t, m.GetCollection(ctx, collectionID))
	require.Empty(t, m.GetByCollection(ctx, collectionID))
	require.Equal(t, 1, catalog.releaseReplicasCalls)
}

func TestReleaseCollectionJobIgnoresMissingCollectionAndReplica(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1003)
	m := meta.NewMeta(func() (int64, error) { return 0, nil }, newReleaseJobCatalog(), session.NewNodeManager())
	dist := meta.NewDistributionManager(session.NewNodeManager())
	targetObserver, checkerController, proxyManager := newReleaseJobDeps(t, m, dist)

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
	)

	require.NoError(t, releaseJob.Execute())
	require.Nil(t, m.GetCollection(ctx, collectionID))
	require.Empty(t, m.GetByCollection(ctx, collectionID))
}

func TestReleaseCollectionJobReturnsCollectionRemovalError(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1004)
	replicaID := int64(14)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, 1)
	catalog.releaseCollectionErr = errors.New("release collection failed")
	dist := meta.NewDistributionManager(session.NewNodeManager())
	targetObserver, checkerController, proxyManager := newReleaseJobDeps(t, m, dist)

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
	)

	err := releaseJob.Execute()
	require.ErrorContains(t, err, "failed to remove collection")
	require.NotNil(t, m.GetCollection(ctx, collectionID))
	require.Len(t, m.GetByCollection(ctx, collectionID), 1)
	require.Equal(t, 1, catalog.releaseCollectionCalls)
	require.Equal(t, 0, catalog.releaseReplicasCalls)
}

func TestReleaseCollectionJobContinuesWhenCacheInvalidationFails(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1005)
	replicaID := int64(15)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, 1)
	dist := meta.NewDistributionManager(session.NewNodeManager())
	targetObserver, checkerController, _ := newReleaseJobDeps(t, m, dist)
	proxyManager := proxyutil.NewMockProxyClientManager(t)
	proxyManager.EXPECT().
		InvalidateCollectionMetaCache(
			mock.Anything,
			mock.MatchedBy(func(req *proxypb.InvalidateCollMetaCacheRequest) bool {
				return req.GetCollectionID() == collectionID
			}),
			mock.Anything,
		).
		Return(errors.New("invalidate collection cache failed"))
	proxyManager.EXPECT().
		InvalidateShardLeaderCache(
			mock.Anything,
			mock.MatchedBy(func(req *proxypb.InvalidateShardLeaderCacheRequest) bool {
				return len(req.GetCollectionIDs()) == 1 && req.GetCollectionIDs()[0] == collectionID
			}),
		).
		Return(errors.New("invalidate shard leader cache failed"))

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
	)

	require.NoError(t, releaseJob.Execute())
	require.Nil(t, m.GetCollection(ctx, collectionID))
	require.Empty(t, m.GetByCollection(ctx, collectionID))
	require.Equal(t, 1, catalog.releaseCollectionCalls)
	require.Equal(t, 1, catalog.releaseReplicasCalls)
}

func TestReleaseCollectionJobReturnsReplicaRemovalError(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1006)
	replicaID := int64(16)
	m, catalog := newReleaseCollectionJobMeta(t, collectionID, replicaID, 1)
	catalog.releaseReplicasErr = errors.New("release replicas failed")
	dist := meta.NewDistributionManager(session.NewNodeManager())
	targetObserver, checkerController, proxyManager := newReleaseJobDeps(t, m, dist)

	releaseJob := NewReleaseCollectionJob(
		ctx,
		buildReleaseCollectionResult(collectionID),
		dist,
		m,
		nil,
		nil,
		targetObserver,
		checkerController,
		proxyManager,
	)

	err := releaseJob.Execute()
	require.ErrorContains(t, err, "failed to remove replicas")
	require.Nil(t, m.GetCollection(ctx, collectionID))
	require.Len(t, m.GetByCollection(ctx, collectionID), 1)
	require.Equal(t, 1, catalog.releaseCollectionCalls)
	require.Equal(t, 1, catalog.releaseReplicasCalls)
}
