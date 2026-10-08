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

package querynodev2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func mixCoordFuture(mc types.MixCoordClient) *syncutil.Future[types.MixCoordClient] {
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	return future
}

// A spawn whose source collection is gone, or whose target never gets a
// seekable position, fails without registering anything for the target.
func TestSpawnSplitChildFailsWithoutRegistering(t *testing.T) {
	paramtable.Init()

	t.Run("collection released", func(t *testing.T) {
		collections := segments.NewMockCollectionManager(t)
		collections.EXPECT().Get(int64(1)).Return(nil)
		node := &QueryNode{
			ctx:        context.Background(),
			delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
			manager:    &segments.Manager{Collection: collections},
		}
		_, err := node.SpawnSplitChild(context.Background(), delegator.SpawnChildParams{CollectionID: 1, TargetVChannel: "v1"})
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		assert.False(t, node.delegators.Contain("v1"))
	})

	t.Run("no coordinator to wait on", func(t *testing.T) {
		collections := segments.NewMockCollectionManager(t)
		collections.EXPECT().Get(int64(1)).Return(segments.NewTestCollection(1, 0, nil))
		node := &QueryNode{
			ctx:        context.Background(),
			delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
			manager:    &segments.Manager{Collection: collections},
		}
		_, err := node.SpawnSplitChild(context.Background(), delegator.SpawnChildParams{CollectionID: 1, TargetVChannel: "v1"})
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.False(t, node.delegators.Contain("v1"))
	})
}

// waitSplitTargetRecovery gives up with the last reason once its retries run
// out. retry.Do is cut to one attempt so the test does not sit out two minutes.
func TestWaitSplitTargetRecoveryGivesUp(t *testing.T) {
	oneAttempt := mockey.Mock(retry.Do).To(func(ctx context.Context, fn func() error, _ ...retry.Option) error {
		return fn()
	}).Build()
	defer oneAttempt.UnPatch()

	t.Run("the coordinator call fails", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(nil, errors.New("coordinator down"))
		node := &QueryNode{ctx: context.Background(), mixCoord: mixCoordFuture(mc)}
		_, err := node.waitSplitTargetRecovery(1, "v1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "split target v1 recovery info not available")
	})

	t.Run("the target has no seekable position", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
			Status: merr.Success(),
			// the earliest-segment fallback on a target nothing has been written
			// to: a timestamp but neither a message ID nor a WAL name.
			Channels: []*datapb.VchannelInfo{{ChannelName: "v1", SeekPosition: &msgpb.MsgPosition{Timestamp: 42}}},
		}, nil)
		node := &QueryNode{ctx: context.Background(), mixCoord: mixCoordFuture(mc)}
		_, err := node.waitSplitTargetRecovery(1, "v1")
		assert.ErrorIs(t, err, merr.ErrChannelNotFound)
	})

	t.Run("the coordinator handle never resolves", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		node := &QueryNode{ctx: ctx, mixCoord: syncutil.NewFuture[types.MixCoordClient]()}
		_, err := node.waitSplitTargetRecovery(1, "v1")
		assert.Error(t, err)
	})
}

// The recovery respawn stops without spawning when there is no way to: the node
// is shutting down, the source was released, or the source refuses the spawn.
// In every case it lifts the source's recovery refusal.
func TestRespawnSplitChildrenOnRecoveryBailsOut(t *testing.T) {
	describe := func(states ...schemapb.ShardState) *milvuspb.DescribeCollectionResponse {
		resp := &milvuspb.DescribeCollectionResponse{Status: merr.Success()}
		for i, state := range states {
			vchannel := []string{"src", "t1", "t2"}[i]
			resp.VirtualChannelNames = append(resp.VirtualChannelNames, vchannel)
			resp.ShardInfos = append(resp.ShardInfos, &schemapb.CollectionShardInfo{VchannelName: vchannel, State: state})
		}
		return resp
	}

	t.Run("the coordinator handle never resolves", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		node := &QueryNode{mixCoord: syncutil.NewFuture[types.MixCoordClient]()}
		source := delegator.NewMockShardDelegator(t)
		source.EXPECT().FinishSplitRecovery().Return().Once()
		node.respawnSplitChildrenOnRecovery(ctx, source, 1, "src", []string{"t1", "t2"})
	})

	t.Run("a released source stops retrying a failing describe", func(t *testing.T) {
		backoff := mockey.Mock(splitRecoveryRetryBackoff).Return(time.Millisecond).Build()
		defer backoff.UnPatch()
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, errors.New("coordinator down"))
		node := &QueryNode{mixCoord: mixCoordFuture(mc)}
		source := delegator.NewMockShardDelegator(t)
		source.EXPECT().Serviceable().Return(false)
		source.EXPECT().FinishSplitRecovery().Return().Once()
		node.respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src", []string{"t1", "t2"})
	})

	t.Run("the source refuses the respawn", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(
			describe(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating), nil)
		node := &QueryNode{mixCoord: mixCoordFuture(mc)}
		source := delegator.NewMockShardDelegator(t)
		source.EXPECT().ProcessSplitShard(mock.Anything, []string{"t1", "t2"}).Return(errors.New("no spawner")).Once()
		source.EXPECT().FinishSplitRecovery().Return().Once()
		node.respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src", []string{"t1", "t2"})
	})
}

// rootcoord not answering at a restart is a case the design anticipates. The
// fence sits behind the source's checkpoint and is not replayed, so giving up
// leaves the source answering from its own view alone -- every target's rows
// and deletes missing, with no error. The recovery keeps describing until it
// answers, and only then lets reads through (the source refuses them while its
// recovery is pending), by which time the respawned targets are pending spawns.
func TestRespawnSplitChildrenOnRecoveryRetriesAFailingDescribe(t *testing.T) {
	backoff := mockey.Mock(splitRecoveryRetryBackoff).Return(time.Millisecond).Build()
	defer backoff.UnPatch()

	mc := mocks.NewMockMixCoordClient(t)
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, errors.New("coordinator down")).Times(2)
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		VirtualChannelNames: []string{"src", "t1", "t2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardCreating},
		},
	}, nil).Once()
	node := &QueryNode{mixCoord: mixCoordFuture(mc)}

	var steps []string
	source := delegator.NewMockShardDelegator(t)
	source.EXPECT().Serviceable().Return(true).Maybe()
	source.EXPECT().ProcessSplitShard(mock.Anything, []string{"t1", "t2"}).RunAndReturn(func(context.Context, []string) error {
		steps = append(steps, "spawn")
		return nil
	}).Once()
	source.EXPECT().FinishSplitRecovery().Run(func() { steps = append(steps, "finish") }).Once()

	node.respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src", []string{"t1", "t2"})
	// the targets are pending spawns (reads refused) before the recovery's own
	// refusal is lifted, so no read is ever served without them.
	assert.Equal(t, []string{"spawn", "finish"}, steps)
}

// Releasing a source only detaches a child QueryCoord has adopted: it keeps
// serving its vchannel. A child no longer registered on the node is skipped.
func TestReleaseSplitChildrenDetachesAdoptedChildren(t *testing.T) {
	paramtable.Init()
	node := &QueryNode{
		ctx:        context.Background(),
		delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		manager:    segments.NewManager(),
	}

	source := delegator.NewMockShardDelegator(t)
	source.EXPECT().MarkReleasing().Once()
	source.EXPECT().SplitChildVChannels().Return([]string{"v1", "gone"})
	source.EXPECT().DetachSplitChild("v1").Once()

	adopted := delegator.NewMockShardDelegator(t)
	adopted.EXPECT().IsUnadoptedSplitChild().Return(false)
	adopted.EXPECT().SetFrontingParent(nil).Once()
	node.delegators.Insert("v1", adopted)

	node.releaseSplitChildren(context.Background(), source, 1)
	assert.True(t, node.delegators.Contain("v1"), "an adopted child keeps serving after its source is released")
}

// A child spawned for a source that was released or stopped mid-spawn is torn
// down entirely: unregistered, its pipeline removed, and the delegator closed.
func TestAbortSplitChild(t *testing.T) {
	paramtable.Init()
	manager := segments.NewManager()
	node := &QueryNode{
		ctx:        context.Background(),
		delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		manager:    manager,
	}
	node.pipelineManager = pipeline.NewManager(manager, nil, node.delegators)

	child := delegator.NewMockShardDelegator(t)
	child.EXPECT().Close().Once()
	node.delegators.Insert("v1", child)

	node.AbortSplitChild(context.Background(), child, 1, "v1")
	assert.False(t, node.delegators.Contain("v1"))
}

func TestSetMixCoordClient(t *testing.T) {
	node := &QueryNode{}
	future := syncutil.NewFuture[types.MixCoordClient]()
	node.SetMixCoordClient(future)
	assert.Same(t, future, node.mixCoord)
}

// The node's Done ends the split child spawns still retrying when it stops.
func TestQueryNodeDoneEndsWithTheNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := &QueryNode{ctx: ctx}
	select {
	case <-node.Done():
		t.Fatal("done before the node stopped")
	default:
	}
	cancel()
	<-node.Done()
}

// One describe that hangs (a coordinator failover can leave the RPC without an
// answer) must not hold the recovery, and so the source's read refusal, for
// good: each attempt has its own deadline and the loop retries.
func TestRespawnSplitChildrenOnRecoveryTimesOutAHungDescribe(t *testing.T) {
	backoff := mockey.Mock(splitRecoveryRetryBackoff).Return(time.Millisecond).Build()
	defer backoff.UnPatch()
	timeout := mockey.Mock(splitRecoveryDescribeTimeout).Return(50 * time.Millisecond).Build()
	defer timeout.UnPatch()

	mc := mocks.NewMockMixCoordClient(t)
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			<-ctx.Done() // hangs until the attempt's deadline
			return nil, ctx.Err()
		}).Once()
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		VirtualChannelNames: []string{"src"},
	}, nil).Once()
	node := &QueryNode{mixCoord: mixCoordFuture(mc)}
	source := delegator.NewMockShardDelegator(t)
	source.EXPECT().Serviceable().Return(true).Maybe()
	source.EXPECT().ProcessSplitShard(mock.Anything, []string{"t1", "t2"}).Return(nil).Once()
	source.EXPECT().FinishSplitRecovery().Return().Once()

	done := make(chan struct{})
	go func() {
		node.respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src", []string{"t1", "t2"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a hung describe held the split recovery")
	}
}
