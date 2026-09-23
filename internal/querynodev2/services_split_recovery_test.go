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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// splitRecoveryCoord is a coordinator that describes the collection as not
// splitting (so the watch's own recovery is a no-op) and serves segment infos.
func (suite *ServiceSuite) splitRecoveryCoord() *mocks.MockMixCoordClient {
	mc := mocks.NewMockMixCoordClient(suite.T())
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		VirtualChannelNames: []string{suite.vchannel},
	}, nil).Maybe()
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	suite.node.mixCoord = future
	return mc
}

// A child respawned mid-window -- the QueryNode serving the source restarted --
// is built from its target's CURRENT recovery view, whose checkpoint may be far
// past T_switch. Everything before that checkpoint must be loaded like a watch
// loads it, or it is in no view: the target's growing data already synced, its
// flushed segments still invisible (every target-flushed segment at defaults,
// until sorted after Done), and its L0 deletes, which must also reach the
// source's own view.
func (suite *ServiceSuite) TestSpawnSplitChildLoadsTheTargetsRecoveryView() {
	ctx := context.Background()
	const (
		target    = "by-dev-rootcoord-dml_2_111v3"
		growingID = int64(3001) // synced growing, or flushed but still invisible
		l0ID      = int64(3002)
		flushedID = int64(3003) // visible: served by the source as attributed sealed
	)
	mc := suite.splitRecoveryCoord()
	mc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *datapb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
			suite.ElementsMatch([]int64{growingID, l0ID}, req.GetSegmentIDs())
			suite.True(req.GetIncludeUnHealthy())
			return &datapb.GetSegmentInfoResponse{Status: merr.Success(), Infos: []*datapb.SegmentInfo{
				{
					ID: growingID, CollectionID: suite.collectionID, PartitionID: suite.partitionIDs[0], InsertChannel: target,
					Binlogs:     []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
					DmlPosition: &msgpb.MsgPosition{Timestamp: 77},
				},
				{
					ID: l0ID, CollectionID: suite.collectionID, PartitionID: suite.partitionIDs[0], InsertChannel: target,
					Level: datapb.SegmentLevel_L0,
				},
			}}, nil
		}).Once()

	status, err := suite.node.WatchDmChannels(ctx, suite.splitWatchRequest(suite.vchannel))
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	source, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)

	recovery := mockey.Mock((*QueryNode).waitSplitTargetRecovery).To(
		func(_ *QueryNode, _ int64, vchannel string) (*datapb.VchannelInfo, error) {
			info := suite.splitTargetRecovery(vchannel)
			info.UnflushedSegmentIds = []int64{growingID}
			info.LevelZeroSegmentIds = []int64{l0ID}
			info.FlushedSegmentIds = []int64{flushedID}
			return info, nil
		}).Build()
	defer recovery.UnPatch()

	var l0Loaded, growingLoaded []int64
	forwarded := 0
	loadL0 := mockey.Mock(loadL0Segments).To(func(_ context.Context, d delegator.ShardDelegator, req *querypb.WatchDmChannelsRequest) error {
		for _, ch := range req.GetInfos() {
			l0Loaded = append(l0Loaded, lo.Filter(ch.GetLevelZeroSegmentIds(), func(id int64, _ int) bool {
				return req.GetSegmentInfos()[id] != nil
			})...)
		}
		return nil
	}).Build()
	defer loadL0.UnPatch()
	loadGrowing := mockey.Mock(loadGrowingSegments).To(func(_ context.Context, d delegator.ShardDelegator, req *querypb.WatchDmChannelsRequest) error {
		for _, ch := range req.GetInfos() {
			growingLoaded = append(growingLoaded, lo.Filter(ch.GetUnflushedSegmentIds(), func(id int64, _ int) bool {
				return len(req.GetSegmentInfos()[id].GetBinlogs()) > 0
			})...)
		}
		return nil
	}).Build()
	defer loadGrowing.UnPatch()
	// the source is a *shardDelegator like the child, so this patches the
	// child's forward; the source itself never calls it.
	forward := mockey.Mock(mockey.GetMethod(source, "ForwardKnownDeletesToParent")).To(func(ctx context.Context) error {
		forwarded++
		return nil
	}).Build()
	defer forward.UnPatch()
	defer suite.node.delegators.GetAndRemove(target)

	child, err := suite.node.SpawnSplitChild(ctx, delegator.SpawnChildParams{
		CollectionID:   suite.collectionID,
		SourceVChannel: suite.vchannel,
		TargetVChannel: target,
		Parent:         source,
	})
	suite.Require().NoError(err)
	suite.NotNil(child)
	suite.Equal([]int64{l0ID}, l0Loaded, "the target's L0 segments were not loaded into the child")
	suite.Equal([]int64{growingID}, growingLoaded, "the target's unflushed (and invisible flushed) segments were not loaded as growing")
	suite.Equal(1, forwarded, "the child's known deletes were not forwarded to its source")
}

// A failure while loading the target's recovery view fails the spawn, which the
// source retries, and leaves nothing of the child on the node.
func (suite *ServiceSuite) TestSpawnSplitChildFailsCleanlyWhenTheRecoveryViewCannotLoad() {
	ctx := context.Background()
	const target = "by-dev-rootcoord-dml_2_111v3"
	mc := suite.splitRecoveryCoord()
	mc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(nil, errors.New("coordinator unavailable")).Once()

	status, err := suite.node.WatchDmChannels(ctx, suite.splitWatchRequest(suite.vchannel))
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	source, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)

	recovery := mockey.Mock((*QueryNode).waitSplitTargetRecovery).To(
		func(_ *QueryNode, _ int64, vchannel string) (*datapb.VchannelInfo, error) {
			info := suite.splitTargetRecovery(vchannel)
			info.UnflushedSegmentIds = []int64{3001}
			return info, nil
		}).Build()
	defer recovery.UnPatch()

	child, err := suite.node.SpawnSplitChild(ctx, delegator.SpawnChildParams{
		CollectionID:   suite.collectionID,
		SourceVChannel: suite.vchannel,
		TargetVChannel: target,
		Parent:         source,
	})
	suite.Error(err)
	suite.Nil(child)
	suite.False(suite.node.delegators.Contain(target))
	suite.Nil(suite.node.pipelineManager.Get(target))
	suite.Empty(suite.node.manager.Segment.GetBy(segments.WithChannel(target)))
}

func TestGetSplitTargetSegmentInfos(t *testing.T) {
	t.Run("fails without a coordinator handle", func(t *testing.T) {
		node := &QueryNode{ctx: context.Background()}
		_, err := node.getSplitTargetSegmentInfos(context.Background(), []int64{1})
		assert.Error(t, err)
	})

	t.Run("fetches in batches, unhealthy segments included", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		var batches [][]int64
		mc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, req *datapb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
				assert.True(t, req.GetIncludeUnHealthy())
				batches = append(batches, req.GetSegmentIDs())
				return &datapb.GetSegmentInfoResponse{
					Status: merr.Success(),
					Infos: lo.Map(req.GetSegmentIDs(), func(id int64, _ int) *datapb.SegmentInfo {
						return &datapb.SegmentInfo{ID: id}
					}),
				}, nil
			})
		future := syncutil.NewFuture[types.MixCoordClient]()
		future.Set(mc)
		node := &QueryNode{ctx: context.Background(), mixCoord: future}

		ids := lo.Range(splitTargetSegmentInfoBatch + 1)
		ids64 := lo.Map(ids, func(id int, _ int) int64 { return int64(id) })
		infos, err := node.getSplitTargetSegmentInfos(context.Background(), ids64)
		require.NoError(t, err)
		assert.Len(t, infos, len(ids64))
		assert.Len(t, batches, 2)
	})

	t.Run("a failed call fails the fetch", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return(&datapb.GetSegmentInfoResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady("datacoord", 0, "starting")),
		}, nil)
		future := syncutil.NewFuture[types.MixCoordClient]()
		future.Set(mc)
		node := &QueryNode{ctx: context.Background(), mixCoord: future}
		_, err := node.getSplitTargetSegmentInfos(context.Background(), []int64{1})
		assert.ErrorIs(t, err, merr.ErrServiceNotReady)
	})

	t.Run("a coordinator that never resolves fails the fetch when the context ends", func(t *testing.T) {
		node := &QueryNode{ctx: context.Background(), mixCoord: syncutil.NewFuture[types.MixCoordClient]()}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := node.getSplitTargetSegmentInfos(ctx, []int64{1})
		assert.Error(t, err)
	})
}

// Each step of loading a child's recovery view fails the spawn with context.
func TestLoadSplitChildRecoveryFailures(t *testing.T) {
	info := &datapb.VchannelInfo{ChannelName: "v1", FlushedSegmentIds: []int64{9}}
	params := delegator.SpawnChildParams{CollectionID: 1, TargetVChannel: "v1"}
	node := &QueryNode{ctx: context.Background()}
	boom := errors.New("boom")

	newChild := func() *delegator.MockShardDelegator {
		child := delegator.NewMockShardDelegator(t)
		child.EXPECT().AddExcludedSegments(mock.Anything).Return()
		return child
	}

	t.Run("L0", func(t *testing.T) {
		child := newChild()
		child.EXPECT().LoadL0(mock.Anything, mock.Anything, mock.Anything).Return(boom)
		assert.ErrorIs(t, node.loadSplitChildRecovery(context.Background(), child, params, info), boom)
	})
	t.Run("growing", func(t *testing.T) {
		child := newChild()
		child.EXPECT().LoadL0(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		child.EXPECT().LoadGrowing(mock.Anything, mock.Anything, mock.Anything).Return(boom)
		assert.ErrorIs(t, node.loadSplitChildRecovery(context.Background(), child, params, info), boom)
	})
	t.Run("forward", func(t *testing.T) {
		child := newChild()
		child.EXPECT().LoadL0(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		child.EXPECT().LoadGrowing(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		child.EXPECT().ForwardKnownDeletesToParent(mock.Anything).Return(boom)
		assert.ErrorIs(t, node.loadSplitChildRecovery(context.Background(), child, params, info), boom)
	})
	t.Run("segment infos", func(t *testing.T) {
		child := delegator.NewMockShardDelegator(t)
		withSegments := &datapb.VchannelInfo{ChannelName: "v1", UnflushedSegmentIds: []int64{1}}
		assert.Error(t, node.loadSplitChildRecovery(context.Background(), child, params, withSegments))
	})
}

// A watch whose split recovery has not read the shard states yet refuses reads
// through the new delegator, retriably, and serves them once it has.
func (suite *ServiceSuite) TestWatchRefusesReadsUntilItsSplitRecoveryDescribes() {
	ctx := context.Background()
	release := make(chan struct{})
	mc := mocks.NewMockMixCoordClient(suite.T())
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			<-release
			return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), VirtualChannelNames: []string{suite.vchannel}}, nil
		}).Once()
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	suite.node.mixCoord = future

	// the targets are pending spawns once the recovery ran; keep them failing.
	spawn := mockey.Mock((*QueryNode).SpawnSplitChild).Return(nil, errors.New("not now")).Build()
	defer spawn.UnPatch()
	req := suite.splitWatchRequest(suite.vchannel)
	req.GetInfos()[0].SplitTargetChannels = []string{"by-dev-rootcoord-dml_2_111v3"}
	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	source, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)

	pendingRefusal := func() bool {
		_, err := source.Search(ctx, &querypb.SearchRequest{Req: &internalpb.SearchRequest{}, DmlChannels: []string{suite.vchannel}})
		return errors.Is(err, merr.ErrServiceUnavailable) && strings.Contains(err.Error(), "shard split recovery pending")
	}
	suite.True(pendingRefusal(), "a read was not refused while the split recovery had not described the collection")
	close(release)
	suite.Eventually(func() bool { return !pendingRefusal() }, 5*time.Second, 10*time.Millisecond)
}

// Every channel watch runs a split recovery, and each needs the collection's
// shard states. A restart re-watches every channel at once, so the recoveries
// of one collection share one DescribeCollection instead of each firing its
// own at rootcoord.
func TestConcurrentSplitRecoveriesOfOneCollectionShareOneDescribe(t *testing.T) {
	const watches = 8
	release := make(chan struct{})
	var calls atomic.Int32
	mc := mocks.NewMockMixCoordClient(t)
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *milvuspb.DescribeCollectionRequest, ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			calls.Add(1)
			<-release
			names := make([]string, watches)
			for i := range names {
				names[i] = fmt.Sprintf("v%d", i)
			}
			return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), VirtualChannelNames: names}, nil
		})
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	node := &QueryNode{ctx: context.Background(), mixCoord: future}

	var wg sync.WaitGroup
	for i := 0; i < watches; i++ {
		source := delegator.NewMockShardDelegator(t)
		source.EXPECT().ProcessSplitShard(mock.Anything, []string{"t"}).Return(nil).Once()
		source.EXPECT().FinishSplitRecovery().Return().Once()
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.respawnSplitChildrenOnRecovery(context.Background(), source, 1, fmt.Sprintf("v%d", i), []string{"t"})
		}()
	}
	require.Eventually(t, func() bool { return calls.Load() >= 1 }, 5*time.Second, time.Millisecond)
	// let every other recovery reach the describe before the first one returns.
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()
	assert.EqualValues(t, 1, calls.Load(), "each recovery described the collection on its own")
}

// Only a watched split source needs the recovery. A channel whose recovery info
// names no split target -- every channel of every collection that is not
// mid-split -- neither describes the collection nor refuses a read, even with
// the coordinator down.
func (suite *ServiceSuite) TestWatchOfANonSplitChannelNeitherDescribesNorRefuses() {
	ctx := context.Background()
	mc := mocks.NewMockMixCoordClient(suite.T())
	// no DescribeCollection expectation: the mock fails the test if it is called.
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	suite.node.mixCoord = future

	status, err := suite.node.WatchDmChannels(ctx, suite.splitWatchRequest(suite.vchannel))
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	source, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)
	suite.Never(func() bool {
		_, err := source.Search(ctx, &querypb.SearchRequest{Req: &internalpb.SearchRequest{}, DmlChannels: []string{suite.vchannel}})
		return errors.Is(err, merr.ErrServiceUnavailable)
	}, 300*time.Millisecond, 20*time.Millisecond, "a read through a non-split channel was refused")
}

// A watched split source recovers from its recovery info's target list: the
// named targets become pending spawns, and reads through it are refused until
// their children publish.
func (suite *ServiceSuite) TestWatchOfASplitSourceRecoversItsNamedTargets() {
	ctx := context.Background()
	const target = "by-dev-rootcoord-dml_2_111v3"
	mc := mocks.NewMockMixCoordClient(suite.T())
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		VirtualChannelNames: []string{suite.vchannel, target},
	}, nil).Once()
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	suite.node.mixCoord = future

	var spawned atomic.Value
	spawn := mockey.Mock((*QueryNode).SpawnSplitChild).To(func(_ *QueryNode, _ context.Context, params delegator.SpawnChildParams) (delegator.ShardDelegator, error) {
		spawned.Store(params.TargetVChannel)
		return nil, errors.New("not now")
	}).Build()
	defer spawn.UnPatch()

	req := suite.splitWatchRequest(suite.vchannel)
	req.GetInfos()[0].SplitTargetChannels = []string{target}
	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	source, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)

	suite.Eventually(func() bool { return spawned.Load() == target }, 5*time.Second, 10*time.Millisecond)
	suite.Eventually(func() bool {
		_, err := source.Search(ctx, &querypb.SearchRequest{Req: &internalpb.SearchRequest{}, DmlChannels: []string{suite.vchannel}})
		return errors.Is(err, merr.ErrServiceUnavailable) && strings.Contains(err.Error(), "still being spawned")
	}, 5*time.Second, 10*time.Millisecond)
	suite.Empty(source.SplitChildVChannels())
}
