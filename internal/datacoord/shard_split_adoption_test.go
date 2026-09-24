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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

// fencedDescribe is rootcoord's record of the split collection after the
// write switch applied: v0 fenced, v1 and v2 created with residues {0} and {2}
// at modulus 4, next to an untouched shard v3 owning {1, 3}.
func fencedDescribe() *milvuspb.DescribeCollectionResponse {
	return splitTestDescribe([]string{splitMgrV3, splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV3, schemapb.ShardState_ShardNormal, 1, 3),
		hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
		hashInfo(splitMgrV1, schemapb.ShardState_ShardCreating, 0),
		hashInfo(splitMgrV2, schemapb.ShardState_ShardCreating, 2),
	}, 4)
}

// adoptedDescribe is the same collection once the adoption applied.
func adoptedDescribe() *milvuspb.DescribeCollectionResponse {
	return splitTestDescribe([]string{splitMgrV3, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV3, schemapb.ShardState_ShardNormal, 1, 3),
		hashInfo(splitMgrV1, schemapb.ShardState_ShardNormal, 0),
		hashInfo(splitMgrV2, schemapb.ShardState_ShardNormal, 2),
	}, 4)
}

func adoptionTask() *datapb.SplitShardTask {
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskAdopting)
	task.Fenced = true
	task.Sources[0].SwitchTimeTick = 2000
	task.Targets[0].Buckets = []uint64{0}
	task.Targets[1].Buckets = []uint64{2}
	task.RoutingModulus = 4
	return task
}

// The adoption is ONE commit: its single post-image delists the source AND
// moves both targets to Normal. The routing judge would also take a step-wise
// adoption, and QueryCoord would then pull the source together with Normal
// targets holding no loaded data -- reading the rows twice.
func TestBuildAdoptionPostImageIsOneCommit(t *testing.T) {
	coll := splitCollectionFromDescribe(fencedDescribe(), []int64{10})
	task := adoptionTask()

	updates, err := buildAdoptionPostImage(task, coll)
	require.NoError(t, err)
	assert.Equal(t, task.GetTaskId(), updates.GetSplitTaskId())
	assert.EqualValues(t, 4, updates.GetRoutingModulus(), "an adoption changes no modulus")
	assert.NotContains(t, updates.GetVirtualChannelNames(), splitMgrV0, "the source is delisted")
	assert.Equal(t, map[string]schemapb.ShardState{
		splitMgrV3: schemapb.ShardState_ShardNormal,
		splitMgrV1: schemapb.ShardState_ShardNormal,
		splitMgrV2: schemapb.ShardState_ShardNormal,
	}, postImageStates(updates), "both targets are Normal in the same commit")
	assert.Equal(t, map[string][]uint64{splitMgrV3: {1, 3}, splitMgrV1: {0}, splitMgrV2: {2}}, postImageResidues(updates))
	assert.Equal(t, []string{"by-dev-rootcoord-dml_3", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"}, updates.GetPhysicalChannelNames())

	// The callback's own judgement accepts it as exactly the adoption's delta.
	delta := routing.AdoptionDelta(splitMgrV0, []string{splitMgrV1, splitMgrV2}, true)
	require.NoError(t, routing.JudgeCommit(coll.Collection, updates, delta))

	// It reaches the control channel, every listed vchannel (the source
	// included, whose own replica retires it) and every post-image vchannel.
	assert.Equal(t, []string{splitMgrControl, splitMgrV3, splitMgrV0, splitMgrV1, splitMgrV2},
		adoptionBroadcastVChannels(splitMgrControl, coll, updates))

	// Re-built after it applied, it is the same commit, already applied.
	adopted := splitCollectionFromDescribe(adoptedDescribe(), []int64{10})
	again, err := buildAdoptionPostImage(task, adopted)
	require.NoError(t, err)
	assert.ErrorIs(t, routing.JudgeCommit(adopted.Collection, again, delta), routing.ErrCommitAlreadyApplied)
}

func TestBuildAdoptionPostImageRefusals(t *testing.T) {
	coll := splitCollectionFromDescribe(fencedDescribe(), []int64{10})

	unapplied := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10})
	_, err := buildAdoptionPostImage(adoptionTask(), unapplied)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	noSource := adoptionTask()
	noSource.Sources = nil
	_, err = buildAdoptionPostImage(noSource, coll)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	oneTarget := adoptionTask()
	oneTarget.Targets = oneTarget.Targets[:1]
	_, err = buildAdoptionPostImage(oneTarget, coll)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func adoptingCase(t *testing.T, state datapb.SplitShardTaskState, desc *milvuspb.DescribeCollectionResponse) (*shardSplitManager, *fakeSplitCoordinator) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{coll: splitCollectionFromDescribe(desc, []int64{10})}
	manager, _ := newSplitTestManager(t, coordinator)
	task := adoptionTask()
	task.State = state
	require.NoError(t, manager.store.create(context.Background(), manager.catalog, task))
	return manager, coordinator
}

// The adoption's callback waits for the drain while it holds the collection's
// keys, so the manager never issues it before its own drain predicate holds.
func TestShardSplitAdoptionWaitsForTheDrain(t *testing.T) {
	manager, coordinator := adoptingCase(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, fencedDescribe())
	coordinator.drainReason = "source by-dev-rootcoord-dml_0_1v0 still has a live segment 9"

	manager.advanceTasks()
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, mustTask(t, manager, 100).GetState())
	assert.Empty(t, coordinator.adopted)

	coordinator.drainReason = ""
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, mustTask(t, manager, 100).GetState())
	assert.Empty(t, coordinator.adopted, "moving to Adopting issues nothing yet")

	// Adopting re-asks the drain before every issue.
	coordinator.drainReason = "an import is still in progress on a source"
	manager.advanceTasks()
	assert.Empty(t, coordinator.adopted)

	coordinator.drainReason = ""
	manager.advanceTasks()
	manager.advanceTasks()
	require.Len(t, coordinator.adopted, 1)
	assert.Equal(t, int64(100), coordinator.adopted[0].GetTaskId())

	// Until it applies, every tick issues it again; the broadcast is
	// idempotent by task id.
	coordinator.adoptErr = merr.WrapErrServiceUnavailableMsg("broadcaster busy")
	manager.advanceTasks()
	assert.Len(t, coordinator.adopted, 2)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, mustTask(t, manager, 100).GetState())

	coordinator.adoptErr = merr.WrapErrCollectionNotFound(splitMgrCollection)
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, mustTask(t, manager, 100).GetState())
}

// A drain that stops holding while the task is Adopting -- data that landed
// on the source after the task moved on, or after its adoption was broadcast
// and while that adoption's callback waits for the drain holding the
// collection's keys -- sends the task back to Redistributing, whose rewrite
// takes the late segments. Without it the task, and the keys of a callback
// already out, would wait forever. Once drained again, the adoption is issued
// again (the broadcast is deduplicated by the task id).
func TestShardSplitAdoptingReopensTheRedistributionWhenItsDrainRegresses(t *testing.T) {
	for _, role := range []replicateutil.Role{replicateutil.RolePrimary, replicateutil.RoleSecondary} {
		t.Run(role.String(), func(t *testing.T) {
			manager, coordinator := adoptingCase(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, fencedDescribe())
			manager.replicationRole = func(context.Context) (replicateutil.Role, error) { return role, nil }
			redistributor := &fakeRedistributor{}
			manager.setRedistributor(redistributor)
			coordinator.drainReason = "source by-dev-rootcoord-dml_0_1v0 still has a live segment 9"

			manager.advanceTasks()
			assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, mustTask(t, manager, 100).GetState())
			assert.Empty(t, coordinator.adopted)

			manager.advanceTasks()
			assert.Equal(t, []int64{100}, redistributor.rounds, "the late segments are rewritten")

			coordinator.drainReason = ""
			manager.advanceTasks()
			assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, mustTask(t, manager, 100).GetState())
			manager.advanceTasks()
			if role == replicateutil.RolePrimary {
				assert.Len(t, coordinator.adopted, 1)
			} else {
				assert.Empty(t, coordinator.adopted)
			}
		})
	}
}

func TestShardSplitAdoptionIsNeverIssuedByASecondary(t *testing.T) {
	manager, coordinator := adoptingCase(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, fencedDescribe())
	manager.replicationRole = func(context.Context) (replicateutil.Role, error) { return replicateutil.RoleSecondary, nil }
	manager.advanceTasks()
	assert.Empty(t, coordinator.adopted)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, mustTask(t, manager, 100).GetState())

	// The primary's adoption replicates; once applied here, the secondary
	// finishes like any other cluster.
	coordinator.coll = splitCollectionFromDescribe(adoptedDescribe(), []int64{10})
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, mustTask(t, manager, 100).GetState())
}

// Done is not at delisting: the task stays Adopting while this cluster's
// QueryCoord still serves the source, and while it cannot be asked.
func TestShardSplitIsNotDoneWhileTheSourceIsStillServed(t *testing.T) {
	manager, coordinator := adoptingCase(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, adoptedDescribe())
	coordinator.served = true

	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, task.GetState(),
		"delisted, but the source is still a shard leader")
	assert.Empty(t, coordinator.adopted, "an applied adoption is not issued again")
	assert.True(t, manager.IsVChannelSplitting(splitMgrV0), "the freeze holds until Done")

	coordinator.served = false
	coordinator.servedErr = merr.WrapErrServiceUnavailableMsg("querycoord busy")
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, mustTask(t, manager, 100).GetState())

	coordinator.servedErr = nil
	manager.advanceTasks()
	task = mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, task.GetState())
	assert.Empty(t, task.GetFailReason())
	assert.NotZero(t, task.GetEndTime())
	assert.False(t, manager.IsVChannelSplitting(splitMgrV0))
	assert.False(t, manager.IsVChannelSplitting(splitMgrV1))

	// Records are never removed.
	_, ok := manager.store.get(100)
	assert.True(t, ok)
}

func TestShardSplitAdoptingFinishesOnADroppedCollection(t *testing.T) {
	manager, coordinator := adoptingCase(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, fencedDescribe())
	coordinator.describeErr = merr.WrapErrCollectionNotFound(splitMgrCollection)
	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, task.GetState())
	assert.Equal(t, "collection dropped during adoption", task.GetFailReason())
}

func TestSplitSourceServed(t *testing.T) {
	ctx := context.Background()
	svr := newShardSplitTestServer(t)
	_, err := svr.splitSourceServed(ctx, splitMgrCollection, splitMgrV0)
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)

	mixCoord := mocks.NewMixCoord(t)
	svr.mixCoord = mixCoord
	request := mock.MatchedBy(func(req *querypb.GetShardLeadersRequest) bool {
		return req.GetCollectionID() == splitMgrCollection && req.GetWithUnserviceableShards()
	})

	mixCoord.EXPECT().GetShardLeaders(mock.Anything, request).Return(&querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{{ChannelName: splitMgrV0}, {ChannelName: splitMgrV3}},
	}, nil).Once()
	served, err := svr.splitSourceServed(ctx, splitMgrCollection, splitMgrV0)
	require.NoError(t, err)
	assert.True(t, served)

	mixCoord.EXPECT().GetShardLeaders(mock.Anything, request).Return(&querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{{ChannelName: splitMgrV1}, {ChannelName: splitMgrV2}},
	}, nil).Once()
	served, err = svr.splitSourceServed(ctx, splitMgrCollection, splitMgrV0)
	require.NoError(t, err)
	assert.False(t, served)

	// A collection released or dropped mid-split is served by nobody.
	for _, gone := range []error{merr.WrapErrCollectionNotLoaded(splitMgrCollection), merr.WrapErrCollectionNotFound(splitMgrCollection)} {
		mixCoord.EXPECT().GetShardLeaders(mock.Anything, request).Return(&querypb.GetShardLeadersResponse{Status: merr.Status(gone)}, nil).Once()
		served, err = svr.splitSourceServed(ctx, splitMgrCollection, splitMgrV0)
		require.NoError(t, err)
		assert.False(t, served)
	}

	// Any other failure is not an answer.
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, request).Return(nil, errors.New("querycoord down")).Once()
	_, err = svr.splitSourceServed(ctx, splitMgrCollection, splitMgrV0)
	assert.Error(t, err)
}

// drainedAdoptionServer is a datacoord whose record of task 100 is fenced and
// whose source has drained: no segment, checkpoint past T_switch, no import.
func drainedAdoptionServer(t *testing.T, descs ...*milvuspb.DescribeCollectionResponse) *Server {
	svr := splitSwitchServer(t, adoptionTask(), descs...)
	svr.importMeta = idleImportMeta(t)
	require.NoError(t, svr.meta.UpdateChannelCheckpoints(context.Background(),
		[]*msgpb.MsgPosition{splitTestPosition(splitMgrV0, 2000)}))
	return svr
}

func TestIssueShardSplitAdoptionBroadcastsOneCommit(t *testing.T) {
	enableShardSplit(t)
	svr := drainedAdoptionServer(t, fencedDescribe())
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	var broadcasted []message.BroadcastMutableMessage
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			broadcasted = append(broadcasted, msg)
			return &types.BroadcastAppendResult{}, nil
		}).Once()
	bapi.EXPECT().Close().Once()
	mocker := mockSplitBroadcast(t, bapi)
	defer mocker.UnPatch()

	require.NoError(t, svr.issueShardSplitAdoption(context.Background(), adoptionTask(), splitMgrControl))
	require.Len(t, broadcasted, 1)
	typed := message.MustAsSpecializedBroadcastMessage[*message.AlterCollectionMessageHeader, *message.AlterCollectionMessageBody](broadcasted[0])
	assert.Equal(t, []string{message.FieldMaskCollectionShardSplitRouting}, typed.Header().GetUpdateMask().GetPaths(),
		"the routing mask travels alone")
	assert.Equal(t, splitMgrCollection, typed.Header().GetCollectionId())
	updates := typed.MustBody().GetUpdates()
	assert.Equal(t, int64(100), updates.GetSplitTaskId())
	assert.NotContains(t, updates.GetVirtualChannelNames(), splitMgrV0)
	for _, state := range postImageStates(updates) {
		assert.Equal(t, schemapb.ShardState_ShardNormal, state)
	}
	assert.ElementsMatch(t, []string{splitMgrControl, splitMgrV3, splitMgrV0, splitMgrV1, splitMgrV2},
		broadcasted[0].BroadcastHeader().VChannels)
}

func TestIssueShardSplitAdoptionRefusals(t *testing.T) {
	enableShardSplit(t)
	ctx := context.Background()
	noBroadcast := func(t *testing.T) {
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Close().Maybe()
		mocker := mockSplitBroadcast(t, bapi)
		t.Cleanup(func() { mocker.UnPatch() })
	}

	t.Run("a re-issue after the apply is success, and sends nothing", func(t *testing.T) {
		svr := drainedAdoptionServer(t, adoptedDescribe())
		noBroadcast(t)
		assert.NoError(t, svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl))
	})

	t.Run("not drained under the keys, nothing is sent", func(t *testing.T) {
		svr := drainedAdoptionServer(t, fencedDescribe())
		svr.meta.segments.SetSegment(9001, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 9001, CollectionID: splitMgrCollection, InsertChannel: splitMgrV0, State: commonpb.SegmentState_Flushed,
		}})
		noBroadcast(t)
		err := svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl)
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
		assert.ErrorContains(t, err, "live segment 9001")
	})

	t.Run("a task this cluster has not fenced is not adopted", func(t *testing.T) {
		svr := splitSwitchServer(t, fencedTask(datapb.SplitShardTaskState_SplitShardTaskFencing), fencedDescribe())
		noBroadcast(t)
		assert.ErrorIs(t, svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl), merr.ErrServiceInternal)
	})

	t.Run("a collection that has not applied the split is refused", func(t *testing.T) {
		svr := drainedAdoptionServer(t, splitTestDescribe([]string{splitMgrV0}, nil, 0))
		noBroadcast(t)
		assert.ErrorIs(t, svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl), merr.ErrServiceInternal)
	})

	t.Run("a post-image the judge refuses is not sent", func(t *testing.T) {
		// This cluster's meta has not fenced the source: the adoption would
		// retire a shard that is not Splitting here.
		desc := fencedDescribe()
		desc.ShardInfos[1] = hashInfo(splitMgrV0, schemapb.ShardState_ShardNormal)
		svr := drainedAdoptionServer(t, desc)
		noBroadcast(t)
		err := svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "judge the adoption")
	})

	t.Run("a failed broadcast is returned for the next tick", func(t *testing.T) {
		svr := drainedAdoptionServer(t, fencedDescribe())
		bapi := mock_broadcaster.NewMockBroadcastAPI(t)
		bapi.EXPECT().Close().Once()
		bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceUnavailableMsg("wal busy")).Once()
		mocker := mockSplitBroadcast(t, bapi)
		defer mocker.UnPatch()
		assert.ErrorIs(t, svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl), merr.ErrServiceUnavailable)
	})

	t.Run("the collection cannot be read", func(t *testing.T) {
		svr := newShardSplitTestServer(t)
		b := broker.NewMockBroker(t)
		b.EXPECT().DescribeCollectionInternal(mock.Anything, splitMgrCollection).Return(nil, merr.WrapErrCollectionNotFound(splitMgrCollection))
		svr.broker = b
		assert.ErrorIs(t, svr.issueShardSplitAdoption(ctx, adoptionTask(), splitMgrControl), merr.ErrCollectionNotFound)
	})
}

// wedgedAdoptionCoordinator's adoption for one task never returns, whatever
// its context says: the resource-key lock the issue takes has no context, and
// waits as long as another broadcast of the collection holds the keys (an
// adoption whose callback waits for a drain, a SplitShard callback retrying).
type wedgedAdoptionCoordinator struct {
	*fakeSplitCoordinator
	wedged  int64
	release chan struct{}
	calls   *atomic.Int32
}

func (w *wedgedAdoptionCoordinator) issueShardSplitAdoption(ctx context.Context, task *datapb.SplitShardTask, controlChannel string) error {
	if task.GetTaskId() == w.wedged {
		w.calls.Inc()
		<-w.release
	}
	return w.fakeSplitCoordinator.issueShardSplitAdoption(ctx, task, controlChannel)
}

// One task whose broadcast issue is wedged on the collection's resource keys
// stops only itself: the manager's loop keeps advancing every other task,
// issues nothing more for the wedged one while its issue is in flight, and
// Stop returns.
func TestShardSplitAWedgedIssueStopsOnlyItsOwnTask(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitTaskInterval.Key, "0.01")
	defer params.Reset(params.DataCoordCfg.ShardSplitTaskInterval.Key)

	manager, fake := adoptingCase(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, fencedDescribe())
	coordinator := &wedgedAdoptionCoordinator{fakeSplitCoordinator: fake, wedged: 100, release: make(chan struct{}), calls: atomic.NewInt32(0)}
	defer close(coordinator.release)
	manager.coordinator = coordinator
	manager.spawn = func(issue func()) { go issue() }
	require.NoError(t, manager.store.create(context.Background(), manager.catalog, &datapb.SplitShardTask{
		TaskId: 101, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced:  true,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV3, SwitchTimeTick: 10}},
		Targets: []*datapb.SplitShardTaskTarget{{Vchannel: splitMgrV4}, {Vchannel: "by-dev-rootcoord-dml_5_1v5"}},
	}))

	ticked := make(chan struct{})
	go func() {
		manager.tick(time.Now())
		manager.tick(time.Now())
		close(ticked)
	}()
	select {
	case <-ticked:
	case <-time.After(10 * time.Second):
		t.Fatal("the manager's loop is blocked behind one task's broadcast issue")
	}
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, mustTask(t, manager, 101).GetState(),
		"another task kept advancing")
	assert.Eventually(t, func() bool { return coordinator.calls.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.EqualValues(t, 1, coordinator.calls.Load(), "no second issue while the first is in flight")

	manager.Start()
	stopped := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		manager.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop waits behind a wedged broadcast issue")
	}
}
