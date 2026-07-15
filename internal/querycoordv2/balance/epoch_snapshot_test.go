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

package balance

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func receiveBalanceTestSignal[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		var zero T
		return zero
	}
}

func closeBalanceTestChannelOnCleanup(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	closeChannel := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(closeChannel)
	return closeChannel
}

const (
	testSnapshotRG      = "rg-snapshot"
	testUnrelatedRG     = "rg-unrelated"
	testEligibleReplica = int64(11)
	testOtherReplica    = int64(12)
)

type pendingInspectorStub struct {
	mu       sync.Mutex
	snapshot task.PendingBalanceSnapshot
}

func (s *pendingInspectorStub) GetPendingBalanceTasks() task.PendingBalanceSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePendingSnapshot(s.snapshot)
}

func (s *pendingInspectorStub) set(snapshot task.PendingBalanceSnapshot) {
	s.mu.Lock()
	s.snapshot = clonePendingSnapshot(snapshot)
	s.mu.Unlock()
}

func clonePendingSnapshot(snapshot task.PendingBalanceSnapshot) task.PendingBalanceSnapshot {
	cloned := task.PendingBalanceSnapshot{
		Revision: snapshot.Revision,
		Tasks:    make([]task.PendingBalanceTaskSnapshot, len(snapshot.Tasks)),
	}
	if snapshot.ResourceGroupRevisions != nil {
		cloned.ResourceGroupRevisions = make(map[string]uint64, len(snapshot.ResourceGroupRevisions))
		for resourceGroup, revision := range snapshot.ResourceGroupRevisions {
			cloned.ResourceGroupRevisions[resourceGroup] = revision
		}
	}
	if snapshot.EpochRevisions != nil {
		cloned.EpochRevisions = make(map[task.BalanceEpochMeta]uint64, len(snapshot.EpochRevisions))
		for epoch, revision := range snapshot.EpochRevisions {
			cloned.EpochRevisions[epoch] = revision
		}
	}
	cloned.UnscopedRevision = snapshot.UnscopedRevision
	for i, pending := range snapshot.Tasks {
		cloned.Tasks[i] = pending
		cloned.Tasks[i].Actions = append([]task.PendingBalanceActionSnapshot(nil), pending.Actions...)
	}
	return cloned
}

type snapshotTargetState struct {
	mu              sync.Mutex
	currentVersion  map[int64]int64
	nextVersion     map[int64]int64
	currentSegments map[int64]map[int64]*datapb.SegmentInfo
	nextSegments    map[int64]map[int64]*datapb.SegmentInfo
	currentChannels map[int64]map[string]*meta.DmChannel
	nextChannels    map[int64]map[string]*meta.DmChannel
	segmentsHook    func(collectionID int64, scope int32)
}

func (s *snapshotTargetState) version(collectionID int64, scope int32) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope == meta.CurrentTarget {
		return s.currentVersion[collectionID]
	}
	return s.nextVersion[collectionID]
}

func (s *snapshotTargetState) segments(collectionID int64, scope int32) map[int64]*datapb.SegmentInfo {
	s.mu.Lock()
	var segments map[int64]*datapb.SegmentInfo
	if scope == meta.CurrentTarget {
		segments = s.currentSegments[collectionID]
	} else {
		segments = s.nextSegments[collectionID]
	}
	hook := s.segmentsHook
	s.mu.Unlock()
	if hook != nil {
		hook(collectionID, scope)
	}
	return segments
}

func (s *snapshotTargetState) channels(collectionID int64, scope int32) map[string]*meta.DmChannel {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope == meta.CurrentTarget {
		return s.currentChannels[collectionID]
	}
	return s.nextChannels[collectionID]
}

type placementSnapshotFixture struct {
	ctx           context.Context
	meta          *meta.Meta
	nodes         *session.NodeManager
	dist          *meta.DistributionManager
	target        *meta.MockTargetManager
	targetState   *snapshotTargetState
	inspector     *pendingInspectorStub
	builder       *PlacementSnapshotBuilder
	node1Segments []*meta.Segment
	node1Channels []*meta.DmChannel
}

func newPlacementSnapshotFixture(t *testing.T) *placementSnapshotFixture {
	t.Helper()
	paramtable.Init()
	ctx := context.Background()
	nodes := session.NewNodeManager()
	for _, node := range []struct {
		id       int64
		capacity float64
	}{
		{id: 1, capacity: 1024},
		{id: 2, capacity: 2048},
		{id: 3, capacity: 4096},
		{id: 4, capacity: 8192},
	} {
		info := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: node.id})
		info.UpdateStats(session.WithMemCapacity(node.capacity))
		nodes.Add(info)
	}

	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.On("GetResourceGroups", mock.Anything).Return([]*querypb.ResourceGroup{
		{
			Name:   testSnapshotRG,
			Nodes:  []int64{1, 3},
			Config: &rgpb.ResourceGroupConfig{Requests: &rgpb.ResourceGroupLimit{NodeNum: 2}, Limits: &rgpb.ResourceGroupLimit{NodeNum: 3}},
		},
		{
			Name:   testUnrelatedRG,
			Nodes:  []int64{4},
			Config: &rgpb.ResourceGroupConfig{Requests: &rgpb.ResourceGroupLimit{NodeNum: 1}, Limits: &rgpb.ResourceGroupLimit{NodeNum: 1}},
		},
	}, nil)
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("RemoveResourceGroup", mock.Anything, mock.Anything).Return(nil).Maybe()

	nextID := int64(100)
	metadata := meta.NewMeta(func() (int64, error) {
		nextID++
		return nextID, nil
	}, catalog, nodes)
	require.NoError(t, metadata.ResourceManager.Recover(ctx))
	require.NoError(t, metadata.ReplicaManager.Put(ctx,
		meta.NewReplica(&querypb.Replica{
			ID:               testEligibleReplica,
			CollectionID:     100,
			ResourceGroup:    testSnapshotRG,
			Nodes:            []int64{1, 3},
			RoNodes:          []int64{2},
			ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{"channel-a": {RwNodes: []int64{1, 3}}, "stale-only": {RwNodes: []int64{3}}},
		}),
		meta.NewReplica(&querypb.Replica{
			ID:               testOtherReplica,
			CollectionID:     200,
			ResourceGroup:    testSnapshotRG,
			Nodes:            []int64{1, 3},
			ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{"channel-b": {RwNodes: []int64{1, 3}}},
		}),
		meta.NewReplica(&querypb.Replica{
			ID:               13,
			CollectionID:     300,
			ResourceGroup:    testUnrelatedRG,
			Nodes:            []int64{4},
			ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{"channel-c": {RwNodes: []int64{4}}},
		}),
	))

	distribution := meta.NewDistributionManager(nodes)
	node1Segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 101, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 100}, Version: 11},
		{SegmentInfo: &datapb.SegmentInfo{ID: 201, CollectionID: 200, PartitionID: 20, InsertChannel: "channel-b", NumOfRows: 200}, Version: 21},
	}
	node1Channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-a", UnflushedSegmentIds: []int64{1001}},
			Version:      31,
			View: &meta.LeaderView{
				ID:              1,
				CollectionID:    100,
				Channel:         "channel-a",
				Version:         31,
				TargetVersion:   1000,
				Status:          &querypb.LeaderViewStatus{Serviceable: true},
				GrowingSegments: map[int64]*meta.Segment{1001: {SegmentInfo: &datapb.SegmentInfo{ID: 1001, CollectionID: 100, InsertChannel: "channel-a", NumOfRows: 10}, Node: 1}},
			},
		},
		{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 200, ChannelName: "channel-b", UnflushedSegmentIds: []int64{2001}},
			Version:      32,
			View: &meta.LeaderView{
				ID:              1,
				CollectionID:    200,
				Channel:         "channel-b",
				Version:         32,
				TargetVersion:   2000,
				Status:          &querypb.LeaderViewStatus{Serviceable: true},
				GrowingSegments: map[int64]*meta.Segment{2001: {SegmentInfo: &datapb.SegmentInfo{ID: 2001, CollectionID: 200, InsertChannel: "channel-b", NumOfRows: 20}, Node: 1}},
			},
		},
	}
	distribution.PublishNodeDistribution(1, node1Segments, node1Channels)
	distribution.PublishNodeDistribution(2,
		[]*meta.Segment{{SegmentInfo: &datapb.SegmentInfo{ID: 102, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 50}, Version: 12}},
		[]*meta.DmChannel{{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-a"},
			Version:      30,
			View:         &meta.LeaderView{ID: 2, CollectionID: 100, Channel: "channel-a", Version: 30, TargetVersion: 1000, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		}},
	)
	distribution.PublishNodeDistribution(4,
		[]*meta.Segment{{SegmentInfo: &datapb.SegmentInfo{ID: 301, CollectionID: 300, PartitionID: 30, InsertChannel: "channel-c", NumOfRows: 300}, Version: 41}},
		[]*meta.DmChannel{{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 300, ChannelName: "channel-c"},
			Version:      41,
			View:         &meta.LeaderView{ID: 4, CollectionID: 300, Channel: "channel-c", Version: 41, TargetVersion: 3000, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		}},
	)

	targetState := &snapshotTargetState{
		currentVersion: map[int64]int64{100: 1000, 200: 2000, 300: 3000},
		nextVersion:    map[int64]int64{100: 1001, 200: 2001, 300: 3001},
		currentSegments: map[int64]map[int64]*datapb.SegmentInfo{
			100: {101: {ID: 101, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 100}},
			200: {201: {ID: 201, CollectionID: 200, PartitionID: 20, InsertChannel: "channel-b", NumOfRows: 200}},
			300: {301: {ID: 301, CollectionID: 300, PartitionID: 30, InsertChannel: "channel-c", NumOfRows: 300}},
		},
		nextSegments: map[int64]map[int64]*datapb.SegmentInfo{
			100: {101: {ID: 101, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 100}, 103: {ID: 103, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 75}},
			200: {201: {ID: 201, CollectionID: 200, PartitionID: 20, InsertChannel: "channel-b", NumOfRows: 200}},
			300: {301: {ID: 301, CollectionID: 300, PartitionID: 30, InsertChannel: "channel-c", NumOfRows: 300}},
		},
		currentChannels: map[int64]map[string]*meta.DmChannel{
			100: {"channel-a": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-a", UnflushedSegmentIds: []int64{1001}}}},
			200: {"channel-b": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 200, ChannelName: "channel-b", UnflushedSegmentIds: []int64{2001}}}},
			300: {"channel-c": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 300, ChannelName: "channel-c"}}},
		},
		nextChannels: map[int64]map[string]*meta.DmChannel{
			100: {"channel-a": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-a", UnflushedSegmentIds: []int64{1001, 1002}}}},
			200: {"channel-b": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 200, ChannelName: "channel-b", UnflushedSegmentIds: []int64{2001}}}},
			300: {"channel-c": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 300, ChannelName: "channel-c"}}},
		},
	}
	target := meta.NewMockTargetManager(t)
	target.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, collectionID int64, scope int32) int64 {
			return targetState.version(collectionID, scope)
		}).Maybe()
	target.EXPECT().GetSealedSegmentsByCollection(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, collectionID int64, scope int32) map[int64]*datapb.SegmentInfo {
			return targetState.segments(collectionID, scope)
		}).Maybe()
	target.EXPECT().GetDmChannelsByCollection(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, collectionID int64, scope int32) map[string]*meta.DmChannel {
			return targetState.channels(collectionID, scope)
		}).Maybe()

	inspector := &pendingInspectorStub{}
	builder := NewPlacementSnapshotBuilder(metadata, distribution, target, nodes, inspector)
	return &placementSnapshotFixture{
		ctx:           ctx,
		meta:          metadata,
		nodes:         nodes,
		dist:          distribution,
		target:        target,
		targetState:   targetState,
		inspector:     inspector,
		builder:       builder,
		node1Segments: node1Segments,
		node1Channels: node1Channels,
	}
}

func buildSnapshot(t *testing.T, fixture *placementSnapshotFixture, carryOver ...task.PendingBalanceTaskSnapshot) *PlacementSnapshot {
	t.Helper()
	snapshot, err := fixture.builder.Build(fixture.ctx, testSnapshotRG, []int64{testEligibleReplica}, carryOver)
	require.NoError(t, err)
	return snapshot
}

func TestPlacementSnapshotIncludesAllRGWorkload(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	snapshot := buildSnapshot(t, fixture)

	require.Contains(t, snapshot.Replicas, testEligibleReplica)
	require.Contains(t, snapshot.Replicas, testOtherReplica)
	require.Contains(t, snapshot.CollectionTargets, int64(100))
	require.Contains(t, snapshot.CollectionTargets, int64(200))
	require.Contains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 101, Scope: querypb.DataScope_Historical})
	require.Contains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: testOtherReplica, SegmentID: 201, Scope: querypb.DataScope_Historical})
	require.Contains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 1001, Scope: querypb.DataScope_Streaming})
	require.Contains(t, snapshot.Channels, ChannelObjectKey{ReplicaID: testOtherReplica, Channel: "channel-b"})
	require.Contains(t, snapshot.EligibleReplicas, testEligibleReplica)
	require.NotContains(t, snapshot.EligibleReplicas, testOtherReplica)
}

func TestPlacementSnapshotIncludesOutgoingRONode(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	snapshot := buildSnapshot(t, fixture)

	require.Contains(t, snapshot.Nodes, int64(2))
	require.True(t, snapshot.Nodes[1].Eligible)
	require.False(t, snapshot.Nodes[2].Eligible)
	placements := snapshot.Segments[SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 102, Scope: querypb.DataScope_Historical}]
	require.Len(t, placements, 1)
	require.Equal(t, int64(2), placements[0].NodeID)
}

func TestPlacementSnapshotDoesNotDuplicateAcrossSameCollectionReplicas(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	require.NoError(t, fixture.meta.ReplicaManager.Put(fixture.ctx, meta.NewReplica(&querypb.Replica{
		ID:            14,
		CollectionID:  100,
		ResourceGroup: testSnapshotRG,
		Nodes:         []int64{3},
	})))

	snapshot := buildSnapshot(t, fixture)
	require.Contains(t, snapshot.Replicas, int64(14))
	require.NotContains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: 14, SegmentID: 101, Scope: querypb.DataScope_Historical})
	require.NotContains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: 14, SegmentID: 1001, Scope: querypb.DataScope_Streaming})
	require.NotContains(t, snapshot.Channels, ChannelObjectKey{ReplicaID: 14, Channel: "channel-a"})
}

func TestPlacementSnapshotExcludesUnrelatedRG(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	snapshot := buildSnapshot(t, fixture)

	require.NotContains(t, snapshot.Nodes, int64(4))
	require.NotContains(t, snapshot.Replicas, int64(13))
	require.NotContains(t, snapshot.CollectionTargets, int64(300))
	require.NotContains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: 13, SegmentID: 301, Scope: querypb.DataScope_Historical})
}

func TestPlacementSnapshotCopiesManagerOwnedData(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	snapshot := buildSnapshot(t, fixture)
	before, err := json.Marshal(snapshot)
	require.NoError(t, err)

	fixture.node1Segments[0].SegmentInfo.NumOfRows = 999
	fixture.node1Channels[0].View.GrowingSegments[1001].SegmentInfo.NumOfRows = 999
	fixture.targetState.currentSegments[100][101].NumOfRows = 999
	fixture.targetState.currentChannels[100]["channel-a"].VchannelInfo.UnflushedSegmentIds[0] = 999
	fixture.nodes.Get(1).UpdateStats(session.WithMemCapacity(999))

	after, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestPlacementSnapshotRetriesWhenTargetOrReplicaDigestChanges(t *testing.T) {
	t.Run("target", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				fixture.targetState.mu.Lock()
				fixture.targetState.currentVersion[100] = 1002
				fixture.targetState.mu.Unlock()
			}
		}

		snapshot := buildSnapshot(t, fixture)
		require.GreaterOrEqual(t, attempts, 2)
		require.Equal(t, int64(1002), snapshot.Token.CurrentTargetVersion[100])
	})

	t.Run("replica", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				require.NoError(t, fixture.meta.ReplicaManager.Put(fixture.ctx, meta.NewReplica(&querypb.Replica{
					ID:               testEligibleReplica,
					CollectionID:     100,
					ResourceGroup:    testSnapshotRG,
					Nodes:            []int64{1},
					RoNodes:          []int64{2, 3},
					ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{"channel-a": {RwNodes: []int64{1}}},
				})))
			}
		}

		snapshot := buildSnapshot(t, fixture)
		require.GreaterOrEqual(t, attempts, 2)
		require.Equal(t, []int64{1}, snapshot.Replicas[testEligibleReplica].RWNodes)
	})
}

func TestPlacementSnapshotStopsAfterThreeUnstableAttempts(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	attempts := make([]int, 0, 3)
	fixture.builder.buildHook = func(attempt int) {
		attempts = append(attempts, attempt)
		fixture.targetState.mu.Lock()
		fixture.targetState.currentVersion[100]++
		fixture.targetState.mu.Unlock()
	}

	snapshot, err := fixture.builder.Build(fixture.ctx, testSnapshotRG, []int64{testEligibleReplica}, nil)
	require.Nil(t, snapshot)
	require.EqualError(t, err, "placement snapshot changed during all capture attempts")
	require.Equal(t, []int{0, 1, 2}, attempts)
}

func TestPlacementSnapshotRetryObserver(t *testing.T) {
	t.Run("notifies once before a successful retry", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		retries := make([]string, 0, 1)
		fixture.builder = NewPlacementSnapshotBuilder(
			fixture.meta,
			fixture.dist,
			fixture.target,
			fixture.nodes,
			fixture.inspector,
			WithSnapshotRetryObserver(func(resourceGroup string) {
				retries = append(retries, resourceGroup)
			}),
		)

		mutated := false
		fixture.targetState.mu.Lock()
		fixture.targetState.segmentsHook = func(collectionID int64, scope int32) {
			if collectionID != 100 || scope != meta.CurrentTarget || mutated {
				return
			}
			mutated = true
			fixture.targetState.mu.Lock()
			fixture.targetState.currentVersion[collectionID]++
			fixture.targetState.mu.Unlock()
		}
		fixture.targetState.mu.Unlock()

		snapshot, err := fixture.builder.Build(fixture.ctx, testSnapshotRG, []int64{testEligibleReplica}, nil)
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, []string{testSnapshotRG}, retries)
	})

	t.Run("notifies twice before third-attempt exhaustion", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		retries := make([]string, 0, 2)
		fixture.builder = NewPlacementSnapshotBuilder(
			fixture.meta,
			fixture.dist,
			fixture.target,
			fixture.nodes,
			fixture.inspector,
			WithSnapshotRetryObserver(func(resourceGroup string) {
				retries = append(retries, resourceGroup)
			}),
		)

		fixture.targetState.mu.Lock()
		fixture.targetState.segmentsHook = func(collectionID int64, scope int32) {
			if collectionID != 100 || scope != meta.CurrentTarget {
				return
			}
			fixture.targetState.mu.Lock()
			fixture.targetState.currentVersion[collectionID]++
			fixture.targetState.mu.Unlock()
		}
		fixture.targetState.mu.Unlock()

		snapshot, err := fixture.builder.Build(fixture.ctx, testSnapshotRG, []int64{testEligibleReplica}, nil)
		require.Nil(t, snapshot)
		require.EqualError(t, err, "placement snapshot changed during all capture attempts")
		require.Equal(t, []string{testSnapshotRG, testSnapshotRG}, retries)
	})
}

func TestPlacementSnapshotRetriesForNilReplicaUnscopedMutations(t *testing.T) {
	pendingTask := task.PendingBalanceTaskSnapshot{
		TaskID:        71,
		CollectionID:  100,
		ReplicaID:     meta.NilReplica.GetID(),
		ResourceGroup: "",
		Actions: []task.PendingBalanceActionSnapshot{{
			NodeID: 2, Type: task.ActionTypeReduce, SegmentID: 102,
			Shard: "channel-a", Scope: querypb.DataScope_Historical, Workload: -50,
		}},
	}

	t.Run("removal retries and invalidates old token", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               1,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0},
			UnscopedRevision:       1,
			Tasks:                  []task.PendingBalanceTaskSnapshot{pendingTask},
		})
		oldSnapshot := buildSnapshot(t, fixture)
		require.Len(t, oldSnapshot.PendingWork.Tasks, 1)

		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				fixture.inspector.set(task.PendingBalanceSnapshot{
					Revision:               2,
					ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0},
					UnscopedRevision:       2,
				})
			}
		}

		newSnapshot := buildSnapshot(t, fixture)
		require.Equal(t, 2, attempts)
		require.Empty(t, newSnapshot.PendingWork.Tasks)
		require.Equal(t, task.BalanceAdmissionStaleEpoch, fixture.builder.Validate(AdmissionToken{
			Snapshot: oldSnapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica,
		}))
	})

	t.Run("add retries and cannot omit committed task", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               0,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0},
			UnscopedRevision:       0,
		})
		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				fixture.inspector.set(task.PendingBalanceSnapshot{
					Revision:               1,
					ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0},
					UnscopedRevision:       1,
					Tasks:                  []task.PendingBalanceTaskSnapshot{pendingTask},
				})
			}
		}

		snapshot := buildSnapshot(t, fixture)
		require.Equal(t, 2, attempts)
		require.Len(t, snapshot.PendingWork.Tasks, 1)
		require.Equal(t, pendingTask.TaskID, snapshot.PendingWork.Tasks[0].TaskID)
	})
}

func TestPlacementSnapshotRetriesOnlyForRelevantRGPlacementChanges(t *testing.T) {
	t.Run("unrelated resource group", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				fixture.dist.PublishNodeDistribution(4,
					[]*meta.Segment{{SegmentInfo: &datapb.SegmentInfo{ID: 302, CollectionID: 300, PartitionID: 30, InsertChannel: "channel-c", NumOfRows: 301}, Version: 42}},
					[]*meta.DmChannel{{
						VchannelInfo: &datapb.VchannelInfo{CollectionID: 300, ChannelName: "channel-c"},
						Version:      42,
						View:         &meta.LeaderView{ID: 4, CollectionID: 300, Channel: "channel-c", Version: 42, TargetVersion: 3000, Status: &querypb.LeaderViewStatus{Serviceable: true}},
					}},
				)
			}
		}

		buildSnapshot(t, fixture)
		require.Equal(t, 1, attempts)
	})

	t.Run("same resource group", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				segments := append([]*meta.Segment(nil), fixture.node1Segments...)
				segments = append(segments, &meta.Segment{
					SegmentInfo: &datapb.SegmentInfo{ID: 202, CollectionID: 200, PartitionID: 20, InsertChannel: "channel-b", NumOfRows: 25},
					Version:     22,
				})
				fixture.dist.PublishNodeDistribution(1, segments, fixture.node1Channels)
			}
		}

		snapshot := buildSnapshot(t, fixture)
		require.GreaterOrEqual(t, attempts, 2)
		require.Contains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: testOtherReplica, SegmentID: 202, Scope: querypb.DataScope_Historical})
	})
}

func TestPlacementSnapshotTargetScopeCaptureIsGenerationConsistent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scope int32
	}{
		{name: "current", scope: meta.CurrentTarget},
		{name: "next", scope: meta.NextTarget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPlacementSnapshotFixture(t)
			oldSegmentsRead := make(chan struct{})
			releaseOldSegments := make(chan struct{})
			releaseTargetRead := closeBalanceTestChannelOnCleanup(t, releaseOldSegments)
			var once sync.Once
			fixture.targetState.mu.Lock()
			fixture.targetState.segmentsHook = func(collectionID int64, scope int32) {
				if collectionID == 100 && scope == tc.scope {
					once.Do(func() {
						close(oldSegmentsRead)
						<-releaseOldSegments
					})
				}
			}
			fixture.targetState.mu.Unlock()

			type result struct {
				snapshot *PlacementSnapshot
				err      error
			}
			resultCh := make(chan result, 1)
			go func() {
				snapshot, err := fixture.builder.Build(fixture.ctx, testSnapshotRG, []int64{testEligibleReplica}, nil)
				resultCh <- result{snapshot: snapshot, err: err}
			}()

			receiveBalanceTestSignal(t, oldSegmentsRead, "old target segment read")
			fixture.targetState.mu.Lock()
			newVersion := int64(4000)
			newSegments := map[int64]*datapb.SegmentInfo{
				401: {ID: 401, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-new", NumOfRows: 401},
			}
			newChannels := map[string]*meta.DmChannel{
				"channel-new": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "channel-new", UnflushedSegmentIds: []int64{4001}}},
			}
			if tc.scope == meta.CurrentTarget {
				fixture.targetState.currentVersion[100] = newVersion
				fixture.targetState.currentSegments[100] = newSegments
				fixture.targetState.currentChannels[100] = newChannels
			} else {
				fixture.targetState.nextVersion[100] = newVersion
				fixture.targetState.nextSegments[100] = newSegments
				fixture.targetState.nextChannels[100] = newChannels
			}
			fixture.targetState.mu.Unlock()
			releaseTargetRead()

			capturedResult := receiveBalanceTestSignal(t, resultCh, "generation-consistent snapshot result")
			require.NoError(t, capturedResult.err)
			var captured TargetScopeSnapshot
			if tc.scope == meta.CurrentTarget {
				captured = capturedResult.snapshot.CollectionTargets[100].Current
			} else {
				captured = capturedResult.snapshot.CollectionTargets[100].Next
			}
			require.Equal(t, newVersion, captured.Version)
			require.Equal(t, map[int64]TargetSegmentSnapshot{
				401: {ID: 401, CollectionID: 100, PartitionID: 10, Channel: "channel-new", RowCount: 401},
			}, captured.Segments)
			require.Equal(t, map[string]TargetChannelSnapshot{
				"channel-new": {CollectionID: 100, Channel: "channel-new", GrowingSegmentIDs: []int64{4001}},
			}, captured.Channels)
		})
	}
}

func TestPlacementSnapshotCapturesCurrentAndNextTargetVersions(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	snapshot := buildSnapshot(t, fixture)

	require.Equal(t, map[int64]int64{100: 1000, 200: 2000}, snapshot.Token.CurrentTargetVersion)
	require.Equal(t, map[int64]int64{100: 1001, 200: 2001}, snapshot.Token.NextTargetVersion)
	require.Equal(t, int64(1000), snapshot.CollectionTargets[100].Current.Version)
	require.Equal(t, int64(1001), snapshot.CollectionTargets[100].Next.Version)
}

func TestPlacementSnapshotProjectsPendingActionsOnce(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	pending := task.PendingBalanceSnapshot{
		Revision: 7,
		Tasks: []task.PendingBalanceTaskSnapshot{
			{
				TaskID: 1, CollectionID: 100, ReplicaID: testEligibleReplica, ResourceGroup: testSnapshotRG,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 1, Type: task.ActionTypeGrow, SegmentID: 101, Shard: "channel-a", Scope: querypb.DataScope_Historical, Workload: 100}},
			},
			{
				TaskID: 2, CollectionID: 100, ReplicaID: testEligibleReplica, ResourceGroup: testSnapshotRG,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeGrow, SegmentID: 999, Shard: "channel-a", Scope: querypb.DataScope_Historical, Workload: 75}},
			},
			{
				TaskID: 3, CollectionID: 100, ReplicaID: testEligibleReplica, ResourceGroup: testSnapshotRG,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 1, Type: task.ActionTypeGrow, Channel: "channel-a", Shard: "channel-a", Workload: 1}},
			},
		},
	}
	fixture.inspector.set(pending)

	snapshot := buildSnapshot(t, fixture, pending.Tasks[0], pending.Tasks[1])
	require.Equal(t, uint64(7), snapshot.PendingWork.Revision)
	require.Len(t, snapshot.PendingWork.Tasks, 3)
	require.Zero(t, snapshot.PendingWork.SegmentWorkloadByNode[1])
	require.Equal(t, 75, snapshot.PendingWork.SegmentWorkloadByNode[3])
	require.Zero(t, snapshot.PendingWork.ChannelWorkloadByNode[1])
}

func TestPlacementSnapshotPendingInvalidationIsRGScopedAndWaveAware(t *testing.T) {
	t.Run("unrelated resource group does not retry or invalidate", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               1,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0},
		})
		attempts := 0
		fixture.builder.buildHook = func(attempt int) {
			attempts++
			if attempt == 0 {
				fixture.inspector.set(task.PendingBalanceSnapshot{
					Revision:               2,
					ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0, testUnrelatedRG: 1},
					Tasks: []task.PendingBalanceTaskSnapshot{{
						TaskID: 91, CollectionID: 300, ReplicaID: 13, ResourceGroup: testUnrelatedRG,
						Actions: []task.PendingBalanceActionSnapshot{{NodeID: 4, Type: task.ActionTypeGrow, SegmentID: 901, Scope: querypb.DataScope_Historical, Workload: 1}},
					}},
				})
			}
		}

		snapshot := buildSnapshot(t, fixture)
		require.Equal(t, 1, attempts)
		require.Equal(t, task.BalanceAdmissionAccepted, fixture.builder.Validate(AdmissionToken{
			Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica,
		}))
	})

	t.Run("same wave admissions are expected but external work is stale", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		epoch := task.BalanceEpochMeta{ResourceGroup: testSnapshotRG, LeaderTerm: 7, Sequence: 9}
		token := AdmissionToken{
			Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica, Epoch: epoch,
		}
		first := task.PendingBalanceTaskSnapshot{
			TaskID: 1, CollectionID: 100, ReplicaID: testEligibleReplica, ResourceGroup: testSnapshotRG, Epoch: epoch,
			Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeGrow, SegmentID: 901, Scope: querypb.DataScope_Historical, Workload: 1}},
		}
		second := task.PendingBalanceTaskSnapshot{
			TaskID: 2, CollectionID: 200, ReplicaID: testOtherReplica, ResourceGroup: testSnapshotRG, Epoch: epoch,
			Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeGrow, SegmentID: 902, Scope: querypb.DataScope_Historical, Workload: 1}},
		}

		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               1,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 1},
			EpochRevisions:         map[task.BalanceEpochMeta]uint64{epoch: 1},
			Tasks:                  []task.PendingBalanceTaskSnapshot{first},
		})
		require.Equal(t, task.BalanceAdmissionAccepted, fixture.builder.Validate(token))
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               2,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 2},
			EpochRevisions:         map[task.BalanceEpochMeta]uint64{epoch: 2},
			Tasks:                  []task.PendingBalanceTaskSnapshot{first, second},
		})
		require.Equal(t, task.BalanceAdmissionAccepted, fixture.builder.Validate(token))

		external := task.PendingBalanceTaskSnapshot{
			TaskID: 3, CollectionID: 100, ReplicaID: testEligibleReplica, ResourceGroup: testSnapshotRG,
			Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeGrow, SegmentID: 903, Scope: querypb.DataScope_Historical, Workload: 1}},
		}
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               3,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 3},
			EpochRevisions:         map[task.BalanceEpochMeta]uint64{epoch: 2},
			Tasks:                  []task.PendingBalanceTaskSnapshot{first, second, external},
		})
		require.Equal(t, task.BalanceAdmissionStaleEpoch, fixture.builder.Validate(token))
	})
}

func TestPlacementSnapshotIncludesUnownedCapacityAndNilReplicaCleanup(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	fixture.dist.PublishNodeDistribution(3,
		[]*meta.Segment{{
			SegmentInfo: &datapb.SegmentInfo{ID: 901, CollectionID: 900, PartitionID: 90, InsertChannel: "orphan", NumOfRows: 90},
			Version:     1,
		}},
		[]*meta.DmChannel{{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 900, ChannelName: "orphan"},
			Version:      1,
			View:         &meta.LeaderView{ID: 3, CollectionID: 900, Channel: "orphan", Version: 1, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		}},
	)
	fixture.inspector.set(task.PendingBalanceSnapshot{
		Revision: 8,
		Tasks: []task.PendingBalanceTaskSnapshot{
			{
				TaskID: 81, CollectionID: 900, ReplicaID: -1,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeReduce, SegmentID: 901, Shard: "orphan", Scope: querypb.DataScope_Historical, Workload: -90}},
			},
			{
				TaskID: 82, CollectionID: 900, ReplicaID: -1,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeReduce, Channel: "orphan", Shard: "orphan", Workload: -1}},
			},
			{
				TaskID: 83, CollectionID: 300, ReplicaID: -1,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 4, Type: task.ActionTypeReduce, SegmentID: 301, Scope: querypb.DataScope_Historical, Workload: -300}},
			},
		},
	})

	snapshot := buildSnapshot(t, fixture)
	segmentKey := SegmentObjectKey{ReplicaID: -1, SegmentID: 901, Scope: querypb.DataScope_Historical}
	channelKey := ChannelObjectKey{ReplicaID: -1, Channel: "orphan"}
	require.Equal(t, []SegmentPlacement{{
		NodeID: 3, CollectionID: 900, PartitionID: 90, Channel: "orphan", RowCount: 90, Version: 1, Present: true,
	}}, snapshot.Segments[segmentKey])
	require.Len(t, snapshot.Channels[channelKey], 1)
	require.NotContains(t, snapshot.Segments, SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 901, Scope: querypb.DataScope_Historical})
	require.NotContains(t, snapshot.Channels, ChannelObjectKey{ReplicaID: testEligibleReplica, Channel: "orphan"})
	require.NotContains(t, snapshot.EligibleReplicas, int64(-1))
	require.Equal(t, -90, snapshot.PendingWork.SegmentWorkloadByNode[3])
	require.Equal(t, -1, snapshot.PendingWork.ChannelWorkloadByNode[3])
	require.Zero(t, snapshot.PendingWork.SegmentWorkloadByNode[4])
	require.Len(t, snapshot.PendingWork.Tasks, 2)
}

func TestPlacementSnapshotValidateTypedReasonsAndIgnoresDistributionRevision(t *testing.T) {
	t.Run("resource group removed", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		require.NoError(t, fixture.meta.ResourceManager.DropResourceGroup(fixture.ctx, testSnapshotRG))
		require.Equal(t, task.BalanceAdmissionRGChanged, fixture.builder.Validate(AdmissionToken{Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica}))
	})

	t.Run("distribution revision", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		token := AdmissionToken{
			Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica, ExpectedSourceNode: 1,
			Segment: &SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 101, Scope: querypb.DataScope_Historical},
		}
		fixture.dist.PublishNodeDistribution(1, fixture.node1Segments, fixture.node1Channels)
		require.Equal(t, task.BalanceAdmissionAccepted, fixture.builder.Validate(token))
	})

	t.Run("source gone", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		token := AdmissionToken{
			Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica, ExpectedSourceNode: 1,
			Segment: &SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 101, Scope: querypb.DataScope_Historical},
		}
		fixture.dist.PublishNodeDistribution(1, fixture.node1Segments[1:], fixture.node1Channels)
		require.Equal(t, task.BalanceAdmissionSourceGone, fixture.builder.Validate(token))
	})

	t.Run("target changed", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		fixture.targetState.mu.Lock()
		fixture.targetState.nextVersion[100]++
		fixture.targetState.mu.Unlock()
		require.Equal(t, task.BalanceAdmissionTargetChanged, fixture.builder.Validate(AdmissionToken{Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica}))
	})

	t.Run("pending changed", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               1,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 0},
		})
		snapshot := buildSnapshot(t, fixture)
		fixture.inspector.set(task.PendingBalanceSnapshot{
			Revision:               2,
			ResourceGroupRevisions: map[string]uint64{testSnapshotRG: 1},
			Tasks: []task.PendingBalanceTaskSnapshot{{
				TaskID: 1, CollectionID: 100, ReplicaID: testEligibleReplica, ResourceGroup: testSnapshotRG,
				Actions: []task.PendingBalanceActionSnapshot{{NodeID: 3, Type: task.ActionTypeGrow, SegmentID: 999, Scope: querypb.DataScope_Historical, Workload: 1}},
			}},
		})
		require.Equal(t, task.BalanceAdmissionStaleEpoch, fixture.builder.Validate(AdmissionToken{Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica}))
	})

	t.Run("leader changed", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		fixture.node1Channels[0].View.Version++
		fixture.dist.PublishNodeDistribution(1, fixture.node1Segments, fixture.node1Channels)
		require.Equal(t, task.BalanceAdmissionLeaderMissing, fixture.builder.Validate(AdmissionToken{Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica}))
	})

	t.Run("node ineligible", func(t *testing.T) {
		fixture := newPlacementSnapshotFixture(t)
		snapshot := buildSnapshot(t, fixture)
		fixture.nodes.Get(1).SetState(session.NodeStateStopping)
		require.Equal(t, task.BalanceAdmissionNodeIneligible, fixture.builder.Validate(AdmissionToken{Snapshot: snapshot.Token, CollectionID: 100, ReplicaID: testEligibleReplica}))
	})
}

func TestSnapshotTokenEqualChecksTargetKeys(t *testing.T) {
	left := SnapshotToken{
		CurrentTargetVersion: map[int64]int64{100: 0},
		NextTargetVersion:    map[int64]int64{100: 0},
	}
	right := SnapshotToken{
		CurrentTargetVersion: map[int64]int64{200: 0},
		NextTargetVersion:    map[int64]int64{200: 0},
	}

	require.False(t, left.Equal(right))
}

func TestSnapshotTokenEqualIgnoresGlobalDiagnosticRevisions(t *testing.T) {
	left := SnapshotToken{
		ResourceGroup:   "rg",
		SegmentRevision: 1, ChannelRevision: 2, PendingTaskRevision: 3, PendingGlobalRevision: 4,
		CurrentTargetVersion: map[int64]int64{100: 1},
		NextTargetVersion:    map[int64]int64{100: 2},
	}
	right := left
	right.SegmentRevision++
	right.ChannelRevision++
	right.PendingGlobalRevision++

	require.True(t, left.Equal(right))
}

func TestReplicaChannelNodeHashEncodesNodeListBoundaries(t *testing.T) {
	encodedLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(encodedLength, 1)
	ambiguousChannel := string(append(encodedLength, 'x'))
	left := map[int64]ReplicaSnapshot{
		1: {
			ID:             1,
			CollectionID:   100,
			ResourceGroup:  "rg",
			ChannelRWNodes: map[string][]int64{"": nil, ambiguousChannel: nil},
		},
	}
	right := map[int64]ReplicaSnapshot{
		1: {
			ID:             1,
			CollectionID:   100,
			ResourceGroup:  "rg",
			ChannelRWNodes: map[string][]int64{"": {int64(len(ambiguousChannel))}, "x": nil},
		},
	}

	require.NotEqual(t, hashReplicas(left), hashReplicas(right))
}
