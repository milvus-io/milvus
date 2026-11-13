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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func enableTestCompactionTargets(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	params.Save(Params.DataCoordCfg.EnableCompaction.Key, "true")
	params.Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true")
	params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "false")
	t.Cleanup(func() {
		params.Reset(Params.DataCoordCfg.EnableAutoCompaction.Key)
		params.Reset(Params.DataCoordCfg.EnableTargetBasedCompaction.Key)
		params.Reset(Params.DataCoordCfg.EnableCompaction.Key)
	})
}

func newTargetRPCServer(t *testing.T, catalog *catalogmocks.DataCoordCatalog, alloc allocator.Allocator) (*Server, *datapb.CompactionTargetResource) {
	t.Helper()
	targetMeta, err := targets.NewCompactionTargetMeta(t.Context(), catalog)
	require.NoError(t, err)
	segmentMeta := meta.NewTestMetaForCompactionTargets()
	collection := &meta.CollectionInfo{
		ID: 1, DatabaseName: "default", DatabaseID: 2,
		Schema: &schemapb.CollectionSchema{Name: "records"},
	}
	segmentMeta.AddCollection(collection)
	s := &Server{allocator: alloc, meta: segmentMeta, targetCompactionEnabled: true}
	s.handler = meta.NewMockHandlerWithMeta(segmentMeta)
	s.compactionTargetCollection = func(_ context.Context, id int64) (*datapb.CompactionTargetResource, bool, error) {
		current := segmentMeta.GetCollection(id)
		if current == nil {
			return &datapb.CompactionTargetResource{CollectionId: id, Deleted: true}, false, nil
		}
		return &datapb.CompactionTargetResource{
			CollectionId: current.ID, DbId: current.DatabaseID, DbName: current.DatabaseName,
			CollectionName: current.Schema.GetName(), Revision: 10,
		}, current.IsExternal(), nil
	}
	s.compactionTargetMeta = targetMeta
	s.stateCode.Store(commonpb.StateCode_Healthy)
	return s, &datapb.CompactionTargetResource{
		CollectionId: 1, DbId: 2, DbName: "default", CollectionName: "records", Revision: 10,
	}
}

func rewriteRPCRequest(resource *datapb.CompactionTargetResource, roots ...int64) *datapb.CreateCompactionTargetRequest {
	return &datapb.CreateCompactionTargetRequest{
		Collection: resource,
		Target: &datapb.CreateCompactionTargetRequest_Rewrite{
			Rewrite: &datapb.CompactionTargetRewrite{RootSegmentIds: roots},
		},
	}
}

func TestStandingSortCompactionTargetConditionAndObservation(t *testing.T) {
	enableTestCompactionTargets(t)
	sortConfig := &Params.DataCoordCfg.EnableSortCompaction
	previous := sortConfig.SwapTempValue("true")
	t.Cleanup(func() { sortConfig.SwapTempValue(previous) })

	ctx := t.Context()
	catalog, records := targets.NewCompactionTargetTestCatalog(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(200, nil).Maybe()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(110), nil).Maybe()
	s, _ := newTargetRPCServer(t, catalog, alloc)
	s.meta.Segments.SetSegment(10, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 10, CollectionID: 1, PartitionID: 1, InsertChannel: "ch", Level: datapb.SegmentLevel_L2,
		State: commonpb.SegmentState_Flushed, IsInvisible: true, NumOfRows: 7,
	}))
	s.meta.SetSegmentCompacting(10, true)
	runRound := func() int {
		t.Helper()
		reconciler := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, s.handler, s.allocator, s.compactionTargetCollectionAvailable)
		events, err := reconciler.Reconcile(ctx)
		require.NoError(t, err)
		n := 0
		for _, pools := range events {
			for _, pool := range pools {
				plans, err := pool.Plan()
				require.NoError(t, err)
				n += len(plans)
			}
		}
		return n
	}
	require.Zero(t, runRound())

	list := func(req *datapb.ListCompactionTargetsRequest) []*datapb.CompactionTargetReadModel {
		t.Helper()
		resp, err := s.ListCompactionTargets(ctx, req)
		require.NoError(t, merr.CheckRPCCall(resp, err))
		return resp.GetTargets()
	}
	require.Empty(t, list(&datapb.ListCompactionTargetsRequest{}))
	require.Empty(t, list(&datapb.ListCompactionTargetsRequest{IncludeSystem: true, DbName: "default"}))
	active := &datapb.ListCompactionTargetsRequest{IncludeSystem: true, States: []datapb.TargetState{datapb.TargetState_TARGET_STATE_ACTIVE}}
	standing := list(active)
	require.Len(t, standing, 1)
	id := standing[0].GetTargetId()
	require.Equal(t, int64(200), id)
	require.True(t, standing[0].GetEnabled())
	require.Nil(t, standing[0].RemainingRows)
	require.Equal(t, datapb.TargetIntent_INTENT_SORT, standing[0].GetIntent())
	require.NotNil(t, records[200])

	describe := func() *datapb.CompactionTargetReadModel {
		t.Helper()
		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		return resp.GetTarget()
	}
	require.Equal(t, int64(7), describe().GetRemainingRows())
	dropped, err := s.DropCompactionTarget(ctx, &datapb.DropCompactionTargetRequest{
		TargetId: id, Collection: &datapb.CompactionTargetResource{},
	})
	require.ErrorIs(t, merr.CheckRPCCall(dropped, err), merr.ErrOperationNotSupported)

	restart := func() {
		t.Helper()
		var err error
		s.compactionTargetMeta, err = targets.NewCompactionTargetMeta(ctx, catalog)
		require.NoError(t, err)
	}
	sortConfig.SwapTempValue("false")
	restart()
	require.Zero(t, runRound())
	require.Len(t, records, 1)
	disabled := describe()
	require.False(t, disabled.GetEnabled())
	require.NotNil(t, disabled.Enabled)
	require.Nil(t, disabled.RemainingRows)
	require.Zero(t, disabled.GetCheckedAt())
	require.Equal(t, id, list(active)[0].GetTargetId())

	sortConfig.SwapTempValue("true")
	restart()
	s.meta.SetSegmentCompacting(10, false)
	require.Equal(t, 1, runRound())
	require.Len(t, records, 1)
	require.True(t, describe().GetEnabled())
}

func TestStandingSortConditionSaveFailureKeepsLastConfirmedDefinition(t *testing.T) {
	enableTestCompactionTargets(t)
	sortConfig := &Params.DataCoordCfg.EnableSortCompaction
	previous := sortConfig.SwapTempValue("false")
	t.Cleanup(func() { sortConfig.SwapTempValue(previous) })

	ctx := t.Context()
	catalog := catalogmocks.NewDataCoordCatalog(t)
	record := &datapb.CompactionTarget{
		TargetID: 44, Intent: datapb.TargetIntent_INTENT_SORT, TailLimit: -1,
		ActivatedAtTS: 1, Enabled: proto.Bool(true), ConditionRevision: 1,
	}
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{record}, nil).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, record.GetTargetID()).Return(record, nil).Twice()
	catalog.EXPECT().UpdateStandingCompactionTarget(mock.Anything, mock.Anything, mock.Anything).Return(merr.ErrIoFailed).Once()

	s, _ := newTargetRPCServer(t, catalog, nil)
	s.meta.Segments.SetSegment(10, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 10, CollectionID: 1, PartitionID: 1, InsertChannel: "ch", Level: datapb.SegmentLevel_L1,
		State: commonpb.SegmentState_Flushed, NumOfRows: 7,
	}))
	reconciler := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, s.handler, s.allocator, s.compactionTargetCollectionAvailable)
	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	for _, views := range events {
		require.Empty(t, views)
	}

	resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
		TargetId: record.GetTargetID(), Collection: &datapb.CompactionTargetResource{},
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.True(t, resp.GetTarget().GetEnabled())
	require.Equal(t, int64(7), resp.GetTarget().GetRemainingRows())
}

func TestCreateRewriteCompactionTargetPersistsCompleteMatch(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(101, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(100, nil).Once()
	catalog, records := targets.NewCompactionTargetTestCatalog(t)
	s, resource := newTargetRPCServer(t, catalog, alloc)
	for _, segment := range []*datapb.SegmentInfo{
		{
			ID: 10, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 7,
			StartPosition: &msgpb.MsgPosition{Timestamp: 90}, DmlPosition: &msgpb.MsgPosition{Timestamp: 110},
		},
		{
			ID: 11, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 8,
			StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		},
		{
			ID: 12, CollectionID: 1, State: commonpb.SegmentState_Growing, NumOfRows: 9,
			StartPosition: &msgpb.MsgPosition{Timestamp: 80},
		},
	} {
		s.meta.Segments.SetSegment(segment.GetID(), meta.NewSegmentInfo(segment))
	}
	s.meta.SetSegmentCompacting(10, true)
	resp, err := s.CreateCompactionTarget(ctx, rewriteRPCRequest(resource, 12, 10, 11, 10))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, int64(101), resp.GetTargetId())
	require.Equal(t, int64(7), resp.GetTotalRows())
	record := records[101]
	require.NotNil(t, record)
	require.Equal(t, []int64{10, 11, 12}, record.GetRewrite().GetRootSegmentIds())
	require.Equal(t, uint64(100), record.GetExpectedTS())
	require.Equal(t, uint64(100), record.GetActivatedAtTS())
	require.Equal(t, int64(7), record.GetTotalRows())
}

func TestCompactionTargetCreateConfirmsCandidateAfterWriteTimeout(t *testing.T) {
	for _, tc := range []struct {
		name        string
		committed   bool
		commitsLate bool
		ambiguous   bool
		want        error
	}{
		{name: "commit with lost acknowledgment", committed: true, ambiguous: true},
		{name: "could not confirm candidate", ambiguous: true, want: merr.ErrCompactionTargetCreateOutcomeUnknown},
		{name: "commit visible after confirmation", commitsLate: true, ambiguous: true, want: merr.ErrCompactionTargetCreateOutcomeUnknown},
		{name: "definitely not committed", want: merr.ErrIoFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableTestCompactionTargets(t)
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocID(mock.Anything).Return(101, nil).Once()
			alloc.EXPECT().AllocTimestamp(mock.Anything).Return(100, nil).Once()
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Once()
			var candidate *datapb.CompactionTarget
			catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, record *datapb.CompactionTarget, _ *metastore.CompactionTargetCollectionVersion) (bool, error) {
					candidate = proto.CloneOf(record)
					if tc.ambiguous {
						return true, context.DeadlineExceeded
					}
					return false, merr.ErrIoFailed
				}).Once()
			if tc.ambiguous {
				catalog.EXPECT().LoadCompactionTarget(mock.Anything, int64(101)).RunAndReturn(
					func(context.Context, int64) (*datapb.CompactionTarget, error) {
						if tc.committed {
							return candidate, nil
						}
						return nil, nil
					}).Once()
			}
			s, resource := newTargetRPCServer(t, catalog, alloc)
			resp, err := s.CreateCompactionTarget(t.Context(), rewriteRPCRequest(resource))
			require.NoError(t, err)
			if tc.want != nil {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), tc.want)
				require.Zero(t, resp.GetTargetId())
				if tc.commitsLate {
					catalog.EXPECT().LoadCompactionTarget(mock.Anything, int64(101)).Return(candidate, nil).Once()
					listed, err := s.ListCompactionTargets(t.Context(), &datapb.ListCompactionTargetsRequest{})
					require.NoError(t, merr.CheckRPCCall(listed, err))
					require.Len(t, listed.GetTargets(), 1)
					require.Equal(t, int64(101), listed.GetTargets()[0].GetTargetId())
				}
				return
			}
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Equal(t, int64(101), resp.GetTargetId())
			require.Zero(t, resp.GetTotalRows())
			listed, err := s.ListCompactionTargets(t.Context(), &datapb.ListCompactionTargetsRequest{})
			require.NoError(t, merr.CheckRPCCall(listed, err))
			require.Len(t, listed.GetTargets(), 1)
			require.Equal(t, int64(101), listed.GetTargets()[0].GetTargetId())
		})
	}
}

func TestCompactionTargetDescribeAndDropReadDurableOutcome(t *testing.T) {
	enableTestCompactionTargets(t)
	catalog, persisted := targets.NewCompactionTargetTestCatalog(t, &datapb.CompactionTarget{
		TargetID: 101, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS: 100, ActivatedAtTS: 100, TotalRows: proto.Int64(0),
		Rewrite: &datapb.CompactionTargetRewrite{},
	})
	s, resource := newTargetRPCServer(t, catalog, nil)
	describe := &datapb.DescribeCompactionTargetRequest{TargetId: 101, Collection: resource}
	first, err := s.DescribeCompactionTarget(t.Context(), describe)
	require.NoError(t, merr.CheckRPCCall(first, err))
	require.NotNil(t, first.GetTarget().RemainingRows)
	require.Zero(t, first.GetTarget().GetRemainingRows())
	require.NotZero(t, first.GetTarget().GetCheckedAt())

	drop := &datapb.DropCompactionTargetRequest{TargetId: 101, Collection: resource}
	resp, err := s.DropCompactionTarget(t.Context(), drop)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	stoppedAt := persisted[101].GetInactivatedAtTS()
	require.NotZero(t, stoppedAt)

	// A later invalid Match prevents observation but leaves the persisted
	// operator outcome readable.
	for id, rows := range map[int64]int64{1: math.MaxInt64, 2: 1} {
		s.meta.Segments.SetSegment(id, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1, State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L1, NumOfRows: rows,
		}))
	}
	ended, err := s.DescribeCompactionTarget(t.Context(), describe)
	require.NoError(t, merr.CheckRPCCall(ended, err))
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, ended.GetTarget().GetState())
	require.Equal(t, "operator_dropped", ended.GetTarget().GetStopReason())
	require.Equal(t, stoppedAt, ended.GetTarget().GetStoppedAt())
	require.NotNil(t, ended.GetTarget().TotalRows)
	require.Nil(t, ended.GetTarget().RemainingRows)
	require.Zero(t, ended.GetTarget().GetCheckedAt())
	resp, err = s.DropCompactionTarget(t.Context(), drop)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, stoppedAt, persisted[101].GetInactivatedAtTS())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled, err := s.DescribeCompactionTarget(ctx, describe)
	require.Nil(t, canceled)
	require.Equal(t, codes.Canceled, status.Code(err))
}

func TestCompactionTargetRewriteFollowsLineageFromCurrentRecords(t *testing.T) {
	enableTestCompactionTargets(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(101, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(100, nil).Once()
	catalog, _ := targets.NewCompactionTargetTestCatalog(t)
	s, resource := newTargetRPCServer(t, catalog, alloc)

	// Root 10 has been reclaimed. It split into 11 and 12, which later
	// merged into 13. A separate collection cannot extend this lineage.
	publish := func(segment *datapb.SegmentInfo) {
		s.meta.Segments.SetSegment(segment.GetID(), meta.NewSegmentInfo(segment))
	}
	for _, segment := range []*datapb.SegmentInfo{
		{ID: 11, CollectionID: 1, State: commonpb.SegmentState_Dropped, CompactionFrom: []int64{10}},
		{ID: 12, CollectionID: 1, State: commonpb.SegmentState_Dropped, CompactionFrom: []int64{10}},
		{
			ID: 13, CollectionID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			CompactionFrom: []int64{11, 12}, CreateTs: 99, NumOfRows: 16,
		},
		{
			ID: 14, CollectionID: 2, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			CompactionFrom: []int64{10}, NumOfRows: 5,
		},
	} {
		publish(segment)
	}
	resp, err := s.CreateCompactionTarget(t.Context(), rewriteRPCRequest(resource, 10, 12))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, int64(16), resp.GetTotalRows())

	req := &datapb.DescribeCompactionTargetRequest{TargetId: resp.GetTargetId(), Collection: resource}
	observed, err := s.DescribeCompactionTarget(t.Context(), req)
	require.NoError(t, merr.CheckRPCCall(observed, err))
	require.Equal(t, int64(16), observed.GetTarget().GetRemainingRows())

	publish(&datapb.SegmentInfo{ID: 11, CollectionID: 1, State: commonpb.SegmentState_Dropped, CompactionFrom: []int64{10, 13}})
	observed, err = s.DescribeCompactionTarget(t.Context(), req)
	require.NoError(t, merr.CheckRPCCall(observed, err))
	require.Nil(t, observed.GetTarget().RemainingRows)
}

func TestCompactionTargetCreateRechecksAuthorizedCollection(t *testing.T) {
	enableTestCompactionTargets(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(101, nil).Once()
	catalog, persisted := targets.NewCompactionTargetTestCatalog(t)
	s, resource := newTargetRPCServer(t, catalog, alloc)
	alloc.EXPECT().AllocTimestamp(mock.Anything).RunAndReturn(func(context.Context) (uint64, error) {
		s.meta.DropCollection(1)
		s.meta.AddCollection(&meta.CollectionInfo{
			ID: 2, DatabaseName: "default", DatabaseID: 2,
			Schema: &schemapb.CollectionSchema{Name: "records"},
		})
		return 100, nil
	}).Once()

	resp, err := s.CreateCompactionTarget(t.Context(), rewriteRPCRequest(resource))
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
	require.Zero(t, resp.GetTargetId())
	require.Empty(t, persisted)
}

func TestCompactionTargetWaitsForDropAndRetainsRecoveredHistory(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	now := time.Now()
	activatedAt := tsoutil.ComposeTSByTime(now.Add(-time.Minute))
	record := &datapb.CompactionTarget{
		TargetID: 1, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		Rewrite: &datapb.CompactionTargetRewrite{}, ExpectedTS: activatedAt, ActivatedAtTS: activatedAt,
	}
	catalog, records := targets.NewCompactionTargetTestCatalog(t, record)
	s, resource := newTargetRPCServer(t, catalog, nil)
	s.compactionTargetMeta.SetCollectionScopeGuard(s.tryLockCompactionTargetScope)
	broadcaster := mock_broadcaster.NewMockBroadcaster(t)
	broadcaster.EXPECT().Close().Maybe()
	broadcaster.EXPECT().TryLockResourceKeys(mock.Anything,
		message.NewSharedDBNameResourceKey("default"),
		message.NewSharedCollectionNameResourceKey("default", "records")).Return(nil, nil).Once()
	broadcast.ResetBroadcaster()
	t.Cleanup(broadcast.ResetBroadcaster)
	broadcast.Register(broadcaster)

	reconciler := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, s.handler, s.allocator, s.compactionTargetCollectionAvailable)
	_, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	req := &datapb.DescribeCompactionTargetRequest{TargetId: record.GetTargetID(), Collection: resource}
	resp, err := s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, resp.GetTarget().GetState())
	require.NotNil(t, resp.GetTarget().RemainingRows)
	require.Zero(t, resp.GetTarget().GetRemainingRows())

	// A restart with the feature disabled leaves the Target for the next
	// enabled initialization, even though RootCoord has finished the drop.
	s.targetCompactionEnabled = false
	s.meta.DropCollection(record.GetCollectionID())
	require.NoError(t, s.DropCollectionCompactionTargets(ctx, record.GetCollectionID()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, records[record.GetTargetID()].GetState())
	s.targetCompactionEnabled = true
	require.NoError(t, s.recoverCompactionTargetScopes(ctx))
	req.Collection = &datapb.CompactionTargetResource{CollectionId: record.GetCollectionID(), Deleted: true}
	resp, err = s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, targets.StopScopeDropped, resp.GetTarget().GetStopReason())
	require.NotZero(t, resp.GetTarget().GetStoppedAt())

	stored := records[record.GetTargetID()]
	dropped, err := s.DropCompactionTarget(ctx, &datapb.DropCompactionTargetRequest{
		TargetId: record.GetTargetID(), Collection: req.GetCollection(),
	})
	require.NoError(t, merr.CheckRPCCall(dropped, err))
	require.True(t, proto.Equal(stored, records[record.GetTargetID()]))

	catalog.EXPECT().DropCompactionTarget(mock.Anything, stored).Return(nil).Once()
	s.compactionTargetMeta.CollectExpired(ctx, tsoutil.PhysicalTime(stored.GetInactivatedAtTS()).Add(8*24*time.Hour))
	resp, err = s.DescribeCompactionTarget(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCompactionTargetNotFound)
}
