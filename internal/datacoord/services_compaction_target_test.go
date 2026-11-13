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
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
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
	segmentMeta := meta.NewEmptyTestMeta()
	collection := &meta.CollectionInfo{
		ID: 1, DatabaseName: "default", DatabaseID: 2, UpdateTimestamp: 10,
		Schema: &schemapb.CollectionSchema{Name: "records"},
	}
	segmentMeta.AddCollection(collection)
	s := CreateServer(t.Context(), nil)
	s.allocator = alloc
	s.meta = segmentMeta
	s.compactionTargetMeta = targetMeta
	s.checkCompactionTargetReplication = func(context.Context) error { return nil }
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

func TestCompactionTargetModeIsFixedAtStartup(t *testing.T) {
	enableTestCompactionTargets(t)
	catalog, _ := targets.NewCompactionTargetTestCatalog(t)
	enabled, _ := newTargetRPCServer(t, catalog, nil)

	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "false"))
	disabled := CreateServer(t.Context(), nil)
	disabled.UpdateStateCode(commonpb.StateCode_Healthy)
	enabled.UpdateStateCode(commonpb.StateCode_Initializing)
	response, err := enabled.ListCompactionTargets(t.Context(), &datapb.ListCompactionTargetsRequest{})
	require.ErrorIs(t, merr.CheckRPCCall(response, err), merr.ErrServiceNotReady)
	enabled.UpdateStateCode(commonpb.StateCode_Healthy)
	response, err = enabled.ListCompactionTargets(t.Context(), &datapb.ListCompactionTargetsRequest{})
	require.NoError(t, merr.CheckRPCCall(response, err))

	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true"))
	response, err = disabled.ListCompactionTargets(t.Context(), &datapb.ListCompactionTargetsRequest{})
	require.ErrorIs(t, merr.CheckRPCCall(response, err), merr.ErrOperationNotSupported)
}

func TestDropCollectionCompactionTargetsRetriesPartialClosure(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	catalog := catalogmocks.NewDataCoordCatalog(t)
	active := &datapb.CompactionTarget{
		TargetID: 1, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		Rewrite: &datapb.CompactionTargetRewrite{},
	}
	second := proto.CloneOf(active)
	second.TargetID = 2
	otherScope := proto.CloneOf(active)
	otherScope.TargetID, otherScope.CollectionID = 3, 9
	catalog.EXPECT().ListCompactionTargets(mock.Anything).
		Return([]*datapb.CompactionTarget{active, second, otherScope}, nil).Once()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.MatchedBy(func(target *datapb.CompactionTarget) bool {
		return target.GetTargetID() == active.GetTargetID()
	})).RunAndReturn(func(_ context.Context, record *datapb.CompactionTarget) (*datapb.CompactionTarget, bool, error) {
		return proto.CloneOf(record), true, nil
	}).Once()
	stopSecond := mock.MatchedBy(func(target *datapb.CompactionTarget) bool {
		return target.GetTargetID() == second.GetTargetID()
	})
	catalog.EXPECT().StopCompactionTarget(mock.Anything, stopSecond).Return(nil, false, merr.ErrIoFailed).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, second.GetTargetID()).Return(second, nil).Once()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, stopSecond).
		RunAndReturn(func(_ context.Context, record *datapb.CompactionTarget) (*datapb.CompactionTarget, bool, error) {
			return proto.CloneOf(record), true, nil
		}).Once()

	s, _ := newTargetRPCServer(t, catalog, nil)
	deleted := *s.meta.GetCollection(1)
	deleted.State = etcdpb.CollectionState_CollectionDropping
	s.meta.AddCollection(&deleted)
	s.SetCollectionDroppedChecker(func(_ context.Context, id int64) (bool, error) {
		return id == 1, nil
	})
	require.ErrorIs(t, s.DropCollectionCompactionTargets(ctx, 1), merr.ErrIoFailed)
	req := &datapb.DescribeCompactionTargetRequest{
		TargetId:   active.GetTargetID(),
		Collection: &datapb.CompactionTargetResource{CollectionId: 1, DbId: 2, DbName: "default", Revision: 10, Deleted: true},
	}
	before, err := s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(before, err))
	require.Equal(t, targets.StopScopeDropped, before.GetTarget().GetStopReason())
	require.NotZero(t, before.GetTarget().GetStoppedAt())

	require.NoError(t, s.DropCollectionCompactionTargets(ctx, 1))
	after, err := s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(after, err))
	require.True(t, proto.Equal(before.GetTarget(), after.GetTarget()))
	req.TargetId = second.GetTargetID()
	closed, err := s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(closed, err))
	require.Equal(t, targets.StopScopeDropped, closed.GetTarget().GetStopReason())

	drop, err := s.DropCompactionTarget(ctx, &datapb.DropCompactionTargetRequest{TargetId: req.GetTargetId(), Collection: req.GetCollection()})
	require.NoError(t, merr.CheckRPCCall(drop, err))
	afterDrop, err := s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(afterDrop, err))
	require.True(t, proto.Equal(closed.GetTarget(), afterDrop.GetTarget()))
}

func TestCompactionTargetRecoveryClosesOnlyConfirmedDeletedScopes(t *testing.T) {
	enableTestCompactionTargets(t)
	active := &datapb.CompactionTarget{
		TargetID: 1, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		Rewrite: &datapb.CompactionTargetRewrite{RootSegmentIds: []int64{11}}, TotalRows: proto.Int64(50),
		ExpectedTS: 10, ActivatedAtTS: 10,
	}
	missing := proto.CloneOf(active)
	missing.TargetID, missing.CollectionID = 2, 22
	restoring := proto.CloneOf(active)
	restoring.TargetID, restoring.CollectionID = 3, 33
	history := proto.CloneOf(missing)
	history.TargetID = 4
	history.State = datapb.TargetState_TARGET_STATE_INACTIVE
	history.StopReason, history.InactivatedAtTS = targets.StopOperatorDropped, 20
	catalog, durable := targets.NewCompactionTargetTestCatalog(t, active, missing, restoring, history)
	ctx := t.Context()

	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "false"))
	disabled := CreateServer(ctx, nil)
	require.NoError(t, disabled.DropCollectionCompactionTargets(ctx, missing.GetCollectionID()))
	require.True(t, proto.Equal(missing, durable[missing.GetTargetID()]))

	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true"))
	restarted, _ := newTargetRPCServer(t, catalog, nil)
	restarted.UpdateStateCode(commonpb.StateCode_Initializing)
	restarted.SetCollectionDroppedChecker(func(_ context.Context, id int64) (bool, error) {
		return id == missing.GetCollectionID(), nil
	})
	require.NoError(t, restarted.recoverCompactionTargetScopes(ctx))
	require.True(t, proto.Equal(active, durable[active.GetTargetID()]))
	require.True(t, proto.Equal(restoring, durable[restoring.GetTargetID()]))
	require.True(t, proto.Equal(history, durable[history.GetTargetID()]))

	restarted.UpdateStateCode(commonpb.StateCode_Healthy)
	req := &datapb.DescribeCompactionTargetRequest{
		TargetId:   missing.GetTargetID(),
		Collection: &datapb.CompactionTargetResource{CollectionId: missing.GetCollectionID(), Deleted: true},
	}
	resp, err := restarted.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, targets.StopScopeDropped, resp.GetTarget().GetStopReason())
	require.Greater(t, resp.GetTarget().GetStoppedAt(), history.GetInactivatedAtTS())
	require.Equal(t, []int64{11}, resp.GetTarget().GetRewrite().GetRootSegmentIds())
	require.Equal(t, int64(50), resp.GetTarget().GetTotalRows())
}

func TestCompactionTargetWithEmptyMatchWaitsForCollectionDropAck(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	record := &datapb.CompactionTarget{
		TargetID: 1, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		Rewrite: &datapb.CompactionTargetRewrite{}, ExpectedTS: 10, ActivatedAtTS: 10,
	}
	catalog, _ := targets.NewCompactionTargetTestCatalog(t, record)
	s, resource := newTargetRPCServer(t, catalog, nil)
	s.compactionTargetMeta.SetCollectionScopeGuard(s.tryLockCompactionTargetScope)
	// A collection may still be Created while its DropCollection message
	// retires all the candidate segments on the business channels.
	b := mock_broadcaster.NewMockBroadcaster(t)
	b.EXPECT().TryLockResourceKeys(mock.Anything,
		message.NewSharedDBNameResourceKey("default"),
		message.NewSharedCollectionNameResourceKey("default", "records")).
		Return(nil, nil).Once()
	broadcast.ResetBroadcaster()
	t.Cleanup(broadcast.ResetBroadcaster)
	broadcast.Register(b)

	reconciler := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, nil)
	_, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	req := &datapb.DescribeCompactionTargetRequest{TargetId: record.GetTargetID(), Collection: resource}
	resp, err := s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, resp.GetTarget().GetState())
	require.Zero(t, resp.GetTarget().GetRemainingRows())
	require.NotZero(t, resp.GetTarget().GetCheckedAt())

	s.meta.DropCollection(record.GetCollectionID())
	s.SetCollectionDroppedChecker(func(context.Context, int64) (bool, error) { return true, nil })
	require.NoError(t, s.DropCollectionCompactionTargets(ctx, record.GetCollectionID()))
	req.Collection = &datapb.CompactionTargetResource{CollectionId: record.GetCollectionID(), Deleted: true}
	resp, err = s.DescribeCompactionTarget(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, targets.StopScopeDropped, resp.GetTarget().GetStopReason())
}

func TestCompactionTargetGCConfirmsDeletionAndKeepsQueryableHistory(t *testing.T) {
	enableTestCompactionTargets(t)
	now := time.Date(2026, time.November, 13, 0, 0, 0, 0, time.UTC)
	const retention = "dataCoord.compaction.target.retentionSeconds"
	require.NoError(t, Params.Save(retention, "86400"))
	t.Cleanup(func() { Params.Reset(retention) })

	newTerminal := func(id int64, stoppedAt time.Time) *datapb.CompactionTarget {
		return &datapb.CompactionTarget{
			TargetID: id, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
			Rewrite: &datapb.CompactionTargetRewrite{}, State: datapb.TargetState_TARGET_STATE_INACTIVE,
			InactivatedAtTS: tsoutil.ComposeTSByTime(stoppedAt), StopReason: targets.StopOperatorDropped,
		}
	}
	retained := newTerminal(1, now.Add(-48*time.Hour))
	removed := newTerminal(2, now.Add(-48*time.Hour))
	atLimit := newTerminal(3, now.Add(-24*time.Hour))
	unknown := newTerminal(4, now.Add(-48*time.Hour))
	unknown.Intent = datapb.TargetIntent(999)
	missingStop := newTerminal(5, now.Add(-48*time.Hour))
	missingStop.InactivatedAtTS = 0
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).
		Return([]*datapb.CompactionTarget{retained, removed, atLimit, unknown, missingStop}, nil).Once()
	catalog.EXPECT().DropCompactionTarget(mock.Anything, retained).Return(context.DeadlineExceeded).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, retained.GetTargetID()).Return(retained, nil)
	catalog.EXPECT().DropCompactionTarget(mock.Anything, removed).Return(context.DeadlineExceeded).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, removed.GetTargetID()).Return(nil, merr.ErrIoFailed).Once()

	s, resource := newTargetRPCServer(t, catalog, nil)
	s.compactionTargetMeta.CollectExpired(t.Context(), now)
	req := &datapb.DescribeCompactionTargetRequest{TargetId: retained.GetTargetID(), Collection: resource}
	resp, err := s.DescribeCompactionTarget(t.Context(), req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, retained.GetInactivatedAtTS(), resp.GetTarget().GetStoppedAt())
	drop, err := s.DropCompactionTarget(t.Context(), &datapb.DropCompactionTargetRequest{TargetId: retained.GetTargetID(), Collection: resource})
	require.NoError(t, merr.CheckRPCCall(drop, err))

	req.TargetId = removed.GetTargetID()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, removed.GetTargetID()).Return(nil, merr.ErrIoFailed).Once()
	resp, err = s.DescribeCompactionTarget(t.Context(), req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrIoFailed)
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, removed.GetTargetID()).Return(nil, nil).Once()
	resp, err = s.DescribeCompactionTarget(t.Context(), req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCompactionTargetNotFound)

	require.NoError(t, Params.Save(retention, "604800"))
	s.compactionTargetMeta.CollectExpired(t.Context(), now)
	listed, err := s.ListCompactionTargets(t.Context(), &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
	require.NoError(t, merr.CheckRPCCall(listed, err))
	require.Len(t, listed.GetTargets(), 4)
	require.Equal(t, []int64{5, 4, 3, 1}, []int64{
		listed.GetTargets()[0].GetTargetId(), listed.GetTargets()[1].GetTargetId(),
		listed.GetTargets()[2].GetTargetId(), listed.GetTargets()[3].GetTargetId(),
	})
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
		{ID: 10, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 7,
			StartPosition: &msgpb.MsgPosition{Timestamp: 90}, DmlPosition: &msgpb.MsgPosition{Timestamp: 110}},
		{ID: 11, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 8,
			StartPosition: &msgpb.MsgPosition{Timestamp: 100}},
		{ID: 12, CollectionID: 1, State: commonpb.SegmentState_Growing, NumOfRows: 9,
			StartPosition: &msgpb.MsgPosition{Timestamp: 80}},
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
		{ID: 13, CollectionID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			CompactionFrom: []int64{11, 12}, CreateTs: 99, NumOfRows: 16},
		{ID: 14, CollectionID: 2, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			CompactionFrom: []int64{10}, NumOfRows: 5},
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
			ID: 2, DatabaseName: "default", DatabaseID: 2, UpdateTimestamp: 11,
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
