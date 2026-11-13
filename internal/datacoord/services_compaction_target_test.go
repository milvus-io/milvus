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
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	s := &Server{allocator: alloc, meta: segmentMeta}
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
			catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, record *datapb.CompactionTarget) (bool, error) {
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
