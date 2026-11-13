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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestStandingCleanupTargetConditions(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.SingleCompactionRatioThreshold.Key:    "0.20",
		cfg.SingleCompactionDeltalogMaxNum.Key:    "200",
		cfg.SingleCompactionDeltaLogMaxSize.Key:   "16777216",
		cfg.SingleCompactionExpiredLogMaxSize.Key: "10485760",
		cfg.CompactionExpiryTolerance.Key:         "-1",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	catalog, persisted := targets.NewCompactionTargetTestCatalog(t)
	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	round := func() map[datapb.TargetIntent]*datapb.CompactionTargetReadModel {
		t.Helper()
		r := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), s.allocator)
		_, err := r.Reconcile(ctx)
		require.NoError(t, err)
		listed, err := s.ListCompactionTargets(ctx, &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
		require.NoError(t, merr.CheckRPCCall(listed, err))
		result := make(map[datapb.TargetIntent]*datapb.CompactionTargetReadModel)
		for _, target := range listed.GetTargets() {
			switch target.GetIntent() {
			case datapb.TargetIntent_INTENT_DELETE_CLEANUP, datapb.TargetIntent_INTENT_TTL:
				require.Zero(t, target.GetResource().GetCollectionId())
				require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, target.GetState())
				require.Nil(t, target.RemainingRows)
				require.NotContains(t, result, target.GetIntent())
				result[target.GetIntent()] = target
			}
		}
		require.Len(t, result, 2)
		return result
	}

	describe := func(id int64) *datapb.CompactionTargetReadModel {
		t.Helper()
		response, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(response, err))
		return response.GetTarget()
	}
	const deleteCleanup, ttl = datapb.TargetIntent_INTENT_DELETE_CLEANUP, datapb.TargetIntent_INTENT_TTL
	initial := round()
	deleteID, ttlID := initial[deleteCleanup].GetTargetId(), initial[ttl].GetTargetId()
	require.Positive(t, deleteID)
	require.Positive(t, ttlID)
	require.NotEqual(t, deleteID, ttlID)
	require.Equal(t, map[string]string{
		"deleted_ratio_threshold": "0.2", "max_deltalog_count": "200", "max_deltalog_bytes": "16777216",
	}, initial[deleteCleanup].GetParameters())
	require.Equal(t, map[string]string{
		"expired_ratio_threshold": "0.2", "max_expired_bytes": "10485760", "strict_tolerance_hours": "-1",
	}, initial[ttl].GetParameters())
	for _, id := range []int64{deleteID, ttlID} {
		require.False(t, describe(id).GetEnabled())
		require.Nil(t, describe(id).RemainingRows)
		resp, err := s.DropCompactionTarget(ctx, &datapb.DropCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrOperationNotSupported)
	}

	require.NoError(t, Params.Save(cfg.CompactionExpiryTolerance.Key, "-2"))
	require.NoError(t, Params.Save(cfg.SingleCompactionRatioThreshold.Key, "2e-1"))
	unchanged := round()
	require.Equal(t, initial[ttl].GetParameters(), unchanged[ttl].GetParameters())
	require.Equal(t, uint64(1), persisted[ttlID].GetConditionRevision())
	require.Equal(t, uint64(1), persisted[deleteID].GetConditionRevision())

	require.NoError(t, Params.Save(cfg.EnableAutoCompaction.Key, "true"))
	require.NoError(t, Params.Save(cfg.CompactionExpiryTolerance.Key, "0"))
	var err error
	s.compactionTargetMeta, err = targets.NewCompactionTargetMetaForTest(ctx, catalog)
	require.NoError(t, err)
	after := round()
	require.Equal(t, deleteID, after[deleteCleanup].GetTargetId())
	require.Equal(t, ttlID, after[ttl].GetTargetId())
	require.Equal(t, "0", after[ttl].GetParameters()["strict_tolerance_hours"])
	for _, id := range []int64{deleteID, ttlID} {
		target := describe(id)
		require.True(t, target.GetEnabled())
		require.NotNil(t, target.RemainingRows)
		require.Zero(t, target.GetRemainingRows())
		require.NotZero(t, target.GetCheckedAt())
	}

	require.NoError(t, Params.Save(cfg.SingleCompactionDeltalogMaxNum.Key, "300"))
	require.NoError(t, Params.Save(cfg.SingleCompactionRatioThreshold.Key, "0.4"))
	updated := round()
	require.Equal(t, deleteID, updated[deleteCleanup].GetTargetId())
	require.Equal(t, ttlID, updated[ttl].GetTargetId())
	require.Equal(t, "300", updated[deleteCleanup].GetParameters()["max_deltalog_count"])
	require.Equal(t, "0.4", updated[deleteCleanup].GetParameters()["deleted_ratio_threshold"])
	require.Equal(t, "0.4", updated[ttl].GetParameters()["expired_ratio_threshold"])
}

func TestStandingCleanupMatchSurvivesSelectionBlockers(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:              "true",
		cfg.EnableSortCompaction.Key:              "false",
		cfg.IndexBasedCompaction.Key:              "false",
		cfg.SingleCompactionRatioThreshold.Key:    "0.2",
		cfg.SingleCompactionDeltalogMaxNum.Key:    "2",
		cfg.SingleCompactionDeltaLogMaxSize.Key:   "100",
		cfg.SingleCompactionExpiredLogMaxSize.Key: "100",
		cfg.CompactionExpiryTolerance.Key:         "1",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	t.Run("selection and observation", func(t *testing.T) {
		now := time.Now()
		catalog, _ := targets.NewCompactionTargetTestCatalog(t)
		s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
		s.indexMeta = index.NewSegmentIndexMeta(catalog)
		for _, collection := range []*meta.CollectionInfo{
			{ID: 1, Schema: &schemapb.CollectionSchema{}, Properties: map[string]string{common.CollectionTTLConfigKey: "3600"}},
			{ID: 2, Schema: &schemapb.CollectionSchema{}, Properties: map[string]string{
				common.CollectionTTLConfigKey: "3600", common.CollectionAutoCompactionKey: "false",
			}},
			{ID: 3, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{ExternalField: "remote"}}}},
			{ID: 4, Schema: &schemapb.CollectionSchema{}},
		} {
			s.meta.AddCollection(collection)
		}
		ts := func(delta time.Duration) uint64 { return tsoutil.ComposeTSByTime(now.Add(delta)) }
		add := func(id, coll, rows int64, level datapb.SegmentLevel, mutate func(*datapb.SegmentInfo)) {
			t.Helper()
			segment := &datapb.SegmentInfo{
				ID: id, CollectionID: coll, PartitionID: 1, InsertChannel: "ch", Level: level,
				State: commonpb.SegmentState_Flushed, IsSorted: true, NumOfRows: rows,
				// Legacy arrays exercise reconstruction through the production constructor.
				Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{
					TimestampFrom: ts(-time.Minute), TimestampTo: ts(-time.Minute), EntriesNum: rows, MemorySize: 300,
				}}}},
			}
			mutate(segment)
			s.meta.Segments.SetSegment(id, meta.NewSegmentInfo(segment))
		}
		const l0, l1, l2 = datapb.SegmentLevel_L0, datapb.SegmentLevel_L1, datapb.SegmentLevel_L2
		delta := func(count int, bytes, rows int64) []*datapb.FieldBinlog {
			logs := make([]*datapb.Binlog, count)
			for i := range logs {
				logs[i] = &datapb.Binlog{EntriesNum: rows, MemorySize: bytes}
			}
			return []*datapb.FieldBinlog{{Binlogs: logs}}
		}
		add(10, 1, 50, l1, func(seg *datapb.SegmentInfo) { seg.Deltalogs = delta(2, 50, 1) })
		add(11, 1, 5, l1, func(seg *datapb.SegmentInfo) { seg.Deltalogs = delta(1, 1, 1); seg.IsImporting = true })
		add(12, 1, 7, l2, func(seg *datapb.SegmentInfo) {
			seg.IsSorted = false
			seg.Deltalogs = delta(3, 1, 1)
			seg.GetBinlogs()[0].GetBinlogs()[0].TimestampFrom = ts(-2*time.Hour - 10*time.Minute)
		})
		add(13, 1, 11, l1, func(seg *datapb.SegmentInfo) {
			seg.IsSorted = false
			seg.Deltalogs = delta(1, 101, 1)
			seg.GetBinlogs()[0].GetBinlogs()[0].TimestampFrom = ts(-2*time.Hour + 10*time.Minute)
		})
		add(14, 2, 13, l1, func(seg *datapb.SegmentInfo) { seg.Deltalogs = delta(1, 101, 1) })
		add(15, 3, 17, l1, func(seg *datapb.SegmentInfo) { seg.Deltalogs = delta(1, 101, 1) })
		add(16, 1, 19, l0, func(seg *datapb.SegmentInfo) { seg.Deltalogs = delta(1, 101, 1) })
		add(17, 1, 23, l1, func(seg *datapb.SegmentInfo) {
			seg.CommitTimestamp = ts(-time.Hour + 10*time.Minute)
			seg.GetBinlogs()[0].GetBinlogs()[0].TimestampFrom = ts(-4 * time.Hour)
			seg.GetBinlogs()[0].GetBinlogs()[0].TimestampTo = ts(-4 * time.Hour)
		})
		add(18, 1, 29, l1, func(seg *datapb.SegmentInfo) {
			seg.CommitTimestamp = ts(-time.Hour - 10*time.Minute)
			seg.GetBinlogs()[0].GetBinlogs()[0].TimestampFrom = ts(-4 * time.Hour)
			seg.GetBinlogs()[0].GetBinlogs()[0].TimestampTo = ts(-4 * time.Hour)
		})
		s.meta.SetSegmentCompacting(18, true)
		add(19, 4, 31, l2, func(seg *datapb.SegmentInfo) {
			seg.IsInvisible = true
			seg.ExpirQuantiles = []int64{now.Add(-10 * time.Minute).UnixMicro()}
		})

		r := targets.NewCompactionTargetReconciler(s.meta, s.indexMeta, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), s.allocator)
		inputs := func(want ...int64) {
			t.Helper()
			events, err := r.Reconcile(ctx)
			require.NoError(t, err)
			var got []int64
			for _, view := range events[policy.TriggerTypeSingle] {
				tasks, err := view.Plan()
				require.NoError(t, err)
				for _, plan := range tasks {
					require.Equal(t, datapb.CompactionType_MixCompaction, plan.GetType())
					got = append(got, plan.GetInputSegments()...)
				}
			}
			require.ElementsMatch(t, want, got)
		}
		// L2 12 matches both targets yet needs exactly one Mix.
		inputs(12)
		list, err := s.ListCompactionTargets(ctx, &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
		require.NoError(t, merr.CheckRPCCall(list, err))
		assertRows := func(intent datapb.TargetIntent, want int64) {
			t.Helper()
			for _, target := range list.GetTargets() {
				if target.GetIntent() != intent {
					continue
				}
				response, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
					TargetId: target.GetTargetId(), Collection: &datapb.CompactionTargetResource{},
				})
				require.NoError(t, merr.CheckRPCCall(response, err))
				require.NotNil(t, response.GetTarget().RemainingRows)
				require.Equal(t, want, response.GetTarget().GetRemainingRows())
				return
			}
			t.Fatalf("missing standing target %s", intent)
		}
		assertRows(datapb.TargetIntent_INTENT_DELETE_CLEANUP, 36)
		assertRows(datapb.TargetIntent_INTENT_TTL, 67)
		require.NoError(t, Params.Save(cfg.IndexBasedCompaction.Key, "true"))
		inputs()
		assertRows(datapb.TargetIntent_INTENT_TTL, 67)
		require.NoError(t, Params.Save(cfg.IndexBasedCompaction.Key, "false"))
		require.NoError(t, Params.Save(cfg.CompactionExpiryTolerance.Key, "-1"))
		inputs(12)
		assertRows(datapb.TargetIntent_INTENT_TTL, 60)
		require.NoError(t, Params.Save(cfg.CompactionExpiryTolerance.Key, "0"))
		inputs(12)
		assertRows(datapb.TargetIntent_INTENT_TTL, 78)
	})
}

func TestStandingCleanupKeepsConfirmedConditionOnSaveFailure(t *testing.T) {
	enableTestCompactionTargets(t)
	ctx := t.Context()
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:              "true",
		cfg.EnableSortCompaction.Key:              "false",
		cfg.IndexBasedCompaction.Key:              "false",
		cfg.SingleCompactionRatioThreshold.Key:    "0.2",
		cfg.SingleCompactionDeltalogMaxNum.Key:    "200",
		cfg.SingleCompactionDeltaLogMaxSize.Key:   "16777216",
		cfg.SingleCompactionExpiredLogMaxSize.Key: "10485760",
		cfg.CompactionExpiryTolerance.Key:         "0",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}

	const ttlID int64 = 101
	const ttlFamily = datapb.TargetIntent_INTENT_TTL
	ttl := &datapb.CompactionTarget{
		TargetID: ttlID, Intent: ttlFamily, TailLimit: -1, ActivatedAtTS: 1,
		Enabled: proto.Bool(true), ConditionRevision: 1,
		Properties: map[string]string{
			"expired_ratio_threshold": "0.2", "max_expired_bytes": "10485760", "strict_tolerance_hours": "-1",
		},
	}
	persisted := map[int64]*datapb.CompactionTarget{ttlID: ttl}
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{ttl}, nil)
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id int64) (*datapb.CompactionTarget, error) {
		return proto.CloneOf(persisted[id]), nil
	})
	catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget, _ *metastore.CompactionTargetCollectionVersion) (bool, error) {
			persisted[record.GetTargetID()] = proto.CloneOf(record)
			return false, nil
		}).Maybe()
	reject := true
	catalog.EXPECT().UpdateStandingCompactionTarget(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _, next *datapb.CompactionTarget) error {
			if reject && next.GetIntent() == ttlFamily {
				return merr.ErrIoFailed
			}
			persisted[next.GetTargetID()] = proto.CloneOf(next)
			return nil
		})

	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	s.meta.AddCollection(&meta.CollectionInfo{
		ID: 1, Schema: &schemapb.CollectionSchema{},
		Properties: map[string]string{common.CollectionTTLConfigKey: "3600"},
	})
	now := time.Now()
	for _, input := range []*datapb.SegmentInfo{
		{ID: 10, Level: datapb.SegmentLevel_L2, NumOfRows: 7, Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{EntriesNum: 2}},
		}}},
		{ID: 11, Level: datapb.SegmentLevel_L2, NumOfRows: 5},
	} {
		input.CollectionID, input.PartitionID, input.InsertChannel = 1, 1, "ch"
		input.State = commonpb.SegmentState_Flushed
		from := now.Add(-time.Minute)
		if input.GetID() == 11 {
			from = now.Add(-2 * time.Hour)
		}
		input.Binlogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{
			TimestampFrom: tsoutil.ComposeTSByTime(from), TimestampTo: tsoutil.ComposeTSByTime(now.Add(-time.Minute)),
			EntriesNum: input.GetNumOfRows(), MemorySize: 1024,
		}}}}
		s.meta.Segments.SetSegment(input.GetID(), meta.NewSegmentInfo(input))
	}
	r := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), s.allocator)
	check := func(tolerance string, rows int64, want ...int64) {
		t.Helper()
		events, err := r.Reconcile(ctx)
		require.NoError(t, err)
		var got []int64
		for _, view := range events[policy.TriggerTypeSingle] {
			tasks, err := view.Plan()
			require.NoError(t, err)
			for _, task := range tasks {
				require.Len(t, task.GetInputSegments(), 1)
				got = append(got, task.GetInputSegments()...)
			}
		}
		require.ElementsMatch(t, want, got)
		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: ttlID, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Equal(t, tolerance, resp.GetTarget().GetParameters()["strict_tolerance_hours"])
		require.NotNil(t, resp.GetTarget().RemainingRows)
		require.Equal(t, rows, resp.GetTarget().GetRemainingRows())
	}
	check("-1", 0, 10)
	reject = false
	check("0", 5, 10, 11)
}
