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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStandingSchemaVersionCompactionTargetCondition(t *testing.T) {
	ctx := t.Context()
	enableTestCompactionTargets(t)
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:               "true",
		cfg.BumpSchemaVersionCompactionEnabled.Key: "true",
		cfg.StorageVersionCompactionEnabled.Key:    "false",
		cfg.StorageFormatCompactionEnabled.Key:     "false",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	catalog, _ := targets.NewCompactionTargetTestCatalog(t)
	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	s.meta.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Version: 2}})
	s.meta.Segments.SetSegment(100, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L2, IsInvisible: true, IsImporting: true, IsSorted: true,
		StorageVersion: storage.StorageV2, SchemaVersion: 1, NumOfRows: 17,
	}))
	var id int64
	check := func(enabled bool) {
		t.Helper()
		reconciler := targets.NewCompactionTargetReconciler(s.meta, s.indexMeta, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), s.allocator)
		events, err := reconciler.Reconcile(ctx)
		require.NoError(t, err)
		for _, pools := range events {
			for _, pool := range pools {
				plans, err := pool.Plan()
				require.NoError(t, err)
				require.Empty(t, plans)
			}
		}

		list, err := s.ListCompactionTargets(ctx, &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
		require.NoError(t, merr.CheckRPCCall(list, err))
		var found []*datapb.CompactionTargetReadModel
		for _, target := range list.GetTargets() {
			if target.GetIntent() == datapb.TargetIntent_INTENT_SCHEMA_VERSION {
				found = append(found, target)
			}
		}
		require.Len(t, found, 1)
		if id == 0 {
			id = found[0].GetTargetId()
			require.Positive(t, id)
		}
		require.Equal(t, id, found[0].GetTargetId())
		require.Zero(t, found[0].GetResource().GetCollectionId())
		require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, found[0].GetState())
		require.NotNil(t, found[0].Enabled)
		require.Equal(t, enabled, found[0].GetEnabled())
		require.Empty(t, found[0].GetParameters())

		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		if enabled {
			require.NotNil(t, resp.GetTarget().RemainingRows)
			require.EqualValues(t, 17, resp.GetTarget().GetRemainingRows())
			require.NotZero(t, resp.GetTarget().GetCheckedAt())
		} else {
			require.Nil(t, resp.GetTarget().RemainingRows)
			require.Zero(t, resp.GetTarget().GetCheckedAt())
		}
	}
	check(true)

	var err error
	s.compactionTargetMeta, err = targets.NewCompactionTargetMetaForTest(ctx, catalog)
	require.NoError(t, err)
	require.NoError(t, Params.Save(cfg.EnableAutoCompaction.Key, "false"))
	check(false)
	require.NoError(t, Params.Save(cfg.EnableAutoCompaction.Key, "true"))
	require.NoError(t, Params.Save(cfg.BumpSchemaVersionCompactionEnabled.Key, "false"))
	check(false)
	require.NoError(t, Params.Save(cfg.BumpSchemaVersionCompactionEnabled.Key, "true"))
	check(true)
}

func TestStandingSchemaVersionPlansEffectiveSchema(t *testing.T) {
	ctx := t.Context()
	enableTestCompactionTargets(t)
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:               "true",
		cfg.EnableSortCompaction.Key:               "false",
		cfg.BumpSchemaVersionCompactionEnabled.Key: "true",
		cfg.StorageVersionCompactionEnabled.Key:    "false",
		cfg.StorageFormatCompactionEnabled.Key:     "false",
		cfg.SegmentMaxSize.Key:                     "16",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	catalog, _ := targets.NewCompactionTargetTestCatalog(t)
	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	s.meta.PreparationSchema = coordinatorPreparationSchema
	pending := &schemapb.CollectionSchema{Version: 2, Properties: []*commonpb.KeyValuePair{
		{Key: common.CollectionTTLConfigKey, Value: "60"},
	}}
	recovering := true
	patch := mockey.Mock(broadcast.GetPendingSchemaTarget).To(func(id int64) (*broadcaster.SchemaTarget, error) {
		switch id {
		case 1:
			return &broadcaster.SchemaTarget{Schema: pending}, nil
		case 3:
			if recovering {
				return nil, merr.WrapErrServiceNotReadyMsg("recovering schema broadcasts")
			}
		}
		return nil, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	for _, id := range []int64{1, 2, 3} {
		s.meta.AddCollection(&meta.CollectionInfo{
			ID: id, Schema: &schemapb.CollectionSchema{Version: 1}, Properties: map[string]string{common.CollectionTTLConfigKey: "10"},
		})
	}
	addSegment := func(id, collectionID int64, version int32) {
		t.Helper()
		s.meta.Segments.SetSegment(id, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: collectionID, PartitionID: 10, InsertChannel: "schema-channel",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2,
			StorageVersion: storage.StorageV3, SchemaVersion: version, ManifestPath: "v3-manifest", NumOfRows: 13,
		}))
	}
	addSegment(101, 1, 1)
	addSegment(102, 1, 2)
	addSegment(201, 2, 0)
	addSegment(301, 3, 0)
	reconciler := targets.NewCompactionTargetReconciler(s.meta, s.indexMeta, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), s.allocator)
	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	views := events[policy.TriggerTypeBumpSchemaVersion]
	require.Len(t, views, 2)
	require.EqualValues(t, 1, s.meta.GetCollection(1).Schema.GetVersion())

	// A new effective version and size must not change an already built View.
	pending = &schemapb.CollectionSchema{Version: 3}
	require.NoError(t, Params.Save(cfg.SegmentMaxSize.Key, "32"))
	for i := 0; i < 2; i++ {
		plans, err := views[0].Plan()
		require.NoError(t, err)
		require.Len(t, plans, 1)
		require.Equal(t, []int64{101}, plans[0].GetInputSegments())
		require.Equal(t, datapb.CompactionType_BumpSchemaVersionCompaction, plans[0].GetType())
		require.EqualValues(t, 2, plans[0].GetSchema().GetVersion())
		require.Equal(t, (60 * time.Second).Nanoseconds(), plans[0].GetCollectionTtl())
		require.EqualValues(t, 16<<20, plans[0].GetMaxSize())
	}
	plan, err := views[1].Plan()
	require.NoError(t, err)
	require.Len(t, plan, 1)
	require.Equal(t, []int64{201}, plan[0].GetInputSegments())
	require.EqualValues(t, 1, plan[0].GetSchema().GetVersion())

	list, err := s.ListCompactionTargets(ctx, &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
	require.NoError(t, merr.CheckRPCCall(list, err))
	var id int64
	for _, target := range list.GetTargets() {
		if target.GetIntent() == datapb.TargetIntent_INTENT_SCHEMA_VERSION {
			id = target.GetTargetId()
		}
	}
	require.Positive(t, id)
	describe := func() *datapb.CompactionTargetReadModel {
		t.Helper()
		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		return resp.GetTarget()
	}
	unknown := describe()
	require.Nil(t, unknown.RemainingRows)
	require.Zero(t, unknown.GetCheckedAt())
	require.NotEmpty(t, unknown.GetMessage())

	recovering = false
	require.EqualValues(t, 52, describe().GetRemainingRows())
	events, err = reconciler.Reconcile(ctx)
	require.NoError(t, err)
	var inputs []int64
	for _, view := range events[policy.TriggerTypeBumpSchemaVersion] {
		plans, err := view.Plan()
		require.NoError(t, err)
		for _, plan := range plans {
			inputs = append(inputs, plan.GetInputSegments()...)
			if plan.GetCollectionID() == 1 {
				require.EqualValues(t, 3, plan.GetSchema().GetVersion())
				require.EqualValues(t, 32<<20, plan.GetMaxSize())
			}
		}
	}
	require.ElementsMatch(t, []int64{101, 102, 201, 301}, inputs)
}

func TestStandingSchemaVersionWaitsForConfirmedCondition(t *testing.T) {
	ctx := t.Context()
	enableTestCompactionTargets(t)
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:               "true",
		cfg.BumpSchemaVersionCompactionEnabled.Key: "true",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	const targetID int64 = 100
	initial := &datapb.CompactionTarget{
		TargetID: targetID, Intent: datapb.TargetIntent_INTENT_SCHEMA_VERSION,
		TailLimit: -1, ActivatedAtTS: 10, ConditionRevision: 1, Enabled: proto.Bool(false),
	}
	catalog := catalogmocks.NewDataCoordCatalog(t)
	persisted := initial
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{initial}, nil).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, targetID).RunAndReturn(func(context.Context, int64) (*datapb.CompactionTarget, error) {
		return proto.CloneOf(persisted), nil
	})
	catalog.EXPECT().UpdateStandingCompactionTarget(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(merr.ErrServiceUnavailable).Once()
	catalog.EXPECT().UpdateStandingCompactionTarget(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _, next *datapb.CompactionTarget, _ uint64) error {
			persisted = proto.CloneOf(next)
			return nil
		}).Once()
	s, _ := newTargetRPCServer(t, catalog, nil)
	s.meta.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Version: 2}})
	s.meta.Segments.SetSegment(200, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 200, CollectionID: 1, PartitionID: 1, InsertChannel: "ch", State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L2, IsSorted: true, StorageVersion: storage.StorageV3,
		ManifestPath: "manifest-v3", SchemaVersion: 1, NumOfRows: 5,
	}))
	reconciler := targets.NewCompactionTargetReconciler(s.meta, nil, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), nil)
	round := func(enabled bool) {
		t.Helper()
		events, err := reconciler.Reconcile(ctx)
		require.NoError(t, err)
		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: targetID, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Equal(t, enabled, resp.GetTarget().GetEnabled())
		plans := events[policy.TriggerTypeBumpSchemaVersion]
		if enabled {
			require.Len(t, plans, 1)
			require.EqualValues(t, 5, resp.GetTarget().GetRemainingRows())
		} else {
			require.Empty(t, plans)
			require.Nil(t, resp.GetTarget().RemainingRows)
		}
	}
	round(false)
	round(true)
}
