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
	"path/filepath"
	"testing"

	"github.com/blang/semver/v4"
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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStorageStandingConditionsSurviveFailuresAndRestart(t *testing.T) {
	enableTestCompactionTargets(t)
	for _, cfg := range []struct {
		param *paramtable.ParamItem
		value string
	}{
		{&Params.DataCoordCfg.EnableAutoCompaction, "true"},
		{&Params.DataCoordCfg.EnableSortCompaction, "false"},
		{&Params.DataCoordCfg.StorageVersionCompactionEnabled, "true"},
		{&Params.DataCoordCfg.StorageFormatCompactionEnabled, "true"},
		{&Params.DataCoordCfg.StorageVersionCompactionSessionVersionRequirement, "2.6.9"},
		{&Params.CommonCfg.UseLoonFFI, "true"},
		{&Params.DataNodeCfg.StorageFormat, "vortex"},
		{&Params.CommonCfg.StorageType, "local"},
		{&Params.LocalStorageCfg.Path, t.TempDir()},
	} {
		require.NoError(t, Params.Save(cfg.param.Key, cfg.value))
		t.Cleanup(func() { Params.Reset(cfg.param.Key) })
	}

	ctx := t.Context()
	const versionID, formatID = int64(100), int64(101)
	saved := func(id int64, intent datapb.TargetIntent, key, value string) *datapb.CompactionTarget {
		return &datapb.CompactionTarget{
			TargetID: id, Intent: intent, TailLimit: -1, ActivatedAtTS: 50, Enabled: proto.Bool(true), ConditionRevision: 1,
			Properties: map[string]string{key: value},
		}
	}
	version := saved(versionID, datapb.TargetIntent_INTENT_STORAGE_VERSION, "storage_version", "2")
	format := saved(formatID, datapb.TargetIntent_INTENT_STORAGE_FORMAT, "storage_format", "parquet")
	catalog := catalogmocks.NewDataCoordCatalog(t)
	persisted := map[int64]*datapb.CompactionTarget{versionID: version, formatID: format}
	catalog.EXPECT().ListCompactionTargets(mock.Anything).RunAndReturn(func(context.Context) ([]*datapb.CompactionTarget, error) {
		records := make([]*datapb.CompactionTarget, 0, len(persisted))
		for _, record := range persisted {
			records = append(records, proto.CloneOf(record))
		}
		return records, nil
	})
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id int64) (*datapb.CompactionTarget, error) {
		return proto.CloneOf(persisted[id]), nil
	}).Maybe()
	catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget, _ *metastore.CompactionTargetCollectionVersion, _ uint64) (bool, error) {
			require.Nil(t, persisted[record.GetTargetID()])
			persisted[record.GetTargetID()] = proto.CloneOf(record)
			return false, nil
		}).Maybe()
	formatSaveFails := true
	catalog.EXPECT().UpdateStandingCompactionTarget(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, previous, next *datapb.CompactionTarget, _ uint64) error {
			require.True(t, proto.Equal(previous, persisted[previous.GetTargetID()]))
			if formatSaveFails && next.GetTargetID() == formatID {
				return merr.ErrIoFailed
			}
			persisted[next.GetTargetID()] = proto.CloneOf(next)
			return nil
		}).Maybe()

	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	t.Cleanup(s.meta.CloseManifestExecutors)
	s.meta.AddCollection(&meta.CollectionInfo{
		ID: 2, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{DataType: schemapb.DataType_Text}}},
	})
	add := func(id, collection, rows, storageVersion int64, manifest string) {
		s.meta.Segments.SetSegment(id, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: collection, PartitionID: 1, InsertChannel: "ch",
			Level: datapb.SegmentLevel_L2, State: commonpb.SegmentState_Flushed,
			StorageVersion: storageVersion, NumOfRows: rows, ManifestPath: manifest,
		}))
	}
	emptyManifest, err := packed.CommitManifestUpdates(
		packed.MarshalManifestPath(filepath.Join(Params.LocalStorageCfg.Path.GetValue(), "empty"), packed.ManifestEarliest),
		meta.CreateStorageConfig(), &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{Path: "delta.parquet", NumEntries: 1}}})
	require.NoError(t, err)
	add(10, 1, 7, storage.StorageV1, "")
	add(11, 1, 3, storage.StorageV3, emptyManifest)
	add(20, 2, 5, storage.StorageV3, emptyManifest)

	versionManager := index.NewMockVersionManager(t)
	session := semver.MustParse("3.0.0")
	versionManager.EXPECT().GetMinimalSessionVer().RunAndReturn(func() semver.Version { return session }).Maybe()
	absent := index.IndexVersionCondition{Mode: index.IndexVersionAbsent}
	versionManager.EXPECT().ResolveIndexVersionConditions(mock.Anything).Return(absent, absent).Maybe()

	round := func() []int64 {
		t.Helper()
		reconciler := targets.NewCompactionTargetReconciler(s.meta, &index.IndexMeta{}, s.compactionTargetMeta, meta.NewMockHandlerWithMeta(s.meta), s.allocator)
		reconciler.SetIndexVersionManager(versionManager)
		events, err := reconciler.Reconcile(ctx)
		require.NoError(t, err)
		var inputs []int64
		for _, view := range events[policy.TriggerTypeSingle] {
			plans, err := view.Plan()
			require.NoError(t, err)
			for _, plan := range plans {
				inputs = append(inputs, plan.GetInputSegments()...)
			}
		}
		return inputs
	}
	describe := func(id int64) *datapb.CompactionTargetReadModel {
		t.Helper()
		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		return resp.GetTarget()
	}

	require.Equal(t, []int64{10}, round())
	require.Equal(t, "3", describe(versionID).GetParameters()["storage_version"])
	require.EqualValues(t, 7, describe(versionID).GetRemainingRows())
	require.Equal(t, "parquet", describe(formatID).GetParameters()["storage_format"])
	require.Nil(t, describe(formatID).RemainingRows)
	require.Zero(t, describe(formatID).GetCheckedAt())

	formatSaveFails = false
	session = semver.MustParse("2.6.8")
	require.Empty(t, round())
	require.EqualValues(t, 7, describe(versionID).GetRemainingRows())
	require.Equal(t, "vortex", describe(formatID).GetParameters()["storage_format"])
	require.Nil(t, describe(formatID).RemainingRows)
	session = semver.MustParse("3.0.0")
	require.NoError(t, Params.Save(Params.CommonCfg.UseLoonFFI.Key, "false"))
	s.compactionTargetMeta, err = targets.NewCompactionTargetMetaForTest(ctx, catalog)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{10, 11}, round())
	require.Equal(t, "2", describe(versionID).GetParameters()["storage_version"])
	require.EqualValues(t, 15, describe(versionID).GetRemainingRows())
	disabled := describe(formatID)
	require.False(t, disabled.GetEnabled())
	require.NotNil(t, disabled.Enabled)
	require.Empty(t, disabled.GetParameters())
	require.Nil(t, disabled.RemainingRows)

	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "false"))
	require.Empty(t, round())
	require.False(t, describe(versionID).GetEnabled())
	for _, id := range []int64{versionID, formatID} {
		require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, describe(id).GetState())
		resp, err := s.DropCompactionTarget(ctx, &datapb.DropCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrOperationNotSupported)
	}
	for _, intent := range []datapb.TargetIntent{version.GetIntent(), format.GetIntent()} {
		count := 0
		for _, record := range persisted {
			if record.GetIntent() == intent {
				count++
			}
		}
		require.Equal(t, 1, count)
	}
}
