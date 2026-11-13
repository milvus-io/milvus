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

package targets

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestStorageStandingPlansLegacyL2WithStorageEligibility(t *testing.T) {
	enableCompactionTargetReconciler(t)
	setStorageTargetParam(t, Params.CommonCfg.UseLoonFFI.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageFormatCompactionEnabled.Key, "false")
	setStorageTargetParam(t, Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.EnableSortCompaction.Key, "false")
	setStorageTargetParam(t, Params.DataCoordCfg.IndexBasedCompaction.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageVersionCompactionSessionVersionRequirement.Key, "2.6.9")

	ctx := t.Context()
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx)
	segment := targetSegmentWithDataTS(17, 1, 10, "ch", 0, 199, false)
	segment.StorageVersion = storage.StorageV1
	segment.Level = datapb.SegmentLevel_L2
	segment.IsInvisible = true
	fixture := newCompactionTargetReconcilerTestMeta(t, targetMeta, segment)
	fixture.AddCollection(&meta.CollectionInfo{
		ID: 1, Schema: &schemapb.CollectionSchema{},
		Properties: map[string]string{common.CollectionAutoCompactionKey: "false"},
	})
	reconciler := newCompactionTargetReconcilerForTest(fixture)
	versionManager := index.NewMockVersionManager(t)
	versionManager.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.6.9")).Maybe()
	reconciler.SetIndexVersionManager(versionManager)

	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
	require.Len(t, plans, 1)
	require.Equal(t, datapb.CompactionType_MixCompaction, plans[0].GetType())
	require.Equal(t, []int64{17}, plans[0].GetInputSegments())

	version := storageTestRecord(t, targetMeta, datapb.TargetIntent_INTENT_STORAGE_VERSION)
	require.True(t, version.GetEnabled())
	require.Equal(t, "3", version.GetProperties()["storage_version"])
}

func TestStorageStandingFormatObservesPublishedManifestRevision(t *testing.T) {
	enableCompactionTargetReconciler(t)
	setStorageTargetParam(t, Params.CommonCfg.StorageType.Key, "local")
	setStorageTargetParam(t, Params.LocalStorageCfg.Path.Key, t.TempDir())
	setStorageTargetParam(t, Params.CommonCfg.UseLoonFFI.Key, "true")
	setStorageTargetParam(t, Params.DataNodeCfg.StorageFormat.Key, "parquet")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageFormatCompactionEnabled.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.EnableSortCompaction.Key, "false")

	ctx := t.Context()
	basePath := filepath.Join(Params.LocalStorageCfg.Path.GetValue(), "segment")
	first := writeStorageTargetManifestColumn(t, packed.MarshalManifestPath(basePath, packed.ManifestEarliest), 100, "parquet", false)
	mixed := writeStorageTargetManifestColumn(t, first, 101, "vortex", true)
	segment := sortedTargetSegment(17, 1, 10, "ch", 0, 199, false)
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = first
	segment.NumOfRows = 1
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx)
	fixture := newCompactionTargetReconcilerTestMeta(t, targetMeta, segment)
	t.Cleanup(fixture.CloseManifestExecutors)
	fixture.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{}})
	reconciler := newCompactionTargetReconcilerForTest(fixture)
	versionManager := index.NewMockVersionManager(t)
	versionManager.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("3.0.0")).Maybe()
	reconciler.SetIndexVersionManager(versionManager)

	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	format := storageTestRecord(t, targetMeta, datapb.TargetIntent_INTENT_STORAGE_FORMAT)
	require.True(t, format.GetEnabled())
	require.Equal(t, "parquet", format.GetProperties()["storage_format"])
	rows, err := ObserveStorage(ctx, fixture.Meta, format)
	require.NoError(t, err)
	require.Zero(t, rows)

	updated := segment.Clone()
	updated.ManifestPath = mixed
	fixture.Segments.SetSegment(updated.GetID(), updated)
	events, err = reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
	require.Len(t, plans, 1)
	require.Equal(t, []int64{17}, plans[0].GetInputSegments())
	rows, err = ObserveStorage(ctx, fixture.Meta, format)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
	version := storageTestRecord(t, targetMeta, datapb.TargetIntent_INTENT_STORAGE_VERSION)
	rows, err = ObserveStorage(ctx, fixture.Meta, version)
	require.NoError(t, err)
	require.Zero(t, rows)
}

func TestStorageStandingKeepsVersionWorkWhenManifestReadFails(t *testing.T) {
	enableCompactionTargetReconciler(t)
	setStorageTargetParam(t, Params.CommonCfg.StorageType.Key, "local")
	setStorageTargetParam(t, Params.LocalStorageCfg.Path.Key, t.TempDir())
	setStorageTargetParam(t, Params.CommonCfg.UseLoonFFI.Key, "true")
	setStorageTargetParam(t, Params.DataNodeCfg.StorageFormat.Key, "vortex")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.StorageFormatCompactionEnabled.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	setStorageTargetParam(t, Params.DataCoordCfg.EnableSortCompaction.Key, "false")

	ctx := t.Context()
	basePath := filepath.Join(Params.LocalStorageCfg.Path.GetValue(), "segment")
	manifest := writeStorageTargetManifestColumn(t, packed.MarshalManifestPath(basePath, packed.ManifestEarliest), 100, "parquet", false)
	manifestFile, err := packed.ManifestFilePath(manifest)
	require.NoError(t, err)
	hidden := manifestFile + ".saved"
	require.NoError(t, os.Rename(manifestFile, hidden))

	segment := sortedTargetSegment(17, 1, 10, "ch", 0, 199, false)
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = manifest
	segment.NumOfRows = 1
	legacy := sortedTargetSegment(18, 1, 10, "ch", 0, 199, false)
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx)
	fixture := newCompactionTargetReconcilerTestMeta(t, targetMeta, segment, legacy)
	t.Cleanup(fixture.CloseManifestExecutors)
	fixture.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{}})
	reconciler := newCompactionTargetReconcilerForTest(fixture)
	versionManager := index.NewMockVersionManager(t)
	versionManager.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("3.0.0")).Maybe()
	reconciler.SetIndexVersionManager(versionManager)

	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
	require.Len(t, plans, 1)
	require.Equal(t, []int64{18}, plans[0].GetInputSegments())
	version := storageTestRecord(t, targetMeta, datapb.TargetIntent_INTENT_STORAGE_VERSION)
	format := storageTestRecord(t, targetMeta, datapb.TargetIntent_INTENT_STORAGE_FORMAT)
	rows, err := ObserveStorage(ctx, fixture.Meta, version)
	require.NoError(t, err)
	require.Equal(t, legacy.GetNumOfRows(), rows)
	_, err = ObserveStorage(ctx, fixture.Meta, format)
	require.Error(t, err)

	require.NoError(t, os.Rename(hidden, manifestFile))
	rows, err = ObserveStorage(ctx, fixture.Meta, format)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
	events, err = reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans = plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
	var plannedIDs []int64
	for _, plan := range plans {
		plannedIDs = append(plannedIDs, plan.GetInputSegments()...)
	}
	require.ElementsMatch(t, []int64{17, 18}, plannedIDs)
}

func writeStorageTargetManifestColumn(t *testing.T, manifest string, fieldID int64, format string, addGroup bool) string {
	t.Helper()
	name := strconv.FormatInt(fieldID, 10)
	schema := arrow.NewSchema([]arrow.Field{{
		Name: name, Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{name}),
	}}, nil)
	base, _, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	config := meta.CreateStorageConfig()
	groups := []storagecommon.ColumnGroup{{Columns: []int{0}, Fields: []int64{fieldID}, GroupID: fieldID}}
	writer, err := packed.NewFFIPackedWriter(base, schema, groups, config, nil, map[string]string{packed.PropertyWriterFormat: format})
	require.NoError(t, err)
	if addGroup {
		writer.AsNewColumnGroups()
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))
	files, err := writer.Close()
	require.NoError(t, err)
	defer files.Destroy()
	updated, err := packed.CommitManifestUpdates(manifest, config, &packed.ManifestUpdates{NewFiles: files})
	require.NoError(t, err)
	return updated
}

func setStorageTargetParam(t *testing.T, key, value string) {
	t.Helper()
	require.NoError(t, Params.Save(key, value))
	t.Cleanup(func() { Params.Reset(key) })
}

func storageTestRecord(t *testing.T, targetMeta *CompactionTargetMeta, intent datapb.TargetIntent) *datapb.CompactionTarget {
	t.Helper()
	var found *datapb.CompactionTarget
	for _, record := range targetMeta.GetCompactionTargets() {
		if record.GetIntent() == intent {
			require.Nil(t, found)
			found = record
		}
	}
	require.NotNil(t, found)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, found.GetState())
	require.Zero(t, found.GetCollectionID())
	return found
}
