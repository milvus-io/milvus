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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	materializationSegmentID = int64(500)
	materializationOutputFID = int64(102)
	materializationPlanID    = int64(7001)
)

func newMaterializationTask(meta *meta, schemaVersion int32) *bumpSchemaVersionTask {
	return newBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:        materializationPlanID,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:        &schemapb.CollectionSchema{Version: schemaVersion},
		InputSegments: []int64{materializationSegmentID},
	}, nil, meta, nil)
}

func addMaterializationSegment(t *testing.T, meta *meta, manifestPath string, schemaVersion int32, opts ...func(*datapb.SegmentInfo)) {
	info := &datapb.SegmentInfo{
		ID:             materializationSegmentID,
		CollectionID:   1,
		PartitionID:    10,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		SchemaVersion:  schemaVersion,
	}
	for _, opt := range opts {
		opt(info)
	}
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(info)))
}

// materializationResult builds an in-place materialization result: one segment
// carrying the manifest delta (column-group descriptors), the InsertLogs that
// map the new column group onto SegmentInfo.Binlogs, and the footprint Stats
// increment.
func materializationResult(stats *datapb.Statistics) *datapb.CompactionPlanResult {
	return &datapb.CompactionPlanResult{
		PlanID: materializationPlanID,
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{{
			SegmentID:  materializationSegmentID,
			NumOfRows:  3,
			InsertLogs: []*datapb.FieldBinlog{{FieldID: materializationOutputFID, Binlogs: []*datapb.Binlog{{LogID: 9001, EntriesNum: 3}}}},
			Stats:      stats,
			ManifestDelta: &datapb.SegmentManifestDelta{
				ColumnGroups: []*datapb.ManifestColumnGroup{{
					Columns: []string{"sparse"},
					Format:  "parquet",
					Files:   []*datapb.ManifestColumnGroupFile{{Path: "cg/9001", StartIndex: 0, EndIndex: 3}},
				}},
			},
		}},
	}
}

func TestIsMaterializationResult(t *testing.T) {
	delta := &datapb.SegmentManifestDelta{ColumnGroups: []*datapb.ManifestColumnGroup{{Format: "parquet"}}}
	seg := func(d *datapb.SegmentManifestDelta) *datapb.CompactionSegment {
		return &datapb.CompactionSegment{SegmentID: 1, ManifestDelta: d}
	}

	assert.True(t, isMaterializationResult(&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{seg(delta)}}))
	// No manifest delta -> version-bump-only / full-rewrite, adopted through
	// CompleteCompactionMutation.
	assert.False(t, isMaterializationResult(&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{seg(nil)}}))
	// A full rewrite produces a fresh segment but never a manifest delta; more
	// than one segment is never in-place.
	assert.False(t, isMaterializationResult(&datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{seg(delta), seg(delta)}}))
	assert.False(t, isMaterializationResult(&datapb.CompactionPlanResult{}))
}

func TestCommitBumpV3MaterializationHappyPath(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/500"
	currentManifest := packed.MarshalManifestPath(basePath, 5)
	newManifest := packed.MarshalManifestPath(basePath, 6)

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, currentManifest, 3)

	// DataCoord must run the loon transaction on the segment's CURRENT manifest
	// (version 5), not on the datanode's plan-time base, and stage the shipped
	// column-group descriptors.
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			assert.Equal(t, basePath, base)
			assert.EqualValues(t, 5, version)
			require.Len(t, updates.ColumnGroups, 1)
			assert.Equal(t, "parquet", updates.ColumnGroups[0].Format)
			require.Len(t, updates.ColumnGroups[0].Files, 1)
			assert.Equal(t, "cg/9001", updates.ColumnGroups[0].Files[0].Path)
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	task := newMaterializationTask(meta, 7)
	ids, err := task.commitBumpV3Materialization(context.Background(), materializationResult(&datapb.Statistics{
		InsertBinlogSize:  256,
		InsertBinlogCount: 1,
		StatsBinlogSize:   64,
	}))
	require.NoError(t, err)
	require.Equal(t, []UniqueID{materializationSegmentID}, ids)

	updated := meta.GetSegment(context.Background(), materializationSegmentID)
	require.NotNil(t, updated)
	// Manifest pointer advanced to the freshly committed revision.
	assert.Equal(t, newManifest, updated.GetManifestPath())
	// Schema version advanced to the task target.
	assert.EqualValues(t, 7, updated.GetSchemaVersion())
	// The new column group is registered so the materialized field is indexable.
	var found bool
	for _, fb := range updated.GetBinlogs() {
		if fb.GetFieldID() == materializationOutputFID {
			found = true
		}
	}
	assert.True(t, found, "materialized output column group must be merged into Binlogs")
	// The footprint increment is folded onto the segment's Stats.
	assert.EqualValues(t, 256, updated.GetStats().GetInsertBinlogSize())
	assert.EqualValues(t, 1, updated.GetStats().GetInsertBinlogCount())
	assert.EqualValues(t, 64, updated.GetStats().GetStatsBinlogSize())
}

func TestCommitBumpV3MaterializationRechecksSnapshotProtectionBeforePublication(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/500"
	currentManifest := packed.MarshalManifestPath(basePath, 5)
	newManifest := packed.MarshalManifestPath(basePath, 6)

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, currentManifest, 3)
	sm := createTestSnapshotMetaLoaded(t)
	meta.snapshotMeta = sm

	task := newMaterializationTask(meta, 7)
	require.NoError(t, meta.ValidateSegmentStateBeforeCompleteCompactionMutation(task.GetTaskProto()))

	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(string, int64, *indexpb.StorageConfig, *packed.ManifestUpdates) (string, error) {
			// Reproduce the seam: snapshot creation begins after the task's initial
			// validation but before the prepared manifest is published under segMu.
			sm.SetSnapshotPending(1)
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	_, err = task.commitBumpV3Materialization(context.Background(), materializationResult(nil))
	require.ErrorIs(t, err, merr.ErrCompactionBlocked)

	updated := meta.GetSegment(context.Background(), materializationSegmentID)
	require.Equal(t, currentManifest, updated.GetManifestPath())
	require.EqualValues(t, 3, updated.GetSchemaVersion())
	require.Empty(t, updated.GetBinlogs())

	// Snapshot protection is a compaction admission gate, so ordinary manifest
	// writers that do not opt into the precondition remain unaffected.
	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     materializationSegmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{},
		},
	}))
	require.Equal(t, newManifest, meta.GetSegment(context.Background(), materializationSegmentID).GetManifestPath())
}

func TestCommitBumpV3MaterializationReplayShortCircuits(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/500"
	currentManifest := packed.MarshalManifestPath(basePath, 5)

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	// Segment already carries the target schema version: a prior attempt
	// committed and persisted. This is the restart-safe replay guard — it must
	// hold even though a recovered V3 segment's in-memory Binlogs are empty.
	addMaterializationSegment(t, meta, currentManifest, 7)

	called := false
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(string, int64, *indexpb.StorageConfig, *packed.ManifestUpdates) (string, error) {
			called = true
			return "", nil
		},
	).Build()
	defer commit.UnPatch()

	task := newMaterializationTask(meta, 7)
	ids, err := task.commitBumpV3Materialization(context.Background(), materializationResult(nil))
	require.NoError(t, err)
	require.Equal(t, []UniqueID{materializationSegmentID}, ids)
	assert.False(t, called, "replay must short-circuit before any manifest transaction")
	assert.Equal(t, currentManifest, meta.GetSegment(context.Background(), materializationSegmentID).GetManifestPath())
}

func TestCommitBumpV3MaterializationRejectsStaleSchemaVersion(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/500"
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	// Segment schema version is newer than this task's target: a newer bump
	// already superseded it.
	addMaterializationSegment(t, meta, packed.MarshalManifestPath(basePath, 5), 9)

	task := newMaterializationTask(meta, 7)
	_, err = task.commitBumpV3Materialization(context.Background(), materializationResult(nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIllegalCompactionPlan)
}

func TestCommitBumpV3MaterializationSegmentNotFound(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	task := newMaterializationTask(meta, 7)
	_, err = task.commitBumpV3Materialization(context.Background(), materializationResult(nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegmentNotFound)
}

func TestCommitBumpV3MaterializationRequiresPublishedV3Manifest(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	// StorageV3 but no manifest path published yet.
	addMaterializationSegment(t, meta, "", 3)
	task := newMaterializationTask(meta, 7)
	_, err = task.commitBumpV3Materialization(context.Background(), materializationResult(nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestCommitBumpV3MaterializationRejectsInvisibleSegment(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/500"
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, packed.MarshalManifestPath(basePath, 5), 3, func(info *datapb.SegmentInfo) {
		info.IsInvisible = true
	})
	task := newMaterializationTask(meta, 7)
	_, err = task.commitBumpV3Materialization(context.Background(), materializationResult(nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIllegalCompactionPlan)
}

func TestCommitBumpV3MaterializationRequiresManifestDelta(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/500"
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, packed.MarshalManifestPath(basePath, 5), 3)
	task := newMaterializationTask(meta, 7)

	result := materializationResult(nil)
	result.Segments[0].ManifestDelta = &datapb.SegmentManifestDelta{} // empty column groups
	_, err = task.commitBumpV3Materialization(context.Background(), result)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIllegalCompactionPlan)
}

func TestUpdateBumpSchemaVersionMaterializationOperatorApplies(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/500", 5), 3, func(info *datapb.SegmentInfo) {
		info.Binlogs = []*datapb.FieldBinlog{{FieldID: 100, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{{LogID: 1}}}}
	})

	newGroups := []*datapb.FieldBinlog{{FieldID: materializationOutputFID, ChildFields: []int64{materializationOutputFID}, Binlogs: []*datapb.Binlog{{LogID: 9001}}}}
	err = meta.UpdateSegmentsInfo(context.Background(), UpdateBumpSchemaVersionMaterializationOperator(
		materializationSegmentID, 7, newGroups, &datapb.Statistics{InsertBinlogSize: 100},
	))
	require.NoError(t, err)

	updated := meta.GetSegment(context.Background(), materializationSegmentID)
	fieldIDs := make([]int64, 0, len(updated.GetBinlogs()))
	for _, fb := range updated.GetBinlogs() {
		fieldIDs = append(fieldIDs, fb.GetFieldID())
	}
	assert.ElementsMatch(t, []int64{100, materializationOutputFID}, fieldIDs)
	assert.EqualValues(t, 7, updated.GetSchemaVersion())
	assert.EqualValues(t, 100, updated.GetStats().GetInsertBinlogSize())
	// Binlogs changed, so querynodes with the segment loaded must Reopen.
	assert.EqualValues(t, 1, updated.GetDataVersion())
}

func TestUpdateBumpSchemaVersionMaterializationOperatorNeverRegressesSchemaVersion(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/500", 5), 9)

	err = meta.UpdateSegmentsInfo(context.Background(), UpdateBumpSchemaVersionMaterializationOperator(
		materializationSegmentID, 7, nil, nil,
	))
	require.NoError(t, err)
	assert.EqualValues(t, 9, meta.GetSegment(context.Background(), materializationSegmentID).GetSchemaVersion())
}

func TestUpdateBumpSchemaVersionMaterializationOperatorRejectsDroppedGroups(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	addMaterializationSegment(t, meta, packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/500", 5), 3, func(info *datapb.SegmentInfo) {
		// Existing group 200's only child field is 102.
		info.Binlogs = []*datapb.FieldBinlog{{FieldID: 200, ChildFields: []int64{materializationOutputFID}, Binlogs: []*datapb.Binlog{{LogID: 1}}}}
	})

	// The new group claims child field 102, which would empty group 200.
	// Materialization only ever ADDS output-field groups, so this must be refused
	// rather than silently orphan a pre-existing group's data.
	newGroups := []*datapb.FieldBinlog{{FieldID: materializationOutputFID, ChildFields: []int64{materializationOutputFID}, Binlogs: []*datapb.Binlog{{LogID: 9001}}}}
	err = meta.UpdateSegmentsInfo(context.Background(), UpdateBumpSchemaVersionMaterializationOperator(
		materializationSegmentID, 7, newGroups, nil,
	))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}
