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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
)

func TestValidateImportResults(t *testing.T) {
	results := []*datapb.ImportTaskV3Result{{
		Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
	}}
	require.NoError(t, validateImportResults(results, 1))
	results[0].Statistics.TimestampFrom = 101
	require.Error(t, validateImportResults(results, 1))
}

func TestApplyImportResultToPreallocatedSegment(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)

	results := []*datapb.ImportTaskV3Result{{
		Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
	}}
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, results))

	segment := meta.GetSegment(ctx, 100)
	require.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
	require.True(t, segment.GetIsImporting())
	require.False(t, segment.GetIsInvisible())
	require.Equal(t, int64(10), segment.GetNumOfRows())
}

// TestApplyImportResultsZeroRowDropsPlaceholderAtAcceptance pins that a
// zero-row result drops the preallocated placeholder at acceptance instead of
// leaving it Importing for worker-drop cleanup, and that replaying the same
// result (crash between acceptance and the task Completed marker) is a no-op.
func TestApplyImportResultsZeroRowDropsPlaceholderAtAcceptance(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())

	results := []*datapb.ImportTaskV3Result{{Rows: 0}}
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, results))
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 100).GetState())

	// Replay: already-Dropped passes validation and the drop is skipped.
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, results))
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 100).GetState())
}

// TestApplyImportResultsCompressesStorageV2Binlogs pins that StorageV2 writer
// results (LogPath set, LogID zero) are compressed before persistence; the
// catalog rejects zero LogIDs and stored LogPaths, so without compression the
// update below would fail.
func TestApplyImportResultsCompressesStorageV2Binlogs(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
	require.NoError(t, err)

	results := []*datapb.ImportTaskV3Result{{
		Rows:       10,
		Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
		InsertLogs: []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/2/3/100/0/555", EntriesNum: 10,
		}}}},
		PkLog: &datapb.FieldBinlog{FieldID: 100, Binlogs: []*datapb.Binlog{{
			LogPath: "files/stats_log/2/3/100/100/666", EntriesNum: 10,
		}}},
	}}
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, results))

	segment := meta.GetSegment(ctx, 100)
	require.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
	require.Len(t, segment.GetBinlogs(), 1)
	require.Equal(t, int64(555), segment.GetBinlogs()[0].GetBinlogs()[0].GetLogID())
	require.Empty(t, segment.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	require.Len(t, segment.GetStatslogs(), 1)
	require.Equal(t, int64(666), segment.GetStatslogs()[0].GetBinlogs()[0].GetLogID())
	require.Empty(t, segment.GetStatslogs()[0].GetBinlogs()[0].GetLogPath())
}

// TestValidateImportResultsRejectsInvalidResults pins the row-count and
// zero-row-with-output branches of the worker result contract: a negative row
// count, a result-count mismatch, and a zero-row segment that still carries any
// output are all data-integrity errors.
func TestValidateImportResultsRejectsInvalidResults(t *testing.T) {
	require.ErrorContains(t, validateImportResults(nil, 1), "segment count mismatch")
	require.ErrorContains(t, validateImportResults([]*datapb.ImportTaskV3Result{{Rows: -1}}, 1), "invalid import segment result")
	require.ErrorContains(t, validateImportResults([]*datapb.ImportTaskV3Result{{Rows: 0, Statistics: &datapb.Statistics{}}}, 1), "has output")
	require.ErrorContains(t, validateImportResults([]*datapb.ImportTaskV3Result{{Rows: 0, InsertLogs: []*datapb.FieldBinlog{{FieldID: 1}}}}, 1), "has output")
	require.ErrorContains(t, validateImportResults([]*datapb.ImportTaskV3Result{{Rows: 0, ManifestPath: "m"}}, 1), "has output")
	require.NoError(t, validateImportResults([]*datapb.ImportTaskV3Result{{Rows: 0}}, 1))
}

// TestApplyImportResultsRejectsInvalidSegmentStates drives the pre-acceptance
// per-segment guards through applyImportResults: a target that is not importing,
// a collection mismatch, and an Importing placeholder that already carries
// output must each abort the whole update batch.
func TestApplyImportResultsRejectsInvalidSegmentStates(t *testing.T) {
	ctx := context.Background()
	newResult := func() []*datapb.ImportTaskV3Result {
		return []*datapb.ImportTaskV3Result{{Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100}}}
	}

	t.Run("segment not importing", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		require.NoError(t, err)
		_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
		require.NoError(t, err)
		require.NoError(t, meta.UpdateSegmentsInfo(ctx, UpdateIsImporting(100, false)))
		err = applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, newResult())
		require.ErrorContains(t, err, "is not importing")
	})

	t.Run("collection mismatch", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		require.NoError(t, err)
		_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
		require.NoError(t, err)
		err = applyImportResults(ctx, meta, 999, 4, []int64{100}, true, false, newResult())
		require.ErrorContains(t, err, "collection mismatch")
		// The failed operator aborts the batch: nothing was persisted.
		require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
	})

	t.Run("importing placeholder carries output", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		require.NoError(t, err)
		_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
		require.NoError(t, err)
		require.NoError(t, meta.UpdateSegmentsInfo(ctx, UpdateImportedRows(100, 5)))
		err = applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, newResult())
		require.ErrorContains(t, err, "contains unaccepted output")
	})
}

// TestApplyImportResultsRejectsReplayConflict pins the non-zero replay path
// (acceptedImportSegmentMatches): a byte-identical replay of an accepted result
// is a clean no-op, while a different non-zero result for the same Flushed
// segment is a conflict.
func TestApplyImportResultsRejectsReplayConflict(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
	require.NoError(t, err)

	accepted := []*datapb.ImportTaskV3Result{{Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100}}}
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, accepted))

	// Identical replay: acceptedImportSegmentMatches holds, nothing changes.
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, accepted))
	require.Equal(t, int64(10), meta.GetSegment(ctx, 100).GetNumOfRows())

	// A different non-zero result conflicts with the accepted projection.
	conflict := []*datapb.ImportTaskV3Result{{Rows: 20, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 200}}}
	require.ErrorContains(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, conflict), "conflicts with segment")
	require.Equal(t, int64(10), meta.GetSegment(ctx, 100).GetNumOfRows())
}

func TestValidateReshardManifest(t *testing.T) {
	manifest := &importv3pb.ReshardManifest{Fragments: []*importv3pb.FragmentDescriptor{{
		Path: "fragment.parquet", Rows: 10, LogicalBytes: 100, PartitionId: 3,
	}}}
	require.NoError(t, validateReshardManifest(manifest))
	manifest.Fragments = append(manifest.Fragments, manifest.Fragments[0])
	require.Error(t, validateReshardManifest(manifest))
}

// TestApplyImportResultsRejectsStorageVersionMismatch pins the post-validation:
// the worker derives its writer layout from live config at dispatch while the
// segment froze its storage version at planning, so a hot-flipped
// common.storage.useLoonFFI between the two produces a result whose shape
// disagrees with the segment record. Acceptance must fail loudly instead of
// persisting data QueryNode cannot load by the recorded version.
func TestApplyImportResultsRejectsStorageVersionMismatch(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
	require.NoError(t, err)

	// V2-recorded segment + V3-shaped result (manifest path, no binlogs).
	importV3Result := []*datapb.ImportTaskV3Result{{
		Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
		ManifestPath: "import_v3/manifest/manifest.pb",
	}}
	require.Error(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, importV3Result))
	// The failed operator aborts the whole update batch: nothing persisted.
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())

	// V3-recorded segment + V2-shaped result (insert logs).
	_, err = addImportSegment(ctx, meta, 200, 1, 20, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV3, 4)
	require.NoError(t, err)
	importV2Result := []*datapb.ImportTaskV3Result{{
		Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
		InsertLogs: []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/2/3/200/0/555", EntriesNum: 10,
		}}}},
	}}
	require.Error(t, applyImportResults(ctx, meta, 2, 4, []int64{200}, true, false, importV2Result))
}

// TestApplyImportResultsV3FieldSizeFromLoadResource reproduces the index-slot
// root cause end to end: the V3 worker ships only the manifest path plus a
// Statistics carrying LoadResource column groups (built by
// storage.BuildStatsFromFieldBinlogs from the writer's fieldBinlogs). After
// acceptance, getFieldBinlogSize must resolve the vector field's size from
// those groups — not fall back to the whole-segment aggregate, which sizes
// index task slots by every field's bytes and starves build concurrency.
func TestApplyImportResultsV3FieldSizeFromLoadResource(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV3, 4)
	require.NoError(t, err)

	vectorGroupBytes := int64(200 * 1024 * 1024)
	results := []*datapb.ImportTaskV3Result{{
		Rows:         10,
		ManifestPath: "import_v3/manifest/manifest.pb",
		Statistics: &datapb.Statistics{
			TimestampFrom:    1,
			TimestampTo:      100,
			InsertBinlogSize: 900 * 1024 * 1024, // whole-segment aggregate
			LoadResource: &datapb.LoadResourceStatistics{
				ColumnGroups: []*datapb.ColumnGroupStatistics{
					{GroupId: 0, FieldIds: []int64{100, 101}, MemorySize: 700 * 1024 * 1024},
					{GroupId: 102, MemorySize: vectorGroupBytes},
				},
			},
		},
	}}
	require.NoError(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, results))

	segment := meta.GetSegment(ctx, 100)
	require.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
	require.Empty(t, segment.GetBinlogs())
	require.Equal(t, vectorGroupBytes, segment.getFieldBinlogSize(102))
	require.Equal(t, int64(700*1024*1024), segment.getFieldBinlogSize(100))
}
