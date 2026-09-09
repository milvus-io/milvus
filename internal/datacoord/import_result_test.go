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
)

func TestValidateImportResults(t *testing.T) {
	results := []*datapb.SegmentResult{{
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

	results := []*datapb.SegmentResult{{
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

	results := []*datapb.SegmentResult{{Rows: 0}}
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

	results := []*datapb.SegmentResult{{
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

func TestValidateReshardManifest(t *testing.T) {
	manifest := &datapb.ReshardManifest{Fragments: []*datapb.FragmentDescriptor{{
		Path: "fragment.parquet", Rows: 10, LogicalBytes: 100,
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
	v3Result := []*datapb.SegmentResult{{
		Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
		ManifestPath: "import_v3/manifest/manifest.pb",
	}}
	require.Error(t, applyImportResults(ctx, meta, 2, 4, []int64{100}, true, false, v3Result))
	// The failed operator aborts the whole update batch: nothing persisted.
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())

	// V3-recorded segment + V2-shaped result (insert logs).
	_, err = addImportSegment(ctx, meta, 200, 1, 20, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV3, 4)
	require.NoError(t, err)
	v2Result := []*datapb.SegmentResult{{
		Rows: 10, Statistics: &datapb.Statistics{TimestampFrom: 1, TimestampTo: 100},
		InsertLogs: []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{
			LogPath: "files/insert_log/2/3/200/0/555", EntriesNum: 10,
		}}}},
	}}
	require.Error(t, applyImportResults(ctx, meta, 2, 4, []int64{200}, true, false, v2Result))
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
	results := []*datapb.SegmentResult{{
		Rows:        10,
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
