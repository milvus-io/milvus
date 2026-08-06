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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// compactionRecordingKV wraps the in-memory meta KV so a test can assert the
// call shape of a compaction completion write (composite single-txn vs the
// legacy chunked MultiSave) and inject crashes on either commit path.
type compactionRecordingKV struct {
	*metaMemoryKV

	// txnOpsLimit overrides MaxTxnOps when > 0, to force the chunked
	// fallback path of the composite txn engine.
	txnOpsLimit int

	multiSaveCalls          int
	multiSaveAndRemoveCalls int

	// failNextMultiSaveAndRemove fails the next N MultiSaveAndRemove calls
	// before touching the store (an atomic-txn crash: nothing persists).
	failNextMultiSaveAndRemove int
	// failMultiSaveAtCall fails the Nth MultiSave call (1-based), once,
	// before touching the store (a mid-fallback crash: earlier chunks stay).
	failMultiSaveAtCall int
}

var errInjectedCompactionCrash = errors.New("injected compaction meta write crash")

func (r *compactionRecordingKV) MaxTxnOps() int {
	if r.txnOpsLimit > 0 {
		return r.txnOpsLimit
	}
	return r.metaMemoryKV.MaxTxnOps()
}

func (r *compactionRecordingKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	r.multiSaveCalls++
	if r.failMultiSaveAtCall > 0 && r.multiSaveCalls == r.failMultiSaveAtCall {
		r.failMultiSaveAtCall = 0
		return errInjectedCompactionCrash
	}
	return r.metaMemoryKV.MultiSave(ctx, kvs)
}

func (r *compactionRecordingKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	r.multiSaveAndRemoveCalls++
	if r.failNextMultiSaveAndRemove > 0 {
		r.failNextMultiSaveAndRemove--
		return errInjectedCompactionCrash
	}
	return r.metaMemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

func dumpMetaStore(t *testing.T, k kv.MetaKv) map[string]string {
	keys, values, err := k.LoadWithPrefix(context.TODO(), "")
	require.NoError(t, err)
	out := make(map[string]string, len(keys))
	for i := range keys {
		out[keys[i]] = values[i]
	}
	return out
}

// replayLegacyAlterSegments applies the pre-composite catalog.AlterSegments
// encoding for the given segments/increments on a fresh store and returns its
// dump - the byte-for-byte reference every converted completion path must
// match.
func replayLegacyAlterSegments(t *testing.T, segments []*datapb.SegmentInfo, increments ...metastore.BinlogsIncrement) map[string]string {
	refKV := NewMetaMemoryKV()
	refCatalog := datacoord.NewCatalog(refKV, "", "")
	require.NoError(t, refCatalog.AlterSegments(context.TODO(), segments, increments...))
	return dumpMetaStore(t, refKV)
}

func newCompactionTestMeta(k kv.MetaKv, segments *SegmentsInfo) *meta {
	return &meta{
		ctx:      context.TODO(),
		catalog:  datacoord.NewCatalog(k, "", ""),
		segments: segments,
	}
}

func clusterCompactionInputs() *SegmentsInfo {
	segments := NewSegmentsInfo()
	for segID, segment := range map[UniqueID]*SegmentInfo{
		1: {SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Flushed,
			Level:        datapb.SegmentLevel_L1,
			Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
			Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
			NumOfRows:    2,
			MaxRowNum:    100,
		}},
		2: {SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: 100,
			PartitionID:  10,
			State:        commonpb.SegmentState_Flushed,
			Level:        datapb.SegmentLevel_L1,
			Binlogs:      []*datapb.FieldBinlog{getFieldBinlogIDs(0, 11000)},
			Statslogs:    []*datapb.FieldBinlog{getFieldBinlogIDs(0, 21000)},
			NumOfRows:    2,
			MaxRowNum:    100,
		}},
	} {
		segments.SetSegment(segID, segment)
	}
	return segments
}

func clusterCompactionTaskAndResult(compactToIDs ...int64) (*datapb.CompactionTask, *datapb.CompactionPlanResult) {
	task := &datapb.CompactionTask{
		PlanID:        19530,
		InputSegments: []UniqueID{1, 2},
		Type:          datapb.CompactionType_ClusteringCompaction,
		Channel:       "ch-1",
		StartTime:     1700000000,
		Schema:        &schemapb.CollectionSchema{Version: 2},
	}
	result := &datapb.CompactionPlanResult{}
	for _, id := range compactToIDs {
		result.Segments = append(result.Segments, &datapb.CompactionSegment{
			SegmentID:           id,
			InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, id*1000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, id*1000+1)},
			NumOfRows:           2,
		})
	}
	return task, result
}

// TestCompleteClusterCompactionMutation_Composite proves the clustering
// completion persists every compactTo segment through the composite catalog
// Update - one atomic txn instead of the legacy chunked AlterSegments
// (SaveByBatch) - while staying byte-for-byte identical to the legacy
// encoding, and that a crash of the composite write is recoverable: an atomic
// crash persists nothing (memory untouched), and a retry converges to the
// legacy bytes.
func TestCompleteClusterCompactionMutation_Composite(t *testing.T) {
	t.Run("single atomic txn, bytes match legacy", func(t *testing.T) {
		rec := &compactionRecordingKV{metaMemoryKV: NewMetaMemoryKV()}
		m := newCompactionTestMeta(rec, clusterCompactionInputs())
		task, result := clusterCompactionTaskAndResult(11, 12)

		infos, mutation, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		require.NoError(t, err)
		require.Len(t, infos, 2)
		require.NotNil(t, mutation)

		// The whole write must be one composite txn - no legacy chunked
		// MultiSave.
		assert.Equal(t, 0, rec.multiSaveCalls)
		assert.Equal(t, 1, rec.multiSaveAndRemoveCalls)

		// Byte-for-byte parity with the legacy AlterSegments encoding
		// (record + binlog KVs per compactTo segment).
		segments := make([]*datapb.SegmentInfo, 0, len(infos))
		increments := make([]metastore.BinlogsIncrement, 0, len(infos))
		for _, info := range infos {
			segments = append(segments, info.SegmentInfo)
			increments = append(increments, metastore.BinlogsIncrement{Segment: info.SegmentInfo})
		}
		assert.Equal(t, replayLegacyAlterSegments(t, segments, increments...), dumpMetaStore(t, rec.metaMemoryKV))
	})

	t.Run("atomic crash persists nothing and retry converges", func(t *testing.T) {
		rec := &compactionRecordingKV{metaMemoryKV: NewMetaMemoryKV(), failNextMultiSaveAndRemove: 1}
		m := newCompactionTestMeta(rec, clusterCompactionInputs())
		task, result := clusterCompactionTaskAndResult(11, 12)

		_, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		require.ErrorIs(t, err, errInjectedCompactionCrash)
		// Atomic all-or-nothing: nothing persisted, memory untouched.
		assert.Empty(t, dumpMetaStore(t, rec.metaMemoryKV))
		assert.Nil(t, m.GetSegment(context.TODO(), 11))
		assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(context.TODO(), 1).GetState())

		// The task-level retry re-runs the mutation with the same result and
		// must converge to the legacy bytes.
		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		require.NoError(t, err)
		require.Len(t, infos, 2)
		segments := make([]*datapb.SegmentInfo, 0, len(infos))
		increments := make([]metastore.BinlogsIncrement, 0, len(infos))
		for _, info := range infos {
			segments = append(segments, info.SegmentInfo)
			increments = append(increments, metastore.BinlogsIncrement{Segment: info.SegmentInfo})
		}
		assert.Equal(t, replayLegacyAlterSegments(t, segments, increments...), dumpMetaStore(t, rec.metaMemoryKV))
	})

	t.Run("chunked fallback crash leaves a clean partial state and retry converges", func(t *testing.T) {
		// 3 compactTo segments x 3 KVs each (record + binlog + statslog)
		// against a 2-op txn limit forces the ordered chunked fallback; the
		// injected crash kills the second chunk.
		rec := &compactionRecordingKV{metaMemoryKV: NewMetaMemoryKV(), txnOpsLimit: 2, failMultiSaveAtCall: 2}
		m := newCompactionTestMeta(rec, clusterCompactionInputs())
		task, result := clusterCompactionTaskAndResult(11, 12, 13)

		_, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		require.ErrorIs(t, err, errInjectedCompactionCrash)
		// Memory is only mutated after a successful persist.
		assert.Nil(t, m.GetSegment(context.TODO(), 11))

		partial := dumpMetaStore(t, rec.metaMemoryKV)

		// Retry converges to the legacy bytes; the earlier partial chunks are
		// re-written identically (pure puts, idempotent), never left stale.
		infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
		require.NoError(t, err)
		require.Len(t, infos, 3)
		segments := make([]*datapb.SegmentInfo, 0, len(infos))
		increments := make([]metastore.BinlogsIncrement, 0, len(infos))
		for _, info := range infos {
			segments = append(segments, info.SegmentInfo)
			increments = append(increments, metastore.BinlogsIncrement{Segment: info.SegmentInfo})
		}
		reference := replayLegacyAlterSegments(t, segments, increments...)
		assert.Equal(t, reference, dumpMetaStore(t, rec.metaMemoryKV))
		// Every key the crashed attempt did persist already carried its final
		// bytes - the fallback never writes a value a retry has to fix up.
		for k, v := range partial {
			assert.Equal(t, reference[k], v, "crashed chunk left non-final bytes at %s", k)
		}
	})
}

// TestCompleteSortCompactionMutation_Composite proves the sort completion
// persists the new segment and retires the input through one composite txn,
// byte-for-byte identical to the legacy
// AlterSegments([dropped, new], BinlogsIncrement{new}) call - including the
// handleDroppedSegment GC-compat binlog write for the dropped input.
func TestCompleteSortCompactionMutation_Composite(t *testing.T) {
	segments := NewSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
		Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
		NumOfRows:     2,
		MaxRowNum:     100,
	}})

	rec := &compactionRecordingKV{metaMemoryKV: NewMetaMemoryKV()}
	m := newCompactionTestMeta(rec, segments)

	task := &datapb.CompactionTask{
		PlanID:        19531,
		InputSegments: []UniqueID{1},
		Type:          datapb.CompactionType_SortCompaction,
		Schema:        &schemapb.CollectionSchema{Version: 2},
	}
	result := &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{
			SegmentID:           3,
			InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50000)},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 50001)},
			NumOfRows:           2,
			IsSorted:            true,
		}},
	}

	infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
	require.NoError(t, err)
	require.Len(t, infos, 1)

	assert.Equal(t, 0, rec.multiSaveCalls)
	assert.Equal(t, 1, rec.multiSaveAndRemoveCalls)

	dropped := m.GetSegment(context.TODO(), 1)
	require.Equal(t, commonpb.SegmentState_Dropped, dropped.GetState())
	reference := replayLegacyAlterSegments(t,
		[]*datapb.SegmentInfo{dropped.SegmentInfo, infos[0].SegmentInfo},
		metastore.BinlogsIncrement{Segment: infos[0].SegmentInfo})
	assert.Equal(t, reference, dumpMetaStore(t, rec.metaMemoryKV))
}

// TestCompleteBumpSchemaVersionCompactionMutation_Composite proves the
// in-place schema bump adoption persists the mutated segment through one
// composite txn, byte-for-byte identical to the legacy
// AlterSegments([cloned], BinlogsIncrement{cloned}) call.
func TestCompleteBumpSchemaVersionCompactionMutation_Composite(t *testing.T) {
	currentManifest := packed.MarshalManifestPath("/data/segments/1", 10)
	resultManifest := packed.MarshalManifestPath("/data/segments/1", 12)

	segments := NewSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   100,
		PartitionID:    10,
		State:          commonpb.SegmentState_Flushed,
		Level:          datapb.SegmentLevel_L1,
		Binlogs:        []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
		NumOfRows:      5,
		SchemaVersion:  1,
		StorageVersion: storage.StorageV3,
		ManifestPath:   currentManifest,
	}})

	rec := &compactionRecordingKV{metaMemoryKV: NewMetaMemoryKV()}
	m := newCompactionTestMeta(rec, segments)

	task := &datapb.CompactionTask{
		InputSegments: []int64{1},
		Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:        &schemapb.CollectionSchema{Version: 3},
	}
	result := &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{
			SegmentID:      1,
			NumOfRows:      5,
			InsertLogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10001)},
			Manifest:       resultManifest,
			BaseManifest:   currentManifest,
			StorageVersion: storage.StorageV3,
		}},
	}

	infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
	require.NoError(t, err)
	require.Len(t, infos, 1)

	assert.Equal(t, 0, rec.multiSaveCalls)
	assert.Equal(t, 1, rec.multiSaveAndRemoveCalls)

	reference := replayLegacyAlterSegments(t,
		[]*datapb.SegmentInfo{infos[0].SegmentInfo},
		metastore.BinlogsIncrement{Segment: infos[0].SegmentInfo})
	assert.Equal(t, reference, dumpMetaStore(t, rec.metaMemoryKV))
}

// TestCompleteBumpSchemaVersionReplacementMutation_Composite proves the
// schema bump full-rewrite replacement persists the new segment and retires
// the old one through one composite txn, byte-for-byte identical to the
// legacy AlterSegments([dropped, new], BinlogsIncrement{new}) call.
func TestCompleteBumpSchemaVersionReplacementMutation_Composite(t *testing.T) {
	segments := NewSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  100,
		PartitionID:   10,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(0, 10000)},
		Statslogs:     []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
		NumOfRows:     5,
		SchemaVersion: 1,
	}})

	rec := &compactionRecordingKV{metaMemoryKV: NewMetaMemoryKV()}
	m := newCompactionTestMeta(rec, segments)

	task := &datapb.CompactionTask{
		InputSegments:          []int64{1},
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		Schema:                 &schemapb.CollectionSchema{Version: 3},
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 5, End: 6},
	}
	result := &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{
			SegmentID:      5,
			NumOfRows:      5,
			Manifest:       packed.MarshalManifestPath("/data/segments/5", 1),
			StorageVersion: storage.StorageV3,
		}},
	}

	infos, _, err := m.CompleteCompactionMutation(context.TODO(), task, result)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.EqualValues(t, 5, infos[0].GetID())

	assert.Equal(t, 0, rec.multiSaveCalls)
	assert.Equal(t, 1, rec.multiSaveAndRemoveCalls)

	dropped := m.GetSegment(context.TODO(), 1)
	require.Equal(t, commonpb.SegmentState_Dropped, dropped.GetState())
	reference := replayLegacyAlterSegments(t,
		[]*datapb.SegmentInfo{dropped.SegmentInfo, infos[0].SegmentInfo},
		metastore.BinlogsIncrement{Segment: infos[0].SegmentInfo})
	assert.Equal(t, reference, dumpMetaStore(t, rec.metaMemoryKV))
}
