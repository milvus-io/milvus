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

package gc

import (
	"context"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	runLargeGCPerfTestEnv          = "MILVUS_RUN_LARGE_GC_PERF_TEST"
	runDropIndexGCPerfTestEnv      = "MILVUS_RUN_DROP_INDEX_GC_PERF_TEST"
	runDroppedSegmentGCPerfTestEnv = "MILVUS_RUN_DROPPED_SEGMENT_GC_PERF_TEST"
	gcPerfCollectionsEnv           = "MILVUS_GC_PERF_COLLECTIONS"
	gcPerfSegmentsEnv              = "MILVUS_GC_PERF_SEGMENTS"
	gcPerfDroppedEveryEnv          = "MILVUS_GC_PERF_DROPPED_EVERY"
	gcPerfIndexDefinitionsEnv      = "MILVUS_GC_PERF_INDEX_DEFINITIONS"
	gcPerfIndexEntriesEnv          = "MILVUS_GC_PERF_INDEX_ENTRIES"
	gcPerfIndexFilesPerEntryEnv    = "MILVUS_GC_PERF_INDEX_FILES_PER_ENTRY"
	gcPerfIndexKVDeleteLatencyEnv  = "MILVUS_GC_PERF_INDEX_KV_DELETE_LATENCY"
	gcPerfIndexFileLatencyEnv      = "MILVUS_GC_PERF_INDEX_FILE_DELETE_LATENCY"
	gcPerfIndexApplyLatencyEnv     = "MILVUS_GC_PERF_INDEX_APPLY_LATENCY"
	gcPerfIndexBatchDeleteEnv      = "MILVUS_GC_PERF_INDEX_NATIVE_FILE_BATCH"
	gcPerfIndexBatchSizeEnv        = "MILVUS_GC_PERF_INDEX_BATCH_SIZE"
	gcPerfDroppedSegmentsEnv       = "MILVUS_GC_PERF_DROPPED_SEGMENTS"
	gcPerfDroppedCollectionsEnv    = "MILVUS_GC_PERF_DROPPED_COLLECTIONS"
	gcPerfDroppedIndexesEnv        = "MILVUS_GC_PERF_DROPPED_INDEXES_PER_SEGMENT"
	gcPerfDroppedFilesEnv          = "MILVUS_GC_PERF_DROPPED_FILES_PER_INDEX"
	gcPerfDroppedChannelLatencyEnv = "MILVUS_GC_PERF_DROPPED_CHANNEL_LATENCY"
	gcPerfDroppedFileLatencyEnv    = "MILVUS_GC_PERF_DROPPED_FILE_LATENCY"
	gcPerfDroppedIndexLatencyEnv   = "MILVUS_GC_PERF_DROPPED_INDEX_KV_LATENCY"
	gcPerfDroppedSegmentLatencyEnv = "MILVUS_GC_PERF_DROPPED_SEGMENT_KV_LATENCY"
	gcPerfDroppedApplyLatencyEnv   = "MILVUS_GC_PERF_DROPPED_APPLY_LATENCY"
	gcPerfDroppedBatchDeleteEnv    = "MILVUS_GC_PERF_DROPPED_NATIVE_FILE_BATCH"
	gcPerfDroppedBatchSizeEnv      = "MILVUS_GC_PERF_DROPPED_BATCH_SIZE"

	gcPerfNativeDeleteRequestLimit = 1000
)

type gcPerfDroppedSegmentCatalog struct {
	metastore.DataCoordCatalog
	indexLatency time.Duration
	applyLatency bool

	markSegmentIndexCalls atomic.Int64
	dropSegmentIndexCalls atomic.Int64
	dropSegmentIndexTxns  atomic.Int64
}

// gcPerfCheckpointCatalog serves every PChannel a physical checkpoint after
// the fixture's DML positions and counts the reads.
type gcPerfCheckpointCatalog struct {
	metastore.StreamingNodeCataLog
	latency      time.Duration
	applyLatency bool

	reads atomic.Int64
}

func (c *gcPerfCheckpointCatalog) GetConsumeCheckpoint(context.Context, string) (*streamingpb.WALCheckpoint, error) {
	c.reads.Add(1)
	if c.applyLatency && c.latency > 0 {
		time.Sleep(c.latency)
	}
	return &streamingpb.WALCheckpoint{TimeTick: 2}, nil
}

// AlterSegmentIndexes persists the deletion marker of one index task per call.
func (c *gcPerfDroppedSegmentCatalog) AlterSegmentIndexes(context.Context, []*model.SegmentIndex) error {
	c.markSegmentIndexCalls.Add(1)
	if c.applyLatency && c.indexLatency > 0 {
		time.Sleep(c.indexLatency)
	}
	return nil
}

func (c *gcPerfDroppedSegmentCatalog) DropSegmentIndexes(_ context.Context, indexes []*model.SegmentIndex) error {
	requests := gcPerfEtcdTxnRequests(len(indexes))
	c.dropSegmentIndexCalls.Add(int64(len(indexes)))
	c.dropSegmentIndexTxns.Add(requests)
	if c.applyLatency && c.indexLatency > 0 {
		time.Sleep(time.Duration(requests) * c.indexLatency)
	}
	return nil
}

// gcPerfSegmentDeleteKV counts actual Catalog.DropSegments requests without
// retaining a second metadata copy. It models the two-phase catalog cleanup,
// including four binlog-prefix removals per segment and exact record deletes.
type gcPerfSegmentDeleteKV struct {
	kv.MetaKv
	deleteLatency time.Duration
	applyLatency  bool
	deleteCalls   atomic.Int64
	getRequests   atomic.Int64
	txnRequests   atomic.Int64
}

func (p *gcPerfSegmentDeleteKV) MaxTxnOps() int {
	return max(1, Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt())
}

func (p *gcPerfSegmentDeleteKV) wait() {
	p.txnRequests.Add(1)
	if p.applyLatency {
		time.Sleep(p.deleteLatency)
	}
}

func (p *gcPerfSegmentDeleteKV) MultiSaveAndRemoveWithPrefix(_ context.Context, _ map[string]string, _ []string, _ ...predicates.Predicate) error {
	p.wait()
	return nil
}

func (p *gcPerfSegmentDeleteKV) MultiRemove(_ context.Context, keys []string) error {
	p.wait()
	p.deleteCalls.Add(int64(len(keys)))
	return nil
}

func gcPerfSegmentCatalogRequests(count int) int64 {
	limit := max(1, Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt())
	var requests int64
	for start := 0; start < count; start += limit {
		n := min(limit, count-start)
		requests += gcPerfEtcdTxnRequests(4*n) + gcPerfEtcdTxnRequests(n)
	}
	return requests
}

type gcPerfDropIndexCatalog struct {
	metastore.DataCoordCatalog
	deleteLatency time.Duration
	applyLatency  bool

	dropIndexCalls              atomic.Int64
	dropSegmentIndexCalls       atomic.Int64
	dropIndexTxnRequests        atomic.Int64
	dropSegmentIndexTxnRequests atomic.Int64
}

func (c *gcPerfDropIndexCatalog) waitForDelete(requests int64) {
	if c.applyLatency && c.deleteLatency > 0 && requests > 0 {
		time.Sleep(time.Duration(requests) * c.deleteLatency)
	}
}

func (c *gcPerfDropIndexCatalog) DropIndex(context.Context, int64, int64) error {
	c.dropIndexCalls.Add(1)
	c.dropIndexTxnRequests.Add(1)
	c.waitForDelete(1)
	return nil
}

func (c *gcPerfDropIndexCatalog) DropSegmentIndex(context.Context, int64, int64, int64, int64) error {
	c.dropSegmentIndexCalls.Add(1)
	c.dropSegmentIndexTxnRequests.Add(1)
	c.waitForDelete(1)
	return nil
}

func gcPerfEtcdTxnRequests(items int) int64 {
	if items <= 0 {
		return 0
	}
	limit := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if limit <= 0 {
		limit = 64
	}
	return int64((items + limit - 1) / limit)
}

type gcPerfDroppedSegmentBatchModel struct {
	nativeFileRequests   int64
	fallbackFileRounds   int64
	segmentIndexRequests int64
	segmentRequests      int64
}

func gcPerfModelDroppedSegmentBatches(
	segmentCount int,
	indexesPerSegment int,
	filesPerSegment int,
	batchSize int,
	removeConcurrency int,
) gcPerfDroppedSegmentBatchModel {
	if segmentCount <= 0 {
		return gcPerfDroppedSegmentBatchModel{}
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if removeConcurrency <= 0 {
		removeConcurrency = 1
	}

	// recycleDroppedSegmentsInBatches limits a batch by both candidate count
	// and the estimated number of exact paths. A candidate with no exact paths
	// still consumes one unit so metadata-only cleanup remains bounded.
	weight := max(1, filesPerSegment)
	segmentsPerBatch := batchSize
	if byWeight := batchSize / weight; byWeight < segmentsPerBatch {
		segmentsPerBatch = byWeight
	}
	segmentsPerBatch = max(1, segmentsPerBatch)

	model := gcPerfDroppedSegmentBatchModel{}
	for remaining := segmentCount; remaining > 0; {
		count := min(segmentsPerBatch, remaining)
		if fileCount := count * filesPerSegment; fileCount > 0 {
			model.nativeFileRequests += int64((fileCount + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit)
			model.fallbackFileRounds += int64((fileCount + removeConcurrency - 1) / removeConcurrency)
		}
		model.segmentIndexRequests += gcPerfEtcdTxnRequests(count * indexesPerSegment)
		model.segmentRequests += gcPerfSegmentCatalogRequests(count)
		remaining -= count
	}
	return model
}

func (c *gcPerfDropIndexCatalog) DropIndexes(_ context.Context, indexes []*model.Index) error {
	requests := gcPerfEtcdTxnRequests(len(indexes))
	c.dropIndexCalls.Add(int64(len(indexes)))
	c.dropIndexTxnRequests.Add(requests)
	c.waitForDelete(requests)
	return nil
}

func (c *gcPerfDropIndexCatalog) DropSegmentIndexes(_ context.Context, indexes []*model.SegmentIndex) error {
	requests := gcPerfEtcdTxnRequests(len(indexes))
	c.dropSegmentIndexCalls.Add(int64(len(indexes)))
	c.dropSegmentIndexTxnRequests.Add(requests)
	c.waitForDelete(requests)
	return nil
}

type gcPerfDropIndexChunkManager struct {
	storage.ChunkManager
	deleteLatency time.Duration
	applyLatency  bool

	removeCalls       atomic.Int64
	batchRequestCalls atomic.Int64
}

func (*gcPerfDropIndexChunkManager) RootPath() string {
	return "gc-perf"
}

func (c *gcPerfDropIndexChunkManager) Remove(context.Context, string) error {
	c.removeCalls.Add(1)
	if c.applyLatency && c.deleteLatency > 0 {
		time.Sleep(c.deleteLatency)
	}
	return nil
}

type gcPerfBatchChunkManager struct {
	*gcPerfDropIndexChunkManager
}

func (c *gcPerfBatchChunkManager) MultiRemoveWithResult(_ context.Context, paths []string) []storage.RemoveResult {
	c.removeCalls.Add(int64(len(paths)))
	requestCount := (len(paths) + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit
	c.batchRequestCalls.Add(int64(requestCount))
	if c.applyLatency && c.deleteLatency > 0 {
		time.Sleep(time.Duration(requestCount) * c.deleteLatency)
	}

	results := make([]storage.RemoveResult, len(paths))
	for i, path := range paths {
		results[i].Path = path
	}
	return results
}

type countingDataViewGarbageCollector struct {
	gcCalls int
}

func (c *countingDataViewGarbageCollector) GarbageCollect(context.Context, int64, int) error {
	c.gcCalls++
	return nil
}

type gcPerfMemSnapshot struct {
	heapAlloc    uint64
	heapInuse    uint64
	totalAlloc   uint64
	numGC        uint32
	pauseTotalNs uint64
}

func readGCPerfMemSnapshot() gcPerfMemSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return gcPerfMemSnapshot{
		heapAlloc:    stats.HeapAlloc,
		heapInuse:    stats.HeapInuse,
		totalAlloc:   stats.TotalAlloc,
		numGC:        stats.NumGC,
		pauseTotalNs: stats.PauseTotalNs,
	}
}

func gcPerfEnvInt(t *testing.T, key string, defaultValue int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer, got %q", key, value)
	}
	return parsed
}

func gcPerfEnvDuration(t *testing.T, key string, defaultValue time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		t.Fatalf("%s must be a non-negative duration, got %q", key, value)
	}
	return parsed
}

func gcPerfEnvBool(t *testing.T, key string, defaultValue bool) bool {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("%s must be a boolean, got %q", key, value)
	}
	return parsed
}

func gcPerfMiB(bytes uint64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func logGCPerfStage(t *testing.T, name string, items int, elapsed time.Duration, before, after gcPerfMemSnapshot) {
	t.Helper()
	t.Logf(
		"GC_PERF stage=%s items=%d elapsed=%s throughput=%.0f_items/s heap_before=%.1f_MiB heap_after=%.1f_MiB heap_inuse_after=%.1f_MiB allocated=%.1f_MiB gc_cycles=%d gc_pause=%s",
		name,
		items,
		elapsed,
		float64(items)/elapsed.Seconds(),
		gcPerfMiB(before.heapAlloc),
		gcPerfMiB(after.heapAlloc),
		gcPerfMiB(after.heapInuse),
		gcPerfMiB(after.totalAlloc-before.totalAlloc),
		after.numGC-before.numGC,
		time.Duration(after.pauseTotalNs-before.pauseTotalNs),
	)
}

// TestDataCoordGCLargeScalePerformance measures the in-memory DataCoord GC
// traversal and eligibility paths at Collection-per-Tenant scale. It is
// deliberately opt-in because its default fixture contains one million
// collections and ten million segments.
//
// The test body never creates an etcd client or a ChunkManager. The process
// must also use the standalone/embed environment below so package-level
// paramtable initialization does not add an external etcd configuration
// source. Object-store listing/deletion latency is intentionally outside this
// measurement; the result isolates DataCoord's metadata traversal and GC
// decision cost.
func TestDataCoordGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runLargeGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the large-scale GC performance test", runLargeGCPerfTestEnv)
	}
	if os.Getenv("DEPLOY_MODE") != "STANDALONE" || os.Getenv("ETCD_USE_EMBED") != "true" {
		t.Fatal("large-scale GC performance test requires DEPLOY_MODE=STANDALONE and ETCD_USE_EMBED=true to isolate external etcd")
	}

	collectionCount := gcPerfEnvInt(t, gcPerfCollectionsEnv, 1_000_000)
	segmentCount := gcPerfEnvInt(t, gcPerfSegmentsEnv, 10_000_000)
	droppedEvery := gcPerfEnvInt(t, gcPerfDroppedEveryEnv, 100)
	droppedCandidates := segmentCount / droppedEvery

	t.Logf(
		"GC_PERF config collections=%d segments=%d dropped_every=%d dropped_candidates=%d gomaxprocs=%d",
		collectionCount,
		segmentCount,
		droppedEvery,
		droppedCandidates,
		runtime.GOMAXPROCS(0),
	)

	ctx := context.Background()
	store := metacache.NewMetaStore(nil)
	m := &meta.Meta{
		Ctx:         ctx,
		MetaStore:   store,
		Segments:    meta.NewSegmentsInfo(store),
		ChannelSync: meta.NewChannelSync(),
	}

	setupBefore := readGCPerfMemSnapshot()
	setupStart := time.Now()
	for collectionID := 1; collectionID <= collectionCount; collectionID++ {
		m.MetaStore.PutCollection(&metacache.CollectionInfo{ID: int64(collectionID)})
	}
	t.Logf("GC_PERF seeded_collections=%d elapsed=%s", collectionCount, time.Since(setupStart))

	progressEvery := segmentCount / 10
	if progressEvery == 0 {
		progressEvery = 1
	}
	for segmentID := 1; segmentID <= segmentCount; segmentID++ {
		state := commonpb.SegmentState_Flushed
		if segmentID%droppedEvery == 0 {
			state = commonpb.SegmentState_Dropped
		}
		collectionID := int64((segmentID-1)%collectionCount + 1)
		m.Segments.SetSegment(int64(segmentID), &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           int64(segmentID),
				CollectionID: collectionID,
				State:        state,
			},
		})
		if segmentID%progressEvery == 0 || segmentID == segmentCount {
			t.Logf("GC_PERF seeded_segments=%d/%d elapsed=%s", segmentID, segmentCount, time.Since(setupStart))
		}
	}

	runtime.GC()
	setupElapsed := time.Since(setupStart)
	setupAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "fixture_setup", collectionCount+segmentCount, setupElapsed, setupBefore, setupAfter)

	dataViewGC := &countingDataViewGarbageCollector{}
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{
		Enabled:       false,
		DropTolerance: 100 * 365 * 24 * time.Hour,
		DataViewGC:    dataViewGC,
	})
	defer gc.Close()

	dataViewBefore := readGCPerfMemSnapshot()
	dataViewStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "recycle_data_views"), func(profileCtx context.Context) {
		gc.recycleDataViews(profileCtx, nil)
	})
	dataViewElapsed := time.Since(dataViewStart)
	dataViewAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "recycle_data_views", collectionCount, dataViewElapsed, dataViewBefore, dataViewAfter)
	// DataView GC decides from in-memory version tables, one call per
	// Collection, without catalog reads.
	if dataViewGC.gcCalls != collectionCount {
		t.Fatalf("DataView GC calls = %d, want %d", dataViewGC.gcCalls, collectionCount)
	}

	// Release the temporary one-million-entry collection slice before the
	// segment traversal so the two stages have independent allocation deltas.
	runtime.GC()

	segmentBefore := readGCPerfMemSnapshot()
	segmentStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "recycle_dropped_segments"), func(profileCtx context.Context) {
		gc.RecycleDroppedSegments(profileCtx, nil)
	})
	segmentElapsed := time.Since(segmentStart)
	segmentAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "recycle_dropped_segments", segmentCount, segmentElapsed, segmentBefore, segmentAfter)
	if len(m.Segments.GetSegments()) != segmentCount {
		t.Fatalf("segment count changed during ineligible-candidate scan: got %d, want %d", len(m.Segments.GetSegments()), segmentCount)
	}

	runtime.KeepAlive(m)
}

// TestDataCoordDropIndexGCLargeScalePerformance measures the production
// DropIndex GC deletion pipeline with lightweight, configurable file and KV
// backends. The default ten-million-entry run does not sleep for every mocked
// I/O operation; it measures GC's traversal and dispatch cost and reports the
// I/O time implied by the configured latencies. Set
// MILVUS_GC_PERF_INDEX_APPLY_LATENCY=true on a smaller fixture to inject the
// latency into every fallback per-path delete or modeled native request and
// validate the projection against wall time.
// MILVUS_GC_PERF_INDEX_NATIVE_FILE_BATCH controls whether the file mock
// exposes native batch deletion. Metadata always uses bounded exact-key
// transactions through DataCoordCatalog.
func TestDataCoordDropIndexGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runDropIndexGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the DropIndex GC performance test", runDropIndexGCPerfTestEnv)
	}

	definitionCount := gcPerfEnvInt(t, gcPerfIndexDefinitionsEnv, 1_000_000)
	entryCount := gcPerfEnvInt(t, gcPerfIndexEntriesEnv, 10_000_000)
	filesPerEntry := gcPerfEnvInt(t, gcPerfIndexFilesPerEntryEnv, 1)
	kvDeleteLatency := gcPerfEnvDuration(t, gcPerfIndexKVDeleteLatencyEnv, time.Millisecond)
	fileDeleteLatency := gcPerfEnvDuration(t, gcPerfIndexFileLatencyEnv, 5*time.Millisecond)
	applyLatency := gcPerfEnvBool(t, gcPerfIndexApplyLatencyEnv, false)
	nativeFileBatchEnabled := gcPerfEnvBool(t, gcPerfIndexBatchDeleteEnv, false)
	batchSize := gcPerfEnvInt(t, gcPerfIndexBatchSizeEnv, Params.DataCoordCfg.GCIndexFileBatchSize.GetAsInt())

	oldBatchSize := Params.DataCoordCfg.GCIndexFileBatchSize.GetValue()
	if err := Params.Save(Params.DataCoordCfg.GCIndexFileBatchSize.Key, strconv.Itoa(batchSize)); err != nil {
		t.Fatalf("set batch size: %v", err)
	}
	batchSize = Params.DataCoordCfg.GCIndexFileBatchSize.GetAsInt()
	t.Cleanup(func() {
		if err := Params.Save(Params.DataCoordCfg.GCIndexFileBatchSize.Key, oldBatchSize); err != nil {
			t.Errorf("restore batch size: %v", err)
		}
	})

	removeConcurrency := Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt()
	if removeConcurrency <= 0 {
		t.Fatalf("DataCoord GC remove concurrency must be positive, got %d", removeConcurrency)
	}

	// The GC groups candidate SegmentIndexes by the configured file weight. A
	// native backend splits flattened files at its request limit; a base
	// ChunkManager removes the same flattened set through the bounded fallback
	// pool.
	candidatesPerBatch := batchSize / filesPerEntry
	if candidatesPerBatch == 0 {
		candidatesPerBatch = 1
	}
	fullCandidateBatches := entryCount / candidatesPerBatch
	remainingCandidates := entryCount % candidatesPerBatch
	modeledFileRequests := int64(entryCount) * int64(filesPerEntry)
	modeledFileLatencyUnits := int64(fullCandidateBatches) * int64((candidatesPerBatch*filesPerEntry+removeConcurrency-1)/removeConcurrency)
	if remainingCandidates > 0 {
		modeledFileLatencyUnits += int64((remainingCandidates*filesPerEntry + removeConcurrency - 1) / removeConcurrency)
	}
	if nativeFileBatchEnabled {
		requestsPerFullBatch := (candidatesPerBatch*filesPerEntry + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit
		modeledFileRequests = int64(fullCandidateBatches * requestsPerFullBatch)
		if remainingCandidates > 0 {
			modeledFileRequests += int64((remainingCandidates*filesPerEntry + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit)
		}
		modeledFileLatencyUnits = modeledFileRequests
	}
	modeledFieldKVRequests := gcPerfEtcdTxnRequests(definitionCount)
	modeledSegmentKVRequests := int64(fullCandidateBatches) * gcPerfEtcdTxnRequests(candidatesPerBatch)
	modeledSegmentKVRequests += gcPerfEtcdTxnRequests(remainingCandidates)
	modeledIOTime := time.Duration(modeledFieldKVRequests+modeledSegmentKVRequests)*kvDeleteLatency +
		time.Duration(modeledFileLatencyUnits)*fileDeleteLatency
	t.Logf(
		"DROP_INDEX_GC_PERF config definitions=%d entries=%d files_per_entry=%d native_file_batch=%t file_batch_size=%d remove_concurrency=%d kv_delete_latency=%s file_delete_latency=%s apply_latency=%t modeled_field_kv_requests=%d modeled_segment_kv_requests=%d modeled_file_requests=%d modeled_total_io=%s",
		definitionCount,
		entryCount,
		filesPerEntry,
		nativeFileBatchEnabled,
		batchSize,
		removeConcurrency,
		kvDeleteLatency,
		fileDeleteLatency,
		applyLatency,
		modeledFieldKVRequests,
		modeledSegmentKVRequests,
		modeledFileRequests,
		modeledIOTime,
	)

	oldLogLevel := mlog.GetLevel()
	mlog.SetLevel(mlog.WarnLevel)
	defer mlog.SetLevel(oldLogLevel)

	ctx := context.Background()
	catalog := &gcPerfDropIndexCatalog{
		deleteLatency: kvDeleteLatency,
		applyLatency:  applyLatency,
	}
	gcCatalog := metastore.DataCoordCatalog(catalog)
	chunkManager := &gcPerfDropIndexChunkManager{
		deleteLatency: fileDeleteLatency,
		applyLatency:  applyLatency,
	}
	gcChunkManager := storage.ChunkManager(chunkManager)
	if nativeFileBatchEnabled {
		gcChunkManager = &gcPerfBatchChunkManager{gcPerfDropIndexChunkManager: chunkManager}
	}

	const (
		partitionID = int64(2)
		segmentID   = int64(3)
		fieldID     = int64(4)
		indexID     = int64(5)
	)
	indexMeta := &index.IndexMeta{
		Catalog:          gcCatalog,
		Indexes:          make(map[UniqueID]map[UniqueID]*model.Index, definitionCount),
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		KeyLock:          lock.NewKeyLock[UniqueID](),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	m := &meta.Meta{
		Ctx:      ctx,
		Segments: meta.NewSegmentsInfo(metacache.NewMetaStore(nil)),
	}
	// A single live segment is sufficient to force the DropIndex eligibility
	// branch (!IsIndexExist) for every SegmentIndex entry after all field-index
	// definitions are removed. Sharing it keeps the fixture focused on file/KV
	// deletion instead of duplicating the separate ten-million SegmentInfo test.
	m.Segments.SetSegment(segmentID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: 1,
		PartitionID:  partitionID,
		State:        commonpb.SegmentState_Flushed,
	}})

	fileKeys := make([]string, filesPerEntry)
	for i := range fileKeys {
		fileKeys[i] = "index-file-" + strconv.Itoa(i)
	}
	setupBefore := readGCPerfMemSnapshot()
	setupStart := time.Now()
	for i := 1; i <= definitionCount; i++ {
		collectionID := int64(i)
		indexMeta.Indexes[collectionID] = map[UniqueID]*model.Index{
			indexID: {
				CollectionID: collectionID,
				FieldID:      fieldID,
				IndexID:      indexID,
				IndexName:    "drop-index-gc-perf",
				IsDeleted:    true,
			},
		}
	}
	for i := 1; i <= entryCount; i++ {
		collectionID := int64((i-1)%definitionCount + 1)
		indexMeta.SegmentBuildInfo.AddForRecovery(&model.SegmentIndex{
			SegmentID:             segmentID,
			CollectionID:          collectionID,
			PartitionID:           partitionID,
			IndexID:               indexID,
			BuildID:               int64(i),
			IndexVersion:          1,
			IndexState:            commonpb.IndexState_Finished,
			IndexFileKeys:         fileKeys,
			IndexStorePathVersion: 1,
		})
	}
	runtime.GC()
	setupElapsed := time.Since(setupStart)
	setupAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_index_fixture_setup", definitionCount+entryCount, setupElapsed, setupBefore, setupAfter)

	gc := NewGarbageCollector(m, nil, GcOption{
		IndexMeta: indexMeta,
		Enabled:   false,
		Cli:       gcChunkManager,
	})
	defer gc.Close()

	fieldIndexBefore := readGCPerfMemSnapshot()
	fieldIndexStart := time.Now()
	gc.recycleUnusedIndexes(ctx, nil)
	fieldIndexElapsed := time.Since(fieldIndexStart)
	fieldIndexAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_field_indexes", definitionCount, fieldIndexElapsed, fieldIndexBefore, fieldIndexAfter)

	segmentIndexBefore := readGCPerfMemSnapshot()
	segmentIndexStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "drop_segment_indexes"), func(profileCtx context.Context) {
		gc.RecycleUnusedSegIndexes(profileCtx, nil)
	})
	segmentIndexElapsed := time.Since(segmentIndexStart)
	segmentIndexAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_segment_indexes", entryCount, segmentIndexElapsed, segmentIndexBefore, segmentIndexAfter)

	wantFileDeletes := int64(entryCount) * int64(filesPerEntry)
	if got := catalog.dropIndexCalls.Load(); got != int64(definitionCount) {
		t.Fatalf("field-index KV deletes = %d, want %d", got, definitionCount)
	}
	if got := catalog.dropSegmentIndexCalls.Load(); got != int64(entryCount) {
		t.Fatalf("segment-index KV deletes = %d, want %d", got, entryCount)
	}
	if got := catalog.dropIndexTxnRequests.Load(); got != modeledFieldKVRequests {
		t.Fatalf("field-index KV transaction requests = %d, want %d", got, modeledFieldKVRequests)
	}
	if got := catalog.dropSegmentIndexTxnRequests.Load(); got != modeledSegmentKVRequests {
		t.Fatalf("segment-index KV transaction requests = %d, want %d", got, modeledSegmentKVRequests)
	}
	if got := chunkManager.removeCalls.Load(); got != wantFileDeletes {
		t.Fatalf("index file deletes = %d, want %d", got, wantFileDeletes)
	}
	wantBatchRequests := int64(0)
	if nativeFileBatchEnabled {
		wantBatchRequests = modeledFileRequests
	}
	if got := chunkManager.batchRequestCalls.Load(); got != wantBatchRequests {
		t.Fatalf("index file batch requests = %d, want %d", got, wantBatchRequests)
	}
	remainingFieldIndexes := len(indexMeta.Indexes)
	if remainingFieldIndexes != 0 {
		t.Fatalf("field-index collections remaining after GC = %d, want 0", remainingFieldIndexes)
	}
	if remaining := len(indexMeta.SegmentBuildInfo.List()); remaining != 0 {
		t.Fatalf("segment-index entries remaining after GC = %d, want 0", remaining)
	}

	t.Logf(
		"DROP_INDEX_GC_PERF result definitions=%d entries=%d native_file_batch=%t field_kv_deletes=%d field_kv_txn_requests=%d segment_kv_deletes=%d segment_kv_txn_requests=%d logical_file_deletes=%d native_file_requests=%d measured_field_stage=%s measured_segment_stage=%s measured_segment_throughput=%.0f_entries/s latency_applied=%t modeled_total_io=%s",
		definitionCount,
		entryCount,
		nativeFileBatchEnabled,
		catalog.dropIndexCalls.Load(),
		catalog.dropIndexTxnRequests.Load(),
		catalog.dropSegmentIndexCalls.Load(),
		catalog.dropSegmentIndexTxnRequests.Load(),
		chunkManager.removeCalls.Load(),
		chunkManager.batchRequestCalls.Load(),
		fieldIndexElapsed,
		segmentIndexElapsed,
		float64(entryCount)/segmentIndexElapsed.Seconds(),
		applyLatency,
		modeledIOTime,
	)

	runtime.KeepAlive(fileKeys)
	runtime.KeepAlive(m)
}

// TestDataCoordDroppedSegmentGCLargeScalePerformance drives the production
// recycleDroppedSegments deletion path with configurable channel,
// object-store, SegmentIndex KV, and Segment KV latencies. Keep latency
// injection disabled for large CPU/allocation runs; enable it on a smaller
// fixture to validate the request model against wall time.
func TestDataCoordDroppedSegmentGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runDroppedSegmentGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the dropped-segment GC performance test", runDroppedSegmentGCPerfTestEnv)
	}

	segmentCount := gcPerfEnvInt(t, gcPerfDroppedSegmentsEnv, 1_000_000)
	collectionCount := gcPerfEnvInt(t, gcPerfDroppedCollectionsEnv, 100_000)
	indexesPerSegment := gcPerfEnvInt(t, gcPerfDroppedIndexesEnv, 1)
	filesPerIndex := gcPerfEnvInt(t, gcPerfDroppedFilesEnv, 1)
	channelLatency := gcPerfEnvDuration(t, gcPerfDroppedChannelLatencyEnv, time.Millisecond)
	fileLatency := gcPerfEnvDuration(t, gcPerfDroppedFileLatencyEnv, 5*time.Millisecond)
	indexLatency := gcPerfEnvDuration(t, gcPerfDroppedIndexLatencyEnv, time.Millisecond)
	segmentLatency := gcPerfEnvDuration(t, gcPerfDroppedSegmentLatencyEnv, time.Millisecond)
	applyLatency := gcPerfEnvBool(t, gcPerfDroppedApplyLatencyEnv, false)
	nativeFileBatchEnabled := gcPerfEnvBool(t, gcPerfDroppedBatchDeleteEnv, false)
	batchSize := gcPerfEnvInt(t, gcPerfDroppedBatchSizeEnv, 1000)

	oldBatchSize := Params.DataCoordCfg.GCDroppedSegmentBatchSize.GetValue()
	if err := Params.Save(Params.DataCoordCfg.GCDroppedSegmentBatchSize.Key, strconv.Itoa(batchSize)); err != nil {
		t.Fatalf("set dropped-segment batch size: %v", err)
	}
	batchSize = Params.DataCoordCfg.GCDroppedSegmentBatchSize.GetAsInt()
	t.Cleanup(func() {
		if err := Params.Save(Params.DataCoordCfg.GCDroppedSegmentBatchSize.Key, oldBatchSize); err != nil {
			t.Errorf("restore dropped-segment batch size: %v", err)
		}
	})

	removeConcurrency := Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt()
	if removeConcurrency <= 0 {
		t.Fatalf("DataCoord GC remove concurrency must be positive, got %d", removeConcurrency)
	}
	filesPerSegment := indexesPerSegment * filesPerIndex
	// Each fixture collection has its own PChannel, whose physical checkpoint
	// is read once per sweep.
	modeledChannelRequests := int64(min(collectionCount, segmentCount))
	modeledFileRequests := int64(segmentCount * filesPerSegment)
	modeledIndexRequests := int64(segmentCount * indexesPerSegment)
	// Index task deletion markers are persisted one task per request before
	// any file of the segment is removed.
	modeledMarkRequests := modeledIndexRequests
	batchModel := gcPerfModelDroppedSegmentBatches(
		segmentCount,
		indexesPerSegment,
		filesPerSegment,
		batchSize,
		removeConcurrency,
	)
	modeledFileBackendRequests := modeledFileRequests
	modeledFileLatencyUnits := batchModel.fallbackFileRounds
	if nativeFileBatchEnabled {
		modeledFileBackendRequests = batchModel.nativeFileRequests
		modeledFileLatencyUnits = batchModel.nativeFileRequests
	}
	modeledIndexTxnRequests := batchModel.segmentIndexRequests
	modeledSegmentGetRequests := int64(0)
	modeledSegmentTxnRequests := batchModel.segmentRequests
	modeledSerialIO := time.Duration(modeledChannelRequests)*channelLatency +
		time.Duration(modeledFileBackendRequests)*fileLatency +
		time.Duration(modeledMarkRequests+modeledIndexTxnRequests)*indexLatency +
		time.Duration(modeledSegmentGetRequests+modeledSegmentTxnRequests)*segmentLatency
	// Checkpoint reads, markers and metadata batches are serial.
	modeledBatchedIO := time.Duration(modeledChannelRequests)*channelLatency +
		time.Duration(modeledFileLatencyUnits)*fileLatency +
		time.Duration(modeledMarkRequests+modeledIndexTxnRequests)*indexLatency +
		time.Duration(modeledSegmentGetRequests+modeledSegmentTxnRequests)*segmentLatency
	t.Logf(
		"DROPPED_SEGMENT_GC_PERF config segments=%d collections=%d indexes_per_segment=%d files_per_index=%d remove_concurrency=%d native_file_batch=%t batch_size=%d channel_latency=%s file_latency=%s index_kv_latency=%s segment_kv_latency=%s apply_latency=%t modeled_serial_io=%s modeled_batched_io=%s",
		segmentCount,
		collectionCount,
		indexesPerSegment,
		filesPerIndex,
		removeConcurrency,
		nativeFileBatchEnabled,
		batchSize,
		channelLatency,
		fileLatency,
		indexLatency,
		segmentLatency,
		applyLatency,
		modeledSerialIO,
		modeledBatchedIO,
	)

	oldLogLevel := mlog.GetLevel()
	mlog.SetLevel(mlog.WarnLevel)
	defer mlog.SetLevel(oldLogLevel)

	ctx := context.Background()
	segmentCatalog := &gcPerfDroppedSegmentCatalog{
		indexLatency: indexLatency,
		applyLatency: applyLatency,
	}
	checkpointCatalog := &gcPerfCheckpointCatalog{
		latency:      channelLatency,
		applyLatency: applyLatency,
	}
	segmentKV := &gcPerfSegmentDeleteKV{
		deleteLatency: segmentLatency,
		applyLatency:  applyLatency,
	}
	chunkManager := &gcPerfDropIndexChunkManager{
		deleteLatency: fileLatency,
		applyLatency:  applyLatency,
	}
	gcChunkManager := storage.ChunkManager(chunkManager)
	if nativeFileBatchEnabled {
		gcChunkManager = &gcPerfBatchChunkManager{gcPerfDropIndexChunkManager: chunkManager}
	}
	indexMeta := &index.IndexMeta{
		Catalog:          segmentCatalog,
		Indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		KeyLock:          lock.NewKeyLock[UniqueID](),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	store := metacache.NewMetaStore(nil)
	m := &meta.Meta{
		Ctx:         ctx,
		Catalog:     segmentCatalog,
		MetaStore:   store,
		Segments:    meta.NewSegmentsInfo(store),
		ChannelSync: meta.NewChannelSync(),
	}
	m.Catalog = kvdatacoord.NewCatalog(segmentKV, "", "")

	fileKeys := make([]string, filesPerIndex)
	for i := range fileKeys {
		fileKeys[i] = "dropped-segment-index-file-" + strconv.Itoa(i)
	}
	setupBefore := readGCPerfMemSnapshot()
	setupStart := time.Now()
	for i := 1; i <= segmentCount; i++ {
		segmentID := int64(i)
		collectionID := int64((i-1)%collectionCount + 1)
		partitionID := collectionID
		m.Segments.SetSegment(segmentID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: "dropped-segment-gc-perf-" + strconv.FormatInt(collectionID, 10),
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     1,
			DmlPosition:   &msgpb.MsgPosition{Timestamp: 1},
		}})

		segmentIndexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		for indexOffset := 0; indexOffset < indexesPerSegment; indexOffset++ {
			indexID := int64(indexOffset + 1)
			buildID := int64((i-1)*indexesPerSegment + indexOffset + 1)
			segmentIndex := &model.SegmentIndex{
				SegmentID:             segmentID,
				CollectionID:          collectionID,
				PartitionID:           partitionID,
				IndexID:               indexID,
				BuildID:               buildID,
				IndexVersion:          1,
				IndexState:            commonpb.IndexState_Finished,
				IndexFileKeys:         fileKeys,
				IndexStorePathVersion: 1,
			}
			segmentIndexes.Insert(indexID, segmentIndex)
			indexMeta.SegmentBuildInfo.AddForRecovery(segmentIndex)
		}
		indexMeta.SegmentIndexes.Insert(segmentID, segmentIndexes)
	}
	runtime.GC()
	setupElapsed := time.Since(setupStart)
	setupAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "dropped_segment_fixture_setup", segmentCount, setupElapsed, setupBefore, setupAfter)

	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{
		IndexMeta:            indexMeta,
		Enabled:              false,
		Cli:                  gcChunkManager,
		DropTolerance:        0,
		StreamingNodeCatalog: checkpointCatalog,
	})
	defer gc.Close()

	stageBefore := readGCPerfMemSnapshot()
	stageStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "drop_segments"), func(profileCtx context.Context) {
		gc.RecycleDroppedSegments(profileCtx, nil)
	})
	stageElapsed := time.Since(stageStart)
	stageAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_segments", segmentCount, stageElapsed, stageBefore, stageAfter)

	if got := checkpointCatalog.reads.Load(); got != modeledChannelRequests {
		t.Fatalf("physical checkpoint reads = %d, want %d", got, modeledChannelRequests)
	}
	if got := chunkManager.removeCalls.Load(); got != modeledFileRequests {
		t.Fatalf("file delete calls = %d, want %d", got, modeledFileRequests)
	}
	if got := segmentCatalog.markSegmentIndexCalls.Load(); got != modeledMarkRequests {
		t.Fatalf("SegmentIndex deletion marker requests = %d, want %d", got, modeledMarkRequests)
	}
	if got := segmentCatalog.dropSegmentIndexCalls.Load(); got != modeledIndexRequests {
		t.Fatalf("SegmentIndex metadata deletes = %d, want %d", got, modeledIndexRequests)
	}
	if got := segmentCatalog.dropSegmentIndexTxns.Load(); got != modeledIndexTxnRequests {
		t.Fatalf("SegmentIndex metadata txn requests = %d, want %d", got, modeledIndexTxnRequests)
	}
	wantNativeFileRequests := int64(0)
	if nativeFileBatchEnabled {
		wantNativeFileRequests = modeledFileBackendRequests
	}
	if got := chunkManager.batchRequestCalls.Load(); got != wantNativeFileRequests {
		t.Fatalf("native file delete requests = %d, want %d", got, wantNativeFileRequests)
	}
	if got := segmentKV.deleteCalls.Load(); got != int64(segmentCount) {
		t.Fatalf("segment metadata deletes = %d, want %d", got, segmentCount)
	}
	if got := segmentKV.getRequests.Load(); got != modeledSegmentGetRequests {
		t.Fatalf("segment metadata get requests = %d, want %d", got, modeledSegmentGetRequests)
	}
	if got := segmentKV.txnRequests.Load(); got != modeledSegmentTxnRequests {
		t.Fatalf("segment metadata txn requests = %d, want %d", got, modeledSegmentTxnRequests)
	}
	if remaining := len(m.Segments.GetSegments()); remaining != 0 {
		t.Fatalf("segments remaining after GC = %d, want 0", remaining)
	}
	if remaining := len(indexMeta.SegmentBuildInfo.List()); remaining != 0 {
		t.Fatalf("SegmentIndex entries remaining after GC = %d, want 0", remaining)
	}

	t.Logf(
		"DROPPED_SEGMENT_GC_PERF result segments=%d collections=%d native_file_batch=%t batch_size=%d checkpoint_reads=%d logical_file_deletes=%d native_file_requests=%d segment_index_marker_requests=%d segment_index_kv_deletes=%d segment_index_kv_txn_requests=%d segment_kv_get_requests=%d segment_kv_txn_requests=%d elapsed=%s throughput=%.0f_segments/s latency_applied=%t modeled_serial_io=%s modeled_batched_io=%s",
		segmentCount,
		collectionCount,
		nativeFileBatchEnabled,
		batchSize,
		checkpointCatalog.reads.Load(),
		chunkManager.removeCalls.Load(),
		chunkManager.batchRequestCalls.Load(),
		segmentCatalog.markSegmentIndexCalls.Load(),
		segmentCatalog.dropSegmentIndexCalls.Load(),
		segmentCatalog.dropSegmentIndexTxns.Load(),
		segmentKV.getRequests.Load(),
		segmentKV.txnRequests.Load(),
		stageElapsed,
		float64(segmentCount)/stageElapsed.Seconds(),
		applyLatency,
		modeledSerialIO,
		modeledBatchedIO,
	)

	runtime.KeepAlive(fileKeys)
	runtime.KeepAlive(m)
}
