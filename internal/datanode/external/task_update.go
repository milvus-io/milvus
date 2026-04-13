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

package external

// RefreshExternalCollectionTask handles updating external collection segments by fetching fragments from external sources
// and organizing them into segments with balanced row counts.
//
// SEGMENT ID ALLOCATION WORKFLOW:
// - DataCoord pre-allocates a batch of segment IDs (default 1000) via allocator.AllocN()
// - Pre-allocated ID range is passed to DataNode via RefreshExternalCollectionTaskRequest.PreAllocatedSegmentIds
// - DataNode extracts the IDRange and uses pre-allocated IDs sequentially for each new segment
// - Manifest files are written directly to final paths: external/{collectionID}/segments/{realID}/manifest
//
// NO TEMPORARY PATHS OR CLEANUP:
// - All segments use real, pre-allocated IDs (no temporary negative IDs)
// - All manifest files are written to final paths immediately
// - No cleanup logic is needed because:
//   * IDs are pre-allocated, ensuring uniqueness
//   * Paths are final, so failed manifests remain in final locations but are not registered in meta
//   * DataCoord validates and registers only successful manifests
//
// KEY IMPROVEMENTS OVER PREVIOUS DESIGN:
// - Eliminates temporary paths and associated cleanup complexity
// - Reduces risk of ID collision or exhaustion
// - Simplifies the segment organization workflow
// - Directly writes to final storage locations

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

func ensureContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// RefreshExternalCollectionTask handles updating external collection segments
type RefreshExternalCollectionTask struct {
	ctx context.Context

	req *datapb.RefreshExternalCollectionTaskRequest
	tr  *timerecord.TimeRecorder

	state      indexpb.JobState
	failReason string

	// Cached parsed spec (populated in PreExecute, reused in Execute)
	parsedSpec *externalspec.ExternalSpec
	columns    []string

	// Result after execution — tracked separately for correct response building
	keptSegmentIDs  []int64               // IDs of current segments that were kept unchanged
	newSegments     []*datapb.SegmentInfo // newly created segments (from orphan fragments)
	updatedSegments []*datapb.SegmentInfo // all segments (kept + new), retained for logging counts

	// Pre-allocated segment IDs
	preallocatedIDRange *datapb.IDRange // pre-allocated segment ID range (begin, end)
	nextAllocID         int64           // next available segment ID from pre-allocated range
}

// NewRefreshExternalCollectionTask creates a new refresh-external-collection task.
// ctx owns the task lifetime; cancellation is driven by the caller's context
// (typically the manager's worker-pool closure).
func NewRefreshExternalCollectionTask(
	ctx context.Context,
	req *datapb.RefreshExternalCollectionTaskRequest,
) *RefreshExternalCollectionTask {
	return &RefreshExternalCollectionTask{
		ctx:   ctx,
		req:   req,
		tr:    timerecord.NewTimeRecorder(fmt.Sprintf("RefreshExternalCollectionTask: %d", req.GetTaskID())),
		state: indexpb.JobState_JobStateInit,
	}
}

func (t *RefreshExternalCollectionTask) Ctx() context.Context {
	return t.ctx
}

func (t *RefreshExternalCollectionTask) Name() string {
	return fmt.Sprintf("RefreshExternalCollectionTask-%d", t.req.GetTaskID())
}

func (t *RefreshExternalCollectionTask) OnEnqueue(ctx context.Context) error {
	t.tr.RecordSpan()
	log.Ctx(ctx).Info("RefreshExternalCollectionTask enqueued",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()))
	return nil
}

func (t *RefreshExternalCollectionTask) SetState(state indexpb.JobState, failReason string) {
	t.state = state
	t.failReason = failReason
}

func (t *RefreshExternalCollectionTask) GetState() indexpb.JobState {
	return t.state
}

func (t *RefreshExternalCollectionTask) GetSlot() int64 {
	return 1
}

func (t *RefreshExternalCollectionTask) Reset() {
	t.ctx = nil
	t.req = nil
	t.tr = nil
	t.keptSegmentIDs = nil
	t.newSegments = nil
	t.updatedSegments = nil
}

func (t *RefreshExternalCollectionTask) PreExecute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log.Ctx(ctx).Info("RefreshExternalCollectionTask PreExecute",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()))

	if t.req == nil {
		return fmt.Errorf("request is nil")
	}
	if t.req.GetSchema() == nil {
		return fmt.Errorf("schema is nil in request")
	}
	if t.req.GetStorageConfig() == nil {
		return fmt.Errorf("storage config is nil in request")
	}
	if t.req.GetExternalSource() == "" {
		return fmt.Errorf("external source is empty in request")
	}

	// Parse and cache external spec for reuse during Execute
	spec, err := externalspec.ParseExternalSpec(t.req.GetExternalSpec())
	if err != nil {
		return fmt.Errorf("failed to parse external spec: %w", err)
	}
	t.parsedSpec = spec
	t.columns = packed.GetColumnNamesFromSchema(t.req.GetSchema())

	return nil
}

func (t *RefreshExternalCollectionTask) Execute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log := log.Ctx(ctx)
	log.Info("RefreshExternalCollectionTask Execute",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()))

	// Initialize pre-allocated segment IDs from request
	if t.req.GetPreAllocatedSegmentIds() == nil {
		return fmt.Errorf("pre-allocated segment IDs not provided in request")
	}

	t.preallocatedIDRange = t.req.GetPreAllocatedSegmentIds()
	t.nextAllocID = t.preallocatedIDRange.Begin

	log.Info("Initialized pre-allocated segment ID range",
		zap.Int64("idBegin", t.preallocatedIDRange.Begin),
		zap.Int64("idEnd", t.preallocatedIDRange.End),
		zap.Int64("count", t.preallocatedIDRange.End-t.preallocatedIDRange.Begin))

	// Fetch fragments from external source
	newFragments, err := t.fetchFragmentsFromExternalSource(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch fragments: %w", err)
	}

	// Build current segment -> fragments mapping
	currentSegmentFragments, err := t.buildCurrentSegmentFragments()
	if err != nil {
		return fmt.Errorf("failed to build current segment fragments: %w", err)
	}

	// Compare and organize segments
	updatedSegments, err := t.organizeSegments(ctx, currentSegmentFragments, newFragments)
	if err != nil {
		return err
	}

	t.updatedSegments = updatedSegments
	return nil
}

// fetchFragmentsFromExternalSource reads file info from the explore manifest
// written by DataCoord and returns fragments for the assigned file range.
func (t *RefreshExternalCollectionTask) fetchFragmentsFromExternalSource(ctx context.Context) ([]packed.Fragment, error) {
	manifestPath := t.req.GetExploreManifestPath()
	if manifestPath == "" {
		return nil, fmt.Errorf("explore manifest path is required but not provided")
	}

	log.Ctx(ctx).Info("reading file list from explore manifest",
		zap.String("manifestPath", manifestPath),
		zap.Int64("fileIndexBegin", t.req.GetFileIndexBegin()),
		zap.Int64("fileIndexEnd", t.req.GetFileIndexEnd()))

	extfsPrefix := packed.ExtfsPrefixForCollection(t.req.GetCollectionID())
	specExtfs := t.parsedSpec.BuildExtfsOverrides(extfsPrefix)

	targetRowsPerSegment := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()

	return packed.FetchFragmentsFromExternalSourceWithRange(
		ctx,
		t.parsedSpec.Format,
		t.req.GetExternalSource(),
		t.req.GetStorageConfig(),
		t.req.GetFileIndexBegin(),
		t.req.GetFileIndexEnd(),
		manifestPath,
		packed.ExternalFetchOptions{
			CollectionID:     t.req.GetCollectionID(),
			SpecExtfs:        specExtfs,
			FormatProperties: t.parsedSpec.BuildFormatProperties(),
		},
		targetRowsPerSegment,
	)
}

func (t *RefreshExternalCollectionTask) PostExecute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log.Ctx(ctx).Info("RefreshExternalCollectionTask PostExecute",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()),
		zap.Int("updatedSegments", len(t.updatedSegments)))
	return nil
}

// GetUpdatedSegments returns all result segments (kept + new) after execution
func (t *RefreshExternalCollectionTask) GetUpdatedSegments() []*datapb.SegmentInfo {
	return t.updatedSegments
}

// GetKeptSegmentIDs returns IDs of current segments that were kept unchanged
func (t *RefreshExternalCollectionTask) GetKeptSegmentIDs() []int64 {
	return t.keptSegmentIDs
}

// GetNewSegments returns only newly created segments (from orphan fragment rebalancing)
func (t *RefreshExternalCollectionTask) GetNewSegments() []*datapb.SegmentInfo {
	return t.newSegments
}

// fragmentKey generates a unique key for a fragment using its FilePath and row range
// This composite key ensures fragments from the same file with different row ranges
// are treated as distinct entities, which is critical for correct data mapping
func fragmentKey(f packed.Fragment) string {
	return fmt.Sprintf("%s:%d:%d", f.FilePath, f.StartRow, f.EndRow)
}

// buildCurrentSegmentFragments builds segment to fragments mapping from current segments
func (t *RefreshExternalCollectionTask) buildCurrentSegmentFragments() (packed.SegmentFragments, error) {
	return packed.BuildCurrentSegmentFragments(t.req.GetCurrentSegments(), t.req.GetStorageConfig())
}

// organizeSegments compares fragments and organizes them into segments
func (t *RefreshExternalCollectionTask) organizeSegments(
	ctx context.Context,
	currentSegmentFragments packed.SegmentFragments,
	newFragments []packed.Fragment,
) ([]*datapb.SegmentInfo, error) {
	if err := ensureContext(ctx); err != nil {
		return nil, err
	}
	log := log.Ctx(ctx)

	// Build new fragment map using composite key (FilePath + StartRow + EndRow)
	// This is necessary because a single file can be split into multiple fragments
	// with different row ranges, and they must be tracked independently
	newFragmentMap := make(map[string]packed.Fragment)
	for _, f := range newFragments {
		key := fragmentKey(f)
		newFragmentMap[key] = f
	}

	// Track which fragments are used by kept segments (use composite key)
	usedFragments := make(map[string]bool)
	var keptSegments []*datapb.SegmentInfo

	// Check each current segment
	for _, seg := range t.req.GetCurrentSegments() {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		fragments := currentSegmentFragments[seg.GetID()]
		allFragmentsExist := true

		// Check if all fragments of this segment still exist
		for _, f := range fragments {
			key := fragmentKey(f)
			if _, exists := newFragmentMap[key]; !exists {
				allFragmentsExist = false
				log.Info("Fragment removed from segment",
					zap.Int64("segmentID", seg.GetID()),
					zap.String("filePath", f.FilePath),
					zap.Int64("startRow", f.StartRow),
					zap.Int64("endRow", f.EndRow))
				break
			}
		}

		if allFragmentsExist {
			// Keep this segment unchanged
			keptSegments = append(keptSegments, seg)
			for _, f := range fragments {
				if err := ensureContext(ctx); err != nil {
					return nil, err
				}
				key := fragmentKey(f)
				usedFragments[key] = true
			}
			log.Debug("Segment kept unchanged",
				zap.Int64("segmentID", seg.GetID()))
		} else {
			// Segment invalidated - its remaining fragments become orphans
			log.Info("Segment invalidated due to removed fragments",
				zap.Int64("segmentID", seg.GetID()))
		}
	}

	// Collect orphan fragments (new + from invalidated segments)
	var orphanFragments []packed.Fragment
	for _, f := range newFragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		key := fragmentKey(f)
		if !usedFragments[key] {
			orphanFragments = append(orphanFragments, f)
		}
	}

	// Organize orphan fragments into new segments with balanced row counts
	createdSegments, err := t.balanceFragmentsToSegments(ctx, orphanFragments)
	if err != nil {
		return nil, err
	}

	// Track kept vs new separately for correct response building
	for _, seg := range keptSegments {
		t.keptSegmentIDs = append(t.keptSegmentIDs, seg.GetID())
	}
	t.newSegments = createdSegments

	// Combine kept and new segments
	result := append(keptSegments, createdSegments...)

	log.Info("Segment organization complete",
		zap.Int("keptSegments", len(keptSegments)),
		zap.Int("newSegments", len(createdSegments)),
		zap.Int("totalSegments", len(result)))

	return result, nil
}

// balanceFragmentsToSegments organizes fragments into segments with balanced row counts
func (t *RefreshExternalCollectionTask) balanceFragmentsToSegments(ctx context.Context, fragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
	if len(fragments) == 0 {
		return nil, nil
	}
	if err := ensureContext(ctx); err != nil {
		return nil, err
	}

	log := log.Ctx(ctx)

	// Calculate total rows
	var totalRows int64
	for _, f := range fragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		totalRows += f.RowCount
	}

	// Get target rows per segment from configuration
	targetRowsPerSegment := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()
	if totalRows < targetRowsPerSegment {
		targetRowsPerSegment = totalRows
	}

	numSegments := (totalRows + targetRowsPerSegment - 1) / targetRowsPerSegment
	if numSegments == 0 {
		numSegments = 1
	}

	avgRowsPerSegment := totalRows / numSegments

	log.Info("Balancing fragments to segments",
		zap.Int("numFragments", len(fragments)),
		zap.Int64("totalRows", totalRows),
		zap.Int64("numSegments", numSegments),
		zap.Int64("avgRowsPerSegment", avgRowsPerSegment))

	// Sort fragments by row count descending for better bin-packing
	sortedFragments := make([]packed.Fragment, len(fragments))
	copy(sortedFragments, fragments)
	sort.Slice(sortedFragments, func(i, j int) bool {
		return sortedFragments[i].RowCount > sortedFragments[j].RowCount
	})

	// Initialize segment bins
	type segmentBin struct {
		fragments []packed.Fragment
		rowCount  int64
	}
	bins := make([]segmentBin, numSegments)

	// Greedy bin-packing: assign each fragment to the bin with lowest current row count
	for _, f := range sortedFragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		// Find bin with minimum row count
		minIdx := 0
		for i := 1; i < len(bins); i++ {
			if bins[i].rowCount < bins[minIdx].rowCount {
				minIdx = i
			}
		}
		bins[minIdx].fragments = append(bins[minIdx].fragments, f)
		bins[minIdx].rowCount += f.RowCount
	}

	// Phase 1: Allocate segment IDs (sequential, lightweight)
	type segmentWork struct {
		segmentID   int64
		binlogLogID int64
		rowCount    int64
		fragments   []packed.Fragment
	}
	var works []segmentWork
	for _, bin := range bins {
		if len(bin.fragments) == 0 {
			continue
		}
		// Each segment needs 2 IDs: one for segment, one for fake binlog logID
		if t.nextAllocID+1 >= t.preallocatedIDRange.End {
			return nil, fmt.Errorf("insufficient pre-allocated IDs: need 2 more but only have %d IDs in range [%d, %d)",
				t.preallocatedIDRange.End-t.nextAllocID,
				t.preallocatedIDRange.Begin,
				t.preallocatedIDRange.End)
		}
		segmentID := t.nextAllocID
		binlogLogID := t.nextAllocID + 1
		t.nextAllocID += 2
		works = append(works, segmentWork{
			segmentID:   segmentID,
			binlogLogID: binlogLogID,
			rowCount:    bin.rowCount,
			fragments:   bin.fragments,
		})
	}

	log.Info("Allocated segment IDs, starting manifest creation",
		zap.Int("numSegments", len(works)))

	// Phase 2: Create manifests concurrently with a fixed-size worker pool.
	const createManifestWorkers = 16
	manifestStart := time.Now()

	workers := createManifestWorkers
	if workers > len(works) {
		workers = len(works)
	}
	pool := conc.NewPool[string](workers)
	defer pool.Release()

	manifestPaths := make([]string, len(works))
	futures := make([]*conc.Future[string], len(works))
	for i := range works {
		i, work := i, works[i]
		futures[i] = pool.Submit(func() (string, error) {
			// Honor ctx cancellation so workers bail out quickly once the
			// caller gives up instead of running createManifestForSegment
			// for every still-queued segment.
			if err := ctx.Err(); err != nil {
				return "", err
			}
			manifestPath, err := t.createManifestForSegment(ctx, work.segmentID, work.fragments)
			if err != nil {
				return "", fmt.Errorf("failed to create manifest for segment %d: %w", work.segmentID, err)
			}
			return manifestPath, nil
		})
	}

	// Wait for all futures; record the first error (if any) but still wait
	// for every future to finish so no goroutine is left running.
	var firstErr error
	for i, f := range futures {
		path, err := f.Await()
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if err == nil {
			manifestPaths[i] = path
		}
	}

	manifestDuration := time.Since(manifestStart)
	log.Info("CreateManifest phase completed",
		zap.Int("numSegments", len(works)),
		zap.Int("workers", workers),
		zap.Duration("duration", manifestDuration))

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if firstErr != nil {
		return nil, firstErr
	}

	// Phase 3: Sample field sizes so that downstream code (QueryNode memory
	// estimation) can use Binlog.MemorySize directly, eliminating the need
	// for a separate Take-sampling step at load time. Two modes:
	//   - samplePerSegment=false (default): sample only the first segment
	//     and reuse that average for all segments in this task. Cheap, but
	//     can be off when row density varies across files.
	//   - samplePerSegment=true: sample each segment independently. More
	//     I/O, but per-segment MemorySize is accurate.
	//
	// If a later segment's sampling fails, we fall back to the first
	// successful avgBytesPerRow so MemorySize is never written as 0 — a
	// zero MemorySize would make QueryNode's memory estimation collapse
	// and risk OOM on load. If ALL samples fail (or return a 0 sum because
	// the schema has no ExternalField-mapped fields, or the sampled rows
	// somehow evaluate to 0 bytes), we fail the task rather than silently
	// producing zero-sized segments that would skew QN's resource estimator
	// and risk OOM on load.
	samplePerSegment := paramtable.Get().QueryNodeCfg.ExternalCollectionSamplePerSegment.GetAsBool()
	sampleRows := paramtable.Get().QueryNodeCfg.ExternalCollectionSampleRows.GetAsInt()
	segmentAvgBytes := make([]int64, len(works))
	var fallbackAvg int64

	extfsPrefix := packed.ExtfsPrefixForCollection(t.req.GetCollectionID())
	specExtfs := t.parsedSpec.BuildExtfsOverrides(extfsPrefix)

	sampleOne := func(manifestPath string) (int64, bool) {
		fieldSizes, err := packed.SampleExternalFieldSizes(
			manifestPath, sampleRows,
			t.req.GetCollectionID(),
			t.req.GetExternalSource(),
			t.req.GetExternalSpec(),
			t.req.GetStorageConfig(),
			specExtfs,
		)
		if err != nil {
			log.Warn("failed to sample external field sizes",
				zap.String("manifestPath", manifestPath),
				zap.Error(err))
			return 0, false
		}
		total := sumFieldSizes(fieldSizes, t.req.GetSchema())
		if total <= 0 {
			// A non-positive total is treated as a sample failure so the
			// caller can decide whether to fall back or fail the task. The
			// zero can come from (a) a schema with no ExternalField-mapped
			// fields, or (b) a Parquet file whose sampled rows really are
			// empty — both are degenerate and must not feed QN a zero.
			log.Warn("external field size sample produced non-positive total",
				zap.String("manifestPath", manifestPath),
				zap.Int64("total", total))
			return 0, false
		}
		return total, true
	}

	if len(manifestPaths) > 0 {
		if samplePerSegment {
			for i, mp := range manifestPaths {
				if avg, ok := sampleOne(mp); ok {
					segmentAvgBytes[i] = avg
					if fallbackAvg == 0 {
						fallbackAvg = avg
					}
				}
			}
			log.Info("per-segment sampling complete",
				zap.Int("numSegments", len(manifestPaths)),
				zap.Int64("fallbackAvgBytesPerRow", fallbackAvg))
		} else {
			if avg, ok := sampleOne(manifestPaths[0]); ok {
				fallbackAvg = avg
				for i := range segmentAvgBytes {
					segmentAvgBytes[i] = avg
				}
			}
			log.Info("single-sample complete",
				zap.Int64("avgBytesPerRow", fallbackAvg))
		}
		// If every sample failed, fail the task rather than emitting
		// zero-MemorySize fake binlogs that would collapse QueryNode's
		// resource estimator and risk OOM on load. We prefer a loud
		// failure (retry-eligible via the task state machine) over a
		// silent success that corrupts downstream accounting.
		if fallbackAvg == 0 {
			return nil, fmt.Errorf(
				"external field size sampling failed for all %d segment(s); "+
					"refusing to emit segments with MemorySize=0 (would risk QueryNode OOM on load). "+
					"check sample logs above for the root cause",
				len(manifestPaths))
		}
		// Fill any zero slots (sampling failed mid-loop) with the first
		// successful average so every segment gets a non-zero MemorySize.
		for i, v := range segmentAvgBytes {
			if v == 0 {
				segmentAvgBytes[i] = fallbackAvg
			}
		}
	}

	// Phase 4: Build result and mappings (sequential, lightweight)
	result := make([]*datapb.SegmentInfo, 0, len(works))
	for i, work := range works {
		memorySize := segmentAvgBytes[i] * work.rowCount
		seg := &datapb.SegmentInfo{
			ID:             work.segmentID,
			CollectionID:   t.req.GetCollectionID(),
			NumOfRows:      work.rowCount,
			ManifestPath:   manifestPaths[i],
			StorageVersion: storage.StorageV3,
			// Fake binlog so downstream treats external segments like normal
			// StorageV3 segments. MemorySize is pre-computed from Take sampling
			// so QueryNode skips the external-specific sampling path.
			Binlogs: buildFakeBinlogs(work.binlogLogID, work.rowCount, memorySize, t.req.GetSchema()),
		}
		result = append(result, seg)
	}

	return result, nil
}

// createManifestForSegment creates a manifest file for the segment
func (t *RefreshExternalCollectionTask) createManifestForSegment(
	ctx context.Context,
	segmentID int64,
	fragments []packed.Fragment,
) (string, error) {
	// All segments now use final paths with real IDs (no temporary paths needed)
	// Pre-allocated IDs ensure we can write directly to final locations
	basePath := fmt.Sprintf(
		"external/%d/segments/%d",
		t.req.GetCollectionID(),
		segmentID,
	)

	return packed.CreateSegmentManifestWithBasePath(
		ctx,
		basePath,
		t.parsedSpec.Format,
		t.columns,
		fragments,
		t.req.GetStorageConfig(),
	)
}

// buildFakeBinlogs creates a synthetic FieldBinlog slice for an external segment.
// It uses DefaultShortColumnGroupID (0) to match StorageV3 convention, so that
// downstream code (row count calculation, index association, memory estimation)
// treats external segments the same as normal packed segments.
// ChildFields must list all field IDs so that QueryNode can resolve field schemas.
func buildFakeBinlogs(logID, numRows, memorySize int64, schema *schemapb.CollectionSchema) []*datapb.FieldBinlog {
	var childFields []int64
	if schema != nil {
		for _, field := range schema.GetFields() {
			childFields = append(childFields, field.GetFieldID())
		}
	}
	return []*datapb.FieldBinlog{
		{
			FieldID:     int64(storagecommon.DefaultShortColumnGroupID),
			ChildFields: childFields,
			Binlogs: []*datapb.Binlog{
				{
					LogID:      logID,
					EntriesNum: numRows,
					MemorySize: memorySize,
					LogSize:    memorySize, // conservative: assume no compression
				},
			},
		},
	}
}

// sumFieldSizes computes total avgBytesPerRow from per-field sampling results.
// Only external fields (those with ExternalField set) are counted to match
// the QueryNode estimation logic.
func sumFieldSizes(fieldSizes map[string]int64, schema *schemapb.CollectionSchema) int64 {
	if schema == nil {
		var total int64
		for _, v := range fieldSizes {
			total += v
		}
		return total
	}
	var total int64
	for _, field := range schema.GetFields() {
		extName := field.GetExternalField()
		if extName == "" {
			continue
		}
		if avgBytes, ok := fieldSizes[extName]; ok && avgBytes > 0 {
			total += avgBytes
		}
	}
	return total
}
