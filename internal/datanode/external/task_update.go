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

// UpdateExternalTask handles updating external collection segments by fetching fragments from external sources
// and organizing them into segments with balanced row counts.
//
// SEGMENT ID ALLOCATION WORKFLOW:
// - DataCoord pre-allocates a batch of segment IDs (default 1000) via allocator.AllocN()
// - Pre-allocated ID range is passed to DataNode via UpdateExternalCollectionRequest.PreAllocatedSegmentIds
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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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

// FragmentRowRange represents the row index range for a fragment within a segment
type FragmentRowRange struct {
	FragmentID int64
	StartRow   int64 // inclusive
	EndRow     int64 // exclusive
}

// SegmentRowMapping holds the row index mapping for all fragments in a segment
type SegmentRowMapping struct {
	SegmentID int64
	TotalRows int64
	Ranges    []FragmentRowRange
	Fragments []packed.Fragment
}

// NewSegmentRowMapping creates a row mapping from fragments
// Fragments are mapped sequentially: fragment1 gets [0, rowCount1), fragment2 gets [rowCount1, rowCount1+rowCount2), etc.
func NewSegmentRowMapping(segmentID int64, fragments []packed.Fragment) *SegmentRowMapping {
	mapping := &SegmentRowMapping{
		SegmentID: segmentID,
		Fragments: fragments,
		Ranges:    make([]FragmentRowRange, len(fragments)),
	}

	var offset int64
	for i, f := range fragments {
		mapping.Ranges[i] = FragmentRowRange{
			FragmentID: f.FragmentID,
			StartRow:   offset,
			EndRow:     offset + f.RowCount,
		}
		offset += f.RowCount
	}
	mapping.TotalRows = offset

	return mapping
}

// GetFragmentByRowIndex returns the fragment range that contains the given row index
// Returns nil if rowIndex is out of range
// To get local index within fragment: rowIndex - range.StartRow
func (m *SegmentRowMapping) GetFragmentByRowIndex(rowIndex int64) *FragmentRowRange {
	if rowIndex < 0 || rowIndex >= m.TotalRows {
		return nil
	}

	// Binary search for efficiency
	left, right := 0, len(m.Ranges)-1
	for left <= right {
		mid := (left + right) / 2
		r := &m.Ranges[mid]
		if rowIndex >= r.StartRow && rowIndex < r.EndRow {
			return r
		} else if rowIndex < r.StartRow {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return nil
}

// SegmentResult holds a segment and its fragment row mapping
type SegmentResult struct {
	Segment    *datapb.SegmentInfo
	RowMapping *SegmentRowMapping
	IsNew      bool // true if this is a newly created segment requiring ID allocation
}

// UpdateExternalTask handles updating external collection segments
type UpdateExternalTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	req *datapb.UpdateExternalCollectionRequest
	tr  *timerecord.TimeRecorder

	state      indexpb.JobState
	failReason string

	// Cached parsed spec (populated in PreExecute, reused in Execute)
	parsedSpec *ExternalSpec
	columns    []string

	// Result after execution — tracked separately for correct response building
	keptSegmentIDs  []int64                      // IDs of current segments that were kept unchanged
	newSegments     []*datapb.SegmentInfo        // newly created segments (from orphan fragments)
	updatedSegments []*datapb.SegmentInfo        // all segments (kept + new), for GetSegmentResults
	segmentMappings map[int64]*SegmentRowMapping // segmentID -> row mapping

	// Pre-allocated segment IDs
	preallocatedIDRange *datapb.IDRange // pre-allocated segment ID range (begin, end)
	nextAllocID         int64           // next available segment ID from pre-allocated range
}

// NewUpdateExternalTask creates a new update external task
func NewUpdateExternalTask(
	ctx context.Context,
	cancel context.CancelFunc,
	req *datapb.UpdateExternalCollectionRequest,
) *UpdateExternalTask {
	return &UpdateExternalTask{
		ctx:             ctx,
		cancel:          cancel,
		req:             req,
		tr:              timerecord.NewTimeRecorder(fmt.Sprintf("UpdateExternalTask: %d", req.GetTaskID())),
		state:           indexpb.JobState_JobStateInit,
		segmentMappings: make(map[int64]*SegmentRowMapping),
	}
}

func (t *UpdateExternalTask) Ctx() context.Context {
	return t.ctx
}

func (t *UpdateExternalTask) Name() string {
	return fmt.Sprintf("UpdateExternalTask-%d", t.req.GetTaskID())
}

func (t *UpdateExternalTask) OnEnqueue(ctx context.Context) error {
	t.tr.RecordSpan()
	log.Ctx(ctx).Info("UpdateExternalTask enqueued",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()))
	return nil
}

func (t *UpdateExternalTask) SetState(state indexpb.JobState, failReason string) {
	t.state = state
	t.failReason = failReason
}

func (t *UpdateExternalTask) GetState() indexpb.JobState {
	return t.state
}

func (t *UpdateExternalTask) GetSlot() int64 {
	return 1
}

func (t *UpdateExternalTask) Reset() {
	t.ctx = nil
	t.cancel = nil
	t.req = nil
	t.tr = nil
	t.keptSegmentIDs = nil
	t.newSegments = nil
	t.updatedSegments = nil
	t.segmentMappings = nil
}

func (t *UpdateExternalTask) PreExecute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log.Ctx(ctx).Info("UpdateExternalTask PreExecute",
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
	spec, err := ParseExternalSpec(t.req.GetExternalSpec())
	if err != nil {
		return fmt.Errorf("failed to parse external spec: %w", err)
	}
	t.parsedSpec = spec
	t.columns = packed.GetColumnNamesFromSchema(t.req.GetSchema())

	return nil
}

func (t *UpdateExternalTask) Execute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log := log.Ctx(ctx)
	log.Info("UpdateExternalTask Execute",
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

// fetchFragmentsFromExternalSource scans the external source and returns fragments
func (t *UpdateExternalTask) fetchFragmentsFromExternalSource(ctx context.Context) ([]packed.Fragment, error) {
	return packed.FetchFragmentsFromExternalSource(
		ctx,
		t.parsedSpec.Format,
		t.columns,
		t.req.GetExternalSource(),
		t.req.GetStorageConfig(),
	)
}

func (t *UpdateExternalTask) PostExecute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log.Ctx(ctx).Info("UpdateExternalTask PostExecute",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()),
		zap.Int("updatedSegments", len(t.updatedSegments)))
	return nil
}

// GetUpdatedSegments returns all result segments (kept + new) after execution
func (t *UpdateExternalTask) GetUpdatedSegments() []*datapb.SegmentInfo {
	return t.updatedSegments
}

// GetKeptSegmentIDs returns IDs of current segments that were kept unchanged
func (t *UpdateExternalTask) GetKeptSegmentIDs() []int64 {
	return t.keptSegmentIDs
}

// GetNewSegments returns only newly created segments (from orphan fragment rebalancing)
func (t *UpdateExternalTask) GetNewSegments() []*datapb.SegmentInfo {
	return t.newSegments
}

// GetSegmentMappings returns the row mappings for all segments (segmentID -> mapping)
func (t *UpdateExternalTask) GetSegmentMappings() map[int64]*SegmentRowMapping {
	return t.segmentMappings
}

// GetSegmentResults returns segment results with metadata for DataCoord processing
func (t *UpdateExternalTask) GetSegmentResults() []*SegmentResult {
	var results []*SegmentResult

	// Build a map of current segment IDs for fast lookup
	currentSegmentIDSet := make(map[int64]bool)
	for _, seg := range t.req.GetCurrentSegments() {
		currentSegmentIDSet[seg.GetID()] = true
	}

	for _, seg := range t.updatedSegments {
		results = append(results, &SegmentResult{
			Segment:    seg,
			RowMapping: t.segmentMappings[seg.GetID()],
			IsNew:      !currentSegmentIDSet[seg.GetID()], // New if not in original current segments
		})
	}

	return results
}

// fragmentKey generates a unique key for a fragment using its FilePath and row range
// This composite key ensures fragments from the same file with different row ranges
// are treated as distinct entities, which is critical for correct data mapping
func fragmentKey(f packed.Fragment) string {
	return fmt.Sprintf("%s:%d:%d", f.FilePath, f.StartRow, f.EndRow)
}

// buildCurrentSegmentFragments builds segment to fragments mapping from current segments
func (t *UpdateExternalTask) buildCurrentSegmentFragments() (packed.SegmentFragments, error) {
	return packed.BuildCurrentSegmentFragments(t.req.GetCurrentSegments(), t.req.GetStorageConfig())
}

// organizeSegments compares fragments and organizes them into segments
func (t *UpdateExternalTask) organizeSegments(
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
			// Compute row mapping for kept segment
			t.segmentMappings[seg.GetID()] = NewSegmentRowMapping(seg.GetID(), fragments)
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
func (t *UpdateExternalTask) balanceFragmentsToSegments(ctx context.Context, fragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
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

	// Convert bins to SegmentInfo
	var result []*datapb.SegmentInfo
	for _, bin := range bins {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		if len(bin.fragments) == 0 {
			continue
		}

		// Allocate segment ID from pre-allocated range
		if t.nextAllocID >= t.preallocatedIDRange.End {
			return nil, fmt.Errorf("insufficient pre-allocated segment IDs: need more but only have %d IDs in range [%d, %d)",
				t.preallocatedIDRange.End-t.preallocatedIDRange.Begin,
				t.preallocatedIDRange.Begin,
				t.preallocatedIDRange.End)
		}
		segmentID := t.nextAllocID
		t.nextAllocID++

		log.Info("Assigned pre-allocated segment ID",
			zap.Int64("segmentID", segmentID),
			zap.Int64("nextID", t.nextAllocID),
			zap.Int64("rowCount", bin.rowCount),
			zap.Int("numFragments", len(bin.fragments)))

		// Create manifest for this segment
		manifestPath, err := t.createManifestForSegment(ctx, segmentID, bin.fragments)
		if err != nil {
			return nil, fmt.Errorf("failed to create manifest for segment %d: %w", segmentID, err)
		}

		seg := &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: t.req.GetCollectionID(),
			NumOfRows:    bin.rowCount,
			ManifestPath: manifestPath,
		}
		result = append(result, seg)

		// Compute and store row mapping for new segment
		t.segmentMappings[segmentID] = NewSegmentRowMapping(segmentID, bin.fragments)

		log.Debug("Created new segment from fragments",
			zap.Int64("segmentID", segmentID),
			zap.Int64("rowCount", bin.rowCount),
			zap.Int("numFragments", len(bin.fragments)),
			zap.String("manifestPath", manifestPath))
	}

	return result, nil
}

// createManifestForSegment creates a manifest file for the segment
func (t *UpdateExternalTask) createManifestForSegment(
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
