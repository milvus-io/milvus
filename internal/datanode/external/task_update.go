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

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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

// Fragment represents a data fragment from external source
type Fragment struct {
	FragmentID int64
	RowCount   int64
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
	Fragments []Fragment
}

// NewSegmentRowMapping creates a row mapping from fragments
// Fragments are mapped sequentially: fragment1 gets [0, rowCount1), fragment2 gets [rowCount1, rowCount1+rowCount2), etc.
func NewSegmentRowMapping(segmentID int64, fragments []Fragment) *SegmentRowMapping {
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

// SegmentFragments maps segment ID to its fragments
type SegmentFragments map[int64][]Fragment

// SegmentResult holds a segment and its fragment row mapping
type SegmentResult struct {
	Segment    *datapb.SegmentInfo
	RowMapping *SegmentRowMapping
}

// UpdateExternalTask handles updating external collection segments
type UpdateExternalTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	req *datapb.UpdateExternalCollectionRequest
	tr  *timerecord.TimeRecorder

	state      indexpb.JobState
	failReason string

	// Result after execution
	updatedSegments []*datapb.SegmentInfo
	segmentMappings map[int64]*SegmentRowMapping // segmentID -> row mapping
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
	return nil
}

func (t *UpdateExternalTask) Execute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	log.Ctx(ctx).Info("UpdateExternalTask Execute",
		zap.Int64("taskID", t.req.GetTaskID()),
		zap.Int64("collectionID", t.req.GetCollectionID()))

	// TODO: Fetch fragments from external source
	// newFragments := fetchFragmentsFromExternalSource(t.req.GetExternalSource(), t.req.GetExternalSpec())
	var newFragments []Fragment

	// Build current segment -> fragments mapping
	// TODO: This mapping should come from metadata or be stored in SegmentInfo
	currentSegmentFragments := t.buildCurrentSegmentFragments()

	// Compare and organize segments
	updatedSegments, err := t.organizeSegments(ctx, currentSegmentFragments, newFragments)
	if err != nil {
		return err
	}

	t.updatedSegments = updatedSegments
	return nil
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

// GetUpdatedSegments returns the result segments after execution
func (t *UpdateExternalTask) GetUpdatedSegments() []*datapb.SegmentInfo {
	return t.updatedSegments
}

// GetSegmentMappings returns the row mappings for all segments (segmentID -> mapping)
func (t *UpdateExternalTask) GetSegmentMappings() map[int64]*SegmentRowMapping {
	return t.segmentMappings
}

// buildCurrentSegmentFragments builds segment to fragments mapping from current segments
func (t *UpdateExternalTask) buildCurrentSegmentFragments() SegmentFragments {
	result := make(SegmentFragments)
	// TODO: Extract fragment information from segment metadata
	// For now, this is a placeholder - fragment info should be stored in segment metadata
	for _, seg := range t.req.GetCurrentSegments() {
		// Placeholder: each segment has its own "virtual" fragment
		result[seg.GetID()] = []Fragment{
			{FragmentID: seg.GetID(), RowCount: seg.GetNumOfRows()},
		}
	}
	return result
}

// organizeSegments compares fragments and organizes them into segments
func (t *UpdateExternalTask) organizeSegments(
	ctx context.Context,
	currentSegmentFragments SegmentFragments,
	newFragments []Fragment,
) ([]*datapb.SegmentInfo, error) {
	if err := ensureContext(ctx); err != nil {
		return nil, err
	}
	log := log.Ctx(ctx)

	// Build new fragment map for quick lookup
	newFragmentMap := make(map[int64]Fragment)
	for _, f := range newFragments {
		newFragmentMap[f.FragmentID] = f
	}

	// Track which fragments are used by kept segments
	usedFragments := make(map[int64]bool)
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
			if _, exists := newFragmentMap[f.FragmentID]; !exists {
				allFragmentsExist = false
				log.Info("Fragment removed from segment",
					zap.Int64("segmentID", seg.GetID()),
					zap.Int64("fragmentID", f.FragmentID))
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
				usedFragments[f.FragmentID] = true
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
	var orphanFragments []Fragment
	for _, f := range newFragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		if !usedFragments[f.FragmentID] {
			orphanFragments = append(orphanFragments, f)
		}
	}

	// Organize orphan fragments into new segments with balanced row counts
	newSegments, err := t.balanceFragmentsToSegments(ctx, orphanFragments)
	if err != nil {
		return nil, err
	}

	// Combine kept and new segments
	result := append(keptSegments, newSegments...)

	log.Info("Segment organization complete",
		zap.Int("keptSegments", len(keptSegments)),
		zap.Int("newSegments", len(newSegments)),
		zap.Int("totalSegments", len(result)))

	return result, nil
}

// balanceFragmentsToSegments organizes fragments into segments with balanced row counts
func (t *UpdateExternalTask) balanceFragmentsToSegments(ctx context.Context, fragments []Fragment) ([]*datapb.SegmentInfo, error) {
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

	// Determine target rows per segment (use a reasonable default)
	// TODO: Make this configurable or based on segment size limits
	targetRowsPerSegment := int64(1000000) // 1M rows per segment as default
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
	sortedFragments := make([]Fragment, len(fragments))
	copy(sortedFragments, fragments)
	sort.Slice(sortedFragments, func(i, j int) bool {
		return sortedFragments[i].RowCount > sortedFragments[j].RowCount
	})

	// Initialize segment bins
	type segmentBin struct {
		fragments []Fragment
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
	for i, bin := range bins {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		if len(bin.fragments) == 0 {
			continue
		}

		// TODO: Generate column groups
		// Just placeholder here. ID will be assigned by coordinator.
		segmentID := int64(i + 1)

		seg := &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: t.req.GetCollectionID(),
			NumOfRows:    bin.rowCount,
			// TODO: Fill other required fields
		}
		result = append(result, seg)

		// Compute and store row mapping for new segment
		t.segmentMappings[segmentID] = NewSegmentRowMapping(segmentID, bin.fragments)

		log.Debug("Created new segment from fragments",
			zap.Int64("segmentID placeholder", segmentID),
			zap.Int64("rowCount", bin.rowCount),
			zap.Int("numFragments", len(bin.fragments)))
	}

	return result, nil
}
