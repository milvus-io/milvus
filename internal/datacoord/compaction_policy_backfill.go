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
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// FuncDiff represents the differences in functions between two schemas.
type FuncDiff struct {
	Added []*schemapb.FunctionSchema
}

type backfillCompactionPolicy struct {
	meta      *meta
	handler   Handler
	allocator allocator.Allocator
}

// Ensure backfillCompactionPolicy implements CompactionPolicy interface
var _ CompactionPolicy = (*backfillCompactionPolicy)(nil)

func newBackfillCompactionPolicy(meta *meta, allocator allocator.Allocator, handler Handler) *backfillCompactionPolicy {
	return &backfillCompactionPolicy{meta: meta, allocator: allocator, handler: handler}
}

func (policy *backfillCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *backfillCompactionPolicy) Name() string {
	return "BackfillCompaction"
}

// getMissingFunctions checks which functions in the collection schema have output fields
// missing from the segment's binlogs. Returns the list of functions that need backfill.
func (policy *backfillCompactionPolicy) getMissingFunctions(segment *SegmentInfo, schema *schemapb.CollectionSchema) []*schemapb.FunctionSchema {
	// Collect all field IDs present in segment binlogs (FieldID + ChildFields)
	existingFields := make(map[int64]struct{})
	for _, binlog := range segment.GetBinlogs() {
		existingFields[binlog.GetFieldID()] = struct{}{}
		for _, childFieldID := range binlog.GetChildFields() {
			existingFields[childFieldID] = struct{}{}
		}
	}

	// Check each function's output fields
	var missing []*schemapb.FunctionSchema
	for _, fn := range schema.GetFunctions() {
		for _, outputFieldID := range fn.GetOutputFieldIds() {
			if _, ok := existingFields[outputFieldID]; !ok {
				missing = append(missing, fn)
				break // one missing output field is enough to mark this function
			}
		}
	}
	return missing
}

func (policy *backfillCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	collections := policy.meta.GetCollections()
	events := make(map[CompactionTriggerType][]CompactionView)

	for _, collection := range collections {
		collectionID := collection.ID
		collectionSchemaVersion := collection.Schema.GetVersion()
		// L0 segments are excluded from both the consistency gate (GetCollectionStatistics)
		// and from backfill processing: they only contain delete logs, so there is no user
		// data to backfill and no need to update their schema version here.
		partSegments := GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return isSegmentHealthy(segment) &&
				isFlushed(segment) &&
				!segment.isCompacting && // not compacting now
				!segment.GetIsImporting() && // not importing now
				!segment.GetIsInvisible() &&
				segment.GetLevel() != datapb.SegmentLevel_L0 // L0 segments only contain deletes, no data to backfill
		}))

		// Check each segment's schema version
		views := make([]CompactionView, 0)
		doPhysicalBackfill := collection.Schema.GetDoPhysicalBackfill()
		// triggerID is allocated per collection so that tasks belonging to the same collection
		// can be grouped and queried independently (e.g. to check backfill progress per collection).
		// Allocating once per tick across all collections would mix tasks from different collections
		// under one ID, making per-collection progress tracking impossible.
		var collectionTriggerID int64
		for _, group := range partSegments {
			for _, segment := range group.segments {
				segmentID := segment.GetID()
				segmentSchemaVersion := segment.GetSchemaVersion()
				// Skip segments with up-to-date schema version
				if segmentSchemaVersion >= collectionSchemaVersion {
					continue
				}
				if !doPhysicalBackfill {
					// No physical backfill needed — update schema version metadata only.
					// If UpdateSegment fails, we log and move on.  This is intentional: the
					// segment's schema version will remain below the collection's, so the next
					// trigger cycle will revisit this segment automatically.  The failure does
					// NOT block the consistency gate or cause permanent state corruption — it
					// is purely a transient metadata write that will self-heal on retry.
					err := policy.meta.UpdateSegment(segmentID, SetSchemaVersion(collectionSchemaVersion))
					if err != nil {
						log.Ctx(ctx).Error("Failed to update segment schema version",
							zap.Int64("segmentID", segmentID),
							zap.Int64("collectionID", collectionID),
							zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
							zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
							zap.Error(err))
					} else {
						log.Ctx(ctx).Info("Updated segment schema version",
							zap.Int64("segmentID", segmentID),
							zap.Int64("collectionID", collectionID),
							zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
							zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
						)
					}
					continue
				}
				// Physical backfill path: check which function output fields are missing from segment binlogs
				missingFunctions := policy.getMissingFunctions(segment, collection.Schema)
				if len(missingFunctions) == 0 {
					// All function output fields exist, just update schema version
					err := policy.meta.UpdateSegment(segmentID, SetSchemaVersion(collectionSchemaVersion))
					if err != nil {
						log.Ctx(ctx).Error("Failed to update segment schema version, no missing functions",
							zap.Int64("segmentID", segmentID),
							zap.Int64("collectionID", collectionID),
							zap.Error(err))
					} else {
						log.Ctx(ctx).Info("All function output fields present, updated schema version directly",
							zap.Int64("segmentID", segmentID),
							zap.Int64("collectionID", collectionID),
							zap.Int32("newSchemaVersion", collectionSchemaVersion))
					}
					continue
				}
				// Lazily allocate a triggerID for this collection on the first segment that needs
				// physical backfill. This avoids an unnecessary AllocID RPC for collections whose
				// segments only require metadata-only updates or are already up-to-date.
				if collectionTriggerID == 0 {
					id, err := policy.allocator.AllocID(ctx)
					if err != nil {
						log.Ctx(ctx).Warn("Failed to allocate triggerID for backfill, skip remaining segments in current group",
							zap.Int64("collectionID", collectionID),
							zap.Error(err))
						break
					}
					collectionTriggerID = id
				}
				// Current compactor only supports one function per backfill task,
				// so pick the first missing function. In practice, the multi-function
				// scenario cannot occur: RootCoord enforces exactly one function per
				// AlterCollectionSchema request (ddl_callbacks_alter_collection_schema.go),
				// and Proxy blocks new schema alterations until previous backfill reaches
				// 100% consistency (checkSchemaVersionConsistency in impl.go).
				backfillFunc := missingFunctions[0]
				log.Ctx(ctx).Info("Found segment missing function output fields, do physical backfill",
					zap.Int64("segmentID", segmentID),
					zap.Int64("collectionID", collectionID),
					zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
					zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
					zap.String("backfillFunction", backfillFunc.GetName()),
					zap.Int("totalMissingFunctions", len(missingFunctions)))

				// Create BackfillSegmentsView for this segment with single function.
				// GetViewsByInfo returns exactly one SegmentView per segment (one segment
				// maps to one channel/partition group label); len > 1 is not expected.
				segmentViews := GetViewsByInfo(segment)
				if len(segmentViews) == 0 {
					log.Ctx(ctx).Warn("GetViewsByInfo returned empty views, skip segment",
						zap.Int64("segmentID", segmentID))
					continue
				}
				if len(segmentViews) != 1 {
					log.Ctx(ctx).Warn("GetViewsByInfo returned unexpected view count, using first view only",
						zap.Int64("segmentID", segmentID),
						zap.Int("viewCount", len(segmentViews)))
				}
				view := &BackfillSegmentsView{
					label:     segmentViews[0].label,
					segments:  segmentViews,
					triggerID: collectionTriggerID,
					funcDiff:  &FuncDiff{Added: []*schemapb.FunctionSchema{backfillFunc}},
				}
				views = append(views, view)
			}
		}

		// Add views to events if any segments need backfill
		if len(views) > 0 {
			if events[TriggerTypeBackfill] == nil {
				events[TriggerTypeBackfill] = make([]CompactionView, 0)
			}
			events[TriggerTypeBackfill] = append(events[TriggerTypeBackfill], views...)
		}
	}

	return events, nil
}

type BackfillSegmentsView struct {
	label     *CompactionGroupLabel
	segments  []*SegmentView
	triggerID int64
	funcDiff  *FuncDiff
}

var _ CompactionView = (*BackfillSegmentsView)(nil)

func (v *BackfillSegmentsView) GetGroupLabel() *CompactionGroupLabel {
	return v.label
}

func (v *BackfillSegmentsView) GetSegmentsView() []*SegmentView {
	return v.segments
}

func (v *BackfillSegmentsView) Append(segments ...*SegmentView) {
	v.segments = append(v.segments, segments...)
}

func (v *BackfillSegmentsView) String() string {
	return fmt.Sprintf("BackfillSegmentsView: label=%s, segments=%d, triggerID=%d",
		v.label.Key(), len(v.segments), v.triggerID)
}

func (v *BackfillSegmentsView) Trigger() (CompactionView, string) {
	return v, "backfill schema version mismatch"
}

func (v *BackfillSegmentsView) ForceTrigger() (CompactionView, string) {
	return v.Trigger()
}

// ForceTriggerAll returns a single-element slice intentionally: the backfill policy
// creates one BackfillSegmentsView per segment, so each view already represents the
// smallest unit of work. Unlike LevelZeroCompactionView or ForceMergeSegmentView
// which aggregate many segments and need multi-round splitting, there is nothing to
// split further here.
func (v *BackfillSegmentsView) ForceTriggerAll() ([]CompactionView, string) {
	view, reason := v.Trigger()
	return []CompactionView{view}, reason
}

func (v *BackfillSegmentsView) GetTriggerID() int64 {
	return v.triggerID
}
