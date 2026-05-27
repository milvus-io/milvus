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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	// Always enabled: both metadata-only and physical backfill are correctness requirements.
	// Without backfill, segments can never reconcile missing function output fields and
	// schema version consistency will never be reached, blocking GetCollectionStatistics.
	return true
}

func (policy *backfillCompactionPolicy) Name() string {
	return "BackfillCompaction"
}

// getMissingFunctions checks which functions in the collection schema have output fields
// missing from the segment's binlogs. Returns the list of functions that need backfill.
func (policy *backfillCompactionPolicy) getMissingFunctions(segment *SegmentInfo, schema *schemapb.CollectionSchema) []*schemapb.FunctionSchema {
	existingFields := getSegmentBinlogFields(segment)
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

// staleFlushedSegments returns the flushed segments for collectionID whose SchemaVersion
// lags behind collectionSchemaVersion. L0 segments are excluded (deletes only, no data).
func (policy *backfillCompactionPolicy) staleFlushedSegments(collectionID int64, collectionSchemaVersion int32) []*chanPartSegments {
	return GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting &&
			!segment.GetIsImporting() &&
			!segment.GetIsInvisible() &&
			segment.GetLevel() != datapb.SegmentLevel_L0 &&
			segment.GetSchemaVersion() < collectionSchemaVersion
	}))
}

// TriggerInline returns metadata-only backfill views that can be applied without
// consuming inspector slots. Two cases qualify:
//  1. DoPhysicalBackfill=false: schema-only change, no function output to compute.
//  2. DoPhysicalBackfill=true but all function outputs already present in binlogs:
//     stale SchemaVersion anomaly — self-heal by bumping the version in-place.
//
// TriggerInline is called before the inspector isFull() check so these lightweight
// updates always proceed regardless of compaction queue capacity.
func (policy *backfillCompactionPolicy) TriggerInline(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	collections := policy.meta.GetCollections()
	events := make(map[CompactionTriggerType][]CompactionView)

	for _, collection := range collections {
		collectionID := collection.ID
		collectionSchemaVersion := collection.Schema.GetVersion()
		doPhysicalBackfill := collection.Schema.GetDoPhysicalBackfill()
		partSegments := policy.staleFlushedSegments(collectionID, collectionSchemaVersion)

		var views []CompactionView
		for _, group := range partSegments {
			for _, segment := range group.segments {
				segmentID := segment.GetID()
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

				// Case 1: no physical data to rewrite — bump SchemaVersion only.
				if !doPhysicalBackfill {
					views = append(views, &BackfillSegmentsView{
						label:               segmentViews[0].label,
						segments:            segmentViews,
						inlineMetaOnly:      true,
						targetSchemaVersion: collectionSchemaVersion,
					})
					continue
				}

				// Case 2: physical mode but all function outputs already present — self-heal.
				missingFunctions := policy.getMissingFunctions(segment, collection.Schema)
				if len(missingFunctions) == 0 {
					log.Ctx(ctx).Error("backfill: segment has all function output fields but schema_version is stale, self-healing via inline meta update",
						zap.Int64("segmentID", segmentID),
						zap.Int64("collectionID", collectionID),
						zap.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
						zap.Int32("collectionSchemaVersion", collectionSchemaVersion))
					views = append(views, &BackfillSegmentsView{
						label:               segmentViews[0].label,
						segments:            segmentViews,
						inlineMetaOnly:      true,
						targetSchemaVersion: collectionSchemaVersion,
					})
				}
				// Segments with missing functions are handled by Trigger() (physical backfill).
			}
		}
		if len(views) > 0 {
			events[TriggerTypeBackfill] = append(events[TriggerTypeBackfill], views...)
		}
	}
	return events, nil
}

// Trigger returns physical backfill views for segments that are missing function output
// fields. It is only called when the inspector has available capacity (after TriggerInline
// and the isFull() gate in handleTicker).
func (policy *backfillCompactionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	collections := policy.meta.GetCollections()
	events := make(map[CompactionTriggerType][]CompactionView)

	for _, collection := range collections {
		if !collection.Schema.GetDoPhysicalBackfill() {
			// Metadata-only collections are handled entirely by TriggerInline.
			continue
		}
		collectionID := collection.ID
		collectionSchemaVersion := collection.Schema.GetVersion()
		partSegments := policy.staleFlushedSegments(collectionID, collectionSchemaVersion)

		var views []CompactionView
		// triggerID is allocated per collection so tasks belonging to the same collection
		// can be grouped and queried independently. Lazy-allocated on the first segment
		// that actually needs a physical backfill task.
		var collectionTriggerID int64
		for _, group := range partSegments {
			for _, segment := range group.segments {
				segmentID := segment.GetID()
				missingFunctions := policy.getMissingFunctions(segment, collection.Schema)
				if len(missingFunctions) == 0 {
					// Self-heal case — already handled by TriggerInline, skip here.
					continue
				}
				// checkSchemaVersionConsistencyAtRootCoord guarantees the cluster-wide
				// schema version increments by exactly 1 per DDL, and a single DDL adds
				// at most one function. More than one missing function is therefore an
				// invariant violation — skip the segment rather than silently backfilling
				// only part of the missing state.
				if len(missingFunctions) > 1 {
					log.Ctx(ctx).Error("backfill: segment is missing more than one function output field — invariant violation, skipping",
						zap.Int64("segmentID", segmentID),
						zap.Int64("collectionID", collectionID),
						zap.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
						zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
						zap.Int("missingFunctionCount", len(missingFunctions)))
					continue
				}

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
				backfillFunc := missingFunctions[0]
				log.Ctx(ctx).Info("Found segment missing function output fields, do physical backfill",
					zap.Int64("segmentID", segmentID),
					zap.Int64("collectionID", collectionID),
					zap.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
					zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
					zap.String("backfillFunction", backfillFunc.GetName()))
				views = append(views, &BackfillSegmentsView{
					label:     segmentViews[0].label,
					segments:  segmentViews,
					triggerID: collectionTriggerID,
					funcDiff:  &FuncDiff{Added: []*schemapb.FunctionSchema{backfillFunc}},
					schema:    collection.Schema,
				})
			}
		}
		if len(views) > 0 {
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

	// inlineMetaOnly marks this view as inline-executable: the trigger manager
	// applies targetSchemaVersion to each segment via meta.UpdateSegment without
	// dispatching any compaction task. funcDiff is unused on this path. Set when
	// the segment needs no physical backfill (DoPhysicalBackfill=false on the
	// collection, or all required function output fields are already present in
	// the segment's binlogs).
	inlineMetaOnly      bool
	targetSchemaVersion int32

	// schema is the collection schema captured at policy-scan time. Used as
	// task.Schema in SubmitBackfillViewToScheduler so that completeBackfillCompactionMutation
	// advances the segment's SchemaVersion only to the version that was actually
	// backfilled — not to a newer version if the live collection raced ahead between
	// scan and submission (possible before the cluster-wide gate in PR #48989 lands).
	schema *schemapb.CollectionSchema
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
	if v.inlineMetaOnly {
		return fmt.Sprintf("BackfillSegmentsView(meta-only): label=%s, segments=%d, targetSchemaVersion=%d",
			v.label.Key(), len(v.segments), v.targetSchemaVersion)
	}
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

func (v *BackfillSegmentsView) IsInlineExecutable() bool {
	return v.inlineMetaOnly
}
