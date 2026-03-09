package datacoord

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

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
	// Check all collections
	collections := policy.meta.GetCollections()
	events := make(map[CompactionTriggerType][]CompactionView, 0)
	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		return nil, err
	}

	for _, collection := range collections {
		collectionID := collection.ID
		collectionSchemaVersion := collection.Schema.GetVersion()
		// Get all segments for this collection
		partSegments := GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return isSegmentHealthy(segment) &&
				isFlushed(segment) &&
				!segment.isCompacting && // not compacting now
				!segment.GetIsImporting() && // not importing now
				!segment.GetIsInvisible()
		}))

		// Check each segment's schema version
		views := make([]CompactionView, 0)
		doPhysicalBackfill := collection.Schema.GetDoPhysicalBackfill()
		for _, group := range partSegments {
			for _, segment := range group.segments {
				segmentID := segment.GetID()
				segmentSchemaVersion := segment.GetSchemaVersion()
				// Skip segments with up-to-date schema version
				if segmentSchemaVersion >= collectionSchemaVersion {
					continue
				}
				if !doPhysicalBackfill {
					// No physical backfill needed, just update schema version directly
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
				// Current compactor only supports one function per backfill task,
				// so pick the first missing function. Remaining functions will be
				// picked up in subsequent trigger rounds since the schema version
				// won't be updated until all functions are backfilled.
				backfillFunc := missingFunctions[0]
				log.Ctx(ctx).Info("Found segment missing function output fields, do physical backfill",
					zap.Int64("segmentID", segmentID),
					zap.Int64("collectionID", collectionID),
					zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
					zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
					zap.String("backfillFunction", backfillFunc.GetName()),
					zap.Int("totalMissingFunctions", len(missingFunctions)))

				// Create BackfillSegmentsView for this segment with single function
				segmentViews := GetViewsByInfo(segment)
				view := &BackfillSegmentsView{
					label:     segmentViews[0].label,
					segments:  segmentViews,
					triggerID: newTriggerID,
					funcDiff:  &util.FuncDiff{Added: []*schemapb.FunctionSchema{backfillFunc}},
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
	label         *CompactionGroupLabel
	segments      []*SegmentView
	triggerID     int64
	collectionTTL time.Duration
	funcDiff      *util.FuncDiff
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

func (v *BackfillSegmentsView) ForceTriggerAll() ([]CompactionView, string) {
	view, reason := v.Trigger()
	return []CompactionView{view}, reason
}

func (v *BackfillSegmentsView) GetTriggerID() int64 {
	return v.triggerID
}
