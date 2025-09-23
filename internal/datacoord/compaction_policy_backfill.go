package datacoord

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type backfillCompactionPolicy struct {
	meta      *meta
	handler   Handler
	allocator allocator.Allocator
	broker    broker.Broker
}

// Ensure backfillCompactionPolicy implements CompactionPolicy interface
var _ CompactionPolicy = (*backfillCompactionPolicy)(nil)

func newBackfillCompactionPolicy(meta *meta, allocator allocator.Allocator, handler Handler, broker broker.Broker) *backfillCompactionPolicy {
	return &backfillCompactionPolicy{meta: meta, allocator: allocator, handler: handler, broker: broker}
}

func (policy *backfillCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *backfillCompactionPolicy) Name() string {
	return "BackfillCompaction"
}

func (policy *backfillCompactionPolicy) getCollectionSchemaByVersion(ctx context.Context, collectionID int64, startPositionTimestamp uint64) (*schemapb.CollectionSchema, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Uint64("startPositionTimestamp", startPositionTimestamp))

	log.Debug("Getting collection schema by version")

	descResp, err := policy.broker.DescribeCollectionInternal(ctx, collectionID, startPositionTimestamp)
	if err != nil {
		log.Warn("Failed to describe collection", zap.Error(err))
		return nil, fmt.Errorf("failed to describe collection %d with startPositionTimestamp %d: %w", collectionID, startPositionTimestamp, err)
	}

	if descResp == nil {
		log.Warn("Describe collection response is nil")
		return nil, fmt.Errorf("describe collection response is nil for collection %d with startPositionTimestamp %d", collectionID, startPositionTimestamp)
	}

	schema := descResp.GetSchema()
	if schema == nil {
		log.Warn("Schema is nil in describe collection response")
		return nil, fmt.Errorf("schema is nil in describe collection response for collection %d with startPositionTimestamp %d", collectionID, startPositionTimestamp)
	}

	log.Debug("Successfully retrieved collection schema",
		zap.Int32("retrievedSchemaVersion", schema.GetVersion()),
		zap.String("collectionName", schema.GetName()),
		zap.Any("schema", schema))

	return schema, nil
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
		for _, group := range partSegments {
			for _, segment := range group.segments {
				segmentID := segment.GetID()
				segmentSchemaVersion := segment.GetSchemaVersion()
				// Segment with nil start position is a serious error state
				if segment.GetStartPosition() == nil {
					return nil, fmt.Errorf("segment %d in collection %d has nil start position, which is a serious error state", segmentID, collectionID)
				}
				segmentSchema, err := policy.getCollectionSchemaByVersion(ctx, collectionID, segment.GetStartPosition().GetTimestamp())
				if err != nil {
					log.Ctx(ctx).Error("Failed to get segment schema", zap.Error(err))
					continue
				}
				// If segment's schema version is smaller than collection's schema version
				if segmentSchemaVersion < collectionSchemaVersion {
					doPhysicalBackfill := collection.Schema.GetDoPhysicalBackfill()
					if doPhysicalBackfill {
						_, funcDiff, err := util.SchemaDiff(segmentSchema, collection.Schema)
						if err != nil {
							log.Ctx(ctx).Error("Failed to compare schemas", zap.Error(err))
							continue
						}
						log.Ctx(ctx).Info("Found segment with outdated schema version do physical backfill",
							zap.Int64("segmentID", segment.GetID()),
							zap.Int64("collectionID", collectionID),
							zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
							zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
							zap.Any("funcDiff", funcDiff))

						// Create BackfillSegmentsView for this segment
						segmentViews := GetViewsByInfo(segment)
						view := &BackfillSegmentsView{
							label:     segmentViews[0].label,
							segments:  segmentViews,
							triggerID: newTriggerID,
							funcDiff:  funcDiff,
						}
						views = append(views, view)
					} else {
						// Use UpdateSegment to atomically update schema version with proper locking and error handling
						err := policy.meta.UpdateSegment(segmentID, SetSchemaVersion(collectionSchemaVersion))
						if err != nil {
							log.Ctx(ctx).Error("Failed to update segment schema version",
								zap.Int64("segmentID", segmentID),
								zap.Int64("collectionID", collectionID),
								zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
								zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
								zap.Error(err))
							// Continue processing other segments instead of failing the entire operation
							continue
						}
						log.Ctx(ctx).Info("Updated segment schema version",
							zap.Int64("segmentID", segmentID),
							zap.Int64("collectionID", collectionID),
							zap.Int32("segmentSchemaVersion", segmentSchemaVersion),
							zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
						)
					}
				}
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
