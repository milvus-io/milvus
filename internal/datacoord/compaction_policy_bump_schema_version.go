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
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type bumpSchemaVersionPolicy struct {
	meta      *meta
	handler   Handler
	allocator allocator.Allocator
}

var _ CompactionPolicy = (*bumpSchemaVersionPolicy)(nil)

func newBumpSchemaVersionPolicy(meta *meta, allocator allocator.Allocator, handler Handler) *bumpSchemaVersionPolicy {
	return &bumpSchemaVersionPolicy{meta: meta, allocator: allocator, handler: handler}
}

func (policy *bumpSchemaVersionPolicy) Enable() bool {
	return paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.GetAsBool()
}

func (policy *bumpSchemaVersionPolicy) Name() string {
	return "BumpSchemaVersion"
}

func isSchemaBumpDataSegment(segment *SegmentInfo) bool {
	return isSegmentHealthy(segment) &&
		isFlushed(segment) &&
		!segment.GetIsImporting() &&
		!segment.GetIsInvisible() &&
		segment.GetLevel() != datapb.SegmentLevel_L0
}

// staleFlushedSegments returns schema-bump-eligible flushed segments for collectionID
// whose SchemaVersion lags behind collectionSchemaVersion.
func (policy *bumpSchemaVersionPolicy) staleFlushedSegments(collectionID int64, collectionSchemaVersion int32) []*chanPartSegments {
	return GetSegmentsChanPart(policy.meta, collectionID, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		if !isSchemaBumpDataSegment(segment) ||
			segment.isCompacting ||
			policy.meta.isSegmentCompactionProtected(segment.GetID()) ||
			segment.GetSchemaVersion() >= collectionSchemaVersion {
			return false
		}
		if segment.GetStorageVersion() < storage.StorageV3 || segment.GetManifestPath() == "" {
			mlog.RatedWarn(policy.meta.ctx, rate.Limit(300), "skip schema bump compaction for stale segment without V3 manifest storage",
				zap.Int64("segmentID", segment.GetID()),
				zap.Int64("collectionID", collectionID),
				zap.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
				zap.Int32("collectionSchemaVersion", collectionSchemaVersion),
				zap.Int64("storageVersion", segment.GetStorageVersion()),
				zap.Bool("hasManifest", segment.GetManifestPath() != ""))
			return false
		}
		return true
	}))
}

func (policy *bumpSchemaVersionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	collections := policy.meta.GetCollections()
	events := make(map[CompactionTriggerType][]CompactionView)

	for _, collection := range collections {
		if collection.Schema == nil {
			continue
		}
		if policy.meta.isCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip schema bump compaction for collection due to snapshot compaction block",
				zap.Int64("collectionID", collection.ID))
			continue
		}
		collectionID := collection.ID
		capturedSchema := proto.Clone(collection.Schema).(*schemapb.CollectionSchema)
		collectionSchemaVersion := capturedSchema.GetVersion()
		partSegments := policy.staleFlushedSegments(collectionID, collectionSchemaVersion)

		var views []CompactionView
		var collectionTriggerID int64
	partSegmentsLoop:
		for _, group := range partSegments {
			for _, segment := range group.segments {
				segmentID := segment.GetID()
				segmentViews := GetViewsByInfo(segment)
				if len(segmentViews) == 0 {
					mlog.Warn(ctx, "GetViewsByInfo returned empty views, skip segment",
						zap.Int64("segmentID", segmentID))
					continue
				}
				if len(segmentViews) != 1 {
					mlog.Warn(ctx, "GetViewsByInfo returned unexpected view count, using first view only",
						zap.Int64("segmentID", segmentID),
						zap.Int("viewCount", len(segmentViews)))
				}

				if collectionTriggerID == 0 {
					id, err := policy.allocator.AllocID(ctx)
					if err != nil {
						mlog.Warn(ctx, "Failed to allocate triggerID for schema version bump, skip remaining segments in current collection",
							zap.Int64("collectionID", collectionID),
							zap.Error(err))
						break partSegmentsLoop
					}
					collectionTriggerID = id
				}

				mlog.Info(ctx, "Found segment needing schema version bump",
					zap.Int64("segmentID", segmentID),
					zap.Int64("collectionID", collectionID),
					zap.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
					zap.Int32("collectionSchemaVersion", collectionSchemaVersion))
				views = append(views, &BumpSchemaVersionView{
					label:     segmentViews[0].label,
					segments:  segmentViews,
					triggerID: collectionTriggerID,
					schema:    capturedSchema,
				})
			}
		}
		if len(views) > 0 {
			events[TriggerTypeBumpSchemaVersion] = append(events[TriggerTypeBumpSchemaVersion], views...)
		}
	}
	return events, nil
}

type BumpSchemaVersionView struct {
	label     *CompactionGroupLabel
	segments  []*SegmentView
	triggerID int64

	// schema is captured at policy-scan time so completion only advances the segment
	// to the schema version that this task reconciled.
	schema *schemapb.CollectionSchema
}

var _ CompactionView = (*BumpSchemaVersionView)(nil)

func (v *BumpSchemaVersionView) GetGroupLabel() *CompactionGroupLabel {
	return v.label
}

func (v *BumpSchemaVersionView) GetSegmentsView() []*SegmentView {
	return v.segments
}

func (v *BumpSchemaVersionView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}
	return sumSegmentSize(v.segments)
}

func (v *BumpSchemaVersionView) GetCollectionTTL() time.Duration {
	return 0
}

func (v *BumpSchemaVersionView) Append(segments ...*SegmentView) {
	v.segments = append(v.segments, segments...)
}

func (v *BumpSchemaVersionView) String() string {
	label := "<nil>"
	if v.label != nil {
		label = v.label.Key()
	}
	schemaVersion := int32(0)
	if v.schema != nil {
		schemaVersion = v.schema.GetVersion()
	}
	return fmt.Sprintf("BumpSchemaVersionView: label=%s, segments=%d, triggerID=%d, schemaVersion=%d",
		label, len(v.segments), v.triggerID, schemaVersion)
}

func (v *BumpSchemaVersionView) Trigger() (CompactionView, string) {
	return v, "segment schema version behind collection schema"
}

func (v *BumpSchemaVersionView) ForceTrigger() (CompactionView, string) {
	return v.Trigger()
}

func (v *BumpSchemaVersionView) ForceTriggerAll() ([]CompactionView, string) {
	view, reason := v.Trigger()
	return []CompactionView{view}, reason
}

func (v *BumpSchemaVersionView) GetTriggerID() int64 {
	return v.triggerID
}
