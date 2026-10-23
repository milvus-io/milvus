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

package policy

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type BumpSchemaVersionPolicy struct {
	meta      *meta.Meta
	handler   meta.Handler
	allocator allocator.Allocator
}

var _ CompactionPolicy = (*BumpSchemaVersionPolicy)(nil)

func NewBumpSchemaVersionPolicy(mt *meta.Meta, allocator allocator.Allocator, handler meta.Handler) *BumpSchemaVersionPolicy {
	return &BumpSchemaVersionPolicy{meta: mt, allocator: allocator, handler: handler}
}

func (policy *BumpSchemaVersionPolicy) Enable() bool {
	return paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.GetAsBool()
}

func (policy *BumpSchemaVersionPolicy) Name() string {
	return "BumpSchemaVersion"
}

func isSchemaBumpDataSegment(segment *meta.SegmentInfo) bool {
	return meta.IsSegmentHealthy(segment) &&
		meta.IsFlushed(segment) &&
		!segment.GetIsImporting() &&
		!segment.GetIsInvisible() &&
		segment.GetLevel() != datapb.SegmentLevel_L0
}

// staleFlushedSegments returns schema-bump-eligible flushed segments for collectionID
// whose SchemaVersion lags behind collectionSchemaVersion.
func (policy *BumpSchemaVersionPolicy) staleFlushedSegments(collectionID int64, collectionSchemaVersion int32) []*meta.ChanPartSegments {
	return meta.GetSegmentsChanPart(policy.meta, collectionID, meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		if !isSchemaBumpDataSegment(segment) ||
			segment.IsCompacting ||
			policy.meta.IsSegmentCompactionProtected(segment.GetID()) ||
			segment.GetSchemaVersion() >= collectionSchemaVersion {
			return false
		}
		if segment.GetStorageVersion() < storage.StorageV3 || segment.GetManifestPath() == "" {
			mlog.RatedWarn(policy.meta.Ctx, rate.Limit(300), "skip schema bump compaction for stale segment without V3 manifest storage",
				mlog.FieldSegmentID(segment.GetID()),
				mlog.FieldCollectionID(collectionID),
				mlog.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
				mlog.Int32("collectionSchemaVersion", collectionSchemaVersion),
				mlog.Int64("storageVersion", segment.GetStorageVersion()),
				mlog.Bool("hasManifest", segment.GetManifestPath() != ""))
			return false
		}
		return true
	}))
}

func (policy *BumpSchemaVersionPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]view.CompactionView, error) {
	collections := policy.meta.GetCollections()
	events := make(map[CompactionTriggerType][]view.CompactionView)

	for _, collection := range collections {
		if collection.Schema == nil {
			continue
		}
		if collection.IsExternal() {
			mlog.Info(ctx, "skip schema bump compaction trigger for external collection", mlog.FieldCollectionID(collection.ID))
			continue
		}
		if policy.meta.IsCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip schema bump compaction for collection due to snapshot compaction block",
				mlog.FieldCollectionID(collection.ID))
			continue
		}
		collectionID := collection.ID
		// The collection cache publishes replacements. Keep this schema reference
		// so candidate selection and the task snapshot use the same version.
		schema := collection.Schema
		collectionSchemaVersion := schema.GetVersion()
		partSegments := policy.staleFlushedSegments(collectionID, collectionSchemaVersion)
		if len(partSegments) == 0 {
			continue
		}
		capturedSchema := proto.Clone(schema).(*schemapb.CollectionSchema)

		var views []view.CompactionView
		var collectionTriggerID int64
	partSegmentsLoop:
		for _, group := range partSegments {
			for _, segment := range group.Segments {
				segmentID := segment.GetID()
				segmentViews := view.GetViewsByInfo(segment)
				if len(segmentViews) == 0 {
					mlog.Warn(ctx, "GetViewsByInfo returned empty views, skip segment",
						mlog.FieldSegmentID(segmentID))
					continue
				}
				if len(segmentViews) != 1 {
					mlog.Warn(ctx, "GetViewsByInfo returned unexpected view count, using first view only",
						mlog.FieldSegmentID(segmentID),
						mlog.Int("viewCount", len(segmentViews)))
				}

				if collectionTriggerID == 0 {
					id, err := policy.allocator.AllocID(ctx)
					if err != nil {
						mlog.Warn(ctx, "Failed to allocate triggerID for schema version bump, skip remaining segments in current collection",
							mlog.FieldCollectionID(collectionID),
							mlog.Err(err))
						break partSegmentsLoop
					}
					collectionTriggerID = id
				}

				mlog.Info(ctx, "Found segment needing schema version bump",
					mlog.FieldSegmentID(segmentID),
					mlog.FieldCollectionID(collectionID),
					mlog.Int32("segmentSchemaVersion", segment.GetSchemaVersion()),
					mlog.Int32("collectionSchemaVersion", collectionSchemaVersion))
				views = append(views, &BumpSchemaVersionView{
					Label:     segmentViews[0].Label,
					Segments:  segmentViews,
					TriggerID: collectionTriggerID,
					Schema:    capturedSchema,
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
	Label     *view.CompactionGroupLabel
	Segments  []*view.SegmentView
	TriggerID int64

	// schema is captured at policy-scan time so completion only advances the segment
	// to the schema version that this task reconciled.
	Schema *schemapb.CollectionSchema
}

var _ view.CompactionView = (*BumpSchemaVersionView)(nil)

func (v *BumpSchemaVersionView) GetGroupLabel() *view.CompactionGroupLabel {
	return v.Label
}

func (v *BumpSchemaVersionView) GetSegmentsView() []*view.SegmentView {
	return v.Segments
}

func (v *BumpSchemaVersionView) GetTotalSize() float64 {
	if v == nil {
		return 0
	}
	return view.SumSegmentSize(v.Segments)
}

func (v *BumpSchemaVersionView) GetCollectionTTL() time.Duration {
	return 0
}

func (v *BumpSchemaVersionView) Append(segments ...*view.SegmentView) {
	v.Segments = append(v.Segments, segments...)
}

func (v *BumpSchemaVersionView) String() string {
	label := "<nil>"
	if v.Label != nil {
		label = v.Label.Key()
	}
	schemaVersion := int32(0)
	if v.Schema != nil {
		schemaVersion = v.Schema.GetVersion()
	}
	return fmt.Sprintf("BumpSchemaVersionView: label=%s, segments=%d, triggerID=%d, schemaVersion=%d",
		label, len(v.Segments), v.TriggerID, schemaVersion)
}

func (v *BumpSchemaVersionView) Trigger() (view.CompactionView, string) {
	return v, "segment schema version behind collection schema"
}

func (v *BumpSchemaVersionView) ForceTrigger() (view.CompactionView, string) {
	return v.Trigger()
}

func (v *BumpSchemaVersionView) ForceTriggerAll() ([]view.CompactionView, string) {
	cv, reason := v.Trigger()
	return []view.CompactionView{cv}, reason
}

func (v *BumpSchemaVersionView) GetTriggerID() int64 {
	return v.TriggerID
}
