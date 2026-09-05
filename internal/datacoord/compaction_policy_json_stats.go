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
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// jsonStatsMigrationPolicy emits one-input rewrites for regular segments with
// JSON stats in an older data format. The rewrite uses the existing single
// compaction path; StatsInspector builds the configured stats format after an
// eligible replacement segment is published without JSON stats.
type jsonStatsMigrationPolicy struct {
	meta                      *meta
	allocator                 allocator.Allocator
	handler                   Handler
	indexEngineVersionManager IndexEngineVersionManager

	// Rate limiting state is private to this policy. Reuse the existing
	// background-rewrite throttle values without coupling migration decisions to
	// storageVersionUpgradePolicy.
	lastPeriod   time.Time
	currentCount int
}

var _ CompactionPolicy = (*jsonStatsMigrationPolicy)(nil)

func newJSONStatsMigrationPolicy(
	meta *meta,
	allocator allocator.Allocator,
	handler Handler,
	versionManager IndexEngineVersionManager,
) *jsonStatsMigrationPolicy {
	return &jsonStatsMigrationPolicy{
		meta:                      meta,
		allocator:                 allocator,
		handler:                   handler,
		indexEngineVersionManager: versionManager,
	}
}

func (policy *jsonStatsMigrationPolicy) Name() string {
	return "jsonStatsMigration"
}

func (policy *jsonStatsMigrationPolicy) Enable() bool {
	_, hasSafeTargets := resolveSafeSegmentRebuildTargets(policy.indexEngineVersionManager)
	return hasSafeTargets &&
		paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetAsBool() &&
		!jsonShreddingDisabledByDeprecatedConfig() &&
		paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.GetAsInt64() == common.JSONStatsDataFormatV4
}

func (policy *jsonStatsMigrationPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	targetFormat := paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.GetAsInt64()
	if targetFormat != common.JSONStatsDataFormatV4 {
		return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: nil}, nil
	}
	targets, ok := resolveSafeSegmentRebuildTargets(policy.indexEngineVersionManager)
	if !ok {
		mlog.Info(ctx, "skip JSON stats migration because a safe segment rebuild target is unavailable")
		return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: nil}, nil
	}

	if time.Since(policy.lastPeriod) > paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.GetAsDuration(time.Second) {
		policy.currentCount = 0
		policy.lastPeriod = time.Now()
	}

	maxCount := paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.GetAsInt()
	views := make([]CompactionView, 0)
	for _, collection := range policy.meta.GetCollections() {
		if policy.currentCount >= maxCount {
			break
		}
		if collection == nil {
			continue
		}
		if policy.meta.isCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip JSON stats migration for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}

		collectionViews, err := policy.triggerOneCollection(ctx, collection.ID, maxCount, targetFormat, targets)
		if err != nil {
			mlog.Warn(ctx, "fail to trigger JSON stats migration",
				mlog.FieldCollectionID(collection.ID), mlog.Err(err))
			continue
		}
		views = append(views, collectionViews...)
	}

	return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: views}, nil
}

func (policy *jsonStatsMigrationPolicy) triggerOneCollection(
	ctx context.Context,
	collectionID int64,
	maxCount int,
	targetFormat int64,
	targets safeSegmentRebuildTargets,
) ([]CompactionView, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply JSON stats migration policy, unable to get collection", mlog.Err(err))
		return nil, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply JSON stats migration policy, collection does not exist")
		return nil, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip JSON stats migration for external collection")
		return nil, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "fail to apply JSON stats migration policy, unable to get collection TTL", mlog.Err(err))
		return nil, err
	}

	fieldIDs := getJSONStatsFieldIDs(collection)
	if len(fieldIDs) == 0 {
		return nil, nil
	}
	vectorIndexIDs := getVectorIndexIDs(policy.meta.indexMeta, collection)

	segments := policy.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		artifacts := classifySegmentRebuildArtifacts(
			policy.meta.indexMeta,
			segment.GetID(),
			vectorIndexIDs,
			segment.GetTextStatsLogs(),
			targets.scalarVersion,
			targets.vectorVersion)
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting &&
			!segment.GetIsImporting() &&
			!segment.GetIsInvisible() &&
			segment.GetLevel() != datapb.SegmentLevel_L0 &&
			!policy.meta.isSegmentCompactionProtected(segment.GetID()) &&
			!hasVersionedStatsTask(policy.meta, segment.GetID()) &&
			artifacts.safeToRebuild() &&
			needsJSONStatsMigration(segment, fieldIDs, targetFormat)
	}))

	remaining := maxCount - policy.currentCount
	if remaining <= 0 || len(segments) == 0 {
		return nil, nil
	}
	if len(segments) > remaining {
		segments = segments[:remaining]
	}

	triggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to apply JSON stats migration policy, unable to allocate trigger ID", mlog.Err(err))
		return nil, err
	}

	views := make([]CompactionView, 0, len(segments))
	for _, segment := range segments {
		segmentViews := GetViewsByInfo(segment)
		views = append(views, &MixSegmentView{
			label:         segmentViews[0].label,
			segments:      segmentViews,
			collectionTTL: collectionTTL,
			triggerID:     triggerID,
		})
		policy.currentCount++
	}
	return views, nil
}

type jsonStatsFormatClassification struct {
	hasMissing bool
	hasOlder   bool
	hasNewer   bool
}

func classifyJSONStatsFormats(segment *SegmentInfo, fieldIDs []int64, targetFormat int64) jsonStatsFormatClassification {
	classification := jsonStatsFormatClassification{}
	for _, fieldID := range fieldIDs {
		stats := segment.GetJsonKeyStats()[fieldID]
		if stats == nil {
			classification.hasMissing = true
			continue
		}

		format := stats.GetJsonKeyStatsDataFormat()
		if format > targetFormat {
			classification.hasNewer = true
		}
		if format < targetFormat {
			classification.hasOlder = true
		}
	}
	return classification
}

func needsJSONStatsMigration(segment *SegmentInfo, fieldIDs []int64, targetFormat int64) bool {
	classification := classifyJSONStatsFormats(segment, fieldIDs, targetFormat)
	// A single compaction rewrites all fields. Never migrate a segment that
	// contains stats produced by a newer binary, even if another field is old.
	return classification.hasOlder && !classification.hasNewer
}
