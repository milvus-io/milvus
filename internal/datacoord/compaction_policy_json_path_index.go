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
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// jsonPathIndexMigrationPolicy gradually rewrites regular sealed segments that
// still contain typed JSON path indexes produced with legacy EXISTS semantics.
// It deliberately uses the ordinary one-input single-compaction path: index
// rebuilding on the replacement segment produces the current scalar index
// engine version without adding a second index migration execution path.
type jsonPathIndexMigrationPolicy struct {
	meta                      *meta
	allocator                 allocator.Allocator
	handler                   Handler
	indexEngineVersionManager IndexEngineVersionManager

	// Reuse the background rewrite throttle configuration, but keep accounting
	// private so this policy does not change storage-version policy state.
	lastPeriod   time.Time
	currentCount int
}

var _ CompactionPolicy = (*jsonPathIndexMigrationPolicy)(nil)

func newJSONPathIndexMigrationPolicy(
	meta *meta,
	allocator allocator.Allocator,
	handler Handler,
	versionManager IndexEngineVersionManager,
) *jsonPathIndexMigrationPolicy {
	policy := &jsonPathIndexMigrationPolicy{
		meta:                      meta,
		allocator:                 allocator,
		handler:                   handler,
		indexEngineVersionManager: versionManager,
	}
	return policy
}

// safeSegmentRebuildTargets are the artifact versions that a one-input
// compaction can safely reproduce with every online DataNode writer and every
// online QueryNode reader. Both dimensions are required because compaction
// replaces the whole segment and IndexInspector rebuilds every active index on
// the replacement segment.
type safeSegmentRebuildTargets struct {
	scalarVersion int32
	vectorVersion int32
}

func resolveSafeSegmentRebuildTargets(versionManager IndexEngineVersionManager) (safeSegmentRebuildTargets, bool) {
	if versionManager == nil {
		return safeSegmentRebuildTargets{}, false
	}
	gate, ok := versionManager.(ScalarIndexMigrationVersionManager)
	if !ok {
		return safeSegmentRebuildTargets{}, false
	}

	scalarVersion := min(
		versionManager.ResolveScalarIndexVersion(),
		common.MaximumScalarIndexEngineVersion)
	if !gate.SupportsScalarIndexVersion(scalarVersion) {
		return safeSegmentRebuildTargets{}, false
	}

	writerMinimum, writerMaximum, ok := gate.GetDataNodeVectorIndexWriterVersionRange()
	if !ok {
		return safeSegmentRebuildTargets{}, false
	}
	vectorVersion := min(versionManager.ResolveVecIndexVersion(), writerMaximum)
	queryMinimum := versionManager.GetMinimalIndexEngineVersion()
	queryMaximum := versionManager.GetMaximumIndexEngineVersion()
	if vectorVersion < writerMinimum || vectorVersion < queryMinimum || vectorVersion > queryMaximum {
		return safeSegmentRebuildTargets{}, false
	}

	return safeSegmentRebuildTargets{
		scalarVersion: scalarVersion,
		vectorVersion: vectorVersion,
	}, true
}

func (policy *jsonPathIndexMigrationPolicy) Name() string {
	return "jsonPathIndexMigration"
}

func (policy *jsonPathIndexMigrationPolicy) Enable() bool {
	targets, ok := resolveSafeSegmentRebuildTargets(policy.indexEngineVersionManager)
	return ok && targets.scalarVersion >= common.MinScalarIndexVersionForJsonPathPresence
}

func (policy *jsonPathIndexMigrationPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	// Session capabilities may change between ticker registration and execution.
	// Revalidate immediately before selecting any segment; the migration follows
	// the cluster's existing monotonic rolling-upgrade assumption.
	if !policy.Enable() {
		return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: nil}, nil
	}

	if time.Since(policy.lastPeriod) > paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.GetAsDuration(time.Second) {
		policy.currentCount = 0
		policy.lastPeriod = time.Now()
	}

	maxCount := paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.GetAsInt()
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	targets, ok := resolveSafeSegmentRebuildTargets(policy.indexEngineVersionManager)
	if !ok || targets.scalarVersion < presenceVersion {
		mlog.Info(ctx, "skip JSON path index migration because a safe segment rebuild target is unavailable")
		return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: nil}, nil
	}
	views := make([]CompactionView, 0)
	for _, collection := range policy.meta.GetCollections() {
		if policy.currentCount >= maxCount {
			break
		}
		if collection == nil {
			continue
		}
		if policy.meta.isCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip JSON path index migration for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}

		collectionViews, err := policy.triggerOneCollection(
			ctx, collection.ID, maxCount, presenceVersion, targets.scalarVersion, targets.vectorVersion)
		if err != nil {
			mlog.Warn(ctx, "fail to trigger JSON path index migration",
				mlog.FieldCollectionID(collection.ID), mlog.Err(err))
			continue
		}
		views = append(views, collectionViews...)
	}

	return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: views}, nil
}

func (policy *jsonPathIndexMigrationPolicy) triggerOneCollection(
	ctx context.Context,
	collectionID int64,
	maxCount int,
	presenceVersion int32,
	rebuildScalarVersion int32,
	rebuildVectorVersion int32,
) ([]CompactionView, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply JSON path index migration policy, unable to get collection", mlog.Err(err))
		return nil, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply JSON path index migration policy, collection does not exist")
		return nil, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip JSON path index migration for external collection")
		return nil, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "fail to apply JSON path index migration policy, unable to get collection TTL", mlog.Err(err))
		return nil, err
	}

	typedPathIndexIDs := getTypedJSONPathIndexIDs(policy.meta.indexMeta, collection)
	if len(typedPathIndexIDs) == 0 {
		return nil, nil
	}
	vectorIndexIDs := getVectorIndexIDs(policy.meta.indexMeta, collection)
	rebuildJSONStatsFormat := paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.GetAsInt64()
	jsonStatsFieldIDs := getJSONStatsFieldIDs(collection)

	segments := policy.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting &&
			!segment.GetIsImporting() &&
			!segment.GetIsInvisible() &&
			segment.GetLevel() != datapb.SegmentLevel_L0 &&
			!policy.meta.isSegmentCompactionProtected(segment.GetID()) &&
			!hasVersionedStatsTask(policy.meta, segment.GetID()) &&
			!classifyJSONStatsFormats(segment, jsonStatsFieldIDs, rebuildJSONStatsFormat).hasNewer &&
			needsJSONPathIndexMigration(
				policy.meta.indexMeta,
				segment.GetID(),
				typedPathIndexIDs,
				vectorIndexIDs,
				segment.GetTextStatsLogs(),
				presenceVersion,
				rebuildScalarVersion,
				rebuildVectorVersion)
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
		log.Warn(ctx, "fail to apply JSON path index migration policy, unable to allocate trigger ID", mlog.Err(err))
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

func hasVersionedStatsTask(meta *meta, segmentID int64) bool {
	return meta != nil && meta.statsTaskMeta != nil &&
		(meta.statsTaskMeta.HasStatsTask(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob) ||
			meta.statsTaskMeta.HasStatsTask(segmentID, indexpb.StatsSubJob_TextIndexJob))
}

func getTypedJSONPathIndexIDs(indexMeta *indexMeta, collection *collectionInfo) map[int64]struct{} {
	if indexMeta == nil || collection == nil || collection.Schema == nil {
		return nil
	}

	jsonFieldIDs := make(map[int64]struct{})
	for _, field := range collection.Schema.GetFields() {
		if typeutil.IsJSONType(field.GetDataType()) {
			jsonFieldIDs[field.GetFieldID()] = struct{}{}
		}
	}

	indexIDs := make(map[int64]struct{})
	for _, index := range indexMeta.GetIndexesForCollection(collection.ID, "") {
		if isTypedJSONPathIndex(index, jsonFieldIDs) {
			indexIDs[index.IndexID] = struct{}{}
		}
	}
	return indexIDs
}

func getVectorIndexIDs(indexMeta *indexMeta, collection *collectionInfo) map[int64]struct{} {
	if indexMeta == nil || collection == nil || collection.Schema == nil {
		return nil
	}

	vectorFieldIDs := make(map[int64]struct{})
	for _, field := range typeutil.GetAllFieldSchemas(collection.Schema) {
		if typeutil.IsVectorType(field.GetDataType()) {
			vectorFieldIDs[field.GetFieldID()] = struct{}{}
		}
	}

	indexIDs := make(map[int64]struct{})
	for _, index := range indexMeta.GetIndexesForCollection(collection.ID, "") {
		if index != nil {
			if _, ok := vectorFieldIDs[index.FieldID]; ok {
				indexIDs[index.IndexID] = struct{}{}
			}
		}
	}
	return indexIDs
}

func isTypedJSONPathIndex(index *model.Index, jsonFieldIDs map[int64]struct{}) bool {
	if index == nil {
		return false
	}
	if _, ok := jsonFieldIDs[index.FieldID]; !ok {
		return false
	}
	return isTypedJSONPathIndexParams(index.IndexParams)
}

func isTypedJSONPathIndexParams(indexParams []*commonpb.KeyValuePair) bool {
	// JSON NGRAM paths accelerate LIKE only. NgramInvertedIndex does not
	// persist a presence bitmap or implement EXISTS, so the V6 presence
	// migration has nothing to rewrite. Index type values are user metadata;
	// accept their case-insensitive canonical spelling.
	if strings.EqualFold(GetIndexType(indexParams), "NGRAM") {
		return false
	}
	hasPath := false
	castType := ""
	for _, param := range indexParams {
		switch param.GetKey() {
		case common.JSONPathKey:
			hasPath = true
		case common.JSONCastTypeKey:
			castType = param.GetValue()
		}
	}
	return hasPath && castType != "" && !common.IsFullJSONCastType(castType)
}

type jsonPathIndexVersionClassification struct {
	hasOlder   bool
	hasNewer   bool
	hasPending bool
}

type segmentRebuildArtifactClassification struct {
	hasNewer   bool
	hasPending bool
}

func (classification segmentRebuildArtifactClassification) safeToRebuild() bool {
	return !classification.hasNewer && !classification.hasPending
}

// classifySegmentRebuildArtifacts checks every versioned artifact that a
// one-input compaction either rebuilds inline or causes IndexInspector to build
// again on the replacement segment. Only active indexes matter: files for a
// deleted index are discarded with the source segment and are not reproduced.
func classifySegmentRebuildArtifacts(
	indexMeta *indexMeta,
	segmentID int64,
	vectorIndexIDs map[int64]struct{},
	textStatsLogs map[int64]*datapb.TextIndexStats,
	rebuildScalarVersion int32,
	rebuildVectorVersion int32,
) segmentRebuildArtifactClassification {
	classification := segmentRebuildArtifactClassification{}
	for _, textStats := range textStatsLogs {
		if textStats != nil && textStats.GetCurrentScalarIndexVersion() > rebuildScalarVersion {
			classification.hasNewer = true
		}
	}
	if indexMeta == nil {
		return classification
	}

	for _, segmentIndex := range indexMeta.GetAllSegmentIndexes(segmentID) {
		if segmentIndex == nil || segmentIndex.IndexState == commonpb.IndexState_Failed {
			continue
		}
		isActiveIndex := indexMeta.IsIndexExist(segmentIndex.CollectionID, segmentIndex.IndexID)
		if segmentIndex.IndexState != commonpb.IndexState_Finished {
			// Unissued, InProgress, Retry, and unknown states do not describe a
			// durable artifact version. An active build may finish after selection,
			// so wait for a terminal state before replacing the source segment.
			if isActiveIndex {
				classification.hasPending = true
			}
			continue
		}
		if len(segmentIndex.IndexFileKeys) == 0 {
			// Finished with no files is the intentional fake-finished state for
			// small/no-train indexes and has no durable artifact to protect.
			continue
		}
		if !isActiveIndex {
			continue
		}

		if _, isVectorIndex := vectorIndexIDs[segmentIndex.IndexID]; isVectorIndex {
			if segmentIndex.CurrentIndexVersion > rebuildVectorVersion {
				classification.hasNewer = true
			}
		} else if segmentIndex.CurrentScalarIndexVersion > rebuildScalarVersion {
			classification.hasNewer = true
		}
	}
	return classification
}

func classifyJSONPathIndexVersions(
	indexMeta *indexMeta,
	segmentID int64,
	typedPathIndexIDs map[int64]struct{},
	vectorIndexIDs map[int64]struct{},
	textStatsLogs map[int64]*datapb.TextIndexStats,
	presenceVersion int32,
	rebuildScalarVersion int32,
	rebuildVectorVersion int32,
) jsonPathIndexVersionClassification {
	artifacts := classifySegmentRebuildArtifacts(
		indexMeta, segmentID, vectorIndexIDs, textStatsLogs,
		rebuildScalarVersion, rebuildVectorVersion)
	classification := jsonPathIndexVersionClassification{
		hasNewer:   artifacts.hasNewer,
		hasPending: artifacts.hasPending,
	}
	if indexMeta == nil {
		return classification
	}

	for _, segmentIndex := range indexMeta.GetAllSegmentIndexes(segmentID) {
		if segmentIndex == nil || segmentIndex.IndexState == commonpb.IndexState_Failed {
			continue
		}
		_, isTypedPathIndex := typedPathIndexIDs[segmentIndex.IndexID]
		if !isTypedPathIndex ||
			!indexMeta.IsIndexExist(segmentIndex.CollectionID, segmentIndex.IndexID) ||
			segmentIndex.IndexState != commonpb.IndexState_Finished ||
			len(segmentIndex.IndexFileKeys) == 0 {
			continue
		}
		if segmentIndex.CurrentScalarIndexVersion < presenceVersion {
			classification.hasOlder = true
		}
	}
	return classification
}

func needsJSONPathIndexMigration(
	indexMeta *indexMeta,
	segmentID int64,
	typedPathIndexIDs map[int64]struct{},
	vectorIndexIDs map[int64]struct{},
	textStatsLogs map[int64]*datapb.TextIndexStats,
	presenceVersion int32,
	rebuildScalarVersion int32,
	rebuildVectorVersion int32,
) bool {
	classification := classifyJSONPathIndexVersions(
		indexMeta, segmentID, typedPathIndexIDs, vectorIndexIDs, textStatsLogs,
		presenceVersion, rebuildScalarVersion, rebuildVectorVersion)
	// One compaction rebuilds every index on the segment. Never rewrite a
	// future-version artifact merely because another JSON path index is old.
	artifacts := segmentRebuildArtifactClassification{
		hasNewer:   classification.hasNewer,
		hasPending: classification.hasPending,
	}
	return classification.hasOlder && artifacts.safeToRebuild()
}
