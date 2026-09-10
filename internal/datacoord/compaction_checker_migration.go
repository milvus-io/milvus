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

// MigrationCompactionChecker gradually rewrites regular sealed segments whose
// persisted artifacts need to be upgraded. It uses the ordinary one-input
// single-compaction path so the replacement segment and all of its indexes are
// rebuilt by the current writers.
//
// Keep migration-specific predicates inside this checker. New data, stats, or
// index migrations can then share capability gating, throttling, segment
// ownership checks, and whole-segment artifact safety.
type MigrationCompactionChecker struct {
	meta                      *meta
	allocator                 allocator.Allocator
	handler                   Handler
	indexEngineVersionManager IndexEngineVersionManager

	// Reuse the background rewrite throttle configuration, but keep accounting
	// private so this checker does not change storage-version policy state.
	lastPeriod   time.Time
	currentCount int
}

var _ CompactionPolicy = (*MigrationCompactionChecker)(nil)

func newMigrationCompactionChecker(
	meta *meta,
	allocator allocator.Allocator,
	handler Handler,
	versionManager IndexEngineVersionManager,
) *MigrationCompactionChecker {
	return &MigrationCompactionChecker{
		meta:                      meta,
		allocator:                 allocator,
		handler:                   handler,
		indexEngineVersionManager: versionManager,
	}
}

// safeSegmentRebuildTargets are the artifact versions that a one-input
// compaction can safely reproduce with every online DataNode writer and every
// online QueryNode reader. Both dimensions are required because compaction
// replaces the whole segment and IndexInspector rebuilds every active index on
// the replacement segment. Bound workers are trusted to reproduce the resolved
// targets without session capabilities; QueryNode reader checks still apply.
type safeSegmentRebuildTargets struct {
	scalarVersion int32
	vectorVersion int32
}

// Index rebuilds remain opt-in. JSON format migrations have their own gates
// and do not enable general index upgrades or forced downgrades.
type indexRebuildOptions struct {
	autoUpgrade bool
	forceScalar bool
	forceVector bool
}

func getIndexRebuildOptions() indexRebuildOptions {
	p := &paramtable.Get().DataCoordCfg
	return indexRebuildOptions{
		autoUpgrade: p.AutoUpgradeSegmentIndex.GetAsBool(),
		forceScalar: p.ForceRebuildScalarSegmentIndex.GetAsBool() && p.TargetScalarIndexVersion.GetAsInt64() != -1,
		forceVector: p.ForceRebuildSegmentIndex.GetAsBool() && p.TargetVecIndexVersion.GetAsInt64() != -1,
	}
}

func (options indexRebuildOptions) enabled() bool {
	return options.autoUpgrade || options.forceScalar || options.forceVector
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

	vectorVersion := versionManager.ResolveVecIndexVersion()
	if !paramtable.Get().DataCoordCfg.BindIndexNodeMode.GetAsBool() {
		writerMinimum, writerMaximum, ok := gate.GetDataNodeVectorIndexWriterVersionRange()
		if !ok {
			return safeSegmentRebuildTargets{}, false
		}
		vectorVersion = min(vectorVersion, writerMaximum)
		if vectorVersion < writerMinimum {
			return safeSegmentRebuildTargets{}, false
		}
	}
	queryMinimum := versionManager.GetMinimalIndexEngineVersion()
	queryMaximum := versionManager.GetMaximumIndexEngineVersion()
	if vectorVersion < queryMinimum || vectorVersion > queryMaximum {
		return safeSegmentRebuildTargets{}, false
	}

	return safeSegmentRebuildTargets{
		scalarVersion: scalarVersion,
		vectorVersion: vectorVersion,
	}, true
}

func (checker *MigrationCompactionChecker) Name() string {
	return "MigrationCompactionChecker"
}

func (checker *MigrationCompactionChecker) jsonPathIndexMigrationEnabled(targets safeSegmentRebuildTargets) bool {
	return targets.scalarVersion >= common.MinScalarIndexVersionForJsonPathPresence
}

func (checker *MigrationCompactionChecker) jsonStatsMigrationEnabled() bool {
	return paramtable.Get().CommonCfg.EnabledJSONKeyStats.GetAsBool() &&
		!jsonShreddingDisabledByDeprecatedConfig() &&
		paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.GetAsInt64() == common.JSONStatsDataFormatV4
}

func (checker *MigrationCompactionChecker) Enable() bool {
	targets, ok := resolveSafeSegmentRebuildTargets(checker.indexEngineVersionManager)
	return ok && (getIndexRebuildOptions().enabled() || checker.jsonPathIndexMigrationEnabled(targets) || checker.jsonStatsMigrationEnabled())
}

func (checker *MigrationCompactionChecker) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	// Session capabilities may change between ticker registration and execution.
	// Resolve them immediately before selecting any segment; migration follows
	// the cluster's existing monotonic rolling-upgrade assumption.
	targets, ok := resolveSafeSegmentRebuildTargets(checker.indexEngineVersionManager)
	if !ok {
		mlog.Info(ctx, "skip migration compaction because a safe segment rebuild target is unavailable")
		return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: nil}, nil
	}
	pathIndexEnabled := checker.jsonPathIndexMigrationEnabled(targets)
	jsonStatsEnabled := checker.jsonStatsMigrationEnabled()
	if !getIndexRebuildOptions().enabled() && !pathIndexEnabled && !jsonStatsEnabled {
		return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: nil}, nil
	}

	if time.Since(checker.lastPeriod) > paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.GetAsDuration(time.Second) {
		checker.currentCount = 0
		checker.lastPeriod = time.Now()
	}

	maxCount := paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.GetAsInt()
	targetStatsFormat := paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.GetAsInt64()
	views := make([]CompactionView, 0)
	for _, collection := range checker.meta.GetCollections() {
		if checker.currentCount >= maxCount {
			break
		}
		if collection == nil {
			continue
		}
		if checker.meta.isCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip migration compaction for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}

		collectionViews, err := checker.triggerOneCollection(
			ctx, collection.ID, maxCount, targetStatsFormat, targets)
		if err != nil {
			mlog.Warn(ctx, "fail to trigger migration compaction",
				mlog.FieldCollectionID(collection.ID), mlog.Err(err))
			continue
		}
		views = append(views, collectionViews...)
	}

	return map[CompactionTriggerType][]CompactionView{TriggerTypeSingle: views}, nil
}

func (checker *MigrationCompactionChecker) triggerOneCollection(
	ctx context.Context,
	collectionID int64,
	maxCount int,
	targetStatsFormat int64,
	targets safeSegmentRebuildTargets,
) ([]CompactionView, error) {
	log := mlog.With(mlog.FieldCollectionID(collectionID))
	collection, err := checker.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn(ctx, "fail to apply migration compaction checker, unable to get collection", mlog.Err(err))
		return nil, err
	}
	if collection == nil {
		log.Warn(ctx, "fail to apply migration compaction checker, collection does not exist")
		return nil, nil
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip migration compaction for external collection")
		return nil, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "fail to apply migration compaction checker, unable to get collection TTL", mlog.Err(err))
		return nil, err
	}

	pathIndexEnabled := checker.jsonPathIndexMigrationEnabled(targets)
	jsonStatsEnabled := checker.jsonStatsMigrationEnabled()
	indexRebuild := getIndexRebuildOptions()
	if !isCollectionAutoCompactionEnabled(collection) {
		indexRebuild = indexRebuildOptions{}
	}
	// The artifact-protection ceiling can be lower than the build request's
	// resolved target during rolling upgrades. Do not use that ceiling as a
	// force-rebuild target: the writer may keep producing the higher version,
	// causing an endless rewrite loop. Wait until all writers support the
	// versions the existing build-request paths will actually send.
	if indexRebuild.enabled() &&
		(targets.scalarVersion != checker.indexEngineVersionManager.ResolveScalarIndexVersion() ||
			targets.vectorVersion != checker.indexEngineVersionManager.ResolveVecIndexVersion()) {
		indexRebuild = indexRebuildOptions{}
	}
	typedPathIndexIDs := getTypedJSONPathIndexIDs(checker.meta.indexMeta, collection)
	if !pathIndexEnabled {
		typedPathIndexIDs = nil
	}
	jsonStatsFieldIDs := getJSONStatsFieldIDs(collection)
	if !indexRebuild.enabled() && len(typedPathIndexIDs) == 0 &&
		(!jsonStatsEnabled || len(jsonStatsFieldIDs) == 0) {
		return nil, nil
	}
	vectorIndexIDs := getVectorIndexIDs(checker.meta.indexMeta, collection)

	segments := checker.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		if !isSegmentHealthy(segment) ||
			!isFlushed(segment) ||
			segment.isCompacting ||
			segment.GetIsImporting() ||
			segment.GetIsInvisible() ||
			segment.GetLevel() == datapb.SegmentLevel_L0 ||
			checker.meta.isSegmentCompactionProtected(segment.GetID()) ||
			hasVersionedStatsTask(checker.meta, segment.GetID()) {
			return false
		}

		canRebuildArtifacts, needsIndexRebuild := checkSegmentIndexRebuild(
			checker.meta.indexMeta,
			segment.GetID(),
			vectorIndexIDs,
			typedPathIndexIDs,
			segment.GetTextStatsLogs(),
			targets,
			indexRebuild)
		statsFormats := classifyJSONStatsFormats(segment, jsonStatsFieldIDs, targetStatsFormat)
		if !canRebuildArtifacts || statsFormats.hasNewer {
			return false
		}

		needsStatsMigration := jsonStatsEnabled && len(jsonStatsFieldIDs) > 0 &&
			statsFormats.hasOlder
		return needsIndexRebuild || needsStatsMigration
	}))

	remaining := maxCount - checker.currentCount
	if remaining <= 0 || len(segments) == 0 {
		return nil, nil
	}
	if len(segments) > remaining {
		segments = segments[:remaining]
	}

	triggerID, err := checker.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "fail to apply migration compaction checker, unable to allocate trigger ID", mlog.Err(err))
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
		checker.currentCount++
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
	// JSON NGRAM paths accelerate LIKE only. NgramInvertedIndex does not
	// persist a presence bitmap or implement EXISTS, so the V6 presence
	// migration has nothing to rewrite. Index type values are user metadata;
	// accept their case-insensitive canonical spelling.
	if strings.EqualFold(GetIndexType(index.IndexParams), "NGRAM") {
		return false
	}
	hasPath := false
	castType := ""
	for _, param := range index.IndexParams {
		switch param.GetKey() {
		case common.JSONPathKey:
			hasPath = true
		case common.JSONCastTypeKey:
			castType = param.GetValue()
		}
	}
	return hasPath && castType != "" && !common.IsFullJSONCastType(castType)
}

// checkSegmentIndexRebuild checks artifact safety and rebuild reasons in one
// pass over a single segment-index snapshot. Finding an outdated index must not
// skip later safety checks: one-input compaction replaces every active artifact.
// Deleted indexes are discarded with the source segment and are not reproduced.
func checkSegmentIndexRebuild(
	indexMeta *indexMeta,
	segmentID int64,
	vectorIndexIDs map[int64]struct{},
	typedPathIndexIDs map[int64]struct{},
	textStatsLogs map[int64]*datapb.TextIndexStats,
	targets safeSegmentRebuildTargets,
	options indexRebuildOptions,
) (canRebuild, needsRebuild bool) {
	for _, textStats := range textStatsLogs {
		if textStats != nil && textStats.GetCurrentScalarIndexVersion() > targets.scalarVersion && !options.forceScalar {
			return false, false
		}
	}
	if indexMeta == nil {
		return true, false
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
				return false, false
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

		current, target, force := segmentIndex.CurrentScalarIndexVersion, targets.scalarVersion, options.forceScalar
		if _, isVectorIndex := vectorIndexIDs[segmentIndex.IndexID]; isVectorIndex {
			current, target, force = segmentIndex.CurrentIndexVersion, targets.vectorVersion, options.forceVector
		}
		if current > target && !force {
			return false, false
		}
		if _, isTypedPathIndex := typedPathIndexIDs[segmentIndex.IndexID]; isTypedPathIndex &&
			segmentIndex.CurrentScalarIndexVersion < common.MinScalarIndexVersionForJsonPathPresence {
			needsRebuild = true
		}
		if (options.autoUpgrade && current < target) || (force && current != target) {
			needsRebuild = true
		}
	}
	return true, needsRebuild
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
