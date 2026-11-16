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

package targets

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	DeletedRatioKey         = "deleted_ratio_threshold"
	MaxDeltalogCountKey     = "max_deltalog_count"
	MaxDeltalogBytesKey     = "max_deltalog_bytes"
	ExpiredRatioKey         = "expired_ratio_threshold"
	MaxExpiredBytesKey      = "max_expired_bytes"
	StrictToleranceHoursKey = "strict_tolerance_hours"
)

func IsStandingCleanup(record *datapb.CompactionTarget) bool {
	return HasStandingCondition(record) &&
		(record.GetIntent() == datapb.TargetIntent_INTENT_DELETE_CLEANUP || record.GetIntent() == datapb.TargetIntent_INTENT_TTL)
}

type ttlCleanupThresholds struct {
	ratio          float64
	maxBytes       int64
	toleranceHours int64
}

func readDeleteCleanupThresholds(record *datapb.CompactionTarget) (policy.DeleteCleanupThresholds, error) {
	properties := record.GetProperties()
	ratio, ratioErr := strconv.ParseFloat(properties[DeletedRatioKey], 64)
	maxBytes, bytesErr := strconv.ParseInt(properties[MaxDeltalogBytesKey], 10, 64)
	maxLogs, logsErr := strconv.ParseInt(properties[MaxDeltalogCountKey], 10, 64)
	if ratioErr != nil || bytesErr != nil || logsErr != nil {
		return policy.DeleteCleanupThresholds{}, merr.WrapErrDataIntegrityMsg(
			"invalid saved Delete Cleanup condition for compaction target %d", record.GetTargetID())
	}
	return policy.DeleteCleanupThresholds{MaxLogs: maxLogs, Ratio: ratio, MaxBytes: maxBytes}, nil
}

func readTTLCleanupThresholds(record *datapb.CompactionTarget) (ttlCleanupThresholds, error) {
	properties := record.GetProperties()
	ratio, ratioErr := strconv.ParseFloat(properties[ExpiredRatioKey], 64)
	maxBytes, bytesErr := strconv.ParseInt(properties[MaxExpiredBytesKey], 10, 64)
	tolerance, toleranceErr := strconv.ParseInt(properties[StrictToleranceHoursKey], 10, 64)
	if ratioErr != nil || bytesErr != nil || toleranceErr != nil {
		return ttlCleanupThresholds{}, merr.WrapErrDataIntegrityMsg(
			"invalid saved TTL condition for compaction target %d", record.GetTargetID())
	}
	return ttlCleanupThresholds{ratio: ratio, maxBytes: maxBytes, toleranceHours: tolerance}, nil
}

type cleanupCollection struct {
	info    *meta.CollectionInfo
	ttl     time.Duration
	ttlErr  error
	horizon uint64
}

type cleanupObservation struct {
	now         time.Time
	collections map[int64]cleanupCollection
}

func (observation *cleanupObservation) collection(mt *meta.Meta, id int64) cleanupCollection {
	if facts, ok := observation.collections[id]; ok {
		return facts
	}
	var facts cleanupCollection
	collection := mt.GetCollection(id)
	if collection.IsCreated() && !collection.IsExternal() {
		facts.info = collection
		facts.ttl, facts.ttlErr = common.GetCollectionTTLFromMap(collection.Properties)
		if facts.ttl > 0 {
			horizon := observation.now.Add(-facts.ttl)
			if horizon.UnixMilli() > 0 {
				facts.horizon = tsoutil.ComposeTSByTime(horizon)
			}
		}
	}
	observation.collections[id] = facts
	return facts
}

// A manifest-backed segment without insert statistics may have been loaded
// with an empty array-based fallback. In that case zero delete totals or
// zero expired bytes cannot establish that cleanup is unnecessary.
func missingCleanupStats(segment *meta.SegmentInfo, stats *datapb.Statistics) bool {
	return segment.GetNumOfRows() > 0 && segment.GetManifestPath() != "" &&
		stats.GetInsertBinlogCount() == 0 && stats.GetInsertBinlogSize() == 0 &&
		stats.GetTimestampFrom() == 0 && stats.GetTimestampTo() == 0
}

func matchDeleteCleanup(segment *meta.SegmentInfo, stats *datapb.Statistics, thresholds policy.DeleteCleanupThresholds) (bool, error) {
	if stats.GetDeltaBinlogCount() < 0 || stats.GetDeltaBinlogSize() < 0 || stats.GetDeleteNumRows() < 0 {
		return false, merr.WrapErrDataIntegrityMsg("invalid delete statistics for segment %d", segment.GetID())
	}
	if thresholds.Match(stats, segment.GetNumOfRows()) {
		return true, nil
	}
	if missingCleanupStats(segment, stats) {
		return false, merr.WrapErrDataIntegrityMsg("missing cleanup statistics for segment %d", segment.GetID())
	}
	return false, nil
}

func matchTTL(segment *meta.SegmentInfo, stats *datapb.Statistics, collection cleanupCollection, now time.Time, condition ttlCleanupThresholds) (bool, error) {
	if policy.IsTTLFieldExpired(segment.GetExpirQuantiles(), now, condition.ratio) {
		return true, nil
	}
	if collection.ttlErr != nil {
		return false, merr.Wrapf(collection.ttlErr, "read TTL for collection %d", collection.info.ID)
	}
	commit := segment.GetCommitTimestamp()
	expired, fraction, _ := policy.ExpiredRowsExceedThreshold(stats, commit, collection.horizon, condition.ratio, condition.maxBytes)
	if expired && (fraction >= condition.ratio || stats.GetInsertBinlogSize() >= 0) {
		return true, nil
	}
	if collection.horizon == 0 || segment.GetNumOfRows() == 0 {
		return false, nil
	}

	// Compute the cutoff in milliseconds so a large valid tolerance in
	// hours cannot wrap a time.Duration into a shorter tolerance.
	var unknownStrict bool
	horizonMillis := tsoutil.PhysicalTime(collection.horizon).UnixMilli()
	const hourMillis = int64(time.Hour / time.Millisecond)
	if hours := condition.toleranceHours; hours >= 0 && hours <= horizonMillis/hourMillis {
		cutoff := time.UnixMilli(horizonMillis - hours*hourMillis)
		earliest := tsoutil.EffectiveTimestamp(stats.GetTimestampFrom(), commit)
		if earliest > 0 && policy.IsStrictlyExpired(earliest, cutoff) {
			return true, nil
		}
		unknownStrict = earliest == 0 && cutoff.UnixMilli() > 0
	}
	if unknownStrict ||
		(commit == 0 && (missingCleanupStats(segment, stats) || len(stats.GetTimestampQuantiles()) == 0)) ||
		(fraction > 0 && (stats.GetInsertBinlogSize() < 0 || missingCleanupStats(segment, stats))) {
		return false, merr.WrapErrDataIntegrityMsg("missing TTL statistics for segment %d", segment.GetID())
	}
	return false, nil
}

func (observation *cleanupObservation) visitMatches(
	ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget, segments []*meta.SegmentInfo,
	visit func(*meta.SegmentInfo, cleanupCollection) error,
) error {
	var matches func(*meta.SegmentInfo, *datapb.Statistics, cleanupCollection) (bool, error)
	switch record.GetIntent() {
	case datapb.TargetIntent_INTENT_DELETE_CLEANUP:
		condition, err := readDeleteCleanupThresholds(record)
		if err != nil {
			return err
		}
		matches = func(segment *meta.SegmentInfo, stats *datapb.Statistics, _ cleanupCollection) (bool, error) {
			return matchDeleteCleanup(segment, stats, condition)
		}
	case datapb.TargetIntent_INTENT_TTL:
		condition, err := readTTLCleanupThresholds(record)
		if err != nil {
			return err
		}
		matches = func(segment *meta.SegmentInfo, stats *datapb.Statistics, collection cleanupCollection) (bool, error) {
			return matchTTL(segment, stats, collection, observation.now, condition)
		}
	default:
		return merr.WrapErrDataIntegrityMsg("invalid cleanup compaction target family %s", record.GetIntent())
	}

	var firstErr error
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection := observation.collection(mt, segment.GetCollectionID())
		if collection.info == nil {
			continue
		}
		var matched bool
		var err error
		if segment.GetNumOfRows() < 0 {
			err = merr.WrapErrDataIntegrityMsg("negative cleanup row count for segment %d", segment.GetID())
		} else {
			matched, err = matches(segment, segment.EnsureStats(), collection)
		}
		if matched {
			err = visit(segment, collection)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return firstErr
}

func ObserveCleanup(ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget) (int64, error) {
	observation := cleanupObservation{now: time.Now(), collections: make(map[int64]cleanupCollection)}
	var rows int64
	err := observation.visitMatches(ctx, mt, record, standingSegments(ctx, mt), func(segment *meta.SegmentInfo, _ cleanupCollection) error {
		count := segment.GetNumOfRows()
		if count > math.MaxInt64-rows {
			return merr.WrapErrDataIntegrityMsg("overflowing cleanup Match row count at segment %d", segment.GetID())
		}
		rows += count
		return nil
	})
	return rows, err
}

func (r *CompactionTargetReconciler) selectableCleanupRewrites(
	ctx context.Context, auto bool, segments []*meta.SegmentInfo, eligible map[int64]collectionPlanningFacts,
) []*meta.SegmentInfo {
	cfg := &Params.DataCoordCfg
	ratio, err := strconv.ParseFloat(cfg.SingleCompactionRatioThreshold.GetValue(), 64)
	if err != nil {
		mlog.RatedWarn(ctx, 1, "could not read cleanup compaction ratio", mlog.String("config", cfg.SingleCompactionRatioThreshold.Key), mlog.Err(err))
		return nil
	}
	if ratio == 0 {
		ratio = 0 // Canonicalize negative zero.
	}
	ratioValue := strconv.FormatFloat(ratio, 'g', -1, 64)
	sources := [...]struct {
		intent                                  datapb.TargetIntent
		ratioKey, bytesKey, countOrToleranceKey string
		bytes, countOrTolerance                 *paramtable.ParamItem
	}{
		{
			datapb.TargetIntent_INTENT_DELETE_CLEANUP, DeletedRatioKey, MaxDeltalogBytesKey, MaxDeltalogCountKey,
			&cfg.SingleCompactionDeltaLogMaxSize, &cfg.SingleCompactionDeltalogMaxNum,
		},
		{
			datapb.TargetIntent_INTENT_TTL, ExpiredRatioKey, MaxExpiredBytesKey, StrictToleranceHoursKey,
			&cfg.SingleCompactionExpiredLogMaxSize, &cfg.CompactionExpiryTolerance,
		},
	}
	var records []*datapb.CompactionTarget
	for _, source := range sources {
		bytes, bytesErr := strconv.ParseInt(source.bytes.GetValue(), 10, 64)
		countOrTolerance, thresholdErr := strconv.ParseInt(source.countOrTolerance.GetValue(), 10, 64)
		if bytesErr != nil || thresholdErr != nil {
			mlog.RatedWarn(ctx, 1, "could not read Standing cleanup compaction thresholds", mlog.Stringer("family", source.intent),
				mlog.Err(errors.Join(bytesErr, thresholdErr)))
			continue
		}
		if source.intent == datapb.TargetIntent_INTENT_TTL {
			countOrTolerance = max(countOrTolerance, -1)
		}
		properties := map[string]string{
			source.ratioKey:            ratioValue,
			source.bytesKey:            strconv.FormatInt(bytes, 10),
			source.countOrToleranceKey: strconv.FormatInt(countOrTolerance, 10),
		}
		record, err := r.CompactionTargetMeta.SyncStanding(ctx, r.allocator, source.intent, auto, properties)
		if err != nil {
			mlog.RatedWarn(ctx, 1, "could not synchronize Standing cleanup compaction target", mlog.Stringer("family", source.intent), mlog.Err(err))
			continue
		}
		if record.GetEnabled() {
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return nil
	}

	observation := cleanupObservation{now: time.Now(), collections: make(map[int64]cleanupCollection)}
	autoCollections := make(map[int64]bool)
	candidates := make(map[int64]*meta.SegmentInfo)
	for _, record := range records {
		var matches, ready int
		err := observation.visitMatches(ctx, r.meta, record, segments, func(segment *meta.SegmentInfo, collection cleanupCollection) error {
			matches++
			if segment.GetIsImporting() || segment.GetIsInvisible() ||
				(segment.GetLevel() != datapb.SegmentLevel_L2 && !policy.IsMixCompactionSelectable(segment)) {
				return nil
			}
			id := segment.GetCollectionID()
			enabled, checked := autoCollections[id]
			if !checked {
				var err error
				enabled, err = policy.CollectionAutoCompactionEnabled(collection.info.Properties, auto)
				if err != nil {
					mlog.RatedWarn(ctx, 1, "could not determine collection auto compaction for cleanup",
						mlog.FieldCollectionID(id), mlog.Err(err))
				}
				autoCollections[id] = enabled
			}
			if !enabled {
				return nil
			}
			if _, checked := eligible[id]; !checked {
				eligible[id] = r.collectionFacts(ctx, id, collection.info, collection.ttl, collection.ttlErr)
			}
			if r.selectable(ctx, segment, eligible) {
				ready++
				candidates[segment.GetID()] = segment
			}
			return nil
		})
		if err != nil && ctx.Err() == nil {
			mlog.RatedWarn(ctx, 1, "could not fully observe Standing cleanup compaction target", mlog.Stringer("family", record.GetIntent()), mlog.Err(err))
		}
		if matches > ready {
			mlog.RatedInfo(ctx, 1, "cleanup compaction target inputs awaiting selection",
				mlog.Stringer("family", record.GetIntent()), mlog.Int("matches", matches), mlog.Int("ready", ready))
		}
	}
	selectable := make([]*meta.SegmentInfo, 0, len(candidates))
	for _, segment := range candidates {
		selectable = append(selectable, segment)
	}
	if len(selectable) > 0 && cfg.IndexBasedCompaction.GetAsBool() {
		if r.IndexMeta == nil {
			return nil
		}
		// Legacy automatic Mix and L2 Single both require index readiness.
		return index.FilterInIndexedSegments(ctx, r.handler, r.IndexMeta, false, selectable...)
	}
	return selectable
}
