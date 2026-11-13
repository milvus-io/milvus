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
	"math"
	"strconv"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	vectorModeKey    = "vector_mode"
	vectorVersionKey = "vector_version"
	scalarModeKey    = "scalar_mode"
	scalarVersionKey = "scalar_version"
)

func indexVersionProperties(vector, scalar index.IndexVersionCondition) map[string]string {
	properties := map[string]string{
		vectorModeKey: string(vector.Mode),
		scalarModeKey: string(scalar.Mode),
	}
	if vector.Mode != index.IndexVersionAbsent {
		properties[vectorVersionKey] = strconv.FormatInt(int64(vector.Version), 10)
	}
	if scalar.Mode != index.IndexVersionAbsent {
		properties[scalarVersionKey] = strconv.FormatInt(int64(scalar.Version), 10)
	}
	return properties
}

func indexVersionCondition(record *datapb.CompactionTarget, modeKey, versionKey string) (index.IndexVersionCondition, error) {
	properties := record.GetProperties()
	mode := index.IndexVersionMode(properties[modeKey])
	switch mode {
	case index.IndexVersionAbsent:
		if _, found := properties[versionKey]; !found {
			return index.IndexVersionCondition{Mode: mode}, nil
		}
	case index.IndexVersionAtLeast, index.IndexVersionExact:
		version, err := strconv.ParseInt(properties[versionKey], 10, 32)
		if err == nil {
			return index.IndexVersionCondition{Mode: mode, Version: int32(version)}, nil
		}
	}
	return index.IndexVersionCondition{}, merr.WrapErrDataIntegrityMsg("invalid %s for Index Version compaction target %d", modeKey, record.GetTargetID())
}

func indexVersionSegment(segment *meta.SegmentInfo) bool {
	return meta.IsSegmentHealthy(segment) && meta.IsFlushed(segment) &&
		segment.GetLevel() != datapb.SegmentLevel_L0 && segment.GetLevel() != datapb.SegmentLevel_L2
}

// ObserveIndexVersion reports the complete Match for the confirmed condition.
// The live index metadata is independent of current build readiness.
func ObserveIndexVersion(ctx context.Context, mt *meta.Meta, indexes *index.IndexMeta, record *datapb.CompactionTarget) (int64, error) {
	segments := mt.SelectSegments(ctx, meta.SegmentFilterFunc(indexVersionSegment))
	match, err := indexVersionMatch(ctx, mt, indexes, record, segments)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, segment := range match {
		rows := segment.GetNumOfRows()
		if rows < 0 || rows > math.MaxInt64-total {
			return 0, merr.WrapErrDataIntegrityMsg("invalid Index Version observation row count at segment %d", segment.GetID())
		}
		total += rows
	}
	return total, ctx.Err()
}

func indexVersionMatch(ctx context.Context, mt *meta.Meta, indexes *index.IndexMeta, record *datapb.CompactionTarget, segments []*meta.SegmentInfo) ([]*meta.SegmentInfo, error) {
	vector, err := indexVersionCondition(record, vectorModeKey, vectorVersionKey)
	if err != nil {
		return nil, err
	}
	scalar, err := indexVersionCondition(record, scalarModeKey, scalarVersionKey)
	if err != nil {
		return nil, err
	}
	if indexes == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("index metadata is unavailable for compaction target %d", record.GetTargetID())
	}
	collections := make(map[int64][]*meta.SegmentInfo)
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !indexVersionSegment(segment) {
			continue
		}
		collectionID := segment.GetCollectionID()
		candidates, checked := collections[collectionID]
		if !checked {
			collection := mt.GetCollection(collectionID)
			if !collection.IsCreated() || collection.IsExternal() || !indexes.HasIndex(collectionID) {
				collections[collectionID] = nil
				continue
			}
		} else if candidates == nil {
			continue
		}
		collections[collectionID] = append(candidates, segment)
	}

	var match []*meta.SegmentInfo
	for id, candidates := range collections {
		if len(candidates) == 0 {
			continue
		}
		ids := make([]int64, len(candidates))
		for i, segment := range candidates {
			ids[i] = segment.GetID()
		}
		stale, err := indexes.MatchMaterializedIndexVersions(ctx, id, ids, vector, scalar)
		if err != nil {
			return nil, err
		}
		for i, segment := range candidates {
			if len(stale) == 0 {
				break
			}
			if ids[i] == stale[0] {
				match = append(match, segment)
				stale = stale[1:]
			}
		}
	}
	return match, ctx.Err()
}

func (r *CompactionTargetReconciler) selectableIndexRewrites(ctx context.Context, auto bool, segments []*meta.SegmentInfo) []*meta.SegmentInfo {
	if r.versionManager == nil {
		return nil
	}
	vector, scalar := r.versionManager.ResolveIndexVersionConditions(ctx)
	enabled := auto && (vector.Mode != index.IndexVersionAbsent || scalar.Mode != index.IndexVersionAbsent)
	record, err := r.CompactionTargetMeta.SyncStanding(ctx, r.allocator, datapb.TargetIntent_INTENT_INDEX_VERSION,
		enabled, indexVersionProperties(vector, scalar))
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not synchronize Standing Index Version compaction target", mlog.Err(err))
		return nil
	}
	if !record.GetEnabled() {
		return nil
	}

	if segments == nil {
		segments = r.meta.SelectSegments(ctx, meta.SegmentFilterFunc(indexVersionSegment))
	}
	match, err := indexVersionMatch(ctx, r.meta, r.IndexMeta, record, segments)
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not observe Standing Index Version compaction target",
			mlog.Int64("targetID", record.GetTargetID()), mlog.Err(err))
		return nil
	}
	candidates := make([]*meta.SegmentInfo, 0, len(match))
	autoCollections := make(map[int64]bool)
	for _, segment := range match {
		id := segment.GetCollectionID()
		enabled, checked := autoCollections[id]
		if !checked {
			enabled = !r.meta.IsCollectionCompactionBlocked(id) &&
				policy.IsCollectionAutoCompactionEnabled(r.meta.GetCollection(id))
			autoCollections[id] = enabled
		}
		if enabled && !segment.GetIsImporting() && policy.IsSharedCompactionSelectable(r.meta, segment) {
			candidates = append(candidates, segment)
		}
	}
	selectable := r.filterMixCompactionSelectable(ctx, candidates)
	if len(selectable) < len(match) {
		mlog.RatedInfo(ctx, rate.Limit(1), "Index Version compaction target inputs awaiting selection",
			mlog.Int("matches", len(match)), mlog.Int("selectable", len(selectable)))
	}
	return selectable
}
