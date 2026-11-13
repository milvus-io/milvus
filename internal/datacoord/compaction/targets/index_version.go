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

// ObserveIndexVersion checks the complete Match against the confirmed condition.
// Build readiness is a separate execution requirement.
func ObserveIndexVersion(ctx context.Context, mt *meta.Meta, indexes *index.IndexMeta, handler meta.Handler, record *datapb.CompactionTarget) (int64, error) {
	matches, err := observeIndexVersion(ctx, mt, indexes, handler, record, standingSegments(ctx, mt))
	if err != nil {
		return 0, err
	}
	var total int64
	for _, match := range matches {
		for _, segment := range match.segments {
			rows := segment.GetNumOfRows()
			if rows < 0 || rows > math.MaxInt64-total {
				return 0, merr.WrapErrDataIntegrityMsg("invalid Index Version observation row count at segment %d", segment.GetID())
			}
			total += rows
		}
	}
	return total, ctx.Err()
}

type indexVersionMatch struct {
	collection *meta.CollectionInfo
	segments   []*meta.SegmentInfo
}

func observeIndexVersion(ctx context.Context, mt *meta.Meta, indexes *index.IndexMeta, handler meta.Handler, record *datapb.CompactionTarget, segments []*meta.SegmentInfo) ([]indexVersionMatch, error) {
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

	byCollection := make(map[int64][]*meta.SegmentInfo)
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if segment.GetLevel() != datapb.SegmentLevel_L2 {
			id := segment.GetCollectionID()
			byCollection[id] = append(byCollection[id], segment)
		}
	}

	var matches []indexVersionMatch
	var firstErr error
	var ids []int64
	for id, candidates := range byCollection {
		ids = ids[:0]
		for _, segment := range candidates {
			ids = append(ids, segment.GetID())
		}
		stale, err := indexes.MatchMaterializedIndexVersions(ctx, id, ids, vector, scalar)
		if err != nil {
			return nil, err
		}
		if len(stale) == 0 {
			continue
		}
		collection, err := indexVersionCollection(ctx, mt, handler, id)
		if err != nil {
			if firstErr == nil {
				firstErr = merr.Wrapf(err, "observe Index Version collection %d", id)
			}
			continue
		}
		if collection == nil || collection.IsExternal() {
			continue
		}
		match := indexVersionMatch{collection: collection, segments: make([]*meta.SegmentInfo, 0, len(stale))}
		for _, segment := range candidates {
			if segment.GetID() == stale[0] {
				match.segments = append(match.segments, segment)
				stale = stale[1:]
				if len(stale) == 0 {
					break
				}
			}
		}
		matches = append(matches, match)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return matches, firstErr
}

func indexVersionCollection(ctx context.Context, mt *meta.Meta, handler meta.Handler, id int64) (*meta.CollectionInfo, error) {
	if collection := mt.GetCollection(id); collection != nil {
		return collection, nil
	}
	// The 3.0 collection cache loads asynchronously. Only stale artifacts
	// need a collection lookup to establish scope.
	if handler != nil {
		collection, err := handler.GetCollection(ctx, id)
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil, nil
		}
		if err != nil || collection != nil {
			return collection, err
		}
	}
	return nil, merr.WrapErrServiceNotReadyMsg("collection metadata is unavailable")
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
		mlog.RatedWarn(ctx, 1, "could not synchronize Standing Index Version compaction target", mlog.Err(err))
		return nil
	}
	if !record.GetEnabled() {
		return nil
	}

	matches, err := observeIndexVersion(ctx, r.meta, r.IndexMeta, r.handler, record, segments)
	if err != nil && ctx.Err() == nil {
		mlog.RatedWarn(ctx, 1, "could not fully observe Standing Index Version compaction target",
			mlog.Int64("targetID", record.GetTargetID()), mlog.Err(err))
	}
	var candidates []*meta.SegmentInfo
	var matchedCount int
	for _, match := range matches {
		matchedCount += len(match.segments)
		if r.meta.IsCollectionCompactionBlocked(match.collection.ID) || !policy.IsCollectionAutoCompactionEnabled(match.collection) {
			continue
		}
		for _, segment := range match.segments {
			if !segment.GetIsImporting() && policy.IsSharedCompactionSelectable(r.meta, segment) {
				candidates = append(candidates, segment)
			}
		}
	}
	selectable := r.filterMixCompactionSelectable(ctx, candidates)
	if len(selectable) < matchedCount {
		mlog.RatedInfo(ctx, 1, "Index Version compaction target inputs awaiting selection",
			mlog.Int("matches", matchedCount), mlog.Int("selectable", len(selectable)))
	}
	return selectable
}
