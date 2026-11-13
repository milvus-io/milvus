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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type RewriteObservation struct {
	Scope []*meta.SegmentInfo
	Match []*meta.SegmentInfo
	Rows  int64
}

func NormalizeRewriteRoots(ids []int64) ([]int64, error) {
	roots := slices.Clone(ids)
	slices.Sort(roots)
	if len(roots) > 0 && roots[0] <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("segment IDs must be positive")
	}
	return slices.Compact(roots), nil
}

// ObserveRewrite reads one atomic collection snapshot, including the retained
// dropped records needed to follow lineage.
func ObserveRewrite(ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget, creating bool) (RewriteObservation, error) {
	if err := ctx.Err(); err != nil {
		return RewriteObservation{}, err
	}
	if !IsRewrite(record) || record.GetCollectionID() <= 0 ||
		record.GetExpectedTS() == 0 || record.GetActivatedAtTS() != record.GetExpectedTS() || record.GetTailLimit() != 0 {
		return RewriteObservation{}, merr.WrapErrDataIntegrityMsg("invalid REWRITE compaction target %d", record.GetTargetID())
	}
	roots := record.GetRewrite().GetRootSegmentIds()
	segments := mt.SelectSegments(ctx, meta.WithCollection(record.GetCollectionID()))
	if len(roots) > 0 {
		resolved, err := resolveRewriteLineage(ctx, mt, record.GetCollectionID(), roots, segments, creating)
		if err != nil {
			return RewriteObservation{}, err
		}
		segments = resolved
	}
	var observation RewriteObservation
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return RewriteObservation{}, err
		}
		if !policy.IsNormalManualCompactionSegment(segment) {
			continue
		}
		start := segment.GetEarliestTs()
		if start == 0 {
			start = meta.SegmentEffectiveTs(segment.SegmentInfo)
		}
		if start >= record.GetExpectedTS() {
			continue
		}
		observation.Scope = append(observation.Scope, segment)
		if segment.GetCreateTs() >= record.GetExpectedTS() {
			continue
		}
		rows := segment.GetNumOfRows()
		if rows < 0 || rows > math.MaxInt64-observation.Rows {
			return RewriteObservation{}, merr.WrapErrDataIntegrityMsg("invalid or overflowing Match row count at segment %d", segment.GetID())
		}
		observation.Rows += rows
		observation.Match = append(observation.Match, segment)
	}
	return observation, ctx.Err()
}

func resolveRewriteLineage(ctx context.Context, mt *meta.Meta, collectionID int64, roots []int64, segments []*meta.SegmentInfo, creating bool) ([]*meta.SegmentInfo, error) {
	byID := make(map[int64]*meta.SegmentInfo, len(segments))
	children := make(map[int64][]*meta.SegmentInfo)
	malformed := make(map[int64]struct{})
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := segment.GetID()
		byID[id] = segment
		for _, source := range segment.GetCompactionFrom() {
			children[source] = append(children[source], segment)
			if source <= 0 || source == id || id <= 0 {
				malformed[id] = struct{}{}
			}
		}
	}
	for _, root := range roots {
		if root <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid REWRITE lineage root %d", root)
		}
		if !creating {
			continue
		}
		segment := byID[root]
		if segment == nil {
			if source := mt.GetSegment(ctx, root); source != nil && source.GetCollectionID() != collectionID {
				return nil, merr.WrapErrParameterInvalidMsg("segment %d does not belong to the target collection", root)
			}
			if len(children[root]) == 0 {
				return nil, merr.Wrapf(merr.ErrCompactionTargetScopeUnavailable, "segment %d", root)
			}
		} else if !meta.IsSegmentHealthy(segment) && segment.GetState() != commonpb.SegmentState_Dropped {
			return nil, merr.WrapErrParameterInvalidMsg("segment %d is not a valid lineage root", root)
		}
	}

	// Visit children even after finding a healthy endpoint to detect invalid
	// lineage. Only follow the children for membership when the input is no
	// longer healthy. Clustering can publish outputs before retiring inputs.
	visiting := make(map[int64]bool)
	visited := make(map[int64]bool)
	var validate func(int64) error
	validate = func(id int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if visiting[id] {
			return merr.WrapErrDataIntegrityMsg("cycle in compaction lineage at segment %d", id)
		}
		if visited[id] {
			return nil
		}
		if _, bad := malformed[id]; bad {
			return merr.WrapErrDataIntegrityMsg("invalid compaction lineage at segment %d", id)
		}
		// Same-collection outputs may retain sources whose metadata was GC'd.
		// An extant source in another collection cannot transfer membership.
		visiting[id] = true
		for _, child := range children[id] {
			if byID[id] == nil {
				if source := mt.GetSegment(ctx, id); source != nil && source.GetCollectionID() != collectionID {
					continue
				}
			}
			if err := validate(child.GetID()); err != nil {
				return err
			}
		}
		delete(visiting, id)
		visited[id] = true
		return nil
	}
	for _, root := range roots {
		if err := validate(root); err != nil {
			return nil, err
		}
	}

	var endpoints []*meta.SegmentInfo
	seen := make(map[int64]struct{})
	pending := slices.Clone(roots)
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		segment := byID[id]
		if meta.IsSegmentHealthy(segment) {
			endpoints = append(endpoints, segment)
			continue
		}
		if segment == nil && len(children[id]) > 0 {
			if source := mt.GetSegment(ctx, id); source != nil && source.GetCollectionID() != collectionID {
				continue
			}
		}
		for _, child := range children[id] {
			pending = append(pending, child.GetID())
		}
	}
	return endpoints, ctx.Err()
}
