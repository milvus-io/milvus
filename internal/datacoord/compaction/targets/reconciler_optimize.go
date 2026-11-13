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
	"slices"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type optimizeInput struct {
	segment  *meta.SegmentInfo
	residual int64
	expected int64
	match    bool
	valid    bool
}

func (r *CompactionTargetReconciler) observeOptimize(ctx context.Context, record *datapb.CompactionTarget, inputs map[int64]optimizeInput) {
	observation, err := ObserveOptimize(ctx, r.meta, record)
	finished := false
	if err == nil && observation.Satisfied {
		defer r.CompactionTargetMeta.LockCollectionAdmission(record.GetCollectionID())()
		err = r.CompactionTargetMeta.Stop(ctx, record.GetTargetID(), StopSatisfied, func(current *datapb.CompactionTarget) (bool, error) {
			if current.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE ||
				!r.meta.GetCollection(current.GetCollectionID()).IsCreated() {
				return false, nil
			}
			currentObservation, err := ObserveOptimize(ctx, r.meta, current)
			if err != nil {
				return false, err
			}
			observation = currentObservation
			finished = observation.Satisfied && !OptimizeHasInFlightWork(r.meta, r.taskMeta, current.GetCollectionID(), observation.Scope)
			return finished, nil
		})
	}
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not reconcile OPTIMIZE target", mlog.Int64("targetID", record.GetTargetID()), mlog.Err(err))
	}
	if finished {
		return
	}
	expected := record.GetOptimize().GetActualTargetSizeBytes()
	for _, segment := range observation.Scope {
		size, valid := observation.sizes[segment.GetID()]
		inputs[segment.GetID()] = optimizeInput{
			segment: segment, residual: size, expected: expected, valid: valid,
			match: valid && !observation.Satisfied && view.ClassifyOptimizeSize(size, expected) != 0,
		}
	}
}

func (r *CompactionTargetReconciler) mixViews(
	ctx context.Context, requiredRewrites map[int64]*meta.SegmentInfo, optimizeInputs map[int64]optimizeInput,
	allocated map[int64]struct{}, eligible map[int64]collectionPlanningFacts, size SizeCondition,
	segments []*meta.SegmentInfo,
) []view.TargetView {
	sizeInputs := r.selectSizeInputs(ctx, size, optimizeInputs, allocated, eligible, segments)

	var optimizeSelectable []*meta.SegmentInfo
	for id, input := range optimizeInputs {
		_, assigned := allocated[id]
		if input.valid && !assigned && r.selectable(ctx, input.segment, eligible) {
			optimizeSelectable = append(optimizeSelectable, input.segment)
		}
	}
	optimizeSelectable = r.filterMixCompactionSelectable(ctx, optimizeSelectable)
	optimizeCandidates := make(map[int64]bool, len(optimizeSelectable))
	selected := make(map[int64]*meta.SegmentInfo, len(optimizeSelectable)+len(requiredRewrites)+len(sizeInputs))
	for _, segment := range optimizeSelectable {
		selected[segment.GetID()] = segment
		optimizeCandidates[segment.GetID()] = true
	}
	for id, segment := range requiredRewrites {
		selected[id] = segment
	}
	for id, input := range sizeInputs {
		selected[id] = input.segment
	}

	ordered := make([]*meta.SegmentInfo, 0, len(selected))
	for _, segment := range selected {
		ordered = append(ordered, segment)
	}
	slices.SortFunc(ordered, compareSegment)
	type poolKey struct {
		group    view.CompactionGroupLabel
		optimize bool
	}
	groups := make(map[poolKey]*view.MixView)
	var views []view.TargetView
	for _, segment := range ordered {
		opt, inOptimize := optimizeInputs[segment.GetID()]
		sizeInput, sizeEligible := sizeInputs[segment.GetID()]
		_, required := requiredRewrites[segment.GetID()]
		if inOptimize && opt.valid {
			segment = opt.segment
		}
		input := view.GetTargetSegmentView(segment)
		if inOptimize && opt.valid {
			input.ResidualSize = opt.residual
		}
		if sizeEligible {
			input.ResidualSize = sizeInput.residual
		}
		key := poolKey{group: *input.Label, optimize: inOptimize}
		pool := groups[key]
		if pool == nil {
			facts := eligible[key.group.CollectionID]
			pool = &view.MixView{
				RewriteInputs: view.RewriteInputs{Group: input.Label, TTL: facts.ttl, Schema: facts.schema, ExpectedSize: facts.expectedSize},
				OptimizeSize:  opt.expected,
				MaxFragments:  size.MaxFragments,
			}
			groups[key] = pool
			views = append(views, pool)
		}
		pool.Segments = append(pool.Segments, input)
		if optimizeCandidates[input.ID] {
			pool.OptimizeInputs = append(pool.OptimizeInputs, input)
		}
		if sizeEligible {
			pool.SizeExpected = sizeInput.expected
			pool.SizeInputs = append(pool.SizeInputs, input)
		}
		if required {
			if pool.RewriteRequired == nil {
				pool.RewriteRequired = make(map[int64]struct{})
			}
			pool.RewriteRequired[input.ID] = struct{}{}
		}
		if opt.match && optimizeCandidates[input.ID] {
			if pool.OptimizeMatch == nil {
				pool.OptimizeMatch = make(map[int64]struct{})
			}
			pool.OptimizeMatch[input.ID] = struct{}{}
		}
	}
	return views
}

func (r *CompactionTargetReconciler) selectSizeInputs(
	ctx context.Context, condition SizeCondition, optimizeInputs map[int64]optimizeInput,
	allocated map[int64]struct{}, eligible map[int64]collectionPlanningFacts,
	segments []*meta.SegmentInfo,
) map[int64]sizeInput {
	if !condition.Enabled {
		return nil
	}
	var candidates []*meta.SegmentInfo
	inputs := make(map[int64]sizeInput)
	auto := make(map[int64]bool)
	err := visitSizeMatches(ctx, r.meta, r.IndexMeta, condition, segments, func(input sizeInput) error {
		segment := input.segment
		id := segment.GetID()
		_, assigned := allocated[id]
		_, inOptimize := optimizeInputs[id]
		if assigned || inOptimize || segment.GetIsImporting() || !r.selectable(ctx, segment, eligible) {
			return nil
		}
		collectionID := segment.GetCollectionID()
		enabled, observed := auto[collectionID]
		if !observed {
			enabled = policy.IsCollectionAutoCompactionEnabled(r.meta.GetCollection(collectionID))
			auto[collectionID] = enabled
		}
		if enabled {
			candidates = append(candidates, segment)
			inputs[id] = input
		}
		return nil
	})
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not fully observe Standing SIZE compaction target", mlog.Err(err))
	}
	selectable := r.filterMixCompactionSelectable(ctx, candidates)
	selected := make(map[int64]sizeInput, len(selectable))
	for _, segment := range selectable {
		selected[segment.GetID()] = inputs[segment.GetID()]
	}
	return selected
}
