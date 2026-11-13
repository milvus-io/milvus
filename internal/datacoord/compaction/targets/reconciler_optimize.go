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
		return
	}
	if finished {
		return
	}
	expected := record.GetOptimize().GetActualTargetSizeBytes()
	for _, segment := range observation.Scope {
		size := observation.sizes[segment.GetID()]
		inputs[segment.GetID()] = optimizeInput{
			segment: segment, residual: size, expected: expected,
			match: !observation.Satisfied && view.ClassifyOptimizeSize(size, expected) != 0,
		}
	}
}

func (r *CompactionTargetReconciler) mixViews(
	ctx context.Context, rewrites []*meta.SegmentInfo, optimizeInputs map[int64]optimizeInput,
	allocated map[int64]struct{}, eligible map[int64]collectionPlanningFacts,
) []view.TargetView {
	requiredRewrites := make(map[int64]*meta.SegmentInfo, len(rewrites))
	for _, segment := range rewrites {
		requiredRewrites[segment.GetID()] = segment
	}

	var optimizeSelectable []*meta.SegmentInfo
	for id, input := range optimizeInputs {
		_, sorting := allocated[id]
		if !sorting && r.selectable(ctx, input.segment, eligible) {
			optimizeSelectable = append(optimizeSelectable, input.segment)
		}
	}
	optimizeSelectable = r.filterMixCompactionSelectable(ctx, optimizeSelectable)
	selected := make(map[int64]*meta.SegmentInfo, len(optimizeSelectable)+len(rewrites))
	for _, segment := range optimizeSelectable {
		selected[segment.GetID()] = segment
	}
	for id, segment := range requiredRewrites {
		selected[id] = segment
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
		input := view.GetTargetSegmentView(segment)
		opt, inOptimize := optimizeInputs[segment.GetID()]
		_, required := requiredRewrites[segment.GetID()]
		if inOptimize {
			input.ResidualSize = opt.residual
		}
		key := poolKey{group: *input.Label, optimize: inOptimize}
		pool := groups[key]
		if pool == nil {
			facts := eligible[key.group.CollectionID]
			pool = &view.MixView{
				RewriteInputs: view.RewriteInputs{Group: input.Label, TTL: facts.ttl, Schema: facts.schema, ExpectedSize: facts.expectedSize},
				OptimizeSize:  opt.expected,
			}
			groups[key] = pool
			views = append(views, pool)
		}
		pool.Segments = append(pool.Segments, input)
		if required {
			if pool.RewriteRequired == nil {
				pool.RewriteRequired = make(map[int64]struct{})
			}
			pool.RewriteRequired[input.ID] = struct{}{}
		}
		if opt.match {
			if pool.OptimizeMatch == nil {
				pool.OptimizeMatch = make(map[int64]struct{})
			}
			pool.OptimizeMatch[input.ID] = struct{}{}
		}
	}
	return views
}
