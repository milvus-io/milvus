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

package view

import (
	"math"
	"slices"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type TargetView interface {
	GetGroupLabel() *CompactionGroupLabel
	GetSegmentsView() []*SegmentView
	Plan() ([]*datapb.CompactionTask, error)
}

// RewriteInputs contains the facts captured for one physical group before
// admission. Independent rewrite inputs retain singleton fallback until a
// size planner groups the compatible inputs.
type RewriteInputs struct {
	Group        *CompactionGroupLabel
	Segments     []*SegmentView
	Schema       *schemapb.CollectionSchema
	TTL          time.Duration
	ExpectedSize int64
}

func (v *RewriteInputs) GetGroupLabel() *CompactionGroupLabel { return v.Group }
func (v *RewriteInputs) GetSegmentsView() []*SegmentView      { return v.Segments }

type (
	SortView    struct{ RewriteInputs }
	RewriteView struct{ RewriteInputs }
)

func (v *SortView) Plan() ([]*datapb.CompactionTask, error) {
	return v.planSingletons(datapb.CompactionType_SortCompaction)
}

func (v *RewriteView) Plan() ([]*datapb.CompactionTask, error) {
	return v.planSingletons(datapb.CompactionType_MixCompaction)
}

// MixView receives one group's already allocated inputs and fixed sizing
// policy. The two maps mark each input's independent work requirements.
type MixView struct {
	RewriteInputs
	OptimizeSize    int64
	OptimizeMatch   map[int64]struct{}
	RewriteRequired map[int64]struct{}
	SizeExpected    int64
	MaxFragments    int64
	SizeInputs      []*SegmentView
}

func (v *MixView) Plan() ([]*datapb.CompactionTask, error) {
	covered := make(map[int64]bool)
	var plans []*datapb.CompactionTask
	if v.OptimizeSize > 0 && (len(v.OptimizeMatch) > 0 || len(v.RewriteRequired) > 0) {
		groups := groupForceMergeInputs(v.Segments, v.OptimizeSize)
		for _, group := range groups {
			if !v.shouldOptimizeGroup(group) {
				continue
			}
			plan, err := v.planMix(group, v.OptimizeSize, v.OptimizeSize, false)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
			for _, input := range group {
				covered[input.ID] = true
			}
		}
	}
	if v.SizeExpected > 0 {
		// The packer sorts its own copy. A second Plan uses the same View.
		groups := GroupTwoTier(slices.Clone(v.SizeInputs), &CompactTime{CollectionTTL: v.TTL}, v.SizeExpected, v.MaxFragments, false)
		for _, group := range groups {
			plan, err := v.planMix(group.Inputs, group.MaxSize, v.SizeExpected, true)
			if err != nil {
				return nil, err
			}
			plan.SizeCompactionHint = true
			plans = append(plans, plan)
			for _, input := range group.Inputs {
				covered[input.ID] = true
			}
		}
	}
	for _, input := range v.Segments {
		if _, required := v.RewriteRequired[input.ID]; !required || covered[input.ID] {
			continue
		}
		plan, err := v.planMix([]*SegmentView{input}, v.ExpectedSize, v.ExpectedSize, false)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (v *MixView) shouldOptimizeGroup(group []*SegmentView) bool {
	for _, input := range group {
		if _, required := v.RewriteRequired[input.ID]; required {
			return true
		}
		if _, matched := v.OptimizeMatch[input.ID]; matched &&
			(len(group) > 1 || ClassifyOptimizeSize(input.ResidualSize, v.OptimizeSize) > 0) {
			return true
		}
	}
	return false
}

func (v *MixView) planMix(group []*SegmentView, maxSize, preferredSize int64, fullInputSize bool) (*datapb.CompactionTask, error) {
	var rows int64
	var size float64
	ids := make([]int64, 0, len(group))
	for _, input := range group {
		if input.NumOfRows < 0 || input.NumOfRows > math.MaxInt64-rows || input.InsertBytes < 0 ||
			input.SegmentSize < 0 || input.ResidualSize < 0 || v.Schema == nil || !v.Group.Equal(input.Label) {
			return nil, merr.WrapErrDataIntegrityMsg("invalid Mix compaction plan input %d", input.ID)
		}
		rows += input.NumOfRows
		if fullInputSize {
			size += float64(input.SegmentSize)
		} else {
			size += float64(input.InsertBytes)
		}
		ids = append(ids, input.ID)
	}
	return &datapb.CompactionTask{
		Type: datapb.CompactionType_MixCompaction, CollectionID: v.Group.CollectionID, PartitionID: v.Group.PartitionID, Channel: v.Group.Channel,
		InputSegments: ids, TotalRows: rows, MaxSize: maxSize, CollectionTtl: v.TTL.Nanoseconds(), Schema: v.Schema,
		OutputSegmentCountHint: EstimateResultSegmentCount(size, float64(preferredSize)),
	}, nil
}

func (v *RewriteInputs) planSingletons(typ datapb.CompactionType) ([]*datapb.CompactionTask, error) {
	plans := make([]*datapb.CompactionTask, 0, len(v.Segments))
	for _, input := range v.Segments {
		if input.NumOfRows < 0 || input.InsertBytes < 0 || v.Schema == nil || !v.Group.Equal(input.Label) {
			return nil, merr.WrapErrDataIntegrityMsg("invalid %s plan input %d", typ, input.ID)
		}
		plans = append(plans, &datapb.CompactionTask{
			Type: typ, CollectionID: v.Group.CollectionID, PartitionID: v.Group.PartitionID, Channel: v.Group.Channel,
			InputSegments: []int64{input.ID}, TotalRows: input.NumOfRows, MaxSize: v.ExpectedSize,
			CollectionTtl: v.TTL.Nanoseconds(), Schema: v.Schema,
			OutputSegmentCountHint: EstimateResultSegmentCount(float64(input.InsertBytes), float64(v.ExpectedSize)),
		})
	}
	return plans, nil
}

// EstimateResultSegmentCount computes the original, unexpanded ID estimate.
// The caller supplies the input and preferred sizes used by its own planner.
func EstimateResultSegmentCount(totalSize, preferredSize float64) int64 {
	if totalSize <= 0 || preferredSize <= 0 {
		return 1
	}
	count := math.Ceil(totalSize / preferredSize)
	if count >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return max(int64(count), 1)
}
