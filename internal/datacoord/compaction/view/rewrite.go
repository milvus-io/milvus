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
	SortView              struct{ RewriteInputs }
	RewriteView           struct{ RewriteInputs }
	BumpSchemaVersionView struct{ RewriteInputs }
)

func (v *SortView) Plan() ([]*datapb.CompactionTask, error) {
	return v.planSingletons(datapb.CompactionType_SortCompaction)
}

func (v *RewriteView) Plan() ([]*datapb.CompactionTask, error) {
	return v.planSingletons(datapb.CompactionType_MixCompaction)
}

func (v *BumpSchemaVersionView) Plan() ([]*datapb.CompactionTask, error) {
	return v.planSingletons(datapb.CompactionType_BumpSchemaVersionCompaction)
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
