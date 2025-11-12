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

package segments

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func validate(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64, segmentFilter ...SegmentFilter) ([]Segment, error) {
	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionIDNotFound(collectionID)
	}

	// validate segment
	segments := make([]Segment, 0, len(segmentIDs))
	var err error
	defer func() {
		if err != nil {
			manager.Segment.Unpin(segments)
		}
	}()
	if len(segmentIDs) == 0 {
		// legacy logic
		segmentFilter = append(segmentFilter, SegmentFilterFunc(func(s Segment) bool {
			return s.Collection() == collectionID
		}))

		segments, err = manager.Segment.GetAndPinBy(segmentFilter...)
		if err != nil {
			return nil, err
		}
	} else {
		segments, err = manager.Segment.GetAndPin(segmentIDs, segmentFilter...)
		if err != nil {
			return nil, err
		}
	}
	return segments, nil
}

func validateOnHistorical(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64, plan *planpb.PlanNode) ([]Segment, error) {
	filters := []SegmentFilter{WithType(SegmentTypeSealed)}
	var filteredCount int
	sparseFilterEnabled := paramtable.Get().QueryNodeCfg.EnableSparseFilterInQuery.GetAsBool() && plan != nil
	if sparseFilterEnabled {
		filters = append(filters, WithSparseFilter(plan, &filteredCount))
	}

	segments, err := validate(ctx, manager, collectionID, partitionIDs, segmentIDs, filters...)
	if err != nil {
		return nil, err
	}

	log.Ctx(ctx).Debug("sparse filter filtered out segments in historical",
		zap.Int64("collectionID", collectionID),
		zap.Int("filtered", filteredCount),
		zap.Int("remaining", len(segments)))

	return segments, nil
}

func validateOnStream(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64, plan *planpb.PlanNode) ([]Segment, error) {
	filters := []SegmentFilter{WithType(SegmentTypeGrowing)}
	var filteredCount int
	sparseFilterEnabled := paramtable.Get().QueryNodeCfg.EnableSparseFilterInQuery.GetAsBool() && plan != nil
	if sparseFilterEnabled {
		filters = append(filters, WithSparseFilter(plan, &filteredCount))
	}

	segments, err := validate(ctx, manager, collectionID, partitionIDs, segmentIDs, filters...)
	if err != nil {
		return nil, err
	}

	log.Ctx(ctx).Debug("sparse filter filtered out segments in streaming",
		zap.Int64("collectionID", collectionID),
		zap.Int("filtered", filteredCount),
		zap.Int("remaining", len(segments)))

	return segments, nil
}
