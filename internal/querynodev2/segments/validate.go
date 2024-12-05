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

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func validate(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64, segmentFilter SegmentFilter) ([]Segment, error) {
	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	log.Ctx(ctx).Debug("read target partitions", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))

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
		segments, err = manager.Segment.GetAndPinBy(segmentFilter, SegmentFilterFunc(func(s Segment) bool {
			return s.Collection() == collectionID
		}))
		if err != nil {
			return nil, err
		}
	} else {
		segments, err = manager.Segment.GetAndPin(segmentIDs, segmentFilter)
		if err != nil {
			return nil, err
		}
	}
	return segments, nil
}

func validateOnHistorical(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64) ([]Segment, error) {
	return validate(ctx, manager, collectionID, partitionIDs, segmentIDs, WithType(SegmentTypeSealed))
}

func validateOnStream(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64) ([]Segment, error) {
	return validate(ctx, manager, collectionID, partitionIDs, segmentIDs, WithType(SegmentTypeGrowing))
}
