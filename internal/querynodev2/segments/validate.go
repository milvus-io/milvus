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
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func validate(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64, segmentFilter SegmentFilter) ([]int64, []int64, error) {
	var searchPartIDs []int64
	var newSegmentIDs []int64

	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	//validate partition
	// no partition id specified, get all partition ids in collection
	if len(partitionIDs) == 0 {
		searchPartIDs = collection.GetPartitions()
	} else {
		collection.ExistPartition()
		if collection.ExistPartition(partitionIDs...) {
			searchPartIDs = partitionIDs
		}
	}

	log.Ctx(ctx).Debug("read target partitions", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", searchPartIDs))

	// all partitions have been released
	if len(searchPartIDs) == 0 && collection.GetLoadType() == querypb.LoadType_LoadPartition {
		return searchPartIDs, newSegmentIDs, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collectionID) + "target partitionIDs = " + fmt.Sprintln(searchPartIDs))
	}

	if len(searchPartIDs) == 0 && collection.GetLoadType() == querypb.LoadType_LoadCollection {
		return searchPartIDs, newSegmentIDs, nil
	}

	//validate segment
	if len(segmentIDs) == 0 {
		for _, partID := range searchPartIDs {
			segments := manager.Segment.GetBy(WithPartition(partID), segmentFilter)
			for _, seg := range segments {
				newSegmentIDs = append(segmentIDs, seg.ID())
			}
		}
	} else {
		newSegmentIDs = segmentIDs
		for _, segmentID := range newSegmentIDs {
			segments := manager.Segment.GetBy(WithID(segmentID), segmentFilter)
			if len(segments) != 1 {
				continue
			}
			segment := segments[0]
			if !funcutil.SliceContain(searchPartIDs, segment.Partition()) {
				err := fmt.Errorf("segment %d belongs to partition %d, which is not in %v", segmentID, segment.Partition(), searchPartIDs)
				return searchPartIDs, newSegmentIDs, err
			}
		}
	}
	return searchPartIDs, newSegmentIDs, nil
}

func validateOnHistorical(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64) ([]int64, []int64, error) {
	return validate(ctx, manager, collectionID, partitionIDs, segmentIDs, WithType(SegmentTypeSealed))
}

func validateOnStream(ctx context.Context, manager *Manager, collectionID int64, partitionIDs []int64, segmentIDs []int64) ([]int64, []int64, error) {
	return validate(ctx, manager, collectionID, partitionIDs, segmentIDs, WithType(SegmentTypeGrowing))
}
