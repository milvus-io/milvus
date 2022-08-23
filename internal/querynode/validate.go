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

package querynode

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// TODO: merge validate?
func validateOnHistoricalReplica(ctx context.Context, replica ReplicaInterface, collectionID UniqueID, partitionIDs []UniqueID, segmentIDs []UniqueID) ([]UniqueID, []UniqueID, error) {
	var err error
	var searchPartIDs []UniqueID

	// no partition id specified, get all partition ids in collection
	if len(partitionIDs) == 0 {
		searchPartIDs, err = replica.getPartitionIDs(collectionID)
		if err != nil {
			return searchPartIDs, segmentIDs, err
		}
	} else {
		for _, id := range partitionIDs {
			_, err = replica.getPartitionByID(id)
			if err != nil {
				return searchPartIDs, segmentIDs, err
			}
			searchPartIDs = append(searchPartIDs, id)
		}
	}

	log.Ctx(ctx).Debug("read target partitions", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", searchPartIDs))
	col, err2 := replica.getCollectionByID(collectionID)
	if err2 != nil {
		return searchPartIDs, segmentIDs, err2
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		return searchPartIDs, segmentIDs, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collectionID) + "target partitionIDs = " + fmt.Sprintln(searchPartIDs))
	}
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		return searchPartIDs, segmentIDs, nil
	}

	var newSegmentIDs []UniqueID
	if len(segmentIDs) == 0 {
		for _, partID := range searchPartIDs {
			segIDs, err2 := replica.getSegmentIDs(partID, segmentTypeSealed)
			if err2 != nil {
				return searchPartIDs, newSegmentIDs, err
			}
			newSegmentIDs = append(segmentIDs, segIDs...)
		}
	} else {
		newSegmentIDs = segmentIDs
		for _, segmentID := range newSegmentIDs {
			var segment *Segment
			if segment, err = replica.getSegmentByID(segmentID, segmentTypeSealed); err != nil {
				return searchPartIDs, newSegmentIDs, err
			}
			if !inList(searchPartIDs, segment.partitionID) {
				err = fmt.Errorf("segment %d belongs to partition %d, which is not in %v", segmentID, segment.partitionID, searchPartIDs)
				return searchPartIDs, newSegmentIDs, err
			}
		}
	}
	return searchPartIDs, newSegmentIDs, nil
}

func validateOnStreamReplica(ctx context.Context, replica ReplicaInterface, collectionID UniqueID, partitionIDs []UniqueID, vChannel Channel) ([]UniqueID, []UniqueID, error) {
	var err error
	var searchPartIDs []UniqueID
	var segmentIDs []UniqueID

	// no partition id specified, get all partition ids in collection
	if len(partitionIDs) == 0 {
		searchPartIDs, err = replica.getPartitionIDs(collectionID)
		if err != nil {
			return searchPartIDs, segmentIDs, err
		}
	} else {
		for _, id := range partitionIDs {
			_, err = replica.getPartitionByID(id)
			if err != nil {
				return searchPartIDs, segmentIDs, err
			}
			searchPartIDs = append(searchPartIDs, id)
		}
	}

	log.Ctx(ctx).Debug("read target partitions", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", searchPartIDs))
	col, err2 := replica.getCollectionByID(collectionID)
	if err2 != nil {
		return searchPartIDs, segmentIDs, err2
	}

	// all partitions have been released
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypePartition {
		return searchPartIDs, segmentIDs, errors.New("partitions have been released , collectionID = " +
			fmt.Sprintln(collectionID) + "target partitionIDs = " + fmt.Sprintln(searchPartIDs))
	}
	if len(searchPartIDs) == 0 && col.getLoadType() == loadTypeCollection {
		return searchPartIDs, segmentIDs, nil
	}

	segmentIDs, err = replica.getSegmentIDsByVChannel(searchPartIDs, vChannel, segmentTypeGrowing)
	log.Ctx(ctx).Debug("validateOnStreamReplica getSegmentIDsByVChannel",
		zap.Any("collectionID", collectionID),
		zap.Any("vChannel", vChannel),
		zap.Any("partitionIDs", searchPartIDs),
		zap.Any("segmentIDs", segmentIDs),
		zap.Error(err))
	return searchPartIDs, segmentIDs, nil
}
