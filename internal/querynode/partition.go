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

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// Partition is a logical division of Collection and can be considered as an attribute of Segment.
type Partition struct {
	collectionID      UniqueID
	partitionID       UniqueID
	growingSegmentIDs []UniqueID
	sealedSegmentIDs  []UniqueID
}

// ID returns the identity of the partition.
func (p *Partition) ID() UniqueID {
	return p.partitionID
}

// getSegmentIDs returns segment ids by DataScope
func (p *Partition) getSegmentIDs(segType segmentType) ([]UniqueID, error) {
	switch segType {
	case segmentTypeGrowing:
		dst := make([]UniqueID, len(p.growingSegmentIDs))
		copy(dst, p.growingSegmentIDs)
		return dst, nil
	case segmentTypeSealed:
		dst := make([]UniqueID, len(p.sealedSegmentIDs))
		copy(dst, p.sealedSegmentIDs)
		return dst, nil
	default:
		return nil, fmt.Errorf("unexpected segmentType %s", segType.String())
	}
}

// addSegmentID add segmentID to segmentIDs
func (p *Partition) addSegmentID(segmentID UniqueID, segType segmentType) {
	switch segType {
	case segmentTypeGrowing:
		p.growingSegmentIDs = append(p.growingSegmentIDs, segmentID)
	case segmentTypeSealed:
		p.sealedSegmentIDs = append(p.sealedSegmentIDs, segmentID)
	default:
		return
	}
	log.Info("add a segment to replica",
		zap.Int64("collectionID", p.collectionID),
		zap.Int64("partitionID", p.partitionID),
		zap.Int64("segmentID", segmentID),
		zap.String("segmentType", segType.String()))
}

// removeSegmentID removes segmentID from segmentIDs
func (p *Partition) removeSegmentID(segmentID UniqueID, segType segmentType) {
	deleteFunc := func(segmentIDs []UniqueID) []UniqueID {
		tmpIDs := make([]UniqueID, 0)
		for _, id := range segmentIDs {
			if id != segmentID {
				tmpIDs = append(tmpIDs, id)
			}
		}
		return tmpIDs
	}
	switch segType {
	case segmentTypeGrowing:
		p.growingSegmentIDs = deleteFunc(p.growingSegmentIDs)
	case segmentTypeSealed:
		p.sealedSegmentIDs = deleteFunc(p.sealedSegmentIDs)
	default:
		return
	}
	log.Info("remove a segment from replica", zap.Int64("collectionID", p.collectionID), zap.Int64("partitionID", p.partitionID), zap.Int64("segmentID", segmentID))
}

// newPartition returns a new Partition
func newPartition(collectionID UniqueID, partitionID UniqueID) *Partition {
	var newPartition = &Partition{
		collectionID: collectionID,
		partitionID:  partitionID,
	}

	log.Info("create partition", zap.Int64("partitionID", partitionID))
	return newPartition
}
