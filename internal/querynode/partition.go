// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"
import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// Partition is a logical division of Collection and can be considered as an attribute of Segment.
type Partition struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentIDs   []UniqueID
}

// ID returns the identity of the partition.
func (p *Partition) ID() UniqueID {
	return p.partitionID
}

func (p *Partition) addSegmentID(segmentID UniqueID) {
	p.segmentIDs = append(p.segmentIDs, segmentID)
}

func (p *Partition) removeSegmentID(segmentID UniqueID) {
	tmpIDs := make([]UniqueID, 0)
	for _, id := range p.segmentIDs {
		if id != segmentID {
			tmpIDs = append(tmpIDs, id)
		}
	}
	p.segmentIDs = tmpIDs
}

func newPartition(collectionID UniqueID, partitionID UniqueID) *Partition {
	var newPartition = &Partition{
		collectionID: collectionID,
		partitionID:  partitionID,
	}

	log.Debug("create partition", zap.Int64("partitionID", partitionID))
	return newPartition
}
