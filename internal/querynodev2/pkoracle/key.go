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

package pkoracle

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type candidateKey struct {
	segmentID   int64
	partitionID int64
	typ         commonpb.SegmentState
}

// MayPkExist checks whether primary key could exists in this candidate.
func (k candidateKey) MayPkExist(lc *storage.LocationsCache) bool {
	// always return true to prevent miuse
	return true
}

func (k candidateKey) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	ret := make([]bool, 0)
	for i := 0; i < lc.Size(); i++ {
		ret = append(ret, true)
	}
	return ret
}

// ID implements Candidate.
func (k candidateKey) ID() int64 {
	return k.segmentID
}

// Partition implements Candidate.
func (k candidateKey) Partition() int64 {
	return k.partitionID
}

// Type implements Candidate.
func (k candidateKey) Type() commonpb.SegmentState {
	return k.typ
}

// NewCandidateKey creates a candidateKey and returns as Candidate.
func NewCandidateKey(id int64, partitionID int64, typ commonpb.SegmentState) Candidate {
	return candidateKey{
		segmentID:   id,
		partitionID: partitionID,
		typ:         typ,
	}
}
