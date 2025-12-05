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

var _ Candidate = (*ExternalSegmentCandidate)(nil)

// ExternalSegmentCandidate is a Candidate implementation for external collections.
// External collections use virtual PKs in the format: (segmentID << 32) | offset.
// Instead of using bloom filters, this candidate uses segment-based PK matching:
// a PK belongs to this segment if (pk >> 32) == (segmentID & 0xFFFFFFFF).
// Note: Only the lower 32 bits of segmentID are preserved in the virtual PK.
type ExternalSegmentCandidate struct {
	segmentID          int64
	truncatedSegmentID int64 // Lower 32 bits of segmentID for comparison with virtual PK
	partitionID        int64
	segType            commonpb.SegmentState
}

// NewExternalSegmentCandidate creates a new ExternalSegmentCandidate.
func NewExternalSegmentCandidate(segmentID int64, partitionID int64, segType commonpb.SegmentState) *ExternalSegmentCandidate {
	return &ExternalSegmentCandidate{
		segmentID:          segmentID,
		truncatedSegmentID: segmentID & 0xFFFFFFFF, // Keep only lower 32 bits
		partitionID:        partitionID,
		segType:            segType,
	}
}

// MayPkExist checks if the primary key could exist in this segment.
// For external collections, virtual PK format is (segmentID << 32) | offset,
// so we determine segment membership by extracting segmentID from the PK.
func (c *ExternalSegmentCandidate) MayPkExist(lc *storage.LocationsCache) bool {
	pk := lc.GetPk()
	if int64Pk, ok := pk.(*storage.Int64PrimaryKey); ok {
		extractedSegmentID := int64Pk.Value >> 32
		return extractedSegmentID == c.truncatedSegmentID
	}
	// External collections only support int64 virtual PK
	return false
}

// BatchPkExist checks if multiple primary keys could exist in this segment.
func (c *ExternalSegmentCandidate) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	pks := lc.PKs()
	hits := make([]bool, len(pks))

	for i, pk := range pks {
		if int64Pk, ok := pk.(*storage.Int64PrimaryKey); ok {
			extractedSegmentID := int64Pk.Value >> 32
			hits[i] = extractedSegmentID == c.truncatedSegmentID
		}
	}

	return hits
}

// ID returns the segment ID.
func (c *ExternalSegmentCandidate) ID() int64 {
	return c.segmentID
}

// Partition returns the partition ID.
func (c *ExternalSegmentCandidate) Partition() int64 {
	return c.partitionID
}

// Type returns the segment type.
func (c *ExternalSegmentCandidate) Type() commonpb.SegmentState {
	return c.segType
}
