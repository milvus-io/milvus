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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
)

// Helper function to create virtual PK: (segmentID << 32) | offset
func createVirtualPK(segmentID int64, offset int64) int64 {
	return (segmentID << 32) | (offset & 0xFFFFFFFF)
}

func TestNewExternalSegmentCandidate(t *testing.T) {
	segmentID := int64(12345)
	partitionID := int64(100)
	segType := commonpb.SegmentState_Sealed

	candidate := NewExternalSegmentCandidate(segmentID, partitionID, segType)

	assert.Equal(t, segmentID, candidate.ID())
	assert.Equal(t, partitionID, candidate.Partition())
	assert.Equal(t, segType, candidate.Type())
	assert.Equal(t, segmentID&0xFFFFFFFF, candidate.truncatedSegmentID)
}

func TestExternalSegmentCandidate_MayPkExist(t *testing.T) {
	segmentID := int64(100)
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// Test with PK from this segment
	virtualPK := createVirtualPK(segmentID, 42)
	pk := storage.NewInt64PrimaryKey(virtualPK)
	lc := storage.NewLocationsCache(pk)
	assert.True(t, candidate.MayPkExist(lc))

	// Test with PK from different segment
	differentPK := createVirtualPK(segmentID+1, 42)
	pk2 := storage.NewInt64PrimaryKey(differentPK)
	lc2 := storage.NewLocationsCache(pk2)
	assert.False(t, candidate.MayPkExist(lc2))

	// Test with PK from segment 0
	zeroPK := createVirtualPK(0, 42)
	pk3 := storage.NewInt64PrimaryKey(zeroPK)
	lc3 := storage.NewLocationsCache(pk3)
	assert.False(t, candidate.MayPkExist(lc3))
}

func TestExternalSegmentCandidate_MayPkExist_LargeSegmentID(t *testing.T) {
	// Test with segment ID that exceeds 32 bits
	segmentID := int64(0x100000001) // 33-bit value, lower 32 bits = 1
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// Truncated segment ID should be 1
	assert.Equal(t, int64(1), candidate.truncatedSegmentID)

	// Virtual PK created with truncated segment ID should match
	virtualPK := createVirtualPK(1, 100)
	pk := storage.NewInt64PrimaryKey(virtualPK)
	lc := storage.NewLocationsCache(pk)
	assert.True(t, candidate.MayPkExist(lc))

	// Virtual PK created with different segment should not match
	differentPK := createVirtualPK(2, 100)
	pk2 := storage.NewInt64PrimaryKey(differentPK)
	lc2 := storage.NewLocationsCache(pk2)
	assert.False(t, candidate.MayPkExist(lc2))
}

func TestExternalSegmentCandidate_MayPkExist_VarCharPK(t *testing.T) {
	segmentID := int64(100)
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// VarChar PKs should always return false for external collections
	pk := storage.NewVarCharPrimaryKey("test-pk")
	lc := storage.NewLocationsCache(pk)
	assert.False(t, candidate.MayPkExist(lc))
}

func TestExternalSegmentCandidate_BatchPkExist(t *testing.T) {
	segmentID := int64(100)
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// Create a batch of PKs
	pks := []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(createVirtualPK(segmentID, 0)),   // Match
		storage.NewInt64PrimaryKey(createVirtualPK(segmentID, 10)),  // Match
		storage.NewInt64PrimaryKey(createVirtualPK(segmentID+1, 0)), // No match
		storage.NewInt64PrimaryKey(createVirtualPK(segmentID, 100)), // Match
		storage.NewInt64PrimaryKey(createVirtualPK(0, 0)),           // No match
	}

	lc := storage.NewBatchLocationsCache(pks)
	results := candidate.BatchPkExist(lc)

	assert.Equal(t, len(pks), len(results))
	assert.True(t, results[0])  // Match
	assert.True(t, results[1])  // Match
	assert.False(t, results[2]) // No match (different segment)
	assert.True(t, results[3])  // Match
	assert.False(t, results[4]) // No match (segment 0)
}

func TestExternalSegmentCandidate_BatchPkExist_AllMatch(t *testing.T) {
	segmentID := int64(50)
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// All PKs from this segment
	pks := make([]storage.PrimaryKey, 100)
	for i := 0; i < 100; i++ {
		pks[i] = storage.NewInt64PrimaryKey(createVirtualPK(segmentID, int64(i)))
	}

	lc := storage.NewBatchLocationsCache(pks)
	results := candidate.BatchPkExist(lc)

	for i, result := range results {
		assert.True(t, result, "Expected PK at index %d to match", i)
	}
}

func TestExternalSegmentCandidate_BatchPkExist_NoneMatch(t *testing.T) {
	segmentID := int64(50)
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// All PKs from different segment
	pks := make([]storage.PrimaryKey, 100)
	for i := 0; i < 100; i++ {
		pks[i] = storage.NewInt64PrimaryKey(createVirtualPK(segmentID+1, int64(i)))
	}

	lc := storage.NewBatchLocationsCache(pks)
	results := candidate.BatchPkExist(lc)

	for i, result := range results {
		assert.False(t, result, "Expected PK at index %d to not match", i)
	}
}

func TestExternalSegmentCandidate_BatchPkExist_MixedTypes(t *testing.T) {
	segmentID := int64(100)
	partitionID := int64(1)
	candidate := NewExternalSegmentCandidate(segmentID, partitionID, commonpb.SegmentState_Sealed)

	// Mix of Int64 and VarChar PKs
	pks := []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(createVirtualPK(segmentID, 0)), // Match
		storage.NewVarCharPrimaryKey("test1"),                     // No match (wrong type)
		storage.NewInt64PrimaryKey(createVirtualPK(segmentID, 1)), // Match
		storage.NewVarCharPrimaryKey("test2"),                     // No match (wrong type)
	}

	lc := storage.NewBatchLocationsCache(pks)
	results := candidate.BatchPkExist(lc)

	assert.True(t, results[0])  // Int64 match
	assert.False(t, results[1]) // VarChar no match
	assert.True(t, results[2])  // Int64 match
	assert.False(t, results[3]) // VarChar no match
}

func TestExternalSegmentCandidate_CandidateInterface(t *testing.T) {
	// Verify that ExternalSegmentCandidate implements Candidate interface
	var _ Candidate = (*ExternalSegmentCandidate)(nil)

	segmentID := int64(123)
	partitionID := int64(456)
	segType := commonpb.SegmentState_Growing

	candidate := NewExternalSegmentCandidate(segmentID, partitionID, segType)

	// Test interface methods
	assert.Equal(t, segmentID, candidate.ID())
	assert.Equal(t, partitionID, candidate.Partition())
	assert.Equal(t, segType, candidate.Type())
}

func TestExternalSegmentCandidate_EdgeCases(t *testing.T) {
	t.Run("ZeroSegmentID", func(t *testing.T) {
		candidate := NewExternalSegmentCandidate(0, 0, commonpb.SegmentState_Sealed)

		// PK from segment 0 should match
		pk := storage.NewInt64PrimaryKey(createVirtualPK(0, 42))
		lc := storage.NewLocationsCache(pk)
		assert.True(t, candidate.MayPkExist(lc))

		// PK from segment 1 should not match
		pk2 := storage.NewInt64PrimaryKey(createVirtualPK(1, 42))
		lc2 := storage.NewLocationsCache(pk2)
		assert.False(t, candidate.MayPkExist(lc2))
	})

	t.Run("MaxOffset", func(t *testing.T) {
		segmentID := int64(100)
		candidate := NewExternalSegmentCandidate(segmentID, 0, commonpb.SegmentState_Sealed)

		// Max 32-bit offset
		maxOffset := int64(0xFFFFFFFF)
		pk := storage.NewInt64PrimaryKey(createVirtualPK(segmentID, maxOffset))
		lc := storage.NewLocationsCache(pk)
		assert.True(t, candidate.MayPkExist(lc))
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		candidate := NewExternalSegmentCandidate(100, 0, commonpb.SegmentState_Sealed)

		pks := []storage.PrimaryKey{}
		lc := storage.NewBatchLocationsCache(pks)
		results := candidate.BatchPkExist(lc)
		assert.Empty(t, results)
	})
}
