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

package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
)

func newCollectionSegmentReplica(ms types.MasterService, collectionID UniqueID) *CollectionSegmentReplica {
	metaService := newMetaService(ms, collectionID)
	segments := make(map[UniqueID]*Segment)

	replica := &CollectionSegmentReplica{
		segments:       segments,
		collectionID:   collectionID,
		metaService:    metaService,
		startPositions: make(map[UniqueID][]*internalpb.MsgPosition),
		endPositions:   make(map[UniqueID][]*internalpb.MsgPosition),
	}
	return replica
}

func TestReplica_Collection(t *testing.T) {
	// collID := UniqueID(100)
}

func TestReplica_Segment(t *testing.T) {
	mockMaster := &MasterServiceFactory{}
	collID := UniqueID(1)

	t.Run("Test segment", func(t *testing.T) {
		replica := newReplica(mockMaster, collID)
		assert.False(t, replica.hasSegment(0))

		err := replica.addSegment(0, 1, 2, "insert-01")
		assert.NoError(t, err)
		assert.True(t, replica.hasSegment(0))

		seg, err := replica.getSegmentByID(0)
		assert.NoError(t, err)
		assert.NotNil(t, seg)
		assert.Equal(t, UniqueID(1), seg.collectionID)
		assert.Equal(t, UniqueID(2), seg.partitionID)

		assert.Equal(t, int64(0), seg.numRows)

		err = replica.updateStatistics(0, 100)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), seg.numRows)

		update, err := replica.getSegmentStatisticsUpdates(0)
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(0), update.SegmentID)
		assert.Equal(t, int64(100), update.NumRows)

		update, err = replica.getSegmentStatisticsUpdates(0)
		assert.NoError(t, err)

		err = replica.removeSegment(0)
		assert.NoError(t, err)
		assert.False(t, replica.hasSegment(0))
	})

	t.Run("Test errors", func(t *testing.T) {
		replica := newReplica(mockMaster, collID)
		require.False(t, replica.hasSegment(0))

		seg, err := replica.getSegmentByID(0)
		assert.Error(t, err)
		assert.Nil(t, seg)

		err = replica.updateStatistics(0, 0)
		assert.Error(t, err)

		update, err := replica.getSegmentStatisticsUpdates(0)
		assert.Error(t, err)
		assert.Nil(t, update)
	})
}
