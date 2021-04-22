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

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplica_Collection(t *testing.T) {
	Factory := &MetaFactory{}
	collID := UniqueID(100)
	collMetaMock := Factory.CollectionMetaFactory(collID, "test-coll-name-0")

	t.Run("get_collection_num", func(t *testing.T) {
		replica := newReplica()
		assert.Zero(t, replica.getCollectionNum())

		replica = new(CollectionSegmentReplica)
		assert.Zero(t, replica.getCollectionNum())

		replica = &CollectionSegmentReplica{
			collections: map[UniqueID]*Collection{
				0: {id: 0},
				1: {id: 1},
				2: {id: 2},
			},
		}
		assert.Equal(t, 3, replica.getCollectionNum())
	})

	t.Run("add_collection", func(t *testing.T) {
		replica := newReplica()
		require.Zero(t, replica.getCollectionNum())

		err := replica.addCollection(collID, nil)
		assert.Error(t, err)
		assert.Zero(t, replica.getCollectionNum())

		err = replica.addCollection(collID, collMetaMock.Schema)
		assert.NoError(t, err)
		assert.Equal(t, 1, replica.getCollectionNum())
		assert.True(t, replica.hasCollection(collID))
		coll, err := replica.getCollectionByID(collID)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		assert.Equal(t, collID, coll.GetID())
		assert.Equal(t, collMetaMock.Schema.GetName(), coll.GetName())
		assert.Equal(t, collMetaMock.Schema, coll.GetSchema())

		sameID := collID
		otherSchema := Factory.CollectionMetaFactory(sameID, "test-coll-name-1").GetSchema()
		err = replica.addCollection(sameID, otherSchema)
		assert.Error(t, err)

	})

	t.Run("remove_collection", func(t *testing.T) {
		replica := newReplica()
		require.False(t, replica.hasCollection(collID))
		require.Zero(t, replica.getCollectionNum())

		err := replica.removeCollection(collID)
		assert.NoError(t, err)

		err = replica.addCollection(collID, collMetaMock.Schema)
		require.NoError(t, err)
		require.True(t, replica.hasCollection(collID))
		require.Equal(t, 1, replica.getCollectionNum())

		err = replica.removeCollection(collID)
		assert.NoError(t, err)
		assert.False(t, replica.hasCollection(collID))
		assert.Zero(t, replica.getCollectionNum())
		err = replica.removeCollection(collID)
		assert.NoError(t, err)
	})

	t.Run("get_collection_by_id", func(t *testing.T) {
		replica := newReplica()
		require.False(t, replica.hasCollection(collID))

		coll, err := replica.getCollectionByID(collID)
		assert.Error(t, err)
		assert.Nil(t, coll)

		err = replica.addCollection(collID, collMetaMock.Schema)
		require.NoError(t, err)
		require.True(t, replica.hasCollection(collID))
		require.Equal(t, 1, replica.getCollectionNum())

		coll, err = replica.getCollectionByID(collID)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		assert.Equal(t, collID, coll.GetID())
		assert.Equal(t, collMetaMock.Schema.GetName(), coll.GetName())
		assert.Equal(t, collMetaMock.Schema, coll.GetSchema())
	})
}

func TestReplica_Segment(t *testing.T) {
	t.Run("Test segment", func(t *testing.T) {
		replica := newReplica()
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
		assert.NotNil(t, update.StartPosition)
		assert.Nil(t, update.EndPosition)

		err = replica.setIsFlushed(0)
		assert.NoError(t, err)
		err = replica.setStartPosition(0, &internalpb.MsgPosition{})
		assert.NoError(t, err)
		err = replica.setEndPosition(0, &internalpb.MsgPosition{})
		assert.NoError(t, err)
		update, err = replica.getSegmentStatisticsUpdates(0)
		assert.NoError(t, err)
		assert.Nil(t, update.StartPosition)
		assert.NotNil(t, update.EndPosition)

		err = replica.removeSegment(0)
		assert.NoError(t, err)
		assert.False(t, replica.hasSegment(0))
	})

	t.Run("Test errors", func(t *testing.T) {
		replica := newReplica()
		require.False(t, replica.hasSegment(0))

		seg, err := replica.getSegmentByID(0)
		assert.Error(t, err)
		assert.Nil(t, seg)

		err = replica.setIsFlushed(0)
		assert.Error(t, err)
		err = replica.setStartPosition(0, &internalpb.MsgPosition{})
		assert.Error(t, err)
		err = replica.setStartPosition(0, nil)
		assert.Error(t, err)
		err = replica.setEndPosition(0, &internalpb.MsgPosition{})
		assert.Error(t, err)
		err = replica.setEndPosition(0, nil)
		assert.Error(t, err)

		err = replica.updateStatistics(0, 0)
		assert.Error(t, err)

		update, err := replica.getSegmentStatisticsUpdates(0)
		assert.Error(t, err)
		assert.Nil(t, update)
	})
}
