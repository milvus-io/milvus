package datanode

import (
	"testing"

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
		assert.True(t, update.IsNewSegment)

		update, err = replica.getSegmentStatisticsUpdates(0)
		assert.NoError(t, err)
		assert.False(t, update.IsNewSegment)
		assert.NotNil(t, update.StartPosition)
		assert.Equal(t, UniqueID(0), update.SegmentID)
		assert.Equal(t, int64(100), update.NumRows)
		assert.Zero(t, update.StartPosition.Timestamp)
		assert.Zero(t, update.StartPosition.MsgID)
	})

	t.Run("Test errors", func(t *testing.T) {
		replica := newReplica()
		require.False(t, replica.hasSegment(0))

		seg, err := replica.getSegmentByID(0)
		assert.Error(t, err)
		assert.Nil(t, seg)

		err = replica.removeSegment(0)
		assert.Error(t, err)

		err = replica.updateStatistics(0, 0)
		assert.Error(t, err)

		update, err := replica.getSegmentStatisticsUpdates(0)
		assert.Error(t, err)
		assert.Nil(t, update)
	})
}
