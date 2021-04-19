package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initTestReplicaMeta(t *testing.T, replica collectionReplica, collectionName string, collectionID UniqueID, segmentID UniqueID) {
	// GOOSE TODO remove
	Factory := &MetaFactory{}
	collectionMeta := Factory.CollectionMetaFactory(collectionID, collectionName)

	var err = replica.addCollection(collectionMeta.ID, collectionMeta.Schema)
	require.NoError(t, err)

	collection, err := replica.getCollectionByName(collectionName)
	require.NoError(t, err)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, replica.getCollectionNum(), 1)

}

func TestReplica_Collection(t *testing.T) {
	Factory := &MetaFactory{}
	collMetaMock := Factory.CollectionMetaFactory(0, "collection0")

	t.Run("Test add collection", func(t *testing.T) {

		replica := newReplica()
		assert.False(t, replica.hasCollection(0))
		num := replica.getCollectionNum()
		assert.Equal(t, 0, num)

		err := replica.addCollection(0, collMetaMock.GetSchema())
		assert.NoError(t, err)

		assert.True(t, replica.hasCollection(0))
		num = replica.getCollectionNum()
		assert.Equal(t, 1, num)

		coll, err := replica.getCollectionByID(0)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		assert.Equal(t, UniqueID(0), coll.ID())
		assert.Equal(t, "collection0", coll.Name())
		assert.Equal(t, collMetaMock.GetSchema(), coll.Schema())

		coll, err = replica.getCollectionByName("collection0")
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		assert.Equal(t, UniqueID(0), coll.ID())
		assert.Equal(t, "collection0", coll.Name())
		assert.Equal(t, collMetaMock.GetSchema(), coll.Schema())

		collID, err := replica.getCollectionIDByName("collection0")
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(0), collID)

	})

	t.Run("Test remove collection", func(t *testing.T) {
		replica := newReplica()
		err := replica.addCollection(0, collMetaMock.GetSchema())
		require.NoError(t, err)

		numsBefore := replica.getCollectionNum()
		coll, err := replica.getCollectionByID(0)
		require.NotNil(t, coll)
		require.NoError(t, err)

		err = replica.removeCollection(0)
		assert.NoError(t, err)
		numsAfter := replica.getCollectionNum()
		assert.Equal(t, 1, numsBefore-numsAfter)

		coll, err = replica.getCollectionByID(0)
		assert.Nil(t, coll)
		assert.Error(t, err)
		err = replica.removeCollection(999999999)
		assert.Error(t, err)
	})

	t.Run("Test errors", func(t *testing.T) {
		replica := newReplica()
		require.False(t, replica.hasCollection(0))
		require.Equal(t, 0, replica.getCollectionNum())

		coll, err := replica.getCollectionByName("Name-not-exist")
		assert.Error(t, err)
		assert.Nil(t, coll)

		coll, err = replica.getCollectionByID(0)
		assert.Error(t, err)
		assert.Nil(t, coll)

		collID, err := replica.getCollectionIDByName("Name-not-exist")
		assert.Error(t, err)
		assert.Zero(t, collID)

		err = replica.removeCollection(0)
		assert.Error(t, err)
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
