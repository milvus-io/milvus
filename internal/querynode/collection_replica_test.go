package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//----------------------------------------------------------------------------------------------------- collection
func TestCollectionReplica_getCollectionNum(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	assert.Equal(t, node.replica.getCollectionNum(), 1)
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_addCollection(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_removeCollection(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	assert.Equal(t, node.replica.getCollectionNum(), 1)

	err := node.replica.removeCollection(0)
	assert.NoError(t, err)
	assert.Equal(t, node.replica.getCollectionNum(), 0)
	err = node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_getCollectionByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)
	targetCollection, err := node.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.ID(), collectionID)
	err = node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_hasCollection(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	hasCollection := node.replica.hasCollection(collectionID)
	assert.Equal(t, hasCollection, true)
	hasCollection = node.replica.hasCollection(UniqueID(1))
	assert.Equal(t, hasCollection, false)

	err := node.Stop()
	assert.NoError(t, err)
}

//----------------------------------------------------------------------------------------------------- partition
func TestCollectionReplica_getPartitionNum(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}
	for _, id := range partitionIDs {
		err := node.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
	}

	partitionNum := node.replica.getPartitionNum()
	assert.Equal(t, partitionNum, len(partitionIDs)+1)
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_addPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}
	for _, id := range partitionIDs {
		err := node.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
	}
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_removePartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}

	for _, id := range partitionIDs {
		err := node.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
		err = node.replica.removePartition(id)
		assert.NoError(t, err)
	}
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_getPartitionByTag(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	collectionMeta := genTestCollectionMeta(collectionID, false)

	for _, id := range collectionMeta.PartitionIDs {
		err := node.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
		assert.NotNil(t, partition)
	}
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_hasPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	collectionMeta := genTestCollectionMeta(collectionID, false)
	err := node.replica.addPartition(collectionID, collectionMeta.PartitionIDs[0])
	assert.NoError(t, err)
	hasPartition := node.replica.hasPartition(defaultPartitionID)
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(defaultPartitionID + 1)
	assert.Equal(t, hasPartition, false)
	err = node.Stop()
	assert.NoError(t, err)
}

//----------------------------------------------------------------------------------------------------- segment
func TestCollectionReplica_addSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, segTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_removeSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3

	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, segTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		err = node.replica.removeSegment(UniqueID(i))
		assert.NoError(t, err)
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_getSegmentByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3

	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, segTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_hasSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3

	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, segTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		hasSeg := node.replica.hasSegment(UniqueID(i))
		assert.Equal(t, hasSeg, true)
		hasSeg = node.replica.hasSegment(UniqueID(i + 100))
		assert.Equal(t, hasSeg, false)
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_freeAll(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	err := node.Stop()
	assert.NoError(t, err)
}

func TestReplaceGrowingSegmentBySealedSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	segmentID := UniqueID(520)
	initTestMeta(t, node, collectionID, segmentID)

	_, _, segIDs := node.replica.getSegmentsBySegmentType(segTypeGrowing)
	assert.Equal(t, len(segIDs), 1)

	collection, err := node.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	ns := newSegment(collection, segmentID, defaultPartitionID, collectionID, segTypeSealed)
	err = node.replica.replaceGrowingSegmentBySealedSegment(ns)
	assert.NoError(t, err)

	segmentNums := node.replica.getSegmentNum()
	assert.Equal(t, segmentNums, 1)

	segment, err := node.replica.getSegmentByID(segmentID)
	assert.NoError(t, err)

	assert.Equal(t, segment.getType(), segTypeSealed)

	_, _, segIDs = node.replica.getSegmentsBySegmentType(segTypeGrowing)
	assert.Equal(t, len(segIDs), 0)
	_, _, segIDs = node.replica.getSegmentsBySegmentType(segTypeSealed)
	assert.Equal(t, len(segIDs), 1)

	err = node.Stop()
	assert.NoError(t, err)
}
