package querynodeimp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//----------------------------------------------------------------------------------------------------- collection
func TestCollectionReplica_getCollectionNum(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, "collection0", 0, 0)
	assert.Equal(t, node.replica.getCollectionNum(), 1)
	node.Close()
}

func TestCollectionReplica_addCollection(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, "collection0", 0, 0)
	node.Close()
}

func TestCollectionReplica_removeCollection(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, "collection0", 0, 0)
	assert.Equal(t, node.replica.getCollectionNum(), 1)

	err := node.replica.removeCollection(0)
	assert.NoError(t, err)
	assert.Equal(t, node.replica.getCollectionNum(), 0)
	node.Close()
}

func TestCollectionReplica_getCollectionByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)
	targetCollection, err := node.replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.Name(), collectionName)
	assert.Equal(t, targetCollection.ID(), collectionID)
	node.Close()
}

func TestCollectionReplica_getCollectionByName(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	targetCollection, err := node.replica.getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.Name(), collectionName)
	assert.Equal(t, targetCollection.ID(), collectionID)

	node.Close()
}

func TestCollectionReplica_hasCollection(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	hasCollection := node.replica.hasCollection(collectionID)
	assert.Equal(t, hasCollection, true)
	hasCollection = node.replica.hasCollection(UniqueID(1))
	assert.Equal(t, hasCollection, false)

	node.Close()
}

//----------------------------------------------------------------------------------------------------- partition
func TestCollectionReplica_getPartitionNum(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	partitionTags := []string{"a", "b", "c"}
	for _, tag := range partitionTags {
		err := node.replica.addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, tag)
	}

	partitionNum, err := node.replica.getPartitionNum(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, len(partitionTags)+1) // _default
	node.Close()
}

func TestCollectionReplica_addPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	partitionTags := []string{"a", "b", "c"}
	for _, tag := range partitionTags {
		err := node.replica.addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, tag)
	}
	node.Close()
}

func TestCollectionReplica_removePartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	partitionTags := []string{"a", "b", "c"}

	for _, tag := range partitionTags {
		err := node.replica.addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, tag)
		err = node.replica.removePartition(collectionID, tag)
		assert.NoError(t, err)
	}
	node.Close()
}

func TestCollectionReplica_addPartitionsByCollectionMeta(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	collectionMeta.PartitionTags = []string{"p0", "p1", "p2"}

	err := node.replica.addPartitionsByCollectionMeta(collectionMeta)
	assert.NoError(t, err)
	partitionNum, err := node.replica.getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, len(collectionMeta.PartitionTags)+1)
	hasPartition := node.replica.hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, true)

	node.Close()
}

func TestCollectionReplica_removePartitionsByCollectionMeta(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	collectionMeta.PartitionTags = []string{"p0"}

	err := node.replica.addPartitionsByCollectionMeta(collectionMeta)
	assert.NoError(t, err)
	partitionNum, err := node.replica.getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, len(collectionMeta.PartitionTags)+1)

	hasPartition := node.replica.hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, false)
	hasPartition = node.replica.hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, false)

	node.Close()
}

func TestCollectionReplica_getPartitionByTag(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)

	for _, tag := range collectionMeta.PartitionTags {
		err := node.replica.addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := node.replica.getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, tag)
		assert.NotNil(t, partition)
	}
	node.Close()
}

func TestCollectionReplica_hasPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)
	err := node.replica.addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)
	hasPartition := node.replica.hasPartition(collectionID, "default")
	assert.Equal(t, hasPartition, true)
	hasPartition = node.replica.hasPartition(collectionID, "default1")
	assert.Equal(t, hasPartition, false)
	node.Close()
}

//----------------------------------------------------------------------------------------------------- segment
func TestCollectionReplica_addSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	const segmentNum = 3
	tag := "default"
	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), tag, collectionID)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}

	node.Close()
}

func TestCollectionReplica_removeSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	const segmentNum = 3
	tag := "default"

	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), tag, collectionID)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		err = node.replica.removeSegment(UniqueID(i))
		assert.NoError(t, err)
	}

	node.Close()
}

func TestCollectionReplica_getSegmentByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	const segmentNum = 3
	tag := "default"

	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), tag, collectionID)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}

	node.Close()
}

func TestCollectionReplica_hasSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	const segmentNum = 3
	tag := "default"

	for i := 0; i < segmentNum; i++ {
		err := node.replica.addSegment(UniqueID(i), tag, collectionID)
		assert.NoError(t, err)
		targetSeg, err := node.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		hasSeg := node.replica.hasSegment(UniqueID(i))
		assert.Equal(t, hasSeg, true)
		hasSeg = node.replica.hasSegment(UniqueID(i + 100))
		assert.Equal(t, hasSeg, false)
	}

	node.Close()
}

func TestCollectionReplica_freeAll(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	node.Close()

}
