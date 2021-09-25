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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//----------------------------------------------------------------------------------------------------- collection
func TestCollectionReplica_getCollectionNum(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	assert.Equal(t, node.historical.replica.getCollectionNum(), 1)
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
	assert.Equal(t, node.historical.replica.getCollectionNum(), 1)

	err := node.historical.replica.removeCollection(0)
	assert.NoError(t, err)
	assert.Equal(t, node.historical.replica.getCollectionNum(), 0)
	err = node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_getCollectionByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)
	targetCollection, err := node.historical.replica.getCollectionByID(collectionID)
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

	hasCollection := node.historical.replica.hasCollection(collectionID)
	assert.Equal(t, hasCollection, true)
	hasCollection = node.historical.replica.hasCollection(UniqueID(1))
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
		err := node.historical.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.replica.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
	}

	partitionNum := node.historical.replica.getPartitionNum()
	assert.Equal(t, partitionNum, len(partitionIDs))
	err := node.Stop()
	assert.NoError(t, err)
}

func TestCollectionReplica_addPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}
	for _, id := range partitionIDs {
		err := node.historical.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.replica.getPartitionByID(id)
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
		err := node.historical.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.replica.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
		err = node.historical.replica.removePartition(id)
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
		err := node.historical.replica.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.replica.getPartitionByID(id)
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
	err := node.historical.replica.addPartition(collectionID, collectionMeta.PartitionIDs[0])
	assert.NoError(t, err)
	hasPartition := node.historical.replica.hasPartition(defaultPartitionID)
	assert.Equal(t, hasPartition, true)
	hasPartition = node.historical.replica.hasPartition(defaultPartitionID + 1)
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
		err := node.historical.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
		assert.NoError(t, err)
		targetSeg, err := node.historical.replica.getSegmentByID(UniqueID(i))
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
		err := node.historical.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
		assert.NoError(t, err)
		targetSeg, err := node.historical.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		err = node.historical.replica.removeSegment(UniqueID(i))
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
		err := node.historical.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
		assert.NoError(t, err)
		targetSeg, err := node.historical.replica.getSegmentByID(UniqueID(i))
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
		err := node.historical.replica.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
		assert.NoError(t, err)
		targetSeg, err := node.historical.replica.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		hasSeg := node.historical.replica.hasSegment(UniqueID(i))
		assert.Equal(t, hasSeg, true)
		hasSeg = node.historical.replica.hasSegment(UniqueID(i + 100))
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

func TestCollectionReplica_statistic(t *testing.T) {
	t.Run("test getCollectionIDs", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		ids := replica.getCollectionIDs()
		assert.Len(t, ids, 1)
		assert.Equal(t, defaultCollectionID, ids[0])
	})

	t.Run("test getCollectionIDs", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		num := replica.getSegmentNum()
		assert.Equal(t, 0, num)
	})
}
