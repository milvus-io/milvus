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

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

//----------------------------------------------------------------------------------------------------- collection
func TestMetaReplica_getCollectionNum(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	assert.Equal(t, node.historical.getCollectionNum(), 1)
	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_addCollection(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_removeCollection(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	assert.Equal(t, node.historical.getCollectionNum(), 1)

	err := node.historical.removeCollection(0)
	assert.NoError(t, err)
	assert.Equal(t, node.historical.getCollectionNum(), 0)
	err = node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_getCollectionByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)
	targetCollection, err := node.historical.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.ID(), collectionID)
	err = node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_hasCollection(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	hasCollection := node.historical.hasCollection(collectionID)
	assert.Equal(t, hasCollection, true)
	hasCollection = node.historical.hasCollection(UniqueID(1))
	assert.Equal(t, hasCollection, false)

	err := node.Stop()
	assert.NoError(t, err)
}

//----------------------------------------------------------------------------------------------------- partition
func TestMetaReplica_getPartitionNum(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}
	for _, id := range partitionIDs {
		err := node.historical.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
	}

	partitionNum := node.historical.getPartitionNum()
	assert.Equal(t, partitionNum, len(partitionIDs))
	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_addPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}
	for _, id := range partitionIDs {
		err := node.historical.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
	}
	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_removePartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	partitionIDs := []UniqueID{1, 2, 3}

	for _, id := range partitionIDs {
		err := node.historical.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
		err = node.historical.removePartition(id)
		assert.NoError(t, err)
	}
	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_getPartitionByTag(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	collection, err := node.historical.getCollectionByID(collectionID)
	assert.NoError(t, err)

	for _, id := range collection.partitionIDs {
		err := node.historical.addPartition(collectionID, id)
		assert.NoError(t, err)
		partition, err := node.historical.getPartitionByID(id)
		assert.NoError(t, err)
		assert.Equal(t, partition.ID(), id)
		assert.NotNil(t, partition)
	}
	err = node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_hasPartition(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	collection, err := node.historical.getCollectionByID(collectionID)
	assert.NoError(t, err)
	err = node.historical.addPartition(collectionID, collection.partitionIDs[0])
	assert.NoError(t, err)
	hasPartition := node.historical.hasPartition(defaultPartitionID)
	assert.Equal(t, hasPartition, true)
	hasPartition = node.historical.hasPartition(defaultPartitionID + 1)
	assert.Equal(t, hasPartition, false)
	err = node.Stop()
	assert.NoError(t, err)
}

//----------------------------------------------------------------------------------------------------- segment
func TestMetaReplica_addSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := node.historical.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.historical.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_removeSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3

	for i := 0; i < segmentNum; i++ {
		err := node.historical.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.historical.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		err = node.historical.removeSegment(UniqueID(i))
		assert.NoError(t, err)
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_getSegmentByID(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3

	for i := 0; i < segmentNum; i++ {
		err := node.historical.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.historical.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_getSegmentInfosByColID(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := node.historical.addCollection(collectionID, schema)
	node.historical.addPartition(collectionID, defaultPartitionID)

	// test get indexed segment info
	vectorFieldIDDs, err := node.historical.getVecFieldIDsByCollectionID(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vectorFieldIDDs))
	fieldID := vectorFieldIDDs[0]

	indexID := UniqueID(10000)
	indexInfo := &IndexedFieldInfo{
		indexInfo: &querypb.FieldIndexInfo{
			IndexName:   "test-index-name",
			IndexID:     indexID,
			EnableIndex: true,
		},
	}

	segment1, err := newSegment(collection, UniqueID(1), defaultPartitionID, collectionID, "", segmentTypeGrowing)
	assert.NoError(t, err)
	err = node.historical.setSegment(segment1)
	assert.NoError(t, err)

	segment2, err := newSegment(collection, UniqueID(2), defaultPartitionID, collectionID, "", segmentTypeSealed)
	assert.NoError(t, err)
	segment2.setIndexedFieldInfo(fieldID, indexInfo)
	err = node.historical.setSegment(segment2)
	assert.NoError(t, err)

	targetSegs, err := node.historical.getSegmentInfosByColID(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(targetSegs))
	for _, segment := range targetSegs {
		if segment.GetSegmentState() == segmentTypeGrowing {
			assert.Equal(t, UniqueID(0), segment.IndexID)
		} else {
			assert.Equal(t, indexID, segment.IndexID)
		}
	}

	err = node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_hasSegment(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	const segmentNum = 3

	for i := 0; i < segmentNum; i++ {
		err := node.historical.addSegment(UniqueID(i), defaultPartitionID, collectionID, "", segmentTypeGrowing)
		assert.NoError(t, err)
		targetSeg, err := node.historical.getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		hasSeg := node.historical.hasSegment(UniqueID(i))
		assert.Equal(t, hasSeg, true)
		hasSeg = node.historical.hasSegment(UniqueID(i + 100))
		assert.Equal(t, hasSeg, false)
	}

	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_freeAll(t *testing.T) {
	node := newQueryNodeMock()
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionID, 0)

	err := node.Stop()
	assert.NoError(t, err)
}

func TestMetaReplica_statistic(t *testing.T) {
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
