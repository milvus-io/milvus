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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestMetaReplica_collection(t *testing.T) {
	t.Run("test getCollectionNum", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()
		assert.Equal(t, 1, replica.getCollectionNum())
	})

	t.Run("test addCollection", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()
		replica.addCollection(defaultCollectionID+1, genTestCollectionSchema())
		assert.Equal(t, 2, replica.getCollectionNum())
	})

	t.Run("test removeCollection", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()
		err = replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
	})

	t.Run("test getCollectionByID", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		targetCollection, err := replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		assert.NotNil(t, targetCollection)
		assert.Equal(t, defaultCollectionID, targetCollection.ID())
	})

	t.Run("test hasCollection", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		hasCollection := replica.hasCollection(defaultCollectionID)
		assert.Equal(t, true, hasCollection)
		hasCollection = replica.hasCollection(defaultCollectionID + 1)
		assert.Equal(t, false, hasCollection)
	})

	t.Run("test getCollectionIDs", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()
		ids := replica.getCollectionIDs()
		assert.Len(t, ids, 1)
		assert.Equal(t, defaultCollectionID, ids[0])
	})
}

func TestMetaReplica_partition(t *testing.T) {
	t.Run("test addPartition, getPartitionNum and getPartitionByID", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		partitionIDs := []UniqueID{1, 2, 3}
		for _, id := range partitionIDs {
			err := replica.addPartition(defaultCollectionID, id)
			assert.NoError(t, err)
			partition, err := replica.getPartitionByID(id)
			assert.NoError(t, err)
			assert.Equal(t, id, partition.ID())
		}

		partitionNum := replica.getPartitionNum()
		assert.Equal(t, len(partitionIDs), partitionNum)
	})

	t.Run("test removePartition", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		partitionIDs := []UniqueID{1, 2, 3}

		for _, id := range partitionIDs {
			err := replica.addPartition(defaultCollectionID, id)
			assert.NoError(t, err)
			partition, err := replica.getPartitionByID(id)
			assert.NoError(t, err)
			assert.Equal(t, id, partition.ID())
			err = replica.removePartition(id)
			assert.NoError(t, err)
			_, err = replica.getPartitionByID(id)
			assert.Error(t, err)
		}
	})

	t.Run("test hasPartition", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		collection, err := replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		err = replica.addPartition(defaultCollectionID, collection.partitionIDs[0])
		assert.NoError(t, err)
		hasPartition := replica.hasPartition(defaultPartitionID)
		assert.Equal(t, true, hasPartition)
		hasPartition = replica.hasPartition(defaultPartitionID + 1)
		assert.Equal(t, false, hasPartition)
	})
}

func TestMetaReplica_segment(t *testing.T) {
	t.Run("test addSegment and getSegmentByID", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		const segmentNum = 3
		for i := 0; i < segmentNum; i++ {
			err := replica.addSegment(UniqueID(i), defaultPartitionID, defaultCollectionID, "", segmentTypeGrowing)
			assert.NoError(t, err)
			targetSeg, err := replica.getSegmentByID(UniqueID(i), segmentTypeGrowing)
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(i), targetSeg.segmentID)
		}
	})

	t.Run("test removeSegment", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		const segmentNum = 3
		for i := 0; i < segmentNum; i++ {
			err := replica.addSegment(UniqueID(i), defaultPartitionID, defaultCollectionID, "", segmentTypeGrowing)
			assert.NoError(t, err)
			targetSeg, err := replica.getSegmentByID(UniqueID(i), segmentTypeGrowing)
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(i), targetSeg.segmentID)
			err = replica.removeSegment(UniqueID(i), segmentTypeGrowing)
			assert.NoError(t, err)
		}
	})

	t.Run("test hasSegment", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		const segmentNum = 3
		for i := 0; i < segmentNum; i++ {
			err := replica.addSegment(UniqueID(i), defaultPartitionID, defaultCollectionID, "", segmentTypeGrowing)
			assert.NoError(t, err)
			targetSeg, err := replica.getSegmentByID(UniqueID(i), segmentTypeGrowing)
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(i), targetSeg.segmentID)
			hasSeg, err := replica.hasSegment(UniqueID(i), segmentTypeGrowing)
			assert.NoError(t, err)
			assert.Equal(t, true, hasSeg)
			hasSeg, err = replica.hasSegment(UniqueID(i+100), segmentTypeGrowing)
			assert.NoError(t, err)
			assert.Equal(t, false, hasSeg)
		}
	})

	t.Run("test invalid segment type", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		invalidType := commonpb.SegmentState_NotExist
		err = replica.addSegment(defaultSegmentID, defaultPartitionID, defaultCollectionID, "", invalidType)
		assert.Error(t, err)
		_, err = replica.getSegmentByID(defaultSegmentID, invalidType)
		assert.Error(t, err)
		_, err = replica.getSegmentIDs(defaultPartitionID, invalidType)
		assert.Error(t, err)
		err = replica.removeSegment(defaultSegmentID, invalidType)
		assert.Error(t, err)
		_, err = replica.hasSegment(defaultSegmentID, invalidType)
		assert.Error(t, err)
		num := replica.getSegmentNum(invalidType)
		assert.Equal(t, 0, num)
	})

	t.Run("test getSegmentInfosByColID", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		defer replica.freeAll()

		schema := genTestCollectionSchema()
		collection := replica.addCollection(defaultCollectionID, schema)
		replica.addPartition(defaultCollectionID, defaultPartitionID)

		// test get indexed segment info
		vectorFieldIDDs, err := replica.getVecFieldIDsByCollectionID(defaultCollectionID)
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

		segment1, err := newSegment(collection, UniqueID(1), defaultPartitionID, defaultCollectionID, "", segmentTypeGrowing)
		assert.NoError(t, err)
		err = replica.setSegment(segment1)
		assert.NoError(t, err)

		segment2, err := newSegment(collection, UniqueID(2), defaultPartitionID, defaultCollectionID, "", segmentTypeSealed)
		assert.NoError(t, err)
		segment2.setIndexedFieldInfo(fieldID, indexInfo)
		err = replica.setSegment(segment2)
		assert.NoError(t, err)

		targetSegs := replica.getSegmentInfosByColID(defaultCollectionID)
		assert.Equal(t, 2, len(targetSegs))
		for _, segment := range targetSegs {
			if segment.GetSegmentState() == segmentTypeGrowing {
				assert.Equal(t, UniqueID(0), segment.IndexID)
			} else {
				assert.Equal(t, indexID, segment.IndexID)
			}
		}
	})
}

func TestMetaReplica_freeAll(t *testing.T) {
	replica, err := genSimpleReplica()
	assert.NoError(t, err)
	replica.freeAll()
	num := replica.getCollectionNum()
	assert.Equal(t, 0, num)
	num = replica.getPartitionNum()
	assert.Equal(t, 0, num)
	num = replica.getSegmentNum(segmentTypeGrowing)
	assert.Equal(t, 0, num)
	num = replica.getSegmentNum(segmentTypeSealed)
	assert.Equal(t, 0, num)
}
