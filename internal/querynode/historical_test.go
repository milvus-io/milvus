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
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestHistorical_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test search", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		collection, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		plan, searchReqs, err := genSearchPlanAndRequests(collection, IndexFaissIDMap)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.NoError(t, err)
	})

	t.Run("test no collection - search partitions", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		collection, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		plan, searchReqs, err := genSearchPlanAndRequests(collection, IndexFaissIDMap)
		assert.NoError(t, err)

		err = his.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test no collection - search all collection", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		collection, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		plan, searchReqs, err := genSearchPlanAndRequests(collection, IndexFaissIDMap)
		assert.NoError(t, err)

		err = his.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test load partition and partition has been released", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		collection, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		plan, searchReqs, err := genSearchPlanAndRequests(collection, IndexFaissIDMap)
		assert.NoError(t, err)

		col, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		_, _, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test no partition in collection", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)

		collection, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		plan, searchReqs, err := genSearchPlanAndRequests(collection, IndexFaissIDMap)
		assert.NoError(t, err)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		res, ids, _, err := his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Equal(t, 0, len(res))
		assert.Equal(t, 0, len(ids))
		assert.NoError(t, err)
	})
}

func TestHistorical_validateSegmentIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test normal validate", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.NoError(t, err)
	})

	t.Run("test normal validate2", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID, []UniqueID{})
		assert.NoError(t, err)
	})

	t.Run("test validate non-existent collection", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID+1, []UniqueID{defaultPartitionID})
		assert.Error(t, err)
	})

	t.Run("test validate non-existent partition", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID, []UniqueID{defaultPartitionID + 1})
		assert.Error(t, err)
	})

	t.Run("test validate non-existent segment", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID + 1}, defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.Error(t, err)
	})

	t.Run("test validate segment not in given partition", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.replica.addPartition(defaultCollectionID, defaultPartitionID+1)
		assert.NoError(t, err)
		pkType := schemapb.DataType_Int64
		schema := genTestCollectionSchema(pkType)
		seg, err := genSealedSegment(schema,
			defaultCollectionID,
			defaultPartitionID+1,
			defaultSegmentID+1,
			defaultDMLChannel,
			defaultMsgLength)
		assert.NoError(t, err)
		err = his.replica.setSegment(seg)
		assert.NoError(t, err)
		// Scenario: search for a segment (segmentID = defaultSegmentID + 1, partitionID = defaultPartitionID+1)
		// that does not belong to defaultPartition
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID + 1}, defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.Error(t, err)
	})

	t.Run("test validate after partition release", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID, []UniqueID{})
		assert.Error(t, err)
	})

	t.Run("test validate after partition release2", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		col, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)
		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID, []UniqueID{})
		assert.Error(t, err)
	})

	t.Run("test validate after partition release3", func(t *testing.T) {
		tSafe := newTSafeReplica()
		his, err := genSimpleHistorical(ctx, tSafe)
		assert.NoError(t, err)
		col, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypeCollection)
		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		err = his.validateSegmentIDs([]UniqueID{defaultSegmentID}, defaultCollectionID, []UniqueID{})
		assert.Error(t, err)
	})
}
