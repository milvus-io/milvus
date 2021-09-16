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
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestHistorical_GlobalSealedSegments(t *testing.T) {
	n := newQueryNodeMock()

	// init meta
	segmentID := UniqueID(0)
	partitionID := UniqueID(1)
	collectionID := UniqueID(2)
	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}

	emptySegmentCheck := func() {
		segmentIDs := n.historical.getGlobalSegmentIDsByCollectionID(collectionID)
		assert.Equal(t, 0, len(segmentIDs))
		segmentIDs = n.historical.getGlobalSegmentIDsByPartitionIds([]UniqueID{partitionID})
		assert.Equal(t, 0, len(segmentIDs))
	}

	// static test
	emptySegmentCheck()
	n.historical.addGlobalSegmentInfo(segmentID, segmentInfo)
	segmentIDs := n.historical.getGlobalSegmentIDsByCollectionID(collectionID)
	assert.Equal(t, 1, len(segmentIDs))
	assert.Equal(t, segmentIDs[0], segmentID)

	segmentIDs = n.historical.getGlobalSegmentIDsByPartitionIds([]UniqueID{partitionID})
	assert.Equal(t, 1, len(segmentIDs))
	assert.Equal(t, segmentIDs[0], segmentID)

	n.historical.removeGlobalSegmentInfo(segmentID)
	emptySegmentCheck()

	n.historical.addGlobalSegmentInfo(segmentID, segmentInfo)
	n.historical.removeGlobalSegmentIDsByCollectionID(collectionID)
	emptySegmentCheck()

	n.historical.addGlobalSegmentInfo(segmentID, segmentInfo)
	n.historical.removeGlobalSegmentIDsByPartitionIds([]UniqueID{partitionID})
	emptySegmentCheck()

	// watch test
	go n.historical.watchGlobalSegmentMeta()
	time.Sleep(100 * time.Millisecond) // for etcd latency
	segmentInfoStr := proto.MarshalTextString(segmentInfo)
	assert.NotNil(t, n.etcdKV)
	segmentKey := segmentMetaPrefix + "/" + strconv.FormatInt(segmentID, 10)
	err := n.etcdKV.Save(segmentKey, segmentInfoStr)
	assert.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // for etcd latency
	segmentIDs = n.historical.getGlobalSegmentIDsByCollectionID(collectionID)
	assert.Equal(t, 1, len(segmentIDs))
	assert.Equal(t, segmentIDs[0], segmentID)

	segmentIDs = n.historical.getGlobalSegmentIDsByPartitionIds([]UniqueID{partitionID})
	assert.Equal(t, 1, len(segmentIDs))
	assert.Equal(t, segmentIDs[0], segmentID)

	err = n.etcdKV.Remove(segmentKey)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // for etcd latency
	emptySegmentCheck()
}

func TestHistorical_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test search", func(t *testing.T) {
		his, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		_, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.NoError(t, err)
	})

	t.Run("test no collection - search partitions", func(t *testing.T) {
		his, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		err = his.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		_, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test no collection - search all collection", func(t *testing.T) {
		his, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		err = his.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		_, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test load partition and partition has been released", func(t *testing.T) {
		his, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		col, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		_, _, err = his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Error(t, err)
	})

	t.Run("test no partition in collection", func(t *testing.T) {
		his, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		res, ids, err := his.search(searchReqs, defaultCollectionID, []UniqueID{}, plan, Timestamp(0))
		assert.Nil(t, res)
		assert.Nil(t, ids)
		assert.NoError(t, err)
	})

	t.Run("test load collection partition released in collection", func(t *testing.T) {
		his, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		plan, searchReqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		col, err := his.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.addReleasedPartition(defaultPartitionID)

		err = his.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		res, ids, err := his.search(searchReqs, defaultCollectionID, []UniqueID{defaultPartitionID}, plan, Timestamp(0))
		assert.Nil(t, res)
		assert.Nil(t, ids)
		assert.Error(t, err)
	})
}
