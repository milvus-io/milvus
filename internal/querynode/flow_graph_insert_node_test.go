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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func genFlowGraphInsertData() (*insertData, error) {
	insertMsg, err := genSimpleInsertMsg()
	if err != nil {
		return nil, err
	}

	iData := &insertData{
		insertIDs: map[UniqueID][]UniqueID{
			defaultSegmentID: insertMsg.RowIDs,
		},
		insertTimestamps: map[UniqueID][]Timestamp{
			defaultSegmentID: insertMsg.Timestamps,
		},
		insertRecords: map[UniqueID][]*commonpb.Blob{
			defaultSegmentID: insertMsg.RowData,
		},
		insertOffset: map[UniqueID]int64{
			defaultSegmentID: 0,
		},
	}
	return iData, nil
}

func genFlowGraphDeleteData() (*deleteData, error) {
	deleteMsg, err := genSimpleDeleteMsg()
	if err != nil {
		return nil, err
	}
	dData := &deleteData{
		deleteIDs: map[UniqueID][]UniqueID{
			defaultSegmentID: deleteMsg.PrimaryKeys,
		},
		deleteTimestamps: map[UniqueID][]Timestamp{
			defaultSegmentID: deleteMsg.Timestamps,
		},
		deleteOffset: map[UniqueID]int64{
			defaultSegmentID: 0,
		},
	}
	return dData, nil
}

func TestFlowGraphInsertNode_insert(t *testing.T) {
	t.Run("test insert", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData()
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.insert(insertData, defaultSegmentID, wg)
	})

	t.Run("test segment insert error", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData()
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertData.insertRecords[defaultSegmentID][0].Value = insertData.insertRecords[defaultSegmentID][0].Value[:len(insertData.insertRecords[defaultSegmentID][0].Value)/2]
		insertNode.insert(insertData, defaultSegmentID, wg)
	})

	t.Run("test no target segment", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.insert(nil, defaultSegmentID, wg)
	})

	t.Run("test invalid segmentType", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeSealed,
			true)
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.insert(nil, defaultSegmentID, wg)
	})
}

func TestFlowGraphInsertNode_delete(t *testing.T) {
	t.Run("test insert and delete", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData()
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.insert(insertData, defaultSegmentID, wg)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg.Add(1)
		insertNode.delete(deleteData, defaultSegmentID, wg)
	})

	t.Run("test only delete", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.delete(deleteData, defaultSegmentID, wg)
	})

	t.Run("test segment delete error", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		deleteData.deleteTimestamps[defaultSegmentID] = deleteData.deleteTimestamps[defaultSegmentID][:len(deleteData.deleteTimestamps)/2]
		insertNode.delete(deleteData, defaultSegmentID, wg)
	})

	t.Run("test no target segment", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.delete(nil, defaultSegmentID, wg)
	})
}

func TestFlowGraphInsertNode_operate(t *testing.T) {
	t.Run("test operate", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgInsertMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		iMsg := insertMsg{
			insertMessages: []*msgstream.InsertMsg{
				msgInsertMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		insertNode.Operate(msg)
	})

	t.Run("test invalid input length", func(t *testing.T) {
		replica, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(replica)

		err = replica.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgInsertMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		iMsg := insertMsg{
			insertMessages: []*msgstream.InsertMsg{
				msgInsertMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg, &iMsg}
		insertNode.Operate(msg)
	})
}
