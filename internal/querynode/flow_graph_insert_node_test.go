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
	"sync"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
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
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
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
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
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
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.insert(nil, defaultSegmentID, wg)
	})

	t.Run("test invalid segmentType", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
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
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
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
		insertNode.delete(deleteData, defaultSegmentID, defaultCollectionID, wg)
	})

	t.Run("test only delete", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.delete(deleteData, defaultSegmentID, defaultCollectionID, wg)
	})

	t.Run("test segment delete error", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		deleteData.deleteTimestamps[defaultSegmentID] = deleteData.deleteTimestamps[defaultSegmentID][:len(deleteData.deleteTimestamps)/2]
		insertNode.delete(deleteData, defaultSegmentID, defaultCollectionID, wg)
	})

	t.Run("test no target segment", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertNode.delete(nil, defaultSegmentID, defaultCollectionID, wg)
	})
}

func TestFlowGraphInsertNode_operate(t *testing.T) {
	t.Run("test operate", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgInsertMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		msgDeleteMsg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		iMsg := insertMsg{
			insertMessages: []*msgstream.InsertMsg{
				msgInsertMsg,
			},
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		insertNode.Operate(msg)
		s, err := streaming.getSegmentByID(defaultSegmentID)
		assert.Nil(t, err)
		buf := make([]byte, 8)
		for i := 0; i < defaultMsgLength; i++ {
			common.Endian.PutUint64(buf, uint64(i))
			assert.True(t, s.pkFilter.Test(buf))
		}

	})

	t.Run("test invalid partitionID", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgDeleteMsg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		msgDeleteMsg.PartitionID = common.InvalidPartitionID
		assert.NoError(t, err)
		iMsg := insertMsg{
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		insertNode.Operate(msg)
	})

	t.Run("test collection partition not exist", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgDeleteMsg, err := genSimpleDeleteMsg()
		msgDeleteMsg.CollectionID = 9999
		msgDeleteMsg.PartitionID = -1
		assert.NoError(t, err)
		iMsg := insertMsg{
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		insertNode.Operate(msg)
	})

	t.Run("test partition not exist", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgDeleteMsg, err := genSimpleDeleteMsg()
		msgDeleteMsg.PartitionID = 9999
		assert.NoError(t, err)
		iMsg := insertMsg{
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		insertNode.Operate(msg)
	})

	t.Run("test invalid input length", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming)

		err = streaming.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeGrowing,
			true)
		assert.NoError(t, err)

		msgInsertMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)
		msgDeleteMsg, err := genSimpleDeleteMsg()
		assert.NoError(t, err)
		iMsg := insertMsg{
			insertMessages: []*msgstream.InsertMsg{
				msgInsertMsg,
			},
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg, &iMsg}
		insertNode.Operate(msg)
	})
}

func TestGetSegmentsByPKs(t *testing.T) {
	buf := make([]byte, 8)
	filter := bloom.NewWithEstimates(1000000, 0.01)
	for i := 0; i < 3; i++ {
		common.Endian.PutUint64(buf, uint64(i))
		filter.Add(buf)
	}
	segment := &Segment{
		segmentID: 1,
		pkFilter:  filter,
	}
	pks, err := filterSegmentsByPKs([]int64{0, 1, 2, 3, 4}, segment)
	assert.Nil(t, err)
	assert.Equal(t, len(pks), 3)

	pks, err = filterSegmentsByPKs([]int64{}, segment)
	assert.Nil(t, err)
	assert.Equal(t, len(pks), 0)
	_, err = filterSegmentsByPKs(nil, segment)
	assert.NotNil(t, err)
	_, err = filterSegmentsByPKs([]int64{0, 1, 2, 3, 4}, nil)
	assert.NotNil(t, err)
}
