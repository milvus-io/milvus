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
	"fmt"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func getInsertNode() (*insertNode, error) {
	streaming, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}

	err = streaming.addSegment(defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultDMLChannel,
		defaultSegmentVersion,
		segmentTypeGrowing)
	if err != nil {
		return nil, err
	}

	return newInsertNode(streaming, defaultCollectionID, defaultDMLChannel), nil
}

func genFlowGraphInsertData(schema *schemapb.CollectionSchema, numRows int) (*insertData, error) {
	insertMsg, err := genSimpleInsertMsg(schema, numRows)
	if err != nil {
		return nil, err
	}
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(schema, insertMsg)
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

		insertRecords: map[UniqueID][]*schemapb.FieldData{
			defaultSegmentID: insertRecord.FieldsData,
		},
		insertOffset: map[UniqueID]int64{
			defaultSegmentID: 0,
		},
	}
	return iData, nil
}

func genFlowGraphDeleteData() (*deleteData, error) {
	deleteMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
	dData := &deleteData{
		deleteIDs: map[UniqueID][]primaryKey{
			defaultSegmentID: storage.ParseIDs2PrimaryKeys(deleteMsg.PrimaryKeys),
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
	schema := genTestCollectionSchema()

	t.Run("test insert", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData(schema, defaultMsgLength)
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = insertNode.insert(insertData, defaultSegmentID, wg)
		assert.NoError(t, err)
	})

	t.Run("test segment insert error", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData(schema, defaultMsgLength)
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		insertData.insertRecords[defaultSegmentID] = insertData.insertRecords[defaultSegmentID][:len(insertData.insertRecords[defaultSegmentID])/2]
		err = insertNode.insert(insertData, defaultSegmentID, wg)
		assert.Error(t, err)
	})

	t.Run("test no target segment", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming, defaultCollectionID, defaultDMLChannel)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = insertNode.insert(nil, defaultSegmentID, wg)
		assert.Error(t, err)
	})

	t.Run("test invalid segmentType", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData(schema, defaultMsgLength)
		assert.NoError(t, err)

		seg, err := insertNode.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeGrowing)
		assert.NoError(t, err)
		seg.setType(segmentTypeSealed)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = insertNode.insert(insertData, defaultSegmentID, wg)
		assert.Error(t, err)
	})
}

func TestFlowGraphInsertNode_delete(t *testing.T) {
	schema := genTestCollectionSchema()

	t.Run("test insert and delete", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		insertData, err := genFlowGraphInsertData(schema, defaultMsgLength)
		assert.NoError(t, err)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = insertNode.insert(insertData, defaultSegmentID, wg)
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg.Add(1)
		err = insertNode.delete(deleteData, defaultSegmentID, wg)
		assert.NoError(t, err)
	})

	t.Run("test only delete", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = insertNode.delete(deleteData, defaultSegmentID, wg)
		assert.NoError(t, err)
	})

	t.Run("test segment delete error", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		deleteData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		deleteData.deleteTimestamps[defaultSegmentID] = deleteData.deleteTimestamps[defaultSegmentID][:len(deleteData.deleteTimestamps)/2]
		err = insertNode.delete(deleteData, defaultSegmentID, wg)
		assert.Error(t, err)
	})

	t.Run("test no target segment", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming, defaultCollectionID, defaultDMLChannel)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = insertNode.delete(nil, defaultSegmentID, wg)
		assert.Error(t, err)
	})
}

func TestFlowGraphInsertNode_processDeleteMessages(t *testing.T) {
	t.Run("test processDeleteMessages", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)

		dMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		dData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)

		err = processDeleteMessages(streaming, segmentTypeGrowing, dMsg, dData)
		assert.NoError(t, err)
	})

	t.Run("test processDeleteMessages", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)

		dMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		dData, err := genFlowGraphDeleteData()
		assert.NoError(t, err)

		err = processDeleteMessages(streaming, segmentTypeGrowing, dMsg, dData)
		assert.NoError(t, err)
	})
}

func TestFlowGraphInsertNode_operate(t *testing.T) {
	schema := genTestCollectionSchema()

	genMsgStreamInsertMsg := func() *msgstream.InsertMsg {
		iMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		return iMsg
	}

	genInsertMsg := func() *insertMsg {
		msgInsertMsg := genMsgStreamInsertMsg()
		msgDeleteMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		iMsg := insertMsg{
			insertMessages: []*msgstream.InsertMsg{
				msgInsertMsg,
			},
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		return &iMsg
	}

	t.Run("test operate", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		msg := []flowgraph.Msg{genInsertMsg()}
		insertNode.Operate(msg)
		s, err := insertNode.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeGrowing)
		assert.Nil(t, err)
		buf := make([]byte, 8)
		for i := 0; i < defaultMsgLength; i++ {
			common.Endian.PutUint64(buf, uint64(i))
			assert.True(t, s.pkFilter.Test(buf))
		}
	})

	t.Run("test invalid partitionID", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		msgDeleteMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
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
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		msgDeleteMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		msgDeleteMsg.CollectionID = 9999
		msgDeleteMsg.PartitionID = -1
		assert.NoError(t, err)
		iMsg := insertMsg{
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		assert.Panics(t, func() {
			insertNode.Operate(msg)
		})
	})

	t.Run("test partition not exist", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		msgDeleteMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		msgDeleteMsg.PartitionID = 9999
		assert.NoError(t, err)
		iMsg := insertMsg{
			deleteMessages: []*msgstream.DeleteMsg{
				msgDeleteMsg,
			},
		}
		msg := []flowgraph.Msg{&iMsg}
		assert.Panics(t, func() {
			insertNode.Operate(msg)
		})
	})

	t.Run("test invalid input length", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)
		msg := []flowgraph.Msg{genInsertMsg(), genInsertMsg()}
		insertNode.Operate(msg)
	})

	t.Run("test getCollectionByID failed", func(t *testing.T) {
		streaming, err := genSimpleReplica()
		assert.NoError(t, err)
		insertNode := newInsertNode(streaming, defaultCollectionID, defaultDMLChannel)

		msg := []flowgraph.Msg{genInsertMsg()}

		err = insertNode.metaReplica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)
		assert.Panics(t, func() {
			insertNode.Operate(msg)
		})
	})

	t.Run("test TransferInsertMsgToInsertRecord failed", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		col, err := insertNode.metaReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		for i, field := range col.schema.GetFields() {
			if field.DataType == schemapb.DataType_FloatVector {
				col.schema.Fields[i].TypeParams = nil
			}
		}

		iMsg := genInsertMsg()
		iMsg.insertMessages[0].Version = internalpb.InsertDataVersion_RowBased
		msg := []flowgraph.Msg{iMsg}
		assert.Panics(t, func() {
			insertNode.Operate(msg)
		})
	})

	t.Run("test getPrimaryKeys failed", func(t *testing.T) {
		insertNode, err := getInsertNode()
		assert.NoError(t, err)

		iMsg := genInsertMsg()
		iMsg.insertMessages[0].NumRows = 0
		msg := []flowgraph.Msg{iMsg}
		assert.Panics(t, func() {
			insertNode.Operate(msg)
		})
	})
}

func TestFilterSegmentsByPKs(t *testing.T) {
	t.Run("filter int64 pks", func(t *testing.T) {
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

		pk0 := newInt64PrimaryKey(0)
		pk1 := newInt64PrimaryKey(1)
		pk2 := newInt64PrimaryKey(2)
		pk3 := newInt64PrimaryKey(3)
		pk4 := newInt64PrimaryKey(4)

		timestamps := []uint64{1, 1, 1, 1, 1}
		pks, _, err := filterSegmentsByPKs([]primaryKey{pk0, pk1, pk2, pk3, pk4}, timestamps, segment)
		assert.Nil(t, err)
		assert.Equal(t, len(pks), 3)

		pks, _, err = filterSegmentsByPKs([]primaryKey{}, timestamps, segment)
		assert.Nil(t, err)
		assert.Equal(t, len(pks), 0)
		_, _, err = filterSegmentsByPKs(nil, timestamps, segment)
		assert.NoError(t, err)
		_, _, err = filterSegmentsByPKs([]primaryKey{pk0, pk1, pk2, pk3, pk4}, timestamps, nil)
		assert.NotNil(t, err)
	})

	t.Run("filter varChar pks", func(t *testing.T) {
		filter := bloom.NewWithEstimates(1000000, 0.01)
		for i := 0; i < 3; i++ {
			filter.AddString(fmt.Sprintf("test%d", i))
		}
		segment := &Segment{
			segmentID: 1,
			pkFilter:  filter,
		}

		pk0 := newVarCharPrimaryKey("test0")
		pk1 := newVarCharPrimaryKey("test1")
		pk2 := newVarCharPrimaryKey("test2")
		pk3 := newVarCharPrimaryKey("test3")
		pk4 := newVarCharPrimaryKey("test4")

		timestamps := []uint64{1, 1, 1, 1, 1}
		pks, _, err := filterSegmentsByPKs([]primaryKey{pk0, pk1, pk2, pk3, pk4}, timestamps, segment)
		assert.Nil(t, err)
		assert.Equal(t, len(pks), 3)

		pks, _, err = filterSegmentsByPKs([]primaryKey{}, timestamps, segment)
		assert.Nil(t, err)
		assert.Equal(t, len(pks), 0)
		_, _, err = filterSegmentsByPKs(nil, timestamps, segment)
		assert.NoError(t, err)
		_, _, err = filterSegmentsByPKs([]primaryKey{pk0, pk1, pk2, pk3, pk4}, timestamps, nil)
		assert.NotNil(t, err)
	})
}
