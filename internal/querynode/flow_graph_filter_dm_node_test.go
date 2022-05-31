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

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func getFilterDMNode() (*filterDmNode, error) {
	streaming, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}

	streaming.addExcludedSegments(defaultCollectionID, nil)
	return newFilteredDmNode(streaming, defaultCollectionID), nil
}

func TestFlowGraphFilterDmNode_filterDmNode(t *testing.T) {
	fg, err := getFilterDMNode()
	assert.NoError(t, err)
	fg.Name()
}

func TestFlowGraphFilterDmNode_filterInvalidInsertMessage(t *testing.T) {
	schema := genTestCollectionSchema()
	t.Run("valid test", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("test no collection", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		msg.CollectionID = UniqueID(1000)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1000)
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.Error(t, err)
		assert.Nil(t, res)
		fg.collectionID = defaultCollectionID
	})

	t.Run("test no partition", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		msg.PartitionID = UniqueID(1000)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)

		col, err := fg.metaReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)

		res, err := fg.filterInvalidInsertMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test not target collection", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1000)
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test no exclude segment", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		fg.metaReplica.removeExcludedSegments(defaultCollectionID)
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("test segment is exclude segment", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		fg.metaReplica.addExcludedSegments(defaultCollectionID, []*datapb.SegmentInfo{
			{
				ID:           defaultSegmentID,
				CollectionID: defaultCollectionID,
				PartitionID:  defaultPartitionID,
				DmlPosition: &internalpb.MsgPosition{
					Timestamp: Timestamp(1000),
				},
			},
		})
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test misaligned messages", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("test no data", func(t *testing.T) {
		msg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		msg.RowIDs = make([]IntPrimaryKey, 0)
		msg.RowData = make([]*commonpb.Blob, 0)
		msg.NumRows = 0
		msg.FieldsData = nil
		res, err := fg.filterInvalidInsertMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})
}

func TestFlowGraphFilterDmNode_filterInvalidDeleteMessage(t *testing.T) {
	t.Run("delete valid test", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		res, err := fg.filterInvalidDeleteMessage(msg)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("test delete no collection", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		msg.CollectionID = UniqueID(1003)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1003)
		res, err := fg.filterInvalidDeleteMessage(msg)
		assert.Error(t, err)
		assert.Nil(t, res)
		fg.collectionID = defaultCollectionID
	})

	t.Run("test delete no partition", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		msg.PartitionID = UniqueID(1000)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)

		col, err := fg.metaReplica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setLoadType(loadTypePartition)

		res, err := fg.filterInvalidDeleteMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test delete not target collection", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		fg.collectionID = UniqueID(1000)
		res, err := fg.filterInvalidDeleteMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("test delete misaligned messages", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		res, err := fg.filterInvalidDeleteMessage(msg)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("test delete no data", func(t *testing.T) {
		msg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		msg.Timestamps = make([]Timestamp, 0)
		msg.NumRows = 0
		msg.Int64PrimaryKeys = make([]IntPrimaryKey, 0)
		msg.PrimaryKeys = &schemapb.IDs{}
		res, err := fg.filterInvalidDeleteMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
		msg.PrimaryKeys = storage.ParsePrimaryKeys2IDs([]primaryKey{})
		res, err = fg.filterInvalidDeleteMessage(msg)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})
}

func TestFlowGraphFilterDmNode_Operate(t *testing.T) {
	schema := genTestCollectionSchema()

	genFilterDMMsg := func() []flowgraph.Msg {
		iMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
		assert.NoError(t, err)
		msg := flowgraph.GenerateMsgStreamMsg([]msgstream.TsMsg{iMsg}, 0, 1000, nil, nil)
		return []flowgraph.Msg{msg}
	}

	t.Run("valid test", func(t *testing.T) {
		msg := genFilterDMMsg()
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		res := fg.Operate(msg)
		assert.NotNil(t, res)
	})

	t.Run("invalid input length", func(t *testing.T) {
		msg := genFilterDMMsg()
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		var m flowgraph.Msg
		msg = append(msg, m)
		res := fg.Operate(msg)
		assert.NotNil(t, res)
	})

	t.Run("filterInvalidInsertMessage failed", func(t *testing.T) {
		iMsg, err := genSimpleInsertMsg(schema, defaultDelLength)
		assert.NoError(t, err)
		iMsg.NumRows = 0
		msg := flowgraph.GenerateMsgStreamMsg([]msgstream.TsMsg{iMsg}, 0, 1000, nil, nil)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		m := []flowgraph.Msg{msg}
		assert.Panics(t, func() {
			fg.Operate(m)
		})
	})

	t.Run("filterInvalidDeleteMessage failed", func(t *testing.T) {
		dMsg := genDeleteMsg(defaultCollectionID, schemapb.DataType_Int64, defaultDelLength)
		dMsg.NumRows = 0
		msg := flowgraph.GenerateMsgStreamMsg([]msgstream.TsMsg{dMsg}, 0, 1000, nil, nil)
		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		m := []flowgraph.Msg{msg}
		assert.Panics(t, func() {
			fg.Operate(m)
		})
	})

	t.Run("invalid msgType", func(t *testing.T) {
		iMsg, err := genSimpleInsertMsg(genTestCollectionSchema(), defaultDelLength)
		assert.NoError(t, err)
		iMsg.Base.MsgType = commonpb.MsgType_Search
		msg := flowgraph.GenerateMsgStreamMsg([]msgstream.TsMsg{iMsg}, 0, 1000, nil, nil)

		fg, err := getFilterDMNode()
		assert.NoError(t, err)
		res := fg.Operate([]flowgraph.Msg{msg})
		assert.NotNil(t, res)
	})
}
