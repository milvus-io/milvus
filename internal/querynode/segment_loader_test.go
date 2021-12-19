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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func TestSegmentLoader_loadSegment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := genSimpleInsertDataSchema()

	fieldBinlog, err := saveSimpleBinLog(ctx)
	assert.NoError(t, err)

	t.Run("test load segment", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		err = node.historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		loader := node.loader
		assert.NotNil(t, loader)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID: 0,
			Schema:    schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		err = loader.loadSegment(req, segmentTypeSealed)
		assert.NoError(t, err)
	})

	t.Run("test set segment error due to without partition", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		err = node.historical.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)

		loader := node.loader
		assert.NotNil(t, loader)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID: 0,
			Schema:    schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		err = loader.loadSegment(req, segmentTypeSealed)
		assert.Error(t, err)
	})
}

func TestSegmentLoader_loadSegmentFieldsData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runLoadSegmentFieldData := func(dataType schemapb.DataType) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		fieldUID := genConstantField(uidField)
		fieldTimestamp := genConstantField(timestampField)

		schema := &schemapb.CollectionSchema{
			Name:   defaultCollectionName,
			AutoID: true,
			Fields: []*schemapb.FieldSchema{},
		}

		constFieldID := FieldID(105)
		constFieldName := "const-field-test"
		constField := &schemapb.FieldSchema{
			FieldID: constFieldID,
			Name:    constFieldName,
		}

		switch dataType {
		case schemapb.DataType_Bool:
			constField.DataType = schemapb.DataType_Bool
		case schemapb.DataType_Int8:
			constField.DataType = schemapb.DataType_Int8
		case schemapb.DataType_Int16:
			constField.DataType = schemapb.DataType_Int16
		case schemapb.DataType_Int32:
			constField.DataType = schemapb.DataType_Int32
		case schemapb.DataType_Int64:
			constField.DataType = schemapb.DataType_Int64
		case schemapb.DataType_Float:
			constField.DataType = schemapb.DataType_Float
		case schemapb.DataType_Double:
			constField.DataType = schemapb.DataType_Double
		case schemapb.DataType_FloatVector:
			constField.DataType = schemapb.DataType_FloatVector
		case schemapb.DataType_BinaryVector:
			constField.DataType = schemapb.DataType_BinaryVector
		}

		schema.Fields = append(schema.Fields, constField)

		err = loader.historicalReplica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		col := newCollection(defaultCollectionID, schema)
		assert.NotNil(t, col)
		segment := newSegment(col,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeSealed,
			true)

		schema.Fields = append(schema.Fields, fieldUID)
		schema.Fields = append(schema.Fields, fieldTimestamp)

		binlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		err = loader.loadSegmentFieldsData(segment, binlog, segmentTypeSealed)
		assert.NoError(t, err)
	}

	t.Run("test bool", func(t *testing.T) {
		runLoadSegmentFieldData(schemapb.DataType_Bool)
		runLoadSegmentFieldData(schemapb.DataType_Int8)
		runLoadSegmentFieldData(schemapb.DataType_Int16)
		runLoadSegmentFieldData(schemapb.DataType_Int32)
		runLoadSegmentFieldData(schemapb.DataType_Int64)
		runLoadSegmentFieldData(schemapb.DataType_Float)
		runLoadSegmentFieldData(schemapb.DataType_Double)
	})
}

func TestSegmentLoader_invalid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test no collection", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		err = node.historical.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID: 0,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
				},
			},
		}

		err = loader.loadSegment(req, segmentTypeSealed)
		assert.Error(t, err)
	})

	//t.Run("test no collection 2", func(t *testing.T) {
	//	historical, err := genSimpleHistorical(ctx)
	//	assert.NoError(t, err)
	//
	//	err = historical.replica.removeCollection(defaultCollectionID)
	//	assert.NoError(t, err)
	//
	//	err = historical.loader.loadSegmentInternal(defaultCollectionID, nil, nil)
	//	assert.Error(t, err)
	//})
	//
	//t.Run("test no vec field", func(t *testing.T) {
	//	historical, err := genSimpleHistorical(ctx)
	//	assert.NoError(t, err)
	//
	//	err = historical.replica.removeCollection(defaultCollectionID)
	//	assert.NoError(t, err)
	//
	//	schema := &schemapb.CollectionSchema{
	//		Name:   defaultCollectionName,
	//		AutoID: true,
	//		Fields: []*schemapb.FieldSchema{
	//			genConstantField(constFieldParam{
	//				id:       FieldID(100),
	//				dataType: schemapb.DataType_Int8,
	//			}),
	//		},
	//	}
	//	err = historical.loader.historicalReplica.addCollection(defaultCollectionID, schema)
	//	assert.NoError(t, err)
	//
	//	err = historical.loader.loadSegmentInternal(defaultCollectionID, nil, nil)
	//	assert.Error(t, err)
	//})

	t.Run("test no vec field 2", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		err = node.historical.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		schema := &schemapb.CollectionSchema{
			Name:   defaultCollectionName,
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				genConstantField(constFieldParam{
					id:       FieldID(100),
					dataType: schemapb.DataType_Int8,
				}),
			},
		}
		err = loader.historicalReplica.addCollection(defaultCollectionID, schema)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID: 0,
			Schema:    schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
				},
			},
		}
		err = loader.loadSegment(req, segmentTypeSealed)
		assert.Error(t, err)
	})
}

func TestSegmentLoader_checkSegmentSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)
	loader := node.loader
	assert.NotNil(t, loader)

	err = loader.checkSegmentSize(defaultSegmentID, map[UniqueID]int64{defaultSegmentID: 1024})
	assert.NoError(t, err)

	//totalMem, err := getTotalMemory()
	//assert.NoError(t, err)
	//err = historical.loader.checkSegmentSize(defaultSegmentID, map[UniqueID]int64{defaultSegmentID: int64(totalMem * 2)})
	//assert.Error(t, err)
}

func TestSegmentLoader_estimateSegmentSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)
	loader := node.loader
	assert.NotNil(t, loader)

	seg, err := node.historical.replica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	binlog := []*datapb.FieldBinlog{
		{
			FieldID: simpleConstField.id,
			Binlogs: []*datapb.Binlog{{LogPath: "^&^%*&%&&(*^*&"}},
		},
	}

	_, err = loader.estimateSegmentSize(seg, binlog, nil)
	assert.Error(t, err)

	binlog, err = saveSimpleBinLog(ctx)
	assert.NoError(t, err)

	_, err = loader.estimateSegmentSize(seg, binlog, nil)
	assert.NoError(t, err)

	indexPath, err := generateIndex(defaultSegmentID)
	assert.NoError(t, err)

	seg.setIndexInfo(simpleVecField.id, &indexInfo{})

	err = seg.setIndexPaths(simpleVecField.id, indexPath)
	assert.NoError(t, err)

	_, err = loader.estimateSegmentSize(seg, binlog, []FieldID{simpleVecField.id})
	assert.NoError(t, err)

	err = seg.setIndexPaths(simpleVecField.id, []string{"&*^*(^*(&*%^&*^(&"})
	assert.NoError(t, err)

	_, err = loader.estimateSegmentSize(seg, binlog, []FieldID{simpleVecField.id})
	assert.Error(t, err)
}

func TestSegmentLoader_testLoadGrowing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test load growing segments", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		collection, err := node.historical.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		segment := newSegment(collection, defaultSegmentID+1, defaultPartitionID, defaultCollectionID, defaultDMLChannel, segmentTypeGrowing, true)

		insertMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)

		err = loader.loadGrowingSegments(segment, insertMsg.RowIDs, insertMsg.Timestamps, insertMsg.RowData)
		assert.NoError(t, err)
	})

	t.Run("test invalid insert data", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)
		loader := node.loader
		assert.NotNil(t, loader)

		collection, err := node.historical.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)

		segment := newSegment(collection, defaultSegmentID+1, defaultPartitionID, defaultCollectionID, defaultDMLChannel, segmentTypeGrowing, true)

		insertMsg, err := genSimpleInsertMsg()
		assert.NoError(t, err)

		insertMsg.RowData = nil

		err = loader.loadGrowingSegments(segment, insertMsg.RowIDs, insertMsg.Timestamps, insertMsg.RowData)
		assert.Error(t, err)
	})
}

func TestSegmentLoader_testLoadGrowingAndSealed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := genSimpleInsertDataSchema()
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      UniqueID(102),
		Name:         "pk",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	})

	fieldBinlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
	assert.NoError(t, err)

	t.Run("test load growing and sealed segments", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		loader := node.loader
		assert.NotNil(t, loader)
		loader.indexLoader.indexCoord = nil
		loader.indexLoader.rootCoord = nil

		segmentID1 := UniqueID(100)
		req1 := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID: 0,
			Schema:    schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    segmentID1,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		err = loader.loadSegment(req1, segmentTypeSealed)
		assert.NoError(t, err)

		segmentID2 := UniqueID(101)
		req2 := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID: 0,
			Schema:    schema,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    segmentID2,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		err = loader.loadSegment(req2, segmentTypeGrowing)
		assert.NoError(t, err)

		segment1, err := loader.historicalReplica.getSegmentByID(segmentID1)
		assert.NoError(t, err)

		segment2, err := loader.streamingReplica.getSegmentByID(segmentID2)
		assert.NoError(t, err)

		assert.Equal(t, segment1.getRowCount(), segment2.getRowCount())
	})
}
