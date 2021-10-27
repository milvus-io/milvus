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

	kv, err := genEtcdKV()
	assert.NoError(t, err)

	schema := genSimpleInsertDataSchema()

	fieldBinlog, err := saveSimpleBinLog(ctx)
	assert.NoError(t, err)

	t.Run("test load segment", func(t *testing.T) {
		historical, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		err = historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)
		loader := newSegmentLoader(ctx, nil, nil, historical.replica, kv)
		assert.NotNil(t, loader)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID:     0,
			Schema:        schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		err = loader.loadSegment(req)
		assert.NoError(t, err)
	})

	t.Run("test set segment error", func(t *testing.T) {
		historical, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		err = historical.replica.removePartition(defaultPartitionID)
		assert.NoError(t, err)
		loader := newSegmentLoader(ctx, nil, nil, historical.replica, kv)
		assert.NotNil(t, loader)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID:     0,
			Schema:        schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
					BinlogPaths:  fieldBinlog,
				},
			},
		}

		err = loader.loadSegment(req)
		assert.Error(t, err)
	})
}

func TestSegmentLoader_loadSegmentFieldsData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runLoadSegmentFieldData := func(dataType schemapb.DataType) {
		historical, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

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

		err = historical.replica.removeSegment(defaultSegmentID)
		assert.NoError(t, err)

		col := newCollection(defaultCollectionID, schema)
		assert.NotNil(t, col)
		segment := newSegment(col,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeSealed,
			true)

		schema.Fields = append(schema.Fields, fieldUID)
		schema.Fields = append(schema.Fields, fieldTimestamp)

		binlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		err = historical.loader.loadSegmentFieldsData(segment, binlog)
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
		historical, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		err = historical.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID:     0,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
				},
			},
		}

		err = historical.loader.loadSegment(req)
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
		historical, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		err = historical.replica.removeCollection(defaultCollectionID)
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
		err = historical.loader.historicalReplica.addCollection(defaultCollectionID, schema)
		assert.NoError(t, err)

		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			DstNodeID:     0,
			Schema:        schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:    defaultSegmentID,
					PartitionID:  defaultPartitionID,
					CollectionID: defaultCollectionID,
				},
			},
		}
		err = historical.loader.loadSegment(req)
		assert.Error(t, err)
	})
}

func TestSegmentLoader_checkSegmentSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	historical, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)

	err = historical.loader.checkSegmentSize(defaultSegmentID, map[UniqueID]int64{defaultSegmentID: 1024})
	assert.NoError(t, err)

	//totalMem, err := getTotalMemory()
	//assert.NoError(t, err)
	//err = historical.loader.checkSegmentSize(defaultSegmentID, map[UniqueID]int64{defaultSegmentID: int64(totalMem * 2)})
	//assert.Error(t, err)
}

func TestSegmentLoader_estimateSegmentSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	historical, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)

	seg, err := historical.replica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	binlog := []*datapb.FieldBinlog{
		{
			FieldID: simpleConstField.id,
			Binlogs: []string{"^&^%*&%&&(*^*&"},
		},
	}

	_, err = historical.loader.estimateSegmentSize(seg, binlog, nil)
	assert.Error(t, err)

	binlog, err = saveSimpleBinLog(ctx)
	assert.NoError(t, err)

	_, err = historical.loader.estimateSegmentSize(seg, binlog, nil)
	assert.NoError(t, err)

	indexPath, err := generateIndex(defaultSegmentID)
	assert.NoError(t, err)

	err = seg.setIndexInfo(simpleVecField.id, &indexInfo{})
	assert.NoError(t, err)

	err = seg.setIndexPaths(simpleVecField.id, indexPath)
	assert.NoError(t, err)

	_, err = historical.loader.estimateSegmentSize(seg, binlog, []FieldID{simpleVecField.id})
	assert.NoError(t, err)

	err = seg.setIndexPaths(simpleVecField.id, []string{"&*^*(^*(&*%^&*^(&"})
	assert.NoError(t, err)

	_, err = historical.loader.estimateSegmentSize(seg, binlog, []FieldID{simpleVecField.id})
	assert.Error(t, err)
}
