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
	"errors"
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
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

	t.Run("test load segment with nil base message", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		loader := node.loader
		assert.NotNil(t, loader)

		req := &querypb.LoadSegmentsRequest{}

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
		segment, err := newSegment(col,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeSealed,
			true)
		assert.Nil(t, err)

		schema.Fields = append(schema.Fields, fieldUID)
		schema.Fields = append(schema.Fields, fieldTimestamp)

		binlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
		assert.NoError(t, err)

		err = loader.loadFiledBinlogData(segment, binlog)
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
		loader.historicalReplica.addCollection(defaultCollectionID, schema)

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

	err = loader.checkSegmentSize(defaultCollectionID, []*querypb.SegmentLoadInfo{{SegmentID: defaultSegmentID, SegmentSize: 1024}}, runtime.GOMAXPROCS(0))
	assert.NoError(t, err)
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

		segment, err := newSegment(collection, defaultSegmentID+1, defaultPartitionID, defaultCollectionID, defaultDMLChannel, segmentTypeGrowing, true)
		assert.Nil(t, err)

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

		segment, err := newSegment(collection, defaultSegmentID+1, defaultPartitionID, defaultCollectionID, defaultDMLChannel, segmentTypeGrowing, true)
		assert.Nil(t, err)

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
	fieldBinlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
	assert.NoError(t, err)

	t.Run("test load growing and sealed segments", func(t *testing.T) {
		node, err := genSimpleQueryNode(ctx)
		assert.NoError(t, err)

		loader := node.loader
		assert.NotNil(t, loader)

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

func TestSegmentLoader_testLoadSealedSegmentWithIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//generate schema
	fieldUID := genConstantField(uidField)
	fieldTimestamp := genConstantField(timestampField)
	fieldVec := genFloatVectorField(simpleVecField)
	fieldInt := genConstantField(simpleConstField)

	schema := &schemapb.CollectionSchema{ // schema for insertData
		Name:   defaultCollectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			fieldUID,
			fieldTimestamp,
			fieldVec,
			fieldInt,
		},
	}

	// generate insert binlog
	fieldBinlog, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
	assert.NoError(t, err)

	segmentID := UniqueID(100)
	// generate index file for segment
	indexPaths, err := generateIndex(segmentID)
	assert.NoError(t, err)
	indexInfo := &querypb.FieldIndexInfo{
		FieldID:        simpleVecField.id,
		EnableIndex:    true,
		IndexName:      indexName,
		IndexID:        indexID,
		BuildID:        buildID,
		IndexParams:    funcutil.Map2KeyValuePair(genSimpleIndexParams()),
		IndexFilePaths: indexPaths,
	}

	// generate segmentLoader
	node, err := genSimpleQueryNode(ctx)
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
				SegmentID:    segmentID,
				PartitionID:  defaultPartitionID,
				CollectionID: defaultCollectionID,
				BinlogPaths:  fieldBinlog,
				IndexInfos:   []*querypb.FieldIndexInfo{indexInfo},
			},
		},
	}

	err = loader.loadSegment(req, segmentTypeSealed)
	assert.NoError(t, err)

	segment, err := node.historical.replica.getSegmentByID(segmentID)
	assert.NoError(t, err)
	vecFieldInfo, err := segment.getIndexedFieldInfo(simpleVecField.id)
	assert.NoError(t, err)
	assert.NotNil(t, vecFieldInfo)
	assert.Equal(t, true, vecFieldInfo.indexInfo.EnableIndex)
}

func TestSegmentLoader_testFromDmlCPLoadDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	position := &msgstream.MsgPosition{ChannelName: defaultDeltaChannel, MsgID: []byte{1}}

	// test for seek failed
	{
		mockMsg := &mockMsgID{}
		mockMsg.On("AtEarliestPosition").Return(false, nil)
		testSeekFailWhenConsumingDeltaMsg(ctx, t, position, mockMsg)
	}

	//test no more data when get last msg successfully
	{
		mockMsg := &mockMsgID{}
		mockMsg.On("AtEarliestPosition").Return(true, nil)
		assert.Nil(t, testConsumingDeltaMsg(ctx, t, position, true, mockMsg))
	}

	//test consume after seeking when get last msg successfully
	{
		mockMsg := &mockMsgID{}
		mockMsg.On("AtEarliestPosition").Return(false, nil)
		mockMsg.On("LessOrEqualThan", mock.AnythingOfType("string")).Return(true, nil)
		assert.Nil(t, testConsumingDeltaMsg(ctx, t, position, true, mockMsg))
	}

	//test compare msgID failed when get last msg successfully
	{
		mockMsg := &mockMsgID{}
		mockMsg.On("AtEarliestPosition").Return(false, nil)
		mockMsg.On("LessOrEqualThan", mock.AnythingOfType("string")).Return(true, errors.New(""))
		assert.NotNil(t, testConsumingDeltaMsg(ctx, t, position, true, mockMsg))
	}

	//test consume after seeking when get last msg failed
	{
		mockMsg := &mockMsgID{}
		mockMsg.On("AtEarliestPosition").Return(false, nil)
		mockMsg.On("LessOrEqualThan", mock.AnythingOfType("string")).Return(true, errors.New(""))
		assert.NotNil(t, testConsumingDeltaMsg(ctx, t, position, false, mockMsg))
	}
}

func testSeekFailWhenConsumingDeltaMsg(ctx context.Context, t *testing.T, position *msgstream.MsgPosition, mockMsg *mockMsgID) {
	msgStream := &LoadDeleteMsgStream{}
	errMsg := "seek failed"
	err := errors.New(errMsg)
	msgStream.On("AsConsumer", mock.AnythingOfTypeArgument("string"), mock.AnythingOfTypeArgument("string"))
	msgStream.On("Seek", mock.AnythingOfType("string")).Return(err)
	msgStream.On("GetLatestMsgID", mock.AnythingOfType("string")).Return(mockMsg, nil)

	factory := &mockMsgStreamFactory{mockMqStream: msgStream}
	node, err := genSimpleQueryNodeWithMQFactory(ctx, factory)
	assert.NoError(t, err)
	loader := node.loader
	assert.NotNil(t, loader)

	ret := loader.FromDmlCPLoadDelete(ctx, defaultCollectionID, position)
	assert.EqualError(t, ret, errMsg)
}

func testConsumingDeltaMsg(ctx context.Context, t *testing.T, position *msgstream.MsgPosition, getLastSucc bool, mockMsg *mockMsgID) error {
	msgStream := &LoadDeleteMsgStream{}
	msgStream.On("AsConsumer", mock.AnythingOfTypeArgument("string"), mock.AnythingOfTypeArgument("string"))
	msgStream.On("Seek", mock.AnythingOfType("string")).Return(nil)

	if getLastSucc {
		msgStream.On("GetLatestMsgID", mock.AnythingOfType("string")).Return(mockMsg, nil)
	} else {
		msgStream.On("GetLatestMsgID", mock.AnythingOfType("string")).Return(mockMsg, errors.New(""))
	}

	msgChan := make(chan *msgstream.MsgPack)
	go func() {
		msgChan <- nil
		deleteMsg1 := genDeleteMsg(int64(1), defaultCollectionID+1, schemapb.DataType_Int64)
		deleteMsg2 := genDeleteMsg(int64(1), defaultCollectionID, schemapb.DataType_Int64)
		msgChan <- &msgstream.MsgPack{Msgs: []msgstream.TsMsg{deleteMsg1, deleteMsg2}}
	}()

	msgStream.On("Chan").Return(msgChan)
	factory := &mockMsgStreamFactory{mockMqStream: msgStream}
	node, err := genSimpleQueryNodeWithMQFactory(ctx, factory)
	assert.NoError(t, err)

	loader := node.loader
	assert.NotNil(t, loader)

	return loader.FromDmlCPLoadDelete(ctx, defaultCollectionID, position)
}

type mockMsgID struct {
	msgstream.MessageID
	mock.Mock
}

func (m2 *mockMsgID) AtEarliestPosition() bool {
	args := m2.Called()
	return args.Get(0).(bool)
}

func (m2 *mockMsgID) LessOrEqualThan(msgID []byte) (bool, error) {
	args := m2.Called()
	ret := args.Get(0)
	if args.Get(1) != nil {
		return false, args.Get(1).(error)
	}
	return ret.(bool), nil
}

type LoadDeleteMsgStream struct {
	msgstream.MsgStream
	mock.Mock
}

func (ms *LoadDeleteMsgStream) Close() {
}

func (ms *LoadDeleteMsgStream) AsConsumer(channels []string, subName string) {
}

func (ms *LoadDeleteMsgStream) Chan() <-chan *msgstream.MsgPack {
	args := ms.Called()
	return args.Get(0).(chan *msgstream.MsgPack)
}

func (ms *LoadDeleteMsgStream) Seek(offset []*internalpb.MsgPosition) error {
	args := ms.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(error)
}

func (ms *LoadDeleteMsgStream) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	args := ms.Called(channel)
	msg := args.Get(0)
	err := args.Get(1)
	if msg == nil && err == nil {
		return nil, nil
	}

	if msg == nil && err != nil {
		return nil, err.(error)
	}

	if msg != nil && err == nil {
		return msg.(msgstream.MessageID), nil
	}

	return msg.(msgstream.MessageID), err.(error)
}

func (ms *LoadDeleteMsgStream) Start() {}
