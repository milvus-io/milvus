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

package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type InsertNodeSuite struct {
	suite.Suite
	// datas
	collectionName   string
	collectionID     int64
	partitionID      int64
	channel          string
	insertSegmentIDs []int64
	deleteSegmentSum int
	// mocks
	manager   *segments.Manager
	delegator *delegator.MockShardDelegator
}

func (suite *InsertNodeSuite) SetupSuite() {
	paramtable.Init()

	suite.collectionName = "test-collection"
	suite.collectionID = 111
	suite.partitionID = 11
	suite.channel = "test_channel"

	suite.insertSegmentIDs = []int64{4, 3}
	suite.deleteSegmentSum = 2
}

func (suite *InsertNodeSuite) TestBasic() {
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, true)
	in := suite.buildInsertNodeMsg(schema)

	collection, err := segments.NewCollection(suite.collectionID, schema, mock_segcore.GenTestIndexMeta(suite.collectionID, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	}, newTestLocalFileSystem(suite.T()))
	suite.NoError(err)
	collection.AddPartition(suite.partitionID)

	// init mock
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	mockSegmentManager := segments.NewMockSegmentManager(suite.T())

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}

	var transferOrigin func(*schemapb.CollectionSchema, *msgstream.InsertMsg) (*segcorepb.InsertRecord, []int64, error)
	transferMock := mockey.Mock(storage.TransferInsertMsgToInsertRecord).To(func(schema *schemapb.CollectionSchema, msg *msgstream.InsertMsg) (*segcorepb.InsertRecord, []int64, error) {
		suite.True(collection.HasInsertSchemaTransitionReaderForTest())
		return transferOrigin(schema, msg)
	}).Origin(&transferOrigin).Build()
	defer transferMock.UnPatch()

	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegator.EXPECT().ProcessInsert(mock.Anything).Run(func(insertRecords map[int64]*delegator.InsertData) {
		suite.True(collection.HasInsertSchemaTransitionReaderForTest())
		for segID := range insertRecords {
			suite.True(lo.Contains(suite.insertSegmentIDs, segID))
		}
	})

	// TODO mock a delgator for test
	node, err := newInsertNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, schema, 8)
	suite.NoError(err)
	out := node.Operate(in)

	nodeMsg, ok := out.(*deleteNodeMsg)
	suite.True(ok)
	suite.Equal(suite.deleteSegmentSum, len(nodeMsg.deleteMsgs))
}

func (suite *InsertNodeSuite) TestDataTypeNotSupported() {
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, true)
	in := suite.buildInsertNodeMsg(schema)

	collection, err := segments.NewCollection(suite.collectionID, schema, mock_segcore.GenTestIndexMeta(suite.collectionID, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	}, newTestLocalFileSystem(suite.T()))
	suite.NoError(err)
	collection.AddPartition(suite.partitionID)

	// init mock
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	mockSegmentManager := segments.NewMockSegmentManager(suite.T())

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}

	suite.delegator = delegator.NewMockShardDelegator(suite.T())

	for _, msg := range in.insertMsgs {
		for _, field := range msg.GetFieldsData() {
			field.Type = schemapb.DataType_None
		}
	}

	// TODO mock a delgator for test
	node, err := newInsertNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, schema, 8)
	suite.NoError(err)
	suite.Panics(func() {
		node.Operate(in)
	})
}

func (suite *InsertNodeSuite) TestLegacyInsertMaterializesBM25Stats() {
	schema := &schemapb.CollectionSchema{
		Name: suite.collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "1024"},
				},
			},
			{
				FieldID:          102,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
			{
				Name:          "rerank",
				Type:          schemapb.FunctionType_Rerank,
				InputFieldIds: []int64{101},
			},
		},
	}
	in := suite.buildInsertNodeMsg(schema)
	for _, msg := range in.insertMsgs {
		msg.FieldsData = msg.FieldsData[:2]
	}

	collection := segments.NewCollectionWithoutSegcoreForTest(suite.collectionID, schema)
	collection.AddPartition(suite.partitionID)

	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}

	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegator.EXPECT().ProcessInsert(mock.Anything).Run(func(insertRecords map[int64]*delegator.InsertData) {
		for _, insertData := range insertRecords {
			suite.Require().Contains(insertData.BM25Stats, int64(102))
			suite.Equal(int64(2), insertData.BM25Stats[102].NumRow())
		}
	})

	node, err := newInsertNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, schema, 8)
	suite.NoError(err)
	node.Operate(in)
}

func (suite *InsertNodeSuite) buildInsertNodeMsg(schema *schemapb.CollectionSchema) *insertNodeMsg {
	nodeMsg := insertNodeMsg{
		insertMsgs: []*InsertMsg{},
		deleteMsgs: []*DeleteMsg{},
		timeRange: TimeRange{
			timestampMin: 0,
			timestampMax: 0,
		},
	}

	for _, segmentID := range suite.insertSegmentIDs {
		insertMsg := buildInsertMsg(suite.collectionID, suite.partitionID, segmentID, suite.channel, 1)
		insertMsg.FieldsData = genFiledDataWithSchema(schema, 1)
		nodeMsg.insertMsgs = append(nodeMsg.insertMsgs, insertMsg)

		insertMsg = buildInsertMsg(suite.collectionID, suite.partitionID, segmentID, suite.channel, 1)
		insertMsg.FieldsData = genFiledDataWithSchema(schema, 1)
		nodeMsg.insertMsgs = append(nodeMsg.insertMsgs, insertMsg)
	}

	for i := 0; i < suite.deleteSegmentSum; i++ {
		deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionID, suite.channel, 1)
		nodeMsg.deleteMsgs = append(nodeMsg.deleteMsgs, deleteMsg)
	}

	return &nodeMsg
}

func TestInsertNode(t *testing.T) {
	suite.Run(t, new(InsertNodeSuite))
}

const (
	schemaTransitionCollectionID = int64(1000)
	schemaTransitionPartitionID  = int64(1001)
	schemaTransitionSegmentID    = int64(1002)
	schemaTransitionDroppedField = int64(580)
)

func setupSchemaTransitionInsertNodeTest(t *testing.T) (*segments.Manager, *segments.Collection, *schemapb.CollectionSchema, *schemapb.CollectionSchema, *msgstream.InsertMsg) {
	t.Helper()
	paramtable.Init()
	_ = initcore.InitLocalChunkManager(t.Name())
	_ = initcore.InitMmapManager(paramtable.Get(), 1)

	schemaV950 := mock_segcore.GenTestCollectionSchema("schema_transition", schemapb.DataType_Int64, false)
	schemaV950.Version = 950
	schemaV950.Fields = append(schemaV950.Fields, &schemapb.FieldSchema{
		FieldID:  schemaTransitionDroppedField,
		Name:     "stress_extra",
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "max_length", Value: "64"},
		},
	})
	schemaV951 := proto.Clone(schemaV950).(*schemapb.CollectionSchema)
	schemaV951.Version = 951
	schemaV951.Fields = lo.Filter(schemaV951.Fields, func(field *schemapb.FieldSchema, _ int) bool {
		return field.GetFieldID() != schemaTransitionDroppedField
	})

	manager := segments.NewManager()
	require.NoError(t, manager.Collection.PutOrRef(schemaTransitionCollectionID, schemaV950, nil, &querypb.LoadMetaInfo{
		CollectionID: schemaTransitionCollectionID,
		LoadType:     querypb.LoadType_LoadCollection,
	}))
	t.Cleanup(func() {
		manager.Collection.Unref(schemaTransitionCollectionID, 1)
	})

	collection := manager.Collection.Get(schemaTransitionCollectionID)
	require.NotNil(t, collection)
	insertMsg, err := mock_segcore.GenInsertMsg(collection.GetCCollection(), schemaTransitionPartitionID, schemaTransitionSegmentID, 1)
	require.NoError(t, err)
	for _, fieldData := range insertMsg.GetFieldsData() {
		if fieldData.GetFieldId() == schemaTransitionDroppedField {
			fieldData.ValidData = []bool{true}
		}
	}
	return manager, collection, schemaV950, schemaV951, insertMsg
}

func insertIntoNewGrowingSegment(collection *segments.Collection, manager *segments.Manager, segmentID int64, data *delegator.InsertData) error {
	ctx := context.Background()
	growing, err := segments.NewSegment(ctx, collection, manager.Segment, segments.SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
		SegmentID:     segmentID,
		PartitionID:   data.PartitionID,
		CollectionID:  collection.ID(),
		InsertChannel: data.StartPosition.GetChannelName(),
		StartPosition: data.StartPosition,
		DeltaPosition: data.StartPosition,
		Level:         datapb.SegmentLevel_L1,
	})
	if err != nil {
		return err
	}
	defer growing.Release(ctx)
	return growing.Insert(ctx, data.RowIDs, data.Timestamps, data.InsertRecord)
}

func TestInsertNodeBlocksSchemaUpdateUntilGrowingInsertCompletes(t *testing.T) {
	manager, collection, schemaV950, schemaV951, insertMsg := setupSchemaTransitionInsertNodeTest(t)

	converted := make(chan struct{})
	resumeConversion := make(chan struct{})
	var conversionOnce sync.Once
	var releaseOnce sync.Once
	releaseConversion := func() {
		releaseOnce.Do(func() {
			close(resumeConversion)
		})
	}
	var oldFieldConverted, readerHeldDuringConversion bool
	var transferOrigin func(*schemapb.CollectionSchema, *msgstream.InsertMsg) (*segcorepb.InsertRecord, []int64, error)
	transferMock := mockey.Mock(storage.TransferInsertMsgToInsertRecord).To(func(schema *schemapb.CollectionSchema, msg *msgstream.InsertMsg) (*segcorepb.InsertRecord, []int64, error) {
		record, skippedFields, err := transferOrigin(schema, msg)
		conversionOnce.Do(func() {
			oldFieldConverted = lo.ContainsBy(record.GetFieldsData(), func(field *schemapb.FieldData) bool {
				return field.GetFieldId() == schemaTransitionDroppedField
			})
			readerHeldDuringConversion = collection.HasInsertSchemaTransitionReaderForTest()
			close(converted)
			<-resumeConversion
		})
		return record, skippedFields, err
	}).Origin(&transferOrigin).Build()
	t.Cleanup(func() {
		transferMock.UnPatch()
	})

	var (
		insertErr              error
		nativeInsertAttempted  bool
		oldFieldPassedToNative bool
	)
	mockDelegator := delegator.NewMockShardDelegator(t)
	mockDelegator.EXPECT().ProcessInsert(mock.Anything).Run(func(insertRecords map[int64]*delegator.InsertData) {
		insertData, ok := insertRecords[schemaTransitionSegmentID]
		if !ok {
			return
		}
		nativeInsertAttempted = true
		oldFieldPassedToNative = lo.ContainsBy(insertData.InsertRecord.GetFieldsData(), func(field *schemapb.FieldData) bool {
			return field.GetFieldId() == schemaTransitionDroppedField
		})
		insertErr = insertIntoNewGrowingSegment(collection, manager, schemaTransitionSegmentID, insertData)
	})

	node, err := newInsertNode(schemaTransitionCollectionID, insertMsg.GetShardName(), manager, mockDelegator, schemaV950, 8)
	require.NoError(t, err)

	operateDone := make(chan struct{})
	updateDone := make(chan struct{})
	updateErr := make(chan error, 1)
	operateStarted := false
	updateStarted := false
	t.Cleanup(func() {
		releaseConversion()
		if operateStarted {
			select {
			case <-operateDone:
			case <-time.After(5 * time.Second):
				t.Error("insert did not stop during test cleanup")
			}
		}
		if updateStarted {
			select {
			case <-updateDone:
			case <-time.After(5 * time.Second):
				t.Error("schema update did not stop during test cleanup")
			}
		}
	})

	operateStarted = true
	go func() {
		defer close(operateDone)
		node.Operate(&insertNodeMsg{insertMsgs: []*InsertMsg{insertMsg}})
	}()

	select {
	case <-converted:
	case <-time.After(5 * time.Second):
		t.Fatal("insert did not complete old-schema payload conversion")
	}
	require.True(t, oldFieldConverted, "the paused payload must retain the old-schema field to exercise the native race")
	require.True(t, readerHeldDuringConversion, "payload conversion must run inside the schema transition reader")

	updateStarted = true
	go func() {
		defer close(updateDone)
		updateErr <- manager.Collection.UpdateSchema(schemaTransitionCollectionID, schemaV951, 951)
	}()
	require.True(t, collection.WaitForSchemaTransitionWriterForTest(5*time.Second), "schema writer did not queue behind old-schema payload conversion")

	releaseConversion()

	select {
	case <-operateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("insert did not complete after payload conversion resumed")
	}
	require.True(t, nativeInsertAttempted, "old-schema payload did not reach native growing insertion")
	require.True(t, oldFieldPassedToNative, "native growing insertion did not receive the old-schema field")
	require.NoError(t, insertErr)

	select {
	case <-updateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("schema update did not continue after growing insert completed")
	}
	require.NoError(t, <-updateErr)
}

func TestInsertNodeSkipsDroppedFieldAfterSchemaUpdate(t *testing.T) {
	manager, collection, schemaV950, schemaV951, insertMsg := setupSchemaTransitionInsertNodeTest(t)
	require.NoError(t, manager.Collection.UpdateSchema(schemaTransitionCollectionID, schemaV951, 951))

	var (
		insertErr        error
		droppedFieldSeen bool
	)
	mockDelegator := delegator.NewMockShardDelegator(t)
	mockDelegator.EXPECT().ProcessInsert(mock.Anything).Run(func(insertRecords map[int64]*delegator.InsertData) {
		for segmentID, insertData := range insertRecords {
			droppedFieldSeen = lo.ContainsBy(insertData.InsertRecord.GetFieldsData(), func(field *schemapb.FieldData) bool {
				return field.GetFieldId() == schemaTransitionDroppedField
			})
			insertErr = insertIntoNewGrowingSegment(collection, manager, segmentID, insertData)
		}
	})

	node, err := newInsertNode(schemaTransitionCollectionID, insertMsg.GetShardName(), manager, mockDelegator, schemaV950, 8)
	require.NoError(t, err)
	node.Operate(&insertNodeMsg{insertMsgs: []*InsertMsg{insertMsg}})

	require.False(t, droppedFieldSeen)
	require.NoError(t, insertErr)
}
