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
	"log"
	"math"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/storage"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

//-------------------------------------------------------------------------------------- constructor and destructor
func TestSegment_newSegment(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collectionMeta := genCollectionMeta(collectionID, schema)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Nil(t, err)
	assert.Equal(t, segmentID, segment.segmentID)
	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test invalid type", func(t *testing.T) {
		_, err = newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			collectionID, "", 100, true)
		assert.Error(t, err)
	})
}

func TestSegment_deleteSegment(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collectionMeta := genCollectionMeta(collectionID, schema)

	collection := newCollection(collectionMeta.ID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test delete nil ptr", func(t *testing.T) {
		s, err := genSimpleSealedSegment(defaultMsgLength)
		assert.NoError(t, err)
		s.segmentPtr = nil
		deleteSegment(s)
	})
}

//-------------------------------------------------------------------------------------- stats functions
func TestSegment_getRowCount(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)

	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offset, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	err = segment.segmentInsert(offset, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)

	rowCount := segment.getRowCount()
	assert.Equal(t, int64(defaultMsgLength), rowCount)

	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test getRowCount nil ptr", func(t *testing.T) {
		s, err := genSimpleSealedSegment(defaultMsgLength)
		assert.NoError(t, err)
		s.segmentPtr = nil
		res := s.getRowCount()
		assert.Equal(t, int64(-1), res)
	})
}

func TestSegment_retrieve(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offset, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.Equal(t, offset, int64(0))
	err = segment.segmentInsert(offset, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  simpleInt32Field.id,
							DataType: simpleInt32Field.dataType,
						},
						Values: []*planpb.GenericValue{
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 1,
								},
							},
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 2,
								},
							},
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 3,
								},
							},
						},
					},
				},
			},
		},
		OutputFieldIds: []FieldID{simpleInt32Field.id},
	}
	// reqIds := &segcorepb.RetrieveRequest{
	// 	Ids: &schemapb.IDs{
	// 		IdField: &schemapb.IDs_IntId{
	// 			IntId: &schemapb.LongArray{
	// 				Data: []int64{2, 3, 1},
	// 			},
	// 		},
	// 	},
	// 	OutputFieldsId: []int64{100},
	// }
	planExpr, err := proto.Marshal(planNode)
	assert.NoError(t, err)
	plan, err := createRetrievePlanByExpr(collection, planExpr, 100)
	defer plan.delete()
	assert.NoError(t, err)

	res, err := segment.retrieve(plan)
	assert.NoError(t, err)

	assert.Equal(t, res.GetFieldsData()[0].GetScalars().Data.(*schemapb.ScalarField_IntData).IntData.Data, []int32{1, 2, 3})
}

func TestSegment_getDeletedCount(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offsetInsert, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	pks, err := getPKs(insertMsg, collection.schema)
	assert.NoError(t, err)
	err = segment.segmentDelete(offsetDelete, pks, insertMsg.Timestamps)
	assert.NoError(t, err)

	var deletedCount = segment.getDeletedCount()
	// TODO: assert.Equal(t, deletedCount, len(ids))
	assert.Equal(t, deletedCount, int64(0))

	deleteCollection(collection)

	t.Run("test getDeletedCount nil ptr", func(t *testing.T) {
		s, err := genSimpleSealedSegment(defaultMsgLength)
		assert.NoError(t, err)
		s.segmentPtr = nil
		res := s.getDeletedCount()
		assert.Equal(t, int64(-1), res)
	})
}

func TestSegment_getMemSize(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offsetInsert, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)

	var memSize = segment.getMemSize()
	assert.Equal(t, memSize, int64(18776064))

	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- dm & search functions
func TestSegment_segmentInsert(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)

	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)
	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offsetInsert, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)
	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test nil segment", func(t *testing.T) {
		segment, err := genSimpleSealedSegment(defaultMsgLength)
		assert.NoError(t, err)
		segment.setType(segmentTypeGrowing)
		segment.segmentPtr = nil
		err = segment.segmentInsert(0, nil, nil, nil)
		assert.Error(t, err)
	})

	t.Run("test invalid segment type", func(t *testing.T) {
		segment, err := genSimpleSealedSegment(defaultMsgLength)
		assert.NoError(t, err)
		err = segment.segmentInsert(0, nil, nil, nil)
		assert.NoError(t, err)
	})
}

func TestSegment_segmentDelete(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offsetInsert, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	pks, err := getPKs(insertMsg, schema)
	assert.NoError(t, err)
	err = segment.segmentDelete(offsetDelete, pks, insertMsg.Timestamps)
	assert.NoError(t, err)

	deleteCollection(collection)
}

func TestSegment_segmentSearch(t *testing.T) {
	nq := int64(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := genSimpleQueryNode(ctx)
	assert.NoError(t, err)

	collection, err := node.historical.replica.getCollectionByID(defaultCollectionID)
	assert.NoError(t, err)

	segment, err := node.historical.replica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	// TODO: replace below by genPlaceholderGroup(nq)
	vec := generateFloatVectors(1, defaultDim)
	var searchRawData []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData = append(searchRawData, buf...)
	}

	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: [][]byte{},
	}

	for i := 0; i < int(nq); i++ {
		placeholderValue.Values = append(placeholderValue.Values, searchRawData)
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	dslString := "{\"bool\": { \n\"vector\": {\n \"floatVectorField\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"

	plan, err := createSearchPlan(collection, dslString)
	assert.NoError(t, err)
	holder, err := parseSearchRequest(plan, placeGroupByte)
	assert.NoError(t, err)

	placeholderGroups := make([]*searchRequest, 0)
	placeholderGroups = append(placeholderGroups, holder)

	searchResult, err := segment.search(plan, placeholderGroups, []Timestamp{0})
	assert.NoError(t, err)

	err = checkSearchResult(nq, plan, searchResult)
	assert.NoError(t, err)

	plan.delete()
	holder.delete()
	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- preDm functions
func TestSegment_segmentPreInsert(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	offset, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_segmentPreDelete(t *testing.T) {
	collectionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment, err := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Nil(t, err)

	insertMsg, err := genSimpleInsertMsg(schema, defaultMsgLength)
	assert.NoError(t, err)
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: insertMsg.FieldsData,
		NumRows:    int64(insertMsg.NumRows),
	}

	offsetInsert, err := segment.segmentPreInsert(defaultMsgLength)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_segmentLoadDeletedRecord(t *testing.T) {
	fieldParam := constFieldParam{
		id:       100,
		dataType: schemapb.DataType_Int64,
	}
	field := genPKFieldSchema(fieldParam)
	schema := &schemapb.CollectionSchema{
		Name:   defaultCollectionName,
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			field,
		},
	}

	seg, err := newSegment(newCollection(defaultCollectionID, schema),
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultDMLChannel,
		segmentTypeSealed,
		true)
	assert.Nil(t, err)
	ids := []int64{1, 2, 3}
	pks := make([]primaryKey, 0)
	for _, id := range ids {
		pks = append(pks, newInt64PrimaryKey(id))
	}
	timestamps := []Timestamp{10, 10, 10}
	var rowCount int64 = 3
	err = seg.segmentLoadDeletedRecord(pks, timestamps, rowCount)
	assert.NoError(t, err)
}

func TestSegment_segmentLoadFieldData(t *testing.T) {
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	_, err := genSealedSegment(schema,
		defaultCollectionID,
		defaultPartitionID,
		defaultSegmentID,
		defaultDMLChannel,
		defaultMsgLength)
	assert.NoError(t, err)
}

func TestSegment_ConcurrentOperation(t *testing.T) {
	const N = 16
	var ages = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	ageData := &schemapb.FieldData{
		Type:    simpleInt32Field.dataType,
		FieldId: simpleInt32Field.id,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: ages,
					},
				},
			},
		},
	}

	collectionID := UniqueID(0)
	partitionID := UniqueID(0)
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := newCollection(collectionID, schema)
	assert.Equal(t, collection.ID(), collectionID)

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		segmentID := UniqueID(i)
		segment, err := newSegment(collection, segmentID, partitionID, collectionID, "", segmentTypeSealed, true)
		assert.Equal(t, segmentID, segment.segmentID)
		assert.Equal(t, partitionID, segment.partitionID)
		assert.Nil(t, err)

		wg.Add(2)
		go func() {
			deleteSegment(segment)
			wg.Done()
		}()
		go func() {
			// segmentLoadFieldData result error may be nil or not, we just expected this test would not crash.
			_ = segment.segmentLoadFieldData(simpleInt32Field.id, N, ageData)
			wg.Done()
		}()
	}
	wg.Wait()
	deleteCollection(collection)
}

func TestSegment_indexInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSafe := newTSafeReplica()
	h, err := genSimpleHistorical(ctx, tSafe)
	assert.NoError(t, err)

	seg, err := h.replica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	fieldID := simpleFloatVecField.id

	indexName := "query-node-test-index"
	indexParam := make(map[string]string)
	indexParam["index_type"] = "IVF_PQ"
	indexParam["index_mode"] = "cpu"
	indexPaths := []string{"query-node-test-index-path"}
	indexID := UniqueID(0)
	buildID := UniqueID(0)

	indexInfo := &querypb.FieldIndexInfo{
		IndexName:      indexName,
		IndexParams:    funcutil.Map2KeyValuePair(indexParam),
		IndexFilePaths: indexPaths,
		IndexID:        indexID,
		BuildID:        buildID,
	}

	seg.setIndexedFieldInfo(fieldID, &IndexedFieldInfo{indexInfo: indexInfo})

	fieldInfo, err := seg.getIndexedFieldInfo(fieldID)
	assert.Nil(t, err)
	info := fieldInfo.indexInfo
	assert.Equal(t, indexName, info.IndexName)
	params := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	assert.Equal(t, len(indexParam), len(params))
	assert.Equal(t, indexParam["index_type"], params["index_type"])
	assert.Equal(t, indexParam["index_mode"], params["index_mode"])
	assert.Equal(t, len(indexPaths), len(info.IndexFilePaths))
	assert.Equal(t, indexPaths[0], info.IndexFilePaths[0])
	assert.Equal(t, indexID, info.IndexID)
	assert.Equal(t, buildID, info.BuildID)
}

func TestSegment_BasicMetrics(t *testing.T) {
	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := newCollection(defaultCollectionID, schema)
	segment, err := newSegment(collection,
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultDMLChannel,
		segmentTypeSealed,
		true)
	assert.Nil(t, err)

	t.Run("test id binlog row size", func(t *testing.T) {
		size := int64(1024)
		segment.setIDBinlogRowSizes([]int64{size})
		sizes := segment.getIDBinlogRowSizes()
		assert.Len(t, sizes, 1)
		assert.Equal(t, size, sizes[0])
	})

	t.Run("test type", func(t *testing.T) {
		sType := segmentTypeGrowing
		segment.setType(sType)
		resType := segment.getType()
		assert.Equal(t, sType, resType)
	})

	t.Run("test onService", func(t *testing.T) {
		segment.setOnService(false)
		resOnService := segment.getOnService()
		assert.Equal(t, false, resOnService)
	})

	t.Run("test IndexedFieldInfo", func(t *testing.T) {
		fieldID := rowIDFieldID
		info := &IndexedFieldInfo{
			fieldBinlog: &datapb.FieldBinlog{
				FieldID: fieldID,
				Binlogs: []*datapb.Binlog{},
			},
		}
		segment.setIndexedFieldInfo(fieldID, info)
		resInfo, err := segment.getIndexedFieldInfo(fieldID)
		assert.NoError(t, err)
		assert.Equal(t, info, resInfo)

		_, err = segment.getIndexedFieldInfo(FieldID(1000))
		assert.Error(t, err)
	})
}

func TestSegment_fillIndexedFieldsData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pkType := schemapb.DataType_Int64
	schema := genTestCollectionSchema(pkType)
	collection := newCollection(defaultCollectionID, schema)
	segment, err := newSegment(collection,
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultDMLChannel,
		segmentTypeSealed,
		true)
	assert.Nil(t, err)

	vecCM, err := genVectorChunkManager(ctx, collection)
	assert.NoError(t, err)

	t.Run("test fillIndexedFieldsData float-vector invalid vectorChunkManager", func(t *testing.T) {
		fieldID := FieldID(100)
		fieldName := "float-vector-field-0"
		info := &IndexedFieldInfo{
			fieldBinlog: &datapb.FieldBinlog{
				FieldID: fieldID,
				Binlogs: []*datapb.Binlog{},
			},
			indexInfo: &querypb.FieldIndexInfo{EnableIndex: true},
		}
		segment.setIndexedFieldInfo(fieldID, info)
		fieldData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: fieldName,
				FieldId:   fieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: defaultDim,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1.1, 2.2, 3.3, 4.4},
							},
						},
					},
				},
			},
		}
		result := &segcorepb.RetrieveResults{
			Ids:        &schemapb.IDs{},
			Offset:     []int64{0},
			FieldsData: fieldData,
		}
		err = segment.fillIndexedFieldsData(defaultCollectionID, vecCM, result)
		assert.Error(t, err)
	})
}

func Test_getFieldDataPath(t *testing.T) {
	indexedFieldInfo := &IndexedFieldInfo{
		fieldBinlog: &datapb.FieldBinlog{
			FieldID: 0,
			Binlogs: []*datapb.Binlog{
				{
					LogPath: funcutil.GenRandomStr(),
				},
				{
					LogPath: funcutil.GenRandomStr(),
				},
			},
		},
	}
	s := &Segment{
		idBinlogRowSizes: []int64{10, 15},
	}

	path, offsetInBinlog := s.getFieldDataPath(indexedFieldInfo, 4)
	assert.Equal(t, indexedFieldInfo.fieldBinlog.Binlogs[0].LogPath, path)
	assert.Equal(t, int64(4), offsetInBinlog)

	path, offsetInBinlog = s.getFieldDataPath(indexedFieldInfo, 11)
	assert.Equal(t, indexedFieldInfo.fieldBinlog.Binlogs[1].LogPath, path)
	assert.Equal(t, int64(1), offsetInBinlog)
}

func Test_fillBinVecFieldData(t *testing.T) {
	var m storage.ChunkManager

	m = newMockChunkManager(withDefaultReadAt())

	f := newBinaryVectorFieldData(simpleBinVecField.fieldName, 1, 8)

	path := funcutil.GenRandomStr()
	index := 0
	offset := int64(100)
	endian := common.Endian

	assert.NoError(t, fillBinVecFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillBinVecFieldData(m, path, f, index, offset, endian))
}

func Test_fillFloatVecFieldData(t *testing.T) {
	var m storage.ChunkManager

	m = newMockChunkManager(withDefaultReadAt())

	f := newFloatVectorFieldData(simpleFloatVecField.fieldName, 1, 8)

	path := funcutil.GenRandomStr()
	index := 0
	offset := int64(100)
	endian := common.Endian

	assert.NoError(t, fillFloatVecFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillFloatVecFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillFloatVecFieldData(m, path, f, index, offset, endian))
}

func Test_fillBoolFieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withReadBool(offset))

	f := newScalarFieldData(schemapb.DataType_Bool, simpleBoolField.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillBoolFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadErr())
	assert.Error(t, fillBoolFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadIllegalBool())
	assert.Error(t, fillBoolFieldData(m, path, f, index, offset, endian))
}

func Test_fillStringFieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withReadString(offset))

	f := newScalarFieldData(schemapb.DataType_VarChar, simpleVarCharField.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillStringFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadErr())
	assert.Error(t, fillStringFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadIllegalString())
	assert.Error(t, fillStringFieldData(m, path, f, index, offset, endian))
}

func Test_fillInt8FieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withDefaultReadAt())

	f := newScalarFieldData(schemapb.DataType_Int8, simpleInt8Field.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillInt8FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillInt8FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillInt8FieldData(m, path, f, index, offset, endian))
}

func Test_fillInt16FieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withDefaultReadAt())

	f := newScalarFieldData(schemapb.DataType_Int16, simpleInt64Field.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillInt16FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillInt16FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillInt16FieldData(m, path, f, index, offset, endian))
}

func Test_fillInt32FieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withDefaultReadAt())

	f := newScalarFieldData(schemapb.DataType_Int32, simpleInt32Field.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillInt32FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillInt32FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillInt32FieldData(m, path, f, index, offset, endian))
}

func Test_fillInt64FieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withDefaultReadAt())

	f := newScalarFieldData(schemapb.DataType_Int64, simpleInt64Field.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillInt64FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillInt64FieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillInt64FieldData(m, path, f, index, offset, endian))
}

func Test_fillFloatFieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withDefaultReadAt())

	f := newScalarFieldData(schemapb.DataType_Float, simpleFloatField.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillFloatFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillFloatFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillFloatFieldData(m, path, f, index, offset, endian))
}

func Test_fillDoubleFieldData(t *testing.T) {
	var m storage.ChunkManager

	offset := int64(100)
	m = newMockChunkManager(withDefaultReadAt())

	f := newScalarFieldData(schemapb.DataType_Double, simpleDoubleField.fieldName, 1)

	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	assert.NoError(t, fillDoubleFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtErr())
	assert.Error(t, fillDoubleFieldData(m, path, f, index, offset, endian))

	m = newMockChunkManager(withReadAtEmptyContent())
	assert.Error(t, fillDoubleFieldData(m, path, f, index, offset, endian))
}

func Test_fillFieldData(t *testing.T) {
	var m storage.ChunkManager

	fs := []*schemapb.FieldData{
		newBinaryVectorFieldData(simpleBinVecField.fieldName, 1, 8),
		newFloatVectorFieldData(simpleFloatVecField.fieldName, 1, 8),
		newScalarFieldData(schemapb.DataType_Bool, simpleBoolField.fieldName, 1),
		newScalarFieldData(schemapb.DataType_VarChar, simpleVarCharField.fieldName, 1),
		newScalarFieldData(schemapb.DataType_Int8, simpleInt8Field.fieldName, 1),
		newScalarFieldData(schemapb.DataType_Int16, simpleInt16Field.fieldName, 1),
		newScalarFieldData(schemapb.DataType_Int32, simpleInt32Field.fieldName, 1),
		newScalarFieldData(schemapb.DataType_Int64, simpleInt64Field.fieldName, 1),
		newScalarFieldData(schemapb.DataType_Float, simpleFloatField.fieldName, 1),
		newScalarFieldData(schemapb.DataType_Double, simpleDoubleField.fieldName, 1),
	}

	offset := int64(100)
	path := funcutil.GenRandomStr()
	index := 0
	endian := common.Endian

	for _, f := range fs {
		if f.Type == schemapb.DataType_Bool {
			m = newMockChunkManager(withReadBool(offset))
		} else if funcutil.SliceContain([]schemapb.DataType{
			schemapb.DataType_String,
			schemapb.DataType_VarChar,
		}, f.Type) {
			m = newMockChunkManager(withReadString(offset))
		} else {
			m = newMockChunkManager(withDefaultReadAt())
		}

		assert.NoError(t, fillFieldData(m, path, f, index, offset, endian))
	}

	assert.Error(t, fillFieldData(m, path, &schemapb.FieldData{Type: schemapb.DataType_None}, index, offset, endian))
}

func TestUpdateBloomFilter(t *testing.T) {
	t.Run("test int64 pk", func(t *testing.T) {
		historical, err := genSimpleReplica()
		assert.NoError(t, err)
		err = historical.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeSealed,
			true)
		assert.NoError(t, err)
		seg, err := historical.getSegmentByID(defaultSegmentID)
		assert.Nil(t, err)
		pkValues := []int64{1, 2}
		pks := make([]primaryKey, len(pkValues))
		for index, v := range pkValues {
			pks[index] = newInt64PrimaryKey(v)
		}
		seg.updateBloomFilter(pks)
		buf := make([]byte, 8)
		for _, v := range pkValues {
			common.Endian.PutUint64(buf, uint64(v))
			assert.True(t, seg.pkFilter.Test(buf))
		}
	})
	t.Run("test string pk", func(t *testing.T) {
		historical, err := genSimpleReplica()
		assert.NoError(t, err)
		err = historical.addSegment(defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultDMLChannel,
			segmentTypeSealed,
			true)
		assert.NoError(t, err)
		seg, err := historical.getSegmentByID(defaultSegmentID)
		assert.Nil(t, err)
		pkValues := []string{"test1", "test2"}
		pks := make([]primaryKey, len(pkValues))
		for index, v := range pkValues {
			pks[index] = newVarCharPrimaryKey(v)
		}
		seg.updateBloomFilter(pks)
		for _, v := range pkValues {
			assert.True(t, seg.pkFilter.TestString(v))
		}
	})

}
