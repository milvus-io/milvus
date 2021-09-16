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
	"encoding/binary"
	"log"
	"math"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
)

//-------------------------------------------------------------------------------------- constructor and destructor
func TestSegment_newSegment(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)
	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test invalid type", func(t *testing.T) {
		s := newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			collectionID, "", segmentTypeInvalid, true)
		assert.Nil(t, s)
		s = newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			collectionID, "", 100, true)
		assert.Nil(t, s)
	})
}

func TestSegment_deleteSegment(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test delete nil ptr", func(t *testing.T) {
		s, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		s.segmentPtr = nil
		deleteSegment(s)
	})
}

//-------------------------------------------------------------------------------------- stats functions
func TestSegment_getRowCount(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []Timestamp{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offset, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	err = segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	rowCount := segment.getRowCount()
	assert.Equal(t, int64(N), rowCount)

	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test getRowCount nil ptr", func(t *testing.T) {
		s, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		s.segmentPtr = nil
		res := s.getRowCount()
		assert.Equal(t, int64(-1), res)
	})
}

func TestSegment_retrieve(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{}
	timestamps := []Timestamp{}
	const DIM = 16
	const N = 100
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		ids = append(ids, int64(i))
		timestamps = append(timestamps, 0)
		var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		var rawData []byte
		for _, ele := range vec {
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i)*float32(N)))
			rawData = append(rawData, buf...)
		}
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(i+1))
		rawData = append(rawData, bs...)
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}
	offset, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.Equal(t, offset, int64(0))
	err = segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  101,
							DataType: schemapb.DataType_Int32,
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
		OutputFieldIds: []FieldID{101},
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

	res, err := segment.getEntityByIds(plan)
	assert.NoError(t, err)

	assert.Equal(t, res.GetFieldsData()[0].GetScalars().Data.(*schemapb.ScalarField_IntData).IntData.Data, []int32{1, 2, 3})
}

func TestSegment_getDeletedCount(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offsetInsert, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	err = segment.segmentDelete(offsetDelete, &ids, &timestamps)
	assert.NoError(t, err)

	var deletedCount = segment.getDeletedCount()
	// TODO: assert.Equal(t, deletedCount, len(ids))
	assert.Equal(t, deletedCount, int64(0))

	deleteCollection(collection)

	t.Run("test getDeletedCount nil ptr", func(t *testing.T) {
		s, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		s.segmentPtr = nil
		res := s.getDeletedCount()
		assert.Equal(t, int64(-1), res)
	})
}

func TestSegment_getMemSize(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offset, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	err = segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var memSize = segment.getMemSize()
	assert.Equal(t, memSize, int64(2785280))

	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- dm & search functions
func TestSegment_segmentInsert(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)
	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offset, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	err = segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)
	deleteSegment(segment)
	deleteCollection(collection)

	t.Run("test nil segment", func(t *testing.T) {
		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		segment.setType(segmentTypeGrowing)
		segment.segmentPtr = nil
		err = segment.segmentInsert(0, nil, nil, nil)
		assert.Error(t, err)
	})

	t.Run("test invalid segment type", func(t *testing.T) {
		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		err = segment.segmentInsert(0, nil, nil, nil)
		assert.NoError(t, err)
	})
}

func TestSegment_segmentDelete(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offsetInsert, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	err = segment.segmentDelete(offsetDelete, &ids, &timestamps)
	assert.NoError(t, err)

	deleteCollection(collection)
}

func TestSegment_segmentSearch(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offset, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	err = segment.segmentInsert(offset, &ids, &timestamps, &records)
	assert.NoError(t, err)

	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"

	var searchRawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		searchRawData = append(searchRawData, buf...)
	}
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: [][]byte{searchRawData},
	}

	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}

	placeHolderGroupBlob, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	travelTimestamp := Timestamp(1020)
	plan, err := createSearchPlan(collection, dslString)
	assert.NoError(t, err)
	holder, err := parseSearchRequest(plan, placeHolderGroupBlob)
	assert.NoError(t, err)
	placeholderGroups := make([]*searchRequest, 0)
	placeholderGroups = append(placeholderGroups, holder)

	searchResults := make([]*SearchResult, 0)
	searchResult, err := segment.search(plan, placeholderGroups, []Timestamp{travelTimestamp})
	assert.Nil(t, err)
	searchResults = append(searchResults, searchResult)

	///////////////////////////////////
	numSegment := int64(len(searchResults))
	err = reduceSearchResultsAndFillData(plan, searchResults, numSegment)
	assert.NoError(t, err)
	marshaledHits, err := reorganizeSearchResults(searchResults, numSegment)
	assert.NoError(t, err)
	hitsBlob, err := marshaledHits.getHitsBlob()
	assert.NoError(t, err)

	var placeHolderOffset int64 = 0
	for index := range placeholderGroups {
		hitBlobSizePeerQuery, err := marshaledHits.hitBlobSizeInGroup(int64(index))
		assert.NoError(t, err)
		hits := make([][]byte, 0)
		for _, len := range hitBlobSizePeerQuery {
			hits = append(hits, hitsBlob[placeHolderOffset:placeHolderOffset+len])
			placeHolderOffset += len
		}
	}

	deleteSearchResults(searchResults)
	deleteMarshaledHits(marshaledHits)
	///////////////////////////////////

	plan.delete()
	holder.delete()
	deleteSegment(segment)
	deleteCollection(collection)
}

//-------------------------------------------------------------------------------------- preDm functions
func TestSegment_segmentPreInsert(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offset, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offset, int64(0))

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_segmentPreDelete(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	segment := newSegment(collection, segmentID, defaultPartitionID, collectionID, "", segmentTypeGrowing, true)
	assert.Equal(t, segmentID, segment.segmentID)

	ids := []int64{1, 2, 3}
	timestamps := []uint64{0, 0, 0}

	const DIM = 16
	const N = 3
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}

	offsetInsert, err := segment.segmentPreInsert(N)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, offsetInsert, int64(0))

	err = segment.segmentInsert(offsetInsert, &ids, &timestamps, &records)
	assert.NoError(t, err)

	var offsetDelete = segment.segmentPreDelete(10)
	assert.GreaterOrEqual(t, offsetDelete, int64(0))

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_segmentLoadFieldData(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	segmentID := UniqueID(0)
	partitionID := UniqueID(0)
	segment := newSegment(collection, segmentID, partitionID, collectionID, "", segmentTypeSealed, true)
	assert.Equal(t, segmentID, segment.segmentID)
	assert.Equal(t, partitionID, segment.partitionID)

	const N = 16
	var ages = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	err := segment.segmentLoadFieldData(101, N, ages)
	assert.NoError(t, err)

	deleteSegment(segment)
	deleteCollection(collection)
}

func TestSegment_ConcurrentOperation(t *testing.T) {
	const N = 16
	var ages = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	collectionID := UniqueID(0)
	partitionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)
	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		segmentID := UniqueID(i)
		segment := newSegment(collection, segmentID, partitionID, collectionID, "", segmentTypeSealed, true)
		assert.Equal(t, segmentID, segment.segmentID)
		assert.Equal(t, partitionID, segment.partitionID)

		wg.Add(2)
		go func() {
			deleteSegment(segment)
			wg.Done()
		}()
		go func() {
			// segmentLoadFieldData result error may be nil or not, we just expected this test would not crash.
			_ = segment.segmentLoadFieldData(101, N, ages)
			wg.Done()
		}()
	}
	wg.Wait()
	deleteCollection(collection)
}

func TestSegment_indexInfoTest(t *testing.T) {
	t.Run("Test_valid", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		seg, err := h.replica.getSegmentByID(defaultSegmentID)
		assert.NoError(t, err)

		fieldID := simpleVecField.id

		err = seg.setIndexInfo(fieldID, &indexInfo{})
		assert.NoError(t, err)

		indexName := "query-node-test-index"
		err = seg.setIndexName(fieldID, indexName)
		assert.NoError(t, err)
		name := seg.getIndexName(fieldID)
		assert.Equal(t, indexName, name)

		indexParam := make(map[string]string)
		indexParam["index_type"] = "IVF_PQ"
		indexParam["index_mode"] = "cpu"
		err = seg.setIndexParam(fieldID, indexParam)
		assert.NoError(t, err)
		param := seg.getIndexParams(fieldID)
		assert.Equal(t, len(indexParam), len(param))
		assert.Equal(t, indexParam["index_type"], param["index_type"])
		assert.Equal(t, indexParam["index_mode"], param["index_mode"])

		indexPaths := []string{"query-node-test-index-path"}
		err = seg.setIndexPaths(fieldID, indexPaths)
		assert.NoError(t, err)
		paths := seg.getIndexPaths(fieldID)
		assert.Equal(t, len(indexPaths), len(paths))
		assert.Equal(t, indexPaths[0], paths[0])

		indexID := UniqueID(0)
		err = seg.setIndexID(fieldID, indexID)
		assert.NoError(t, err)
		id := seg.getIndexID(fieldID)
		assert.Equal(t, indexID, id)

		buildID := UniqueID(0)
		err = seg.setBuildID(fieldID, buildID)
		assert.NoError(t, err)
		id = seg.getBuildID(fieldID)
		assert.Equal(t, buildID, id)

		// TODO: add match index test
	})

	t.Run("Test_invalid", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h, err := genSimpleHistorical(ctx)
		assert.NoError(t, err)

		seg, err := h.replica.getSegmentByID(defaultSegmentID)
		assert.NoError(t, err)

		fieldID := simpleVecField.id

		indexName := "query-node-test-index"
		err = seg.setIndexName(fieldID, indexName)
		assert.Error(t, err)
		name := seg.getIndexName(fieldID)
		assert.Equal(t, "", name)

		indexParam := make(map[string]string)
		indexParam["index_type"] = "IVF_PQ"
		indexParam["index_mode"] = "cpu"
		err = seg.setIndexParam(fieldID, indexParam)
		assert.Error(t, err)
		err = seg.setIndexParam(fieldID, nil)
		assert.Error(t, err)
		param := seg.getIndexParams(fieldID)
		assert.Nil(t, param)

		indexPaths := []string{"query-node-test-index-path"}
		err = seg.setIndexPaths(fieldID, indexPaths)
		assert.Error(t, err)
		paths := seg.getIndexPaths(fieldID)
		assert.Nil(t, paths)

		indexID := UniqueID(0)
		err = seg.setIndexID(fieldID, indexID)
		assert.Error(t, err)
		id := seg.getIndexID(fieldID)
		assert.Equal(t, int64(-1), id)

		buildID := UniqueID(0)
		err = seg.setBuildID(fieldID, buildID)
		assert.Error(t, err)
		id = seg.getBuildID(fieldID)
		assert.Equal(t, int64(-1), id)

		err = seg.setIndexInfo(fieldID, &indexInfo{
			readyLoad: true,
		})
		assert.NoError(t, err)

		ready := seg.checkIndexReady(fieldID)
		assert.True(t, ready)
		ready = seg.checkIndexReady(FieldID(1000))
		assert.False(t, ready)

		seg.indexInfos = nil
		err = seg.setIndexInfo(fieldID, &indexInfo{
			readyLoad: true,
		})
		assert.Error(t, err)
	})
}

func TestSegment_BasicMetrics(t *testing.T) {
	schema := genSimpleSegCoreSchema()
	collection := newCollection(defaultCollectionID, schema)
	segment := newSegment(collection,
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultVChannel,
		segmentTypeSealed,
		true)

	t.Run("test enable index", func(t *testing.T) {
		segment.setEnableIndex(true)
		enable := segment.getEnableIndex()
		assert.True(t, enable)
	})

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

	t.Run("test VectorFieldInfo", func(t *testing.T) {
		fieldID := rowIDFieldID
		info := &VectorFieldInfo{
			fieldBinlog: &datapb.FieldBinlog{
				FieldID: fieldID,
				Binlogs: []string{},
			},
		}
		segment.setVectorFieldInfo(fieldID, info)
		resInfo, err := segment.getVectorFieldInfo(fieldID)
		assert.NoError(t, err)
		assert.Equal(t, info, resInfo)

		_, err = segment.getVectorFieldInfo(FieldID(1000))
		assert.Error(t, err)
	})
}

func TestSegment_fillVectorFieldsData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := genSimpleSegCoreSchema()
	collection := newCollection(defaultCollectionID, schema)
	segment := newSegment(collection,
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultVChannel,
		segmentTypeSealed,
		true)

	vecCM, err := genVectorChunkManager(ctx)
	assert.NoError(t, err)

	t.Run("test fillVectorFieldsData float-vector invalid vectorChunkManager", func(t *testing.T) {
		fieldID := FieldID(100)
		fieldName := "float-vector-field-0"
		err = segment.setIndexInfo(fieldID, &indexInfo{})
		assert.NoError(t, err)
		info := &VectorFieldInfo{
			fieldBinlog: &datapb.FieldBinlog{
				FieldID: fieldID,
				Binlogs: []string{},
			},
		}
		segment.setVectorFieldInfo(fieldID, info)
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
		err = segment.fillVectorFieldsData(defaultCollectionID, vecCM, result)
		assert.Error(t, err)
	})
}

func TestSegment_indexParam(t *testing.T) {
	schema := genSimpleSegCoreSchema()
	collection := newCollection(defaultCollectionID, schema)
	segment := newSegment(collection,
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultVChannel,
		segmentTypeSealed,
		true)

	t.Run("test indexParam", func(t *testing.T) {
		fieldID := rowIDFieldID
		iParam := genSimpleIndexParams()
		segment.indexInfos[fieldID] = &indexInfo{}
		err := segment.setIndexParam(fieldID, iParam)
		assert.NoError(t, err)
		_ = segment.getIndexParams(fieldID)
		match := segment.matchIndexParam(fieldID, iParam)
		assert.True(t, match)
		match = segment.matchIndexParam(FieldID(1000), nil)
		assert.False(t, match)
	})
}

func TestSegment_dropFieldData(t *testing.T) {
	t.Run("test dropFieldData", func(t *testing.T) {
		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		segment.setType(segmentTypeIndexing)
		err = segment.dropFieldData(simpleVecField.id)
		assert.NoError(t, err)
	})

	t.Run("test nil segment", func(t *testing.T) {
		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		segment.segmentPtr = nil
		err = segment.dropFieldData(simpleVecField.id)
		assert.Error(t, err)
	})

	t.Run("test invalid segment type", func(t *testing.T) {
		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		err = segment.dropFieldData(simpleVecField.id)
		assert.Error(t, err)
	})

	t.Run("test invalid field", func(t *testing.T) {
		segment, err := genSimpleSealedSegment()
		assert.NoError(t, err)
		segment.setType(segmentTypeIndexing)
		err = segment.dropFieldData(FieldID(1000))
		assert.Error(t, err)
	})
}

func TestSegment_updateSegmentIndex(t *testing.T) {
	t.Run("test updateSegmentIndex invalid", func(t *testing.T) {
		schema := genSimpleSegCoreSchema()
		collection := newCollection(defaultCollectionID, schema)
		segment := newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeSealed,
			true)

		fieldID := rowIDFieldID
		iParam := genSimpleIndexParams()
		segment.indexInfos[fieldID] = &indexInfo{}
		err := segment.setIndexParam(fieldID, iParam)
		assert.NoError(t, err)

		indexPaths := make([]string, 0)
		indexPaths = append(indexPaths, "IVF")
		err = segment.setIndexPaths(fieldID, indexPaths)
		assert.NoError(t, err)

		indexBytes, err := genIndexBinarySet()
		assert.NoError(t, err)
		err = segment.updateSegmentIndex(indexBytes, fieldID)
		assert.Error(t, err)

		segment.setType(segmentTypeGrowing)
		err = segment.updateSegmentIndex(indexBytes, fieldID)
		assert.Error(t, err)

		segment.setType(segmentTypeSealed)
		segment.segmentPtr = nil
		err = segment.updateSegmentIndex(indexBytes, fieldID)
		assert.Error(t, err)
	})
}

func TestSegment_dropSegmentIndex(t *testing.T) {
	t.Run("test dropSegmentIndex invalid segment type", func(t *testing.T) {
		schema := genSimpleSegCoreSchema()
		collection := newCollection(defaultCollectionID, schema)
		segment := newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeSealed,
			true)

		fieldID := rowIDFieldID
		err := segment.dropSegmentIndex(fieldID)
		assert.Error(t, err)
	})

	t.Run("test dropSegmentIndex nil segment ptr", func(t *testing.T) {
		schema := genSimpleSegCoreSchema()
		collection := newCollection(defaultCollectionID, schema)
		segment := newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeSealed,
			true)

		segment.segmentPtr = nil
		fieldID := rowIDFieldID
		err := segment.dropSegmentIndex(fieldID)
		assert.Error(t, err)
	})

	t.Run("test dropSegmentIndex nil index", func(t *testing.T) {
		schema := genSimpleSegCoreSchema()
		collection := newCollection(defaultCollectionID, schema)
		segment := newSegment(collection,
			defaultSegmentID,
			defaultPartitionID,
			defaultCollectionID,
			defaultVChannel,
			segmentTypeSealed,
			true)
		segment.setType(segmentTypeIndexing)

		fieldID := rowIDFieldID
		err := segment.dropSegmentIndex(fieldID)
		assert.Error(t, err)
	})
}
