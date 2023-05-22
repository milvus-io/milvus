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

package segments

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type RetrieveSuite struct {
	suite.Suite

	// Dependencies
	// chunkManager storage.ChunkManager

	// Data
	manager      *Manager
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
	sealed       *LocalSegment
	growing      *LocalSegment
}

func (suite *RetrieveSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *RetrieveSuite) SetupTest() {
	var err error
	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	schema := GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64)
	indexMeta := GenTestIndexMeta(suite.collectionID, schema)
	suite.manager.Collection.Put(suite.collectionID,
		schema,
		indexMeta,
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
	)
	suite.collection = suite.manager.Collection.Get(suite.collectionID)

	suite.sealed, err = NewSegment(suite.collection,
		suite.segmentID,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeSealed,
		0,
		nil,
		nil,
	)
	suite.Require().NoError(err)

	suite.growing, err = NewSegment(suite.collection,
		suite.segmentID+1,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeGrowing,
		0,
		nil,
		nil,
	)
	suite.Require().NoError(err)

	insertData, err := genInsertData(100, suite.collection.Schema())
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	suite.Require().NoError(err)
	numRows := insertRecord.NumRows
	for _, fieldData := range insertRecord.FieldsData {
		err = suite.sealed.LoadField(numRows, fieldData)
		suite.Require().NoError(err)
	}

	insertMsg, err := genInsertMsg(suite.collection, suite.partitionID, suite.growing.segmentID, 100)
	suite.Require().NoError(err)
	insertRecord, err = storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = suite.growing.Insert(insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)

	suite.manager.Segment.Put(SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(SegmentTypeGrowing, suite.growing)
}

func (suite *RetrieveSuite) TearDownTest() {
	DeleteSegment(suite.sealed)
	DeleteSegment(suite.growing)
	DeleteCollection(suite.collection)
}

func (suite *RetrieveSuite) TestRetrieveSealed() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
}

func (suite *RetrieveSuite) TestRetrieveGrowing() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	res, _, _, err := RetrieveStreaming(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.growing.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
}

func (suite *RetrieveSuite) TestRetrieveNonExistSegment() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{999})
	suite.NoError(err)
	suite.Len(res, 0)
}

func (suite *RetrieveSuite) TestRetrieveNilSegment() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	DeleteSegment(suite.sealed)
	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.ErrorIs(err, ErrSegmentReleased)
	suite.Len(res, 0)
}

func (suite *RetrieveSuite) TestRetrieveResultByType() {
	plan, err := genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Bool)
	suite.NoError(err)
	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
	boolData := res[0].FieldsData[0].GetScalars().GetBoolData().Data
	suite.Len(boolData, 3)
	fmt.Println("bool type")
	suite.Equal(boolData[0], false)
	suite.Equal(boolData[1], true)
	suite.Equal(boolData[2], false)
	fmt.Println(boolData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Int8)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
	intData := res[0].FieldsData[0].GetScalars().GetIntData().Data
	suite.Len(intData, 3)
	fmt.Println("int8 type")
	suite.Equal(intData[0], int32(1))
	suite.Equal(intData[1], int32(2))
	suite.Equal(intData[2], int32(3))
	fmt.Println(intData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Int16)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
	intData = res[0].FieldsData[0].GetScalars().GetIntData().Data
	suite.Len(intData, 3)
	fmt.Println("int16 type")
	suite.Equal(intData[0], int32(1))
	suite.Equal(intData[1], int32(2))
	suite.Equal(intData[2], int32(3))
	fmt.Println(intData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Int32)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
	intData = res[0].FieldsData[0].GetScalars().GetIntData().Data
	suite.Len(intData, 3)
	fmt.Println("int32 type")
	suite.Equal(intData[0], int32(1))
	suite.Equal(intData[1], int32(2))
	suite.Equal(intData[2], int32(3))
	fmt.Println(intData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Int64)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
	longData := res[0].FieldsData[0].GetScalars().GetLongData().Data
	suite.Len(longData, 3)
	fmt.Println("int64 type")
	suite.Equal(longData[0], int64(1))
	suite.Equal(longData[1], int64(2))
	suite.Equal(longData[2], int64(3))
	fmt.Println(longData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Float)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
	floatData := res[0].FieldsData[0].GetScalars().GetFloatData().Data
	suite.Len(floatData, 3)
	fmt.Println("float type")
	suite.Equal(floatData[0], float32(2.0/101))
	suite.Equal(floatData[1], float32(3.0/102))
	suite.Equal(floatData[2], float32(4.0/103))
	fmt.Println(floatData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Double)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.Len(res[0].Offset, 3)
	suite.NoError(err)
	doubleData := res[0].FieldsData[0].GetScalars().GetDoubleData().Data
	suite.Len(doubleData, 3)
	fmt.Println("double type")
	suite.Equal(doubleData[0], float64(2.0/101))
	suite.Equal(doubleData[1], float64(3.0/102))
	suite.Equal(doubleData[2], float64(4.0/103))
	fmt.Println(doubleData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_VarChar)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.Len(res[0].Offset, 3)
	suite.NoError(err)
	fmt.Println("string/varchar type")
	strData := res[0].FieldsData[0].GetScalars().GetStringData().Data
	suite.Equal(strData[0], "string1")
	suite.Equal(strData[1], "string2")
	suite.Equal(strData[2], "string3")
	fmt.Println(strData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_FloatVector)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.Len(res[0].Offset, 3)
	suite.NoError(err)
	floatVecData := res[0].FieldsData[0].GetVectors().GetFloatVector().Data
	suite.Len(floatVecData, 128*3)
	fmt.Println("float vec type")
	suite.Equal(floatVecData[0], float32(128))
	suite.Equal(floatVecData[383], float32(511))
	fmt.Println(floatVecData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_BinaryVector)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.Len(res[0].Offset, 3)
	suite.NoError(err)
	strVecData := res[0].FieldsData[0].GetVectors().GetBinaryVector()
	fmt.Println("binary vector type")
	suite.Equal(len(strVecData), 3*16)
	fmt.Println(strVecData)

	plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_JSON)
	suite.NoError(err)
	res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.Len(res[0].Offset, 3)
	suite.NoError(err)
	jsonData := res[0].FieldsData[0].GetScalars().GetJsonData().Data
	suite.Equal(len(jsonData), 3)
	fmt.Println("Json type")
	suite.Equal(string(jsonData[0]), fmt.Sprintf(`{"key":%d}`, 2))
	suite.Equal(string(jsonData[1]), fmt.Sprintf(`{"key":%d}`, 3))
	suite.Equal(string(jsonData[2]), fmt.Sprintf(`{"key":%d}`, 4))
	fmt.Println(string(jsonData[0]))
	fmt.Println(string(jsonData[1]))
	fmt.Println(string(jsonData[2]))

	// plan, err = genSimpleRetrievePlanByOutputFieldType(suite.collection, schemapb.DataType_Array)
	// suite.NoError(err)
	// res, _, _, err = RetrieveHistorical(context.TODO(), suite.manager, plan,
	// 	suite.collectionID,
	// 	[]int64{suite.partitionID},
	// 	[]int64{suite.sealed.ID()},
	// 	nil)
	// suite.Len(res[0].Offset, 3)
	// suite.NoError(err)
}

func TestRetrieve(t *testing.T) {
	suite.Run(t, new(RetrieveSuite))
}
