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

package datanode

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type ArrayStructDataNodeSuite struct {
	integration.MiniClusterSuite
	dim               int
	rowsPerCollection int

	generatedFieldData map[int64]*schemapb.FieldData
}

func (s *ArrayStructDataNodeSuite) setupParam() {
	s.dim = 32
	s.rowsPerCollection = 10
	s.generatedFieldData = make(map[int64]*schemapb.FieldData)
}

func (s *ArrayStructDataNodeSuite) loadCollection(collectionName string) {
	c := s.Cluster
	dbName := ""
	schema := integration.ConstructSchema(collectionName, s.dim, true)

	sId := &schemapb.FieldSchema{
		FieldID:      103,
		Name:         integration.StructSubInt32Field,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Array,
		ElementType:  schemapb.DataType_Int32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxCapacityKey,
				Value: "100",
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	sVec := &schemapb.FieldSchema{
		FieldID:      104,
		Name:         integration.StructSubFloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_ArrayOfVector,
		ElementType:  schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(s.dim),
			},
			{
				Key:   common.MaxCapacityKey,
				Value: "100",
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	structF := &schemapb.StructArrayFieldSchema{
		FieldID: 102,
		Name:    integration.StructArrayField,
		Fields:  []*schemapb.FieldSchema{sId, sVec},
	}
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{structF}

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	s.NoError(err)

	showCollectionsResp, err := c.MilvusClient.ShowCollections(context.TODO(), &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))

	rowNum := s.rowsPerCollection
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, s.dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	structColumn := integration.NewStructArrayFieldData(schema.StructArrayFields[0], integration.StructArrayField, rowNum, s.dim)

	s.generatedFieldData[101] = fVecColumn
	s.generatedFieldData[structColumn.FieldId] = structColumn
	s.generatedFieldData[structColumn.GetStructArrays().Fields[0].FieldId] = structColumn.GetStructArrays().Fields[0]
	s.generatedFieldData[structColumn.GetStructArrays().Fields[1].FieldId] = structColumn.GetStructArrays().Fields[1]

	insertResult, err := c.MilvusClient.Insert(context.TODO(), &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn, structColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))
	log.Info("=========================Data insertion finished=========================")

	// flush
	flushResp, err := c.MilvusClient.Flush(context.TODO(), &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.True(has)

	s.WaitForFlush(context.TODO(), ids, flushTs, dbName, collectionName)
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	log.Info("=========================Data flush finished=========================")

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "float_vector_index",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	s.NoError(err)
	log.Info("=========================Index created for float vector=========================")
	s.WaitForIndexBuilt(context.TODO(), collectionName, integration.FloatVecField)

	createIndexResult, err := c.MilvusClient.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      integration.StructSubFloatVecField,
		IndexName:      "array_of_vector_index",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexEmbListHNSW, metric.MaxSim),
	})
	s.NoError(err)
	s.Require().Equal(createIndexResult.GetErrorCode(), commonpb.ErrorCode_Success)
	s.WaitForIndexBuilt(context.TODO(), collectionName, integration.StructSubFloatVecField)

	log.Info("=========================Index created for array of vector=========================")

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(context.TODO(), &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	s.NoError(err)
	s.WaitForLoad(context.TODO(), collectionName)
	log.Info("=========================Collection loaded=========================")
}

func (s *ArrayStructDataNodeSuite) checkCollections() bool {
	req := &milvuspb.ShowCollectionsRequest{
		DbName:    "",
		TimeStamp: 0, // means now
	}
	resp, err := s.Cluster.MilvusClient.ShowCollections(context.TODO(), req)
	s.NoError(err)
	s.Equal(len(resp.CollectionIds), 1)
	notLoaded := 0
	loaded := 0
	for _, name := range resp.CollectionNames {
		loadProgress, err := s.Cluster.MilvusClient.GetLoadingProgress(context.TODO(), &milvuspb.GetLoadingProgressRequest{
			DbName:         "",
			CollectionName: name,
		})
		s.NoError(err)
		if loadProgress.GetProgress() != int64(100) {
			notLoaded++
		} else {
			loaded++
		}
	}
	log.Info(fmt.Sprintf("loading status: %d/%d", loaded, len(resp.GetCollectionNames())))
	return notLoaded == 0
}

func (s *ArrayStructDataNodeSuite) checkFieldsData(fieldsData []*schemapb.FieldData) {
	for _, fieldData := range fieldsData {
		for i := 0; i < s.rowsPerCollection; i++ {
			switch fieldData.FieldName {
			case integration.Int64Field:
				break
			case integration.FloatVecField:
				for j := 0; j < s.dim; j++ {
					s.Equal(fieldData.GetVectors().GetFloatVector().Data[j],
						s.generatedFieldData[fieldData.FieldId].GetVectors().GetFloatVector().Data[j])
				}
			case integration.StructArrayField:
				for _, field := range fieldData.GetStructArrays().Fields {
					if field.FieldName == integration.StructSubInt32Field {
						getData := field.GetScalars().GetArrayData().Data[i]
						generatedData := s.generatedFieldData[field.FieldId].GetScalars().GetArrayData().Data[i]

						arrayLen := len(getData.GetIntData().Data)
						s.Equal(arrayLen, len(generatedData.GetIntData().Data))

						for j := 0; j < arrayLen; j++ {
							s.Equal(getData.GetIntData().Data[j], generatedData.GetIntData().Data[j])
						}

					} else if field.FieldName == integration.StructSubFloatVecField {
						getData := field.GetVectors().GetVectorArray().Data[i]
						generatedData := s.generatedFieldData[field.FieldId].GetVectors().GetVectorArray().Data[i]

						length := len(getData.GetFloatVector().Data)
						s.Equal(length, len(generatedData.GetFloatVector().Data))

						for j := 0; j < length; j++ {
							s.Equal(getData.GetFloatVector().Data[j], generatedData.GetFloatVector().Data[j])
						}

					}
				}
			default:
				s.Fail(fmt.Sprintf("unsupported field type: %s", fieldData.FieldName))
			}
		}
	}
}

func (s *ArrayStructDataNodeSuite) query(collectionName string) {
	c := s.Cluster
	var err error
	// Query
	queryReq := &milvuspb.QueryRequest{
		Base:               nil,
		CollectionName:     collectionName,
		PartitionNames:     nil,
		Expr:               "",
		OutputFields:       []string{"*"},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
		QueryParams: []*commonpb.KeyValuePair{
			{
				Key:   "limit",
				Value: strconv.Itoa(s.rowsPerCollection),
			},
		},
	}
	queryResult, err := c.MilvusClient.Query(context.TODO(), queryReq)
	s.NoError(err)
	s.Equal(len(queryResult.FieldsData), 3)
	s.checkFieldsData(queryResult.FieldsData)

	queryReq = &milvuspb.QueryRequest{
		Base:               nil,
		CollectionName:     collectionName,
		PartitionNames:     nil,
		Expr:               "",
		OutputFields:       []string{integration.StructArrayField},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
		QueryParams: []*commonpb.KeyValuePair{
			{
				Key:   "limit",
				Value: strconv.Itoa(s.rowsPerCollection),
			},
		},
	}
	queryResult, err = c.MilvusClient.Query(context.TODO(), queryReq)
	s.NoError(err)
	// struct array field + pk
	s.Equal(len(queryResult.FieldsData), 2)
	s.checkFieldsData(queryResult.FieldsData)

	// Search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexEmbListHNSW, metric.MaxSim)
	searchReq := integration.ConstructEmbeddingListSearchRequest("", collectionName, expr,
		integration.StructSubFloatVecField, schemapb.DataType_FloatVector, []string{integration.StructArrayField}, metric.MaxSim, params, nq, s.dim, topk, roundDecimal)

	searchResult, _ := c.MilvusClient.Search(context.TODO(), searchReq)

	err = merr.Error(searchResult.GetStatus())
	s.NoError(err)
}

func (s *ArrayStructDataNodeSuite) TestSwapQN() {
	s.setupParam()
	s.Cluster.AddDataNode()
	cn := "new_collection_a"
	s.loadCollection(cn)
	s.query(cn)
	s.checkCollections()
}

func TestArrayStructDataNodeUtil(t *testing.T) {
	// skip struct array test
	suite.Run(t, new(ArrayStructDataNodeSuite))
}
