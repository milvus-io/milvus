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

package getvector

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type TestArrayStructSuite struct {
	integration.MiniClusterSuite

	dbName string

	// test params
	nq         int
	topK       int
	indexType  string
	metricType string
	vecType    schemapb.DataType
}

func (s *TestArrayStructSuite) run() {
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().CommonCfg.EnableStorageV2.Key: "true",
	})
	defer revertGuard()

	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collection := fmt.Sprintf("TestGetVector_%d_%d_%s_%s_%s",
		s.nq, s.topK, s.indexType, s.metricType, funcutil.GenRandomStr())

	const (
		NB  = 10000
		dim = 16
	)

	if len(s.dbName) > 0 {
		createDataBaseStatus, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
			DbName: s.dbName,
		})
		s.Require().NoError(err)
		s.Require().Equal(createDataBaseStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	}

	pkFieldName := "pkField"
	vecFieldName := "vecField"
	structFieldName := "structField"
	structSubVecFieldName := "structSubVecField"
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         pkFieldName,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "100",
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}

	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         vecFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}

	structSubVec := &schemapb.FieldSchema{
		FieldID:      103,
		Name:         structSubVecFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_ArrayOfVector,
		ElementType:  s.vecType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}

	structField := &schemapb.StructArrayFieldSchema{
		FieldID: 102,
		Name:    structFieldName,
		Fields:  []*schemapb.FieldSchema{structSubVec},
	}

	schema := &schemapb.CollectionSchema{
		Name:              collection,
		Description:       "",
		AutoID:            false,
		Fields:            []*schemapb.FieldSchema{pk, fVec},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structField},
	}
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fieldsData := make([]*schemapb.FieldData, 0, 5)
	// pk
	fieldsData = append(fieldsData, integration.NewInt64FieldData(pkFieldName, NB))
	// vec
	fieldsData = append(fieldsData, integration.NewFloatVectorFieldData(vecFieldName, NB, dim))
	// struct
	fieldsData = append(fieldsData, integration.NewStructArrayFieldData(structField, structFieldName, NB, dim))
	hashKeys := integration.GenerateHashKeys(NB)

	insertResult, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         s.dbName,
		CollectionName: collection,
		FieldsData:     fieldsData,
		HashKeys:       hashKeys,
		NumRows:        uint32(NB),
	})
	s.Require().NoError(err)
	s.Require().Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          s.dbName,
		CollectionNames: []string{collection},
	})
	s.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collection]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collection]
	s.Require().True(has)

	s.WaitForFlush(ctx, ids, flushTs, s.dbName, collection)
	segments, err := s.Cluster.ShowSegments(collection)
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)

	// create index for float vector field
	_, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         s.dbName,
		CollectionName: collection,
		FieldName:      vecFieldName,
		IndexName:      "float_vector_index",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, vecFieldName)

	// create index for struct sub-vector field
	createIndexResult, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         s.dbName,
		CollectionName: collection,
		FieldName:      structSubVecFieldName,
		IndexName:      "array_of_vector_index",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	s.Require().NoError(err)
	s.Require().Equal(createIndexResult.GetErrorCode(), commonpb.ErrorCode_Success)

	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, structSubVecFieldName)

	// load
	_, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	s.WaitForLoadWithDB(ctx, s.dbName, collection)

	// search
	nq := s.nq
	topk := s.topK

	outputFields := []string{structSubVecFieldName}
	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructEmbeddingListSearchRequest(s.dbName, collection, "",
		structSubVecFieldName, s.vecType, outputFields, s.metricType, params, nq, dim, topk, -1)

	searchResp, err := s.Cluster.MilvusClient.Search(ctx, searchReq)
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, searchResp.GetStatus().GetErrorCode())

	result := searchResp.GetResults()
	s.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
	s.Require().Len(result.GetScores(), nq*topk)
	s.Require().GreaterOrEqual(len(result.GetFieldsData()), 1)
	s.Require().EqualValues(nq, result.GetNumQueries())
	s.Require().EqualValues(topk, result.GetTopK())
}

func (s *TestArrayStructSuite) TestGetVector_ArrayStruct_FloatVector() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexEmbListHNSW
	s.metricType = metric.MaxSim
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func TestGetVectorArrayStruct(t *testing.T) {
	t.Skip("Skip integration test, need to refactor integration test framework.")
	suite.Run(t, new(TestArrayStructSuite))
}
