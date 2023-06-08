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
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type TestGetVectorSuite struct {
	integration.MiniClusterSuite

	// test params
	nq         int
	topK       int
	indexType  string
	metricType string
	pkType     schemapb.DataType
	vecType    schemapb.DataType

	// expected
	searchFailed bool
}

func (s *TestGetVectorSuite) run() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collection := fmt.Sprintf("TestGetVector_%d_%d_%s_%s_%s",
		s.nq, s.topK, s.indexType, s.metricType, funcutil.GenRandomStr())

	const (
		NB  = 10000
		dim = 128
	)

	pkFieldName := "pkField"
	vecFieldName := "vecField"
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         pkFieldName,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     s.pkType,
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
		DataType:     s.vecType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	schema := integration.ConstructSchema(collection, dim, false, pk, fVec)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := s.Cluster.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fieldsData := make([]*schemapb.FieldData, 0)
	if s.pkType == schemapb.DataType_Int64 {
		fieldsData = append(fieldsData, integration.NewInt64FieldData(pkFieldName, NB))
	} else {
		fieldsData = append(fieldsData, integration.NewStringFieldData(pkFieldName, NB))
	}
	var vecFieldData *schemapb.FieldData
	if s.vecType == schemapb.DataType_FloatVector {
		vecFieldData = integration.NewFloatVectorFieldData(vecFieldName, NB, dim)
	} else {
		vecFieldData = integration.NewBinaryVectorFieldData(vecFieldName, NB, dim)
	}
	fieldsData = append(fieldsData, vecFieldData)
	hashKeys := integration.GenerateHashKeys(NB)
	_, err = s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     fieldsData,
		HashKeys:       hashKeys,
		NumRows:        uint32(NB),
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collection},
	})
	s.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collection]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)

	segments, err := s.Cluster.MetaWatcher.ShowSegments()
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)

	s.WaitForFlush(ctx, ids)

	// create index
	_, err = s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection,
		FieldName:      vecFieldName,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	s.WaitForIndexBuilt(ctx, collection, vecFieldName)

	// load
	_, err = s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	s.WaitForLoad(ctx, collection)

	// search
	nq := s.nq
	topk := s.topK

	outputFields := []string{vecFieldName}
	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructSearchRequest("", collection, "",
		vecFieldName, s.vecType, outputFields, s.metricType, params, nq, dim, topk, -1)

	searchResp, err := s.Cluster.Proxy.Search(ctx, searchReq)
	s.Require().NoError(err)
	if s.searchFailed {
		s.Require().NotEqual(searchResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		s.T().Logf("reason:%s", searchResp.GetStatus().GetReason())
		return
	}
	s.Require().Equal(searchResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	result := searchResp.GetResults()
	if s.pkType == schemapb.DataType_Int64 {
		s.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
	} else {
		s.Require().Len(result.GetIds().GetStrId().GetData(), nq*topk)
	}
	s.Require().Len(result.GetScores(), nq*topk)
	s.Require().GreaterOrEqual(len(result.GetFieldsData()), 1)
	var vecFieldIndex = -1
	for i, fieldData := range result.GetFieldsData() {
		if typeutil.IsVectorType(fieldData.GetType()) {
			vecFieldIndex = i
			break
		}
	}
	s.Require().EqualValues(nq, result.GetNumQueries())
	s.Require().EqualValues(topk, result.GetTopK())

	// check output vectors
	if s.vecType == schemapb.DataType_FloatVector {
		s.Require().Len(result.GetFieldsData()[vecFieldIndex].GetVectors().GetFloatVector().GetData(), nq*topk*dim)
		rawData := vecFieldData.GetVectors().GetFloatVector().GetData()
		resData := result.GetFieldsData()[vecFieldIndex].GetVectors().GetFloatVector().GetData()
		if s.pkType == schemapb.DataType_Int64 {
			for i, id := range result.GetIds().GetIntId().GetData() {
				expect := rawData[int(id)*dim : (int(id)+1)*dim]
				actual := resData[i*dim : (i+1)*dim]
				s.Require().ElementsMatch(expect, actual)
			}
		} else {
			for i, idStr := range result.GetIds().GetStrId().GetData() {
				id, err := strconv.Atoi(idStr)
				s.Require().NoError(err)
				expect := rawData[id*dim : (id+1)*dim]
				actual := resData[i*dim : (i+1)*dim]
				s.Require().ElementsMatch(expect, actual)
			}
		}
	} else {
		s.Require().Len(result.GetFieldsData()[vecFieldIndex].GetVectors().GetBinaryVector(), nq*topk*dim/8)
		rawData := vecFieldData.GetVectors().GetBinaryVector()
		resData := result.GetFieldsData()[vecFieldIndex].GetVectors().GetBinaryVector()
		if s.pkType == schemapb.DataType_Int64 {
			for i, id := range result.GetIds().GetIntId().GetData() {
				dataBytes := dim / 8
				for j := 0; j < dataBytes; j++ {
					expect := rawData[int(id)*dataBytes+j]
					actual := resData[i*dataBytes+j]
					s.Require().Equal(expect, actual)
				}
			}
		} else {
			for i, idStr := range result.GetIds().GetStrId().GetData() {
				dataBytes := dim / 8
				id, err := strconv.Atoi(idStr)
				s.Require().NoError(err)
				for j := 0; j < dataBytes; j++ {
					expect := rawData[id*dataBytes+j]
					actual := resData[i*dataBytes+j]
					s.Require().Equal(expect, actual)
				}
			}
		}
	}

	status, err := s.Cluster.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().Equal(status.GetErrorCode(), commonpb.ErrorCode_Success)
}

func (s *TestGetVectorSuite) TestGetVector_FLAT() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexFaissIDMap
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = false
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IVF_FLAT() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexFaissIvfFlat
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = false
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IVF_PQ() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexFaissIvfPQ
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = true
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IVF_SQ8() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexFaissIvfSQ8
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = true
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_HNSW() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexHNSW
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = false
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IP() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexHNSW
	s.metricType = distance.IP
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = false
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_StringPK() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexHNSW
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_VarChar
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = false
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_BinaryVector() {
	s.nq = 10
	s.topK = 10
	s.indexType = integration.IndexFaissBinIvfFlat
	s.metricType = distance.JACCARD
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_BinaryVector
	s.searchFailed = false
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_Big_NQ_TOPK() {
	s.T().Skip("skip big NQ Top due to timeout")
	s.nq = 10000
	s.topK = 200
	s.indexType = integration.IndexHNSW
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.searchFailed = false
	s.run()
}

//func (s *TestGetVectorSuite) TestGetVector_DISKANN() {
//	s.nq = 10
//	s.topK = 10
//	s.indexType = integration.IndexDISKANN
//	s.metricType = distance.L2
//	s.pkType = schemapb.DataType_Int64
//	s.vecType = schemapb.DataType_FloatVector
//	s.searchFailed = false
//	s.run()
//}

func TestGetVector(t *testing.T) {
	suite.Run(t, new(TestGetVectorSuite))
}
