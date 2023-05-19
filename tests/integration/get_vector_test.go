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

package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type TestGetVectorSuite struct {
	MiniClusterSuite

	// test params
	nq         int
	topK       int
	indexType  string
	metricType string
	pkType     schemapb.DataType
	vecType    schemapb.DataType
}

func (s *TestGetVectorSuite) run() {
	ctx, cancel := context.WithCancel(s.Cluster.ctx)
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
	schema := constructSchema(collection, dim, false, pk, fVec)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := s.Cluster.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fieldsData := make([]*schemapb.FieldData, 0)
	if s.pkType == schemapb.DataType_Int64 {
		fieldsData = append(fieldsData, newInt64FieldData(pkFieldName, NB))
	} else {
		fieldsData = append(fieldsData, newStringFieldData(pkFieldName, NB))
	}
	var vecFieldData *schemapb.FieldData
	if s.vecType == schemapb.DataType_FloatVector {
		vecFieldData = newFloatVectorFieldData(vecFieldName, NB, dim)
	} else {
		vecFieldData = newBinaryVectorFieldData(vecFieldName, NB, dim)
	}
	fieldsData = append(fieldsData, vecFieldData)
	hashKeys := generateHashKeys(NB)
	_, err = s.Cluster.proxy.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     fieldsData,
		HashKeys:       hashKeys,
		NumRows:        uint32(NB),
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := s.Cluster.proxy.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collection},
	})
	s.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collection]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)

	segments, err := s.Cluster.metaWatcher.ShowSegments()
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)

	waitingForFlush(ctx, s.Cluster, ids)

	// create index
	_, err = s.Cluster.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection,
		FieldName:      vecFieldName,
		IndexName:      "_default",
		ExtraParams:    constructIndexParam(dim, s.indexType, s.metricType),
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	waitingForIndexBuilt(ctx, s.Cluster, s.T(), collection, vecFieldName)

	// load
	_, err = s.Cluster.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	waitingForLoad(ctx, s.Cluster, collection)

	// search
	nq := s.nq
	topk := s.topK

	outputFields := []string{vecFieldName}
	params := getSearchParams(s.indexType, s.metricType)
	searchReq := constructSearchRequest("", collection, "",
		vecFieldName, s.vecType, outputFields, s.metricType, params, nq, dim, topk, -1)

	searchResp, err := s.Cluster.proxy.Search(ctx, searchReq)
	s.Require().NoError(err)
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

	status, err := s.Cluster.proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().Equal(status.GetErrorCode(), commonpb.ErrorCode_Success)
}

func (s *TestGetVectorSuite) TestGetVector_FLAT() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexFaissIDMap
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IVF_FLAT() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexFaissIvfFlat
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IVF_PQ() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexFaissIvfPQ
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IVF_SQ8() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexFaissIvfSQ8
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_HNSW() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexHNSW
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_IP() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexHNSW
	s.metricType = distance.IP
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_StringPK() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexHNSW
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_VarChar
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_BinaryVector() {
	s.nq = 10
	s.topK = 10
	s.indexType = IndexFaissBinIvfFlat
	s.metricType = distance.JACCARD
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_BinaryVector
	s.run()
}

func (s *TestGetVectorSuite) TestGetVector_Big_NQ_TOPK() {
	s.T().Skip("skip big NQ Top due to timeout")
	s.nq = 10000
	s.topK = 200
	s.indexType = IndexHNSW
	s.metricType = distance.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

//func (s *TestGetVectorSuite) TestGetVector_DISKANN() {
//	s.nq = 10
//	s.topK = 10
//	s.indexType = IndexDISKANN
//	s.metricType = distance.L2
//	s.pkType = schemapb.DataType_Int64
//	s.vecType = schemapb.DataType_FloatVector
//	s.run()
//}

func TestGetVector(t *testing.T) {
	suite.Run(t, new(TestGetVectorSuite))
}
