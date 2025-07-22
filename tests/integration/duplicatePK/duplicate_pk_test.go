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

package duplicatePK

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type DuplicatePKSuite struct {
	integration.MiniClusterSuite

	batchRows int
	batchCnt  int
}

func (s *DuplicatePKSuite) SetupSuite() {
	s.MiniClusterSuite.SetupSuite()

	s.batchRows = 1000
	s.batchCnt = 3
}

func (s *DuplicatePKSuite) initCollection(ctx context.Context, dbName, collectionName string, dim int, callFlush bool) {
	schema := integration.ConstructSchema(collectionName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	flushFunc := func() {
		// flush
		flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
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
		s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)
	}
	for i := 0; i < s.batchCnt; i++ {
		fPKs := integration.NewInt64FieldData(integration.Int64Field, s.batchRows)
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, s.batchRows, dim)
		hashKeys := integration.GenerateHashKeys(s.batchRows)
		insertResult, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fPKs, fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(s.batchRows),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.Status))

		if callFlush {
			flushFunc()
		}
	}

	if !callFlush {
		flushFunc()
	}

	indexType := integration.IndexFaissIvfFlat
	metricType := metric.L2

	// create index
	createIndexStatus, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricType),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)
}

func (s *DuplicatePKSuite) checkSearchResult(ctx context.Context, dbName, collectionName string, dim int) {
	// search
	expr := fmt.Sprintf("%s == 10", integration.Int64Field)
	nq := 1
	topk := 10
	roundDecimal := -1
	indexType := integration.IndexFaissIvfFlat
	metricType := metric.L2
	vecType := schemapb.DataType_FloatVector

	params := integration.GetSearchParams(indexType, metricType)
	// nprobe == nlist
	params["nprobe"] = 100
	searchReq := integration.ConstructSearchRequest(dbName, collectionName, expr,
		integration.FloatVecField, vecType, nil, metricType, params, nq, dim, topk, roundDecimal)

	searchResult, err := s.Cluster.MilvusClient.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	log.Info("searchResult", zap.Any("result", searchResult))

	s.Equal(s.batchCnt, len(searchResult.GetResults().GetIds().GetIntId().GetData()))
}

func (s *DuplicatePKSuite) Test_DuplicatePKSingleSegment() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := ""
	collectionName := "single_segment_duplicate_pks"
	dim := 128
	s.initCollection(ctx, dbName, collectionName, dim, false)

	s.checkSearchResult(ctx, dbName, collectionName, dim)
}

func (s *DuplicatePKSuite) Test_DuplicatePKMultiSegment() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := ""
	collectionName := "multi_segments_duplicate_pks"
	dim := 128
	s.initCollection(ctx, dbName, collectionName, dim, true)

	s.checkSearchResult(ctx, dbName, collectionName, dim)
}

func TestDuplicatePKs(t *testing.T) {
	suite.Run(t, new(DuplicatePKSuite))
}
