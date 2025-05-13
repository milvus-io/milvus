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

package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type SearchSuite struct {
	integration.MiniClusterSuite

	indexType  string
	metricType string
	vecType    schemapb.DataType
}

func (s *SearchSuite) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 3000
	)

	collectionName := "TestSearch" + funcutil.GenRandomStr()
	groupByField := integration.VarCharField

	schema := integration.ConstructSchema(collectionName, dim, true,
		&schemapb.FieldSchema{
			FieldID:      100,
			Name:         integration.Int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		},
		&schemapb.FieldSchema{
			FieldID:      101,
			Name:         groupByField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: fmt.Sprintf("%d", 256),
				},
			},
			IndexParams: nil,
			Nullable:    true,
		},
		&schemapb.FieldSchema{
			FieldID:      102,
			Name:         integration.FloatVecField,
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
		},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	var fVecColumn *schemapb.FieldData
	if s.vecType == schemapb.DataType_SparseFloatVector {
		fVecColumn = integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	} else {
		fVecColumn = integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	}
	var fVarCharColumn *schemapb.FieldData
	fVarCharColumn = integration.NewVarCharFieldData(integration.VarCharField, rowNum, true)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertCheckReport := func() {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("insert check timeout")
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				log.Info("insert report info", zap.Any("reportInfo", reportInfo))
				s.Equal(hookutil.OpTypeInsert, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.RequestDataSizeKey])
				return
			}
		}
	}
	go insertCheckReport()
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVarCharColumn, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
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

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      fVecColumn.FieldName,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, fVecColumn.FieldName)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		fVecColumn.FieldName, s.vecType, nil, s.metricType, params, nq, dim, topk, roundDecimal)
	searchReq.SearchParams = append(searchReq.SearchParams, &commonpb.KeyValuePair{
		Key:   proxy.GroupByFieldKey,
		Value: groupByField,
	})

	searchCheckReport := func() {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("search check timeout")
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				log.Info("search report info", zap.Any("reportInfo", reportInfo))
				s.Equal(hookutil.OpTypeSearch, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.ResultDataSizeKey])
				s.NotEqualValues(0, reportInfo[hookutil.RelatedDataSizeKey])
				s.EqualValues(rowNum, reportInfo[hookutil.RelatedCntKey])
				return
			}
		}
	}
	go searchCheckReport()
	searchResult, err := c.Proxy.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)

	queryCheckReport := func() {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("query check timeout")
			case report := <-c.Extension.GetReportChan():
				reportInfo := report.(map[string]any)
				log.Info("query report info", zap.Any("reportInfo", reportInfo))
				s.Equal(hookutil.OpTypeQuery, reportInfo[hookutil.OpTypeKey])
				s.NotEqualValues(0, reportInfo[hookutil.ResultDataSizeKey])
				s.NotEqualValues(0, reportInfo[hookutil.RelatedDataSizeKey])
				s.EqualValues(rowNum, reportInfo[hookutil.RelatedCntKey])
				return
			}
		}
	}
	go queryCheckReport()
	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", queryResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())

	status, err := c.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	status, err = c.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	log.Info("TestSearch succeed")
}

func (s *SearchSuite) TestSearch() {
	s.indexType = integration.IndexFaissIvfFlat
	s.metricType = metric.L2
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func TestSearch(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
