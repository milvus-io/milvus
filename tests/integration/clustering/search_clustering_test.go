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

package clustering

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/clustering"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/util"
)

type SearchClusteringSuite struct {
	integration.MiniClusterSuite
}

// test bulk insert with clustering info
// 1, create collection with a vector column and a varchar column
// 2, generate numpy files
// 3, import
// 4, check segment clustering info
func (s *BulkInsertClusteringSuite) TestSearchClustering() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "SearchClusteringSuite"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := util.DIM8

	pkFieldSchema := &schemapb.FieldSchema{Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true}
	varcharFieldSchema := &schemapb.FieldSchema{Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}}
	vecFieldSchema := &schemapb.FieldSchema{Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(dim)}}}

	schema := integration.ConstructSchema(collectionName, dim, true,
		pkFieldSchema,
		varcharFieldSchema,
		vecFieldSchema,
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
		s.FailNow("failed to create collection")
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	err = util.GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"embeddings.npy", 100, vecFieldSchema)
	s.NoError(err)
	err = util.GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"image_path.npy", 100, varcharFieldSchema)
	s.NoError(err)

	bulkInsertFiles := []string{
		c.ChunkManager.RootPath() + "/" + "embeddings.npy",
		c.ChunkManager.RootPath() + "/" + "image_path.npy",
	}

	allocTimestampResp, err := c.Proxy.AllocTimestamp(ctx, &milvuspb.AllocTimestampRequest{})
	s.NoError(err)
	operationID := allocTimestampResp.Timestamp

	err = util.BulkInsertSync(ctx, c, collectionName, bulkInsertFiles, []*commonpb.KeyValuePair{
		{Key: clustering.ClusteringCentroid, Value: "[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]"},
		{Key: clustering.ClusteringSize, Value: "200"},
		{Key: clustering.ClusteringOperationid, Value: fmt.Sprint(operationID)},
	})
	s.NoError(err)

	err = util.BulkInsertSync(ctx, c, collectionName, bulkInsertFiles, []*commonpb.KeyValuePair{
		{Key: clustering.ClusteringCentroid, Value: "[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]"},
		{Key: clustering.ClusteringSize, Value: "200"},
		{Key: clustering.ClusteringOperationid, Value: fmt.Sprint(operationID)},
	})
	s.NoError(err)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
		// check clustering info is inserted
		s.True(len(segment.GetClusteringInfos()) > 0)
		s.True(segment.GetClusteringInfos()[0].GetOperationID() == int64(operationID))
		s.True(segment.GetClusteringInfos()[0].GetSize() == int64(200))
		s.True(segment.GetClusteringInfos()[0].GetId() != 0)
		s.True(segment.GetClusteringInfos()[0].GetCentroid() != nil)
	}

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

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
	expr := "" // fmt.Sprintf("%s > 0", int64Field)
	nq := 1
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexHNSW, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		"embeddings", schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)
	searchReq.SearchParams = append(searchReq.SearchParams, &commonpb.KeyValuePair{Key: clustering.SearchEnableClustering, Value: "true"})
	searchReq.SearchParams = append(searchReq.SearchParams, &commonpb.KeyValuePair{Key: clustering.SearchClusteringFilterRatio, Value: "0.5"})

	searchResult, err := c.Proxy.Search(ctx, searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())

	log.Info("======================")
	log.Info("======================")
	log.Info("SearchClusteringSuite succeed")
	log.Info("======================")
	log.Info("======================")
}

func TestSearchClustering(t *testing.T) {
	t.Skip("Skip integration test, need to refactor integration test framework")
	suite.Run(t, new(SearchClusteringSuite))
}
