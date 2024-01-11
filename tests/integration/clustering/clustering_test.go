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
	"math/rand"
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
)

type ClusteringSuite struct {
	integration.MiniClusterSuite
}

// test bulk insert with clustering info
// 1, create collection with a vector column and a varchar column
// 2, generate numpy files
// 3, import
// 4, check segment clustering info
func (s *ClusteringSuite) TestSearchClustering() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "SearchClusteringSuite"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 8

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

	err = integration.GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"embeddings.npy", 100, vecFieldSchema)
	s.NoError(err)
	err = integration.GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"image_path.npy", 100, varcharFieldSchema)
	s.NoError(err)

	bulkInsertFiles := []string{
		c.ChunkManager.RootPath() + "/" + "embeddings.npy",
		c.ChunkManager.RootPath() + "/" + "image_path.npy",
	}

	floatArr1 := []float32{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
	distributionBytes1, _ := generateSimpleClusteringInfo(floatArr1)
	_, err = integration.BulkInsertSync(ctx, c, collectionName, bulkInsertFiles, nil, distributionBytes1)
	s.NoError(err)

	floatArr2 := []float32{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}
	distributionBytes2, _ := generateSimpleClusteringInfo(floatArr2)
	_, err = integration.BulkInsertSync(ctx, c, collectionName, bulkInsertFiles, nil, distributionBytes2)
	s.NoError(err)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
		// check clustering info is inserted
		s.NotEmpty(segment.GetClusteringInfo())
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

// test bulk insert with clustering info
// 1, create collection with a vector column and a varchar column
// 2, generate numpy files
// 3, import
// 4, check segment clustering info
func (s *ClusteringSuite) TestBulkInsertClustering() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "BulkInsertClusteringSuite"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128

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

	err = integration.GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"embeddings.npy", 100, vecFieldSchema)
	s.NoError(err)
	err = integration.GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"image_path.npy", 100, varcharFieldSchema)
	s.NoError(err)

	bulkInsertFiles := []string{
		c.ChunkManager.RootPath() + "/" + "embeddings.npy",
		c.ChunkManager.RootPath() + "/" + "image_path.npy",
	}

	distributionBytes, _ := generateSimpleClusteringInfo(generateFloatVectors(dim))
	resp, err := integration.BulkInsertSync(ctx, c, collectionName, bulkInsertFiles, nil, distributionBytes)
	log.Info("BulkInsert resp", zap.Any("resp", resp))
	s.NoError(err)

	health2, err := c.DataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	s.NoError(err)
	log.Info("dataCoord health", zap.Any("health2", health2))

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
		// check clustering info is inserted
		s.True(len(segment.GetClusteringInfo().GetVectorClusteringInfos()) > 0)
		s.True(segment.GetClusteringInfo().GetVectorClusteringInfos()[0].GetCentroid() != nil)
	}

	log.Info("======================")
	log.Info("======================")
	log.Info("BulkInsertClusteringSuite succeed")
	log.Info("======================")
	log.Info("======================")
}

func generateFloatVectors(dim int) []float32 {
	total := dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func generateSimpleClusteringInfo(floatArray []float32) ([]byte, error) {
	dim := len(floatArray)
	vector := &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{
				Data: floatArray,
			},
		},
	}
	distribution := &schemapb.ClusteringInfo{
		VectorClusteringInfos: []*schemapb.VectorClusteringInfo{
			{
				Centroid: vector,
			},
		},
	}
	distributionBytes, err := proto.Marshal(distribution)
	return distributionBytes, err
}

func TestClustering(t *testing.T) {
	suite.Run(t, new(ClusteringSuite))
}
