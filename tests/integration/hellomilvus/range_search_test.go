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

package hellomilvus

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *HelloMilvusSuite) TestRangeSearchIP() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestRangeSearchIP"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))

	// flush
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
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
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)
	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10
	filter := 20

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)

	// only pass in radius when range search
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.MilvusClient.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	// pass in radius and range_filter when range search
	params["range_filter"] = filter
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.MilvusClient.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	// pass in illegal radius and range_filter when range search
	params["radius"] = filter
	params["range_filter"] = radius
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.MilvusClient.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.Error(err)

	log.Info("=========================")
	log.Info("=========================")
	log.Info("TestRangeSearchIP succeed")
	log.Info("=========================")
	log.Info("=========================")
}

func (s *HelloMilvusSuite) TestRangeSearchL2() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestRangeSearchL2"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))

	// flush
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
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
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)
	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 20
	filter := 10

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	// only pass in radius when range search
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.MilvusClient.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	// pass in radius and range_filter when range search
	params["range_filter"] = filter
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.MilvusClient.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	// pass in illegal radius and range_filter when range search
	params["radius"] = filter
	params["range_filter"] = radius
	searchReq = integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.MilvusClient.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	s.Error(err)

	log.Info("=========================")
	log.Info("=========================")
	log.Info("TestRangeSearchL2 succeed")
	log.Info("=========================")
	log.Info("=========================")
}

// TestRangeSearchElementLevelL2 runs range search on a VECTOR_ARRAY sub-field
// in element-level mode: HNSW + L2 index on the sub-field, single-vector
// placeholder with element_level=true, radius/range_filter per L2 semantics.
func (s *HelloMilvusSuite) TestRangeSearchElementLevelL2() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestRangeSearchElementLevelL2"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 32
	rowNum := 1000

	schema := integration.ConstructSchemaOfVecDataTypeWithStruct(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.NoError(merr.Error(createCollectionStatus))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	structColumn := integration.NewStructArrayFieldData(schema.StructArrayFields[0], integration.StructArrayField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn, structColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))

	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.Require().True(has)
	s.Require().NotEmpty(segmentIDs)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.Require().True(has)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTs, dbName, collectionName)

	// regular float vector index, required for loading
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.NoError(merr.Error(createIndexStatus))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// non-EmbList metric on ArrayOfVector → each element vector is indexed
	// independently (proxy task_index.go:658).
	subFieldName := typeutil.ConcatStructFieldName(integration.StructArrayField, integration.StructSubFloatVecField)
	createIndexStatus, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      subFieldName,
		IndexName:      "struct_vec_element_index",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	s.NoError(err)
	s.NoError(merr.Error(createIndexStatus))
	s.WaitForIndexBuilt(ctx, collectionName, subFieldName)

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.NoError(merr.Error(loadStatus))
	s.WaitForLoad(ctx, collectionName)

	// L2 semantics: radius is upper bound, range_filter is lower bound.
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 2
	topk := 10
	roundDecimal := -1
	radius := 20
	filter := 10

	params := integration.GetSearchParams(integration.IndexHNSW, metric.L2)

	// only radius
	params["radius"] = radius
	searchReq := integration.ConstructElementLevelSearchRequest("", collectionName, expr,
		subFieldName, schemapb.DataType_FloatVector, []string{integration.StructArrayField},
		metric.L2, params, nq, dim, topk, roundDecimal)
	searchResult, err := c.MilvusClient.Search(ctx, searchReq)
	s.NoError(err)
	if err := merr.Error(searchResult.GetStatus()); err != nil {
		log.Warn("element-level range search with radius only failed", zap.Error(err))
	}
	s.NoError(merr.Error(searchResult.GetStatus()))

	// radius + range_filter
	params["range_filter"] = filter
	searchReq = integration.ConstructElementLevelSearchRequest("", collectionName, expr,
		subFieldName, schemapb.DataType_FloatVector, []string{integration.StructArrayField},
		metric.L2, params, nq, dim, topk, roundDecimal)
	searchResult, err = c.MilvusClient.Search(ctx, searchReq)
	s.NoError(err)
	if err := merr.Error(searchResult.GetStatus()); err != nil {
		log.Warn("element-level range search with radius+range_filter failed", zap.Error(err))
	}
	s.NoError(merr.Error(searchResult.GetStatus()))

	// illegal: for L2, radius must be > range_filter
	params["radius"] = filter
	params["range_filter"] = radius
	searchReq = integration.ConstructElementLevelSearchRequest("", collectionName, expr,
		subFieldName, schemapb.DataType_FloatVector, []string{integration.StructArrayField},
		metric.L2, params, nq, dim, topk, roundDecimal)
	searchResult, _ = c.MilvusClient.Search(ctx, searchReq)
	s.Error(merr.Error(searchResult.GetStatus()))

	log.Info("=========================")
	log.Info("=========================")
	log.Info("TestRangeSearchElementLevelL2 succeed")
	log.Info("=========================")
	log.Info("=========================")
}
