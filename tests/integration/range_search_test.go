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
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestRangeSearchIP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
		cancel()
	}()

	prefix := "TestRangeSearchIP"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := constructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	assert.NoError(t, err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(insertResult.GetStatus()))

	// flush
	flushResp, err := c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	assert.NoError(t, err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	assert.True(t, has)
	ids := segmentIDs.GetData()
	assert.NotEmpty(t, segmentIDs)

	segments, err := c.metaWatcher.ShowSegments()
	assert.NoError(t, err)
	assert.NotEmpty(t, segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
	waitingForFlush(ctx, c, ids)

	// create index
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams:    constructIndexParam(dim, IndexFaissIvfFlat, distance.IP),
	})
	assert.NoError(t, err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}

	// load
	loadStatus, err := c.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	waitingForLoad(ctx, c, collectionName)
	// search
	expr := fmt.Sprintf("%s > 0", int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10
	filter := 20

	params := getSearchParams(IndexFaissIvfFlat, distance.IP)

	// only pass in radius when range search
	params["radius"] = radius
	searchReq := constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, nil, distance.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.proxy.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	assert.NoError(t, err)

	// pass in radius and range_filter when range search
	params["range_filter"] = filter
	searchReq = constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, nil, distance.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.proxy.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	assert.NoError(t, err)

	// pass in illegal radius and range_filter when range search
	params["radius"] = filter
	params["range_filter"] = radius
	searchReq = constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, nil, distance.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.proxy.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	assert.Error(t, err)

	log.Info("=========================")
	log.Info("=========================")
	log.Info("TestRangeSearchIP succeed")
	log.Info("=========================")
	log.Info("=========================")
}

func TestRangeSearchL2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
		cancel()
	}()

	prefix := "TestRangeSearchL2"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := constructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	assert.NoError(t, err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(insertResult.GetStatus()))

	// flush
	flushResp, err := c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	assert.NoError(t, err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	assert.True(t, has)
	ids := segmentIDs.GetData()
	assert.NotEmpty(t, segmentIDs)

	segments, err := c.metaWatcher.ShowSegments()
	assert.NoError(t, err)
	assert.NotEmpty(t, segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
	waitingForFlush(ctx, c, ids)

	// create index
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams:    constructIndexParam(dim, IndexFaissIvfFlat, distance.L2),
	})
	assert.NoError(t, err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}

	// load
	loadStatus, err := c.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	waitingForLoad(ctx, c, collectionName)
	// search
	expr := fmt.Sprintf("%s > 0", int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 20
	filter := 10

	params := getSearchParams(IndexFaissIvfFlat, distance.L2)
	// only pass in radius when range search
	params["radius"] = radius
	searchReq := constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, nil, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.proxy.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	assert.NoError(t, err)

	// pass in radius and range_filter when range search
	params["range_filter"] = filter
	searchReq = constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, nil, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.proxy.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	assert.NoError(t, err)

	// pass in illegal radius and range_filter when range search
	params["radius"] = filter
	params["range_filter"] = radius
	searchReq = constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, nil, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, _ = c.proxy.Search(ctx, searchReq)

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		log.Warn("searchResult fail reason", zap.Error(err))
	}
	assert.Error(t, err)

	log.Info("=========================")
	log.Info("=========================")
	log.Info("TestRangeSearchL2 succeed")
	log.Info("=========================")
	log.Info("=========================")
}
