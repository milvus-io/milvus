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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *HelloMilvusSuite) TestUpsertAutoIDFalse() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	start := 0

	schema := integration.ConstructSchema(collectionName, dim, false)
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
		mlog.Warn(context.TODO(), "createCollectionStatus fail reason", zap.Error(err))
	}

	mlog.Info(context.TODO(), "CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	mlog.Info(context.TODO(), "ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(start))
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

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
		mlog.Info(context.TODO(), "ShowSegments result", zap.String("segment", segment.String()))
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
		mlog.Warn(context.TODO(), "createIndexStatus fail reason", zap.Error(err))
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
		mlog.Warn(context.TODO(), "LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)
	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{integration.Int64Field}, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.MilvusClient.Search(ctx, searchReq)
	checkFunc := func(data int) error {
		if data < start || data > start+rowNum {
			return errors.New("upsert check pk fail")
		}
		return nil
	}
	for _, id := range searchResult.Results.Ids.GetIntId().GetData() {
		s.NoError(checkFunc(int(id)))
	}
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data)))
	}

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		mlog.Warn(context.TODO(), "searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	mlog.Info(context.TODO(), "===========================")
	mlog.Info(context.TODO(), "===========================")
	mlog.Info(context.TODO(), "TestUpsertAutoIDFalse succeed")
	mlog.Info(context.TODO(), "===========================")
	mlog.Info(context.TODO(), "===========================")
}

func (s *HelloMilvusSuite) TestUpsertAutoIDTrue() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	start := 0

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
		mlog.Warn(context.TODO(), "createCollectionStatus fail reason", zap.Error(err))
	}

	mlog.Info(context.TODO(), "CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	mlog.Info(context.TODO(), "ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	pkFieldData := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 0)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	upsertResult, err := c.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(upsertResult.GetStatus()))

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
		mlog.Info(context.TODO(), "ShowSegments result", zap.String("segment", segment.String()))
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
		mlog.Warn(context.TODO(), "createIndexStatus fail reason", zap.Error(err))
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
		mlog.Warn(context.TODO(), "LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)
	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, "")
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, []string{integration.Int64Field}, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.MilvusClient.Search(ctx, searchReq)
	checkFunc := func(data int) error {
		if data >= start && data <= start+rowNum {
			return errors.New("upsert check pk fail")
		}
		return nil
	}
	for _, id := range searchResult.Results.Ids.GetIntId().GetData() {
		s.NoError(checkFunc(int(id)))
	}
	for _, data := range searchResult.Results.FieldsData[0].GetScalars().GetLongData().GetData() {
		s.NoError(checkFunc(int(data)))
	}

	err = merr.Error(searchResult.GetStatus())
	if err != nil {
		mlog.Warn(context.TODO(), "searchResult fail reason", zap.Error(err))
	}
	s.NoError(err)

	mlog.Info(context.TODO(), "===========================")
	mlog.Info(context.TODO(), "===========================")
	mlog.Info(context.TODO(), "TestUpsertAutoIDTrue succeed")
	mlog.Info(context.TODO(), "===========================")
	mlog.Info(context.TODO(), "===========================")
}
