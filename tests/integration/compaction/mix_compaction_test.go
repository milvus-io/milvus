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

package compaction

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *CompactionSuite) assertMixCompaction(ctx context.Context, collectionName string, storageV2 bool) {
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 10000
		batch  = 1000

		indexType  = integration.IndexFaissIvfFlat
		metricType = metric.L2
	)

	var schema *schemapb.CollectionSchema
	// todo(SpadeA): fix this when v2 is supported
	if storageV2 {
		schema = integration.ConstructSchemaOfVecDataType(collectionName, dim, true, schemapb.DataType_FloatVector)
	} else {
		schema = integration.ConstructSchemaOfVecDataTypeWithStruct(collectionName, dim, true)
	}
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	// create collection
	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(createCollectionStatus, err)
	s.NoError(err)
	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricType),
	})
	err = merr.CheckRPCCall(createIndexStatus, err)
	s.NoError(err)
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// show collection
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collectionName},
	})
	err = merr.CheckRPCCall(showCollectionsResp, err)
	s.NoError(err)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < rowNum/batch; i++ {
		// insert
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, batch, dim)
		fieldsData := []*schemapb.FieldData{fVecColumn}
		if !storageV2 {
			structArrayField := integration.NewStructArrayFieldData(schema.StructArrayFields[0], integration.StructArrayField, batch, dim)
			fieldsData = append(fieldsData, structArrayField)
		}
		// geoColumn := integration.NewGeometryFieldData(integration.GeometryField, batch)
		hashKeys := integration.GenerateHashKeys(batch)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     fieldsData,
			HashKeys:       hashKeys,
			NumRows:        uint32(batch),
		})
		err = merr.CheckRPCCall(insertResult, err)
		s.NoError(err)
		s.Equal(int64(batch), insertResult.GetInsertCnt())

		// flush
		flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		err = merr.CheckRPCCall(flushResp, err)
		s.NoError(err)
		segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(segmentIDs)
		s.Require().True(has)
		flushTs, has := flushResp.GetCollFlushTs()[collectionName]
		s.True(has)
		s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

		log.Info("insert done", zap.Int("i", i))
	}

	showSegments := func() {
		segments, err := c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)
		// The stats task of segments will create a new segment, potentially triggering compaction simultaneously,
		// which may lead to an increase or decrease in the number of segments.
		s.True(len(segments) > 0)

		for _, segment := range segments {
			log.Info("show segment result", zap.Any("segment", segment))
		}
	}

	showSegments()

	// wait for compaction completed
	waitCompaction := func() bool {
		segments, err := c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)

		compactFromSegments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetState() == commonpb.SegmentState_Dropped
		})
		compactToSegments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetState() == commonpb.SegmentState_Flushed
		})

		log.Info("ShowSegments result", zap.Int("len(compactFromSegments)", len(compactFromSegments)),
			zap.Int("len(compactToSegments)", len(compactToSegments)))

		// The small segments can be merged based on dataCoord.compaction.min.segment
		return len(compactToSegments) <= paramtable.Get().DataCoordCfg.MinSegmentToMerge.GetAsInt()
	}

	for !waitCompaction() {
		select {
		case <-ctx.Done():
			s.Fail("waiting for compaction timeout")
			return
		case <-time.After(1 * time.Second):
		}
	}

	showSegments()
}

func (s *CompactionSuite) assertQuery(ctx context.Context, collectionName string) {
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 10000
		batch  = 1000

		indexType  = integration.IndexFaissIvfFlat
		metricType = metric.L2
		vecType    = schemapb.DataType_FloatVector
	)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.NoError(err)
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	params := integration.GetSearchParams(indexType, metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, vecType, nil, metricType, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.MilvusClient.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))

	// query
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	s.Equal(int64(rowNum), queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])

	// release collection
	status, err := c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)
}

func (s *CompactionSuite) TestMixCompaction() {
	s.T().Skip("skip struct array test")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	collectionName := "TestCompaction_" + funcutil.GenRandomStr()
	s.assertMixCompaction(ctx, collectionName, paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool())
	s.assertQuery(ctx, collectionName)

	// drop collection
	// status, err = c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
	//	 CollectionName: collectionName,
	// })
	// err = merr.CheckRPCCall(status, err)
	// s.NoError(err)

	log.Info("Test compaction succeed")
}

func (s *CompactionSuite) TestMixCompactionV2() {
	s.T().Skip("skip v2 compaction test")
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().CommonCfg.EnableStorageV2.Key:         "true",
		paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key: "false",
	})
	defer revertGuard()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	collectionName := "TestCompaction_" + funcutil.GenRandomStr()
	s.assertMixCompaction(ctx, collectionName, paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool())
}
