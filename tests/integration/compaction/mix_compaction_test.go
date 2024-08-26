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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *CompactionSuite) TestMixCompaction() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
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

	collectionName := "TestCompaction_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, true, vecType)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	// create collection
	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(createCollectionStatus, err)
	s.NoError(err)
	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))

	// show collection
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	err = merr.CheckRPCCall(showCollectionsResp, err)
	s.NoError(err)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < rowNum/batch; i++ {
		// insert
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, batch, dim)
		hashKeys := integration.GenerateHashKeys(batch)
		insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(batch),
		})
		err = merr.CheckRPCCall(insertResult, err)
		s.NoError(err)
		s.Equal(int64(batch), insertResult.GetInsertCnt())

		// flush
		flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
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

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricType),
	})
	err = merr.CheckRPCCall(createIndexStatus, err)
	s.NoError(err)
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	// stats task happened
	s.Equal(rowNum/batch, len(segments)/2)
	for _, segment := range segments {
		log.Info("show segment result", zap.String("segment", segment.String()))
	}

	// wait for compaction completed
	showSegments := func() bool {
		segments, err = c.MetaWatcher.ShowSegments()
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
		return len(compactToSegments) == 1
	}
	for !showSegments() {
		select {
		case <-ctx.Done():
			s.Fail("waiting for compaction timeout")
			return
		case <-time.After(1 * time.Second):
		}
	}

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
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

	searchResult, err := c.Proxy.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))

	// query
	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	s.Equal(int64(rowNum), queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])

	// release collection
	status, err := c.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	// drop collection
	// status, err = c.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
	//	 CollectionName: collectionName,
	// })
	// err = merr.CheckRPCCall(status, err)
	// s.NoError(err)

	log.Info("Test compaction succeed")
}
