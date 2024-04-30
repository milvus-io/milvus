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

package importv2

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *BulkInsertSuite) PrepareCollectionA() (int64, int64, *schemapb.IDs) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 50000
	)

	collectionName := "TestBinlogImport_A_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(merr.CheckRPCCall(showCollectionsResp, err))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	showPartitionsResp, err := c.Proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(showPartitionsResp, err))
	log.Info("ShowPartitions result", zap.Any("showPartitionsResp", showPartitionsResp))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(merr.CheckRPCCall(insertResult, err))
	insertedIDs := insertResult.GetIDs()

	// flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(merr.CheckRPCCall(flushResp, err))
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
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))

	// get collectionID and partitionID
	collectionID := showCollectionsResp.GetCollectionIds()[0]
	partitionID := showPartitionsResp.GetPartitionIDs()[0]
	return collectionID, partitionID, insertedIDs
}

func (s *BulkInsertSuite) TestBinlogImport() {
	const (
		startTs = "0"
		endTs   = "548373346338803234"
	)

	collectionID, partitionID, insertedIDs := s.PrepareCollectionA()

	c := s.Cluster
	ctx := c.GetContext()

	collectionName := "TestBulkInsert_B_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "",
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	files := []*internalpb.ImportFile{
		{
			Paths: []string{
				fmt.Sprintf("/tmp/%s/insert_log/%d/%d/", paramtable.Get().EtcdCfg.RootPath.GetValue(), collectionID, partitionID),
				fmt.Sprintf("/tmp/%s/delta_log/%d/%d/", paramtable.Get().EtcdCfg.RootPath.GetValue(), collectionID, partitionID),
			},
		},
	}
	importResp, err := c.Proxy.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "startTs", Value: startTs},
			{Key: "endTs", Value: endTs},
			{Key: "backup", Value: "true"},
		},
	})
	s.NoError(merr.CheckRPCCall(importResp, err))
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	log.Info("Show segments", zap.Any("segments", segments))

	// load refresh
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
		Refresh:        true,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoadRefresh(ctx, "", collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)

	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))
	// check ids from collectionA, because during binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	insertedIDsMap := lo.SliceToMap(insertedIDs.GetIntId().GetData(), func(id int64) (int64, struct{}) {
		return id, struct{}{}
	})
	for _, id := range searchResult.GetResults().GetIds().GetIntId().GetData() {
		_, ok := insertedIDsMap[id]
		s.True(ok)
	}
}
