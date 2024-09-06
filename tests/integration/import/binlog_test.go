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
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *BulkInsertSuite) PrepareCollectionA(dim, rowNum, delNum, delBatch int) (int64, int64, *schemapb.IDs) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	c := s.Cluster

	collectionName := "TestBinlogImport_A_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
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

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(merr.CheckRPCCall(insertResult, err))
	insertedIDs := insertResult.GetIDs()

	// flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
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
	s.WaitForFlush(ctx, ids, flushTs, "", collectionName)

	// delete
	beginIndex := 0
	for i := 0; i < delBatch; i++ {
		delCnt := delNum / delBatch
		idBegin := insertedIDs.GetIntId().GetData()[beginIndex]
		idEnd := insertedIDs.GetIntId().GetData()[beginIndex+delCnt]
		deleteResult, err := c.Proxy.Delete(ctx, &milvuspb.DeleteRequest{
			CollectionName: collectionName,
			Expr:           fmt.Sprintf("%d <= %s < %d", idBegin, integration.Int64Field, idEnd),
		})
		s.NoError(merr.CheckRPCCall(deleteResult, err))
		beginIndex += delCnt

		flushResp, err = c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
			CollectionNames: []string{collectionName},
		})
		s.NoError(merr.CheckRPCCall(flushResp, err))
		flushTs, has = flushResp.GetCollFlushTs()[collectionName]
		s.True(has)
		s.WaitForFlush(ctx, nil, flushTs, "", collectionName)
	}

	// check l0 segments
	segments, err = c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	l0Segments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetLevel() == datapb.SegmentLevel_L0
	})
	s.Equal(delBatch, len(l0Segments))

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

	// query
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{"count(*)"},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(rowNum-delNum, count)

	// query 2
	expr = fmt.Sprintf("%s < %d", integration.Int64Field, insertedIDs.GetIntId().GetData()[10])
	queryResult, err = c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count = len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal(0, count)

	// get collectionID and partitionID
	collectionID := showCollectionsResp.GetCollectionIds()[0]
	partitionID := showPartitionsResp.GetPartitionIDs()[0]

	return collectionID, partitionID, insertedIDs
}

func (s *BulkInsertSuite) TestBinlogImport() {
	const (
		dim      = 128
		rowNum   = 50000
		delNum   = 30000
		delBatch = 10
	)

	collectionID, partitionID, insertedIDs := s.PrepareCollectionA(dim, rowNum, delNum, delBatch)

	c := s.Cluster
	ctx := c.GetContext()

	collectionName := "TestBinlogImport_B_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	describeCollectionResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))
	newCollectionID := describeCollectionResp.GetCollectionID()

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	flushedSegmentsResp, err := c.DataCoordClient.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{
		CollectionID:     collectionID,
		PartitionID:      partitionID,
		IncludeUnhealthy: false,
	})
	s.NoError(merr.CheckRPCCall(flushedSegmentsResp, err))
	flushedSegments := flushedSegmentsResp.GetSegments()
	log.Info("flushed segments", zap.Int64s("segments", flushedSegments))
	segmentBinlogPrefixes := make([]string, 0)
	for _, segmentID := range flushedSegments {
		segmentBinlogPrefixes = append(segmentBinlogPrefixes,
			fmt.Sprintf("/tmp/%s/insert_log/%d/%d/%d", paramtable.Get().EtcdCfg.RootPath.GetValue(), collectionID, partitionID, segmentID))
	}
	// binlog import
	files := []*internalpb.ImportFile{
		{
			Paths: segmentBinlogPrefixes,
		},
	}
	importResp, err := c.Proxy.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
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
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetCollectionID() == newCollectionID
	})
	log.Info("Show segments", zap.Any("segments", segments))
	s.Equal(1, len(segments))
	segment := segments[0]
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.True(len(segment.GetBinlogs()) > 0)
	s.NoError(CheckLogID(segment.GetBinlogs()))
	s.True(len(segment.GetDeltalogs()) == 0)
	s.NoError(CheckLogID(segment.GetDeltalogs()))
	s.True(len(segment.GetStatslogs()) > 0)
	s.NoError(CheckLogID(segment.GetStatslogs()))

	// l0 import
	files = []*internalpb.ImportFile{
		{
			Paths: []string{
				fmt.Sprintf("/tmp/%s/delta_log/%d/%d/", paramtable.Get().EtcdCfg.RootPath.GetValue(), collectionID, common.AllPartitionsID),
			},
		},
	}
	importResp, err = c.Proxy.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "l0_import", Value: "true"},
		},
	})
	s.NoError(merr.CheckRPCCall(importResp, err))
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID = importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	segments, err = c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetCollectionID() == newCollectionID
	})
	log.Info("Show segments", zap.Any("segments", segments))
	l0Segments := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetCollectionID() == newCollectionID && segment.GetLevel() == datapb.SegmentLevel_L0
	})
	s.Equal(1, len(l0Segments))
	segment = l0Segments[0]
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.Equal(common.AllPartitionsID, segment.GetPartitionID())
	s.True(len(segment.GetBinlogs()) == 0)
	s.True(len(segment.GetDeltalogs()) > 0)
	s.NoError(CheckLogID(segment.GetDeltalogs()))
	s.True(len(segment.GetStatslogs()) == 0)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
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
	// check ids from collectionA, because during binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	insertedIDsMap := lo.SliceToMap(insertedIDs.GetIntId().GetData(), func(id int64) (int64, struct{}) {
		return id, struct{}{}
	})
	for _, id := range searchResult.GetResults().GetIds().GetIntId().GetData() {
		_, ok := insertedIDsMap[id]
		s.True(ok)
	}

	// query
	expr = fmt.Sprintf("%s >= 0", integration.Int64Field)
	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{"count(*)"},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := int(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(rowNum-delNum, count)

	// query 2
	expr = fmt.Sprintf("%s < %d", integration.Int64Field, insertedIDs.GetIntId().GetData()[10])
	queryResult, err = c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count = len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal(0, count)
}
