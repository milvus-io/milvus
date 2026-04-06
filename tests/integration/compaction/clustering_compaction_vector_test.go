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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
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

type VectorClusteringCompactionSuite struct {
	integration.MiniClusterSuite
}

func (s *VectorClusteringCompactionSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().PulsarCfg.MaxMessageSize.Key, strconv.Itoa(500*1024))
	s.WithMilvusConfig(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key, strconv.Itoa(8))
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key, "1.0")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1.0")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.TaskCheckInterval.Key, "1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.TaskScheduleInterval.Key, "100")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.EnableVectorClusteringKey.Key, "true")
	s.MiniClusterSuite.SetupSuite()
}

// TestVectorClusteringCompaction verifies the end-to-end flow of clustering compaction
// when FloatVector is used as the clustering key. This exercises the full pipeline:
// pipelining → analyzing → pipelining (with AnalyzeVersion) → executing → meta_saved → completed
func (s *VectorClusteringCompactionSuite) TestVectorClusteringCompaction() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 30000
	)

	collectionName := "TestVectorClusteringCompaction" + funcutil.GenRandomStr()

	schema := ConstructVectorClusteringSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, showCollectionsResp.GetStatus().GetErrorCode())

	// insert — autoID=true so only vector field is needed
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
	s.Equal(commonpb.ErrorCode_Success, insertResult.GetStatus().GetErrorCode())

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

	// record segment count before compaction
	segmentsBefore, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segmentsBefore)
	segCountBefore := 0
	for _, seg := range segmentsBefore {
		if seg.GetState() == commonpb.SegmentState_Flushed {
			segCountBefore++
		}
		log.Info("segment before compaction",
			zap.Int64("id", seg.ID),
			zap.String("state", seg.GetState().String()),
			zap.String("level", seg.GetLevel().String()),
			zap.Int64("numOfRows", seg.GetNumOfRows()))
	}
	log.Info("segments before compaction", zap.Int("count", segCountBefore))

	// reduce SegmentMaxSize to force clustering compaction
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().DataCoordCfg.SegmentMaxSize.Key: "1",
	})
	defer revertGuard()

	indexType := integration.IndexFaissIvfFlat
	metricType := metric.L2
	vecType := schemapb.DataType_FloatVector

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      fVecColumn.FieldName,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricType),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, fVecColumn.FieldName)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// trigger clustering compaction
	compactReq := &milvuspb.ManualCompactionRequest{
		CollectionID:    showCollectionsResp.CollectionIds[0],
		MajorCompaction: true,
	}
	compactResp, err := c.MilvusClient.ManualCompaction(ctx, compactReq)
	s.NoError(err)
	s.Greater(compactResp.GetCompactionID(), int64(0), "compaction should be triggered, got zero compactionID")
	log.Info("compaction triggered", zap.Int64("compactionID", compactResp.GetCompactionID()))

	// wait for compaction to complete with timeout
	compacted := func() bool {
		resp, err := c.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compactResp.GetCompactionID(),
		})
		if err != nil {
			return false
		}
		if resp.GetState() == commonpb.CompactionState_Completed {
			return true
		}
		log.Info("waiting for compaction", zap.String("state", resp.GetState().String()))
		return false
	}
	for !compacted() {
		select {
		case <-ctx.Done():
			s.FailNow("compaction did not complete within timeout")
		default:
			time.Sleep(3 * time.Second)
		}
	}
	log.Info("compaction completed")

	// verify: get segment info after compaction
	desCollResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, desCollResp.GetStatus().GetErrorCode())

	flushedSegmentsResp, err := c.MixCoordClient.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{
		CollectionID: desCollResp.GetCollectionID(),
		PartitionID:  -1,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, flushedSegmentsResp.GetStatus().GetErrorCode())

	segsInfoResp, err := c.MixCoordClient.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs: flushedSegmentsResp.GetSegments(),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, segsInfoResp.GetStatus().GetErrorCode())

	// verify 1: total row count unchanged
	totalRows := int64(0)
	segCountAfter := 0
	hasPartitionStats := false
	for _, segInfo := range segsInfoResp.GetInfos() {
		totalRows += segInfo.GetNumOfRows()
		if segInfo.GetState() == commonpb.SegmentState_Flushed {
			segCountAfter++
		}
		if segInfo.GetPartitionStatsVersion() > 0 {
			hasPartitionStats = true
		}
		log.Info("segment after compaction",
			zap.Int64("id", segInfo.GetID()),
			zap.String("state", segInfo.GetState().String()),
			zap.String("level", segInfo.GetLevel().String()),
			zap.Int64("numOfRows", segInfo.GetNumOfRows()),
			zap.Int64("partitionStatsVersion", segInfo.GetPartitionStatsVersion()))
	}
	s.Equal(int64(rowNum), totalRows, "total row count should be unchanged after compaction")
	log.Info("segments after compaction", zap.Int("count", segCountAfter))

	// verify 2: segment count changed (compaction merged segments)
	s.NotEqual(segCountBefore, segCountAfter, "segment count should change after clustering compaction")

	// verify 3: result segments have PartitionStatsVersion > 0 (proves clustering compaction ran)
	s.True(hasPartitionStats, "at least one result segment should have PartitionStatsVersion > 0")

	// verify 4: search still works correctly
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(indexType, metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		fVecColumn.FieldName, vecType, nil, metricType, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.MilvusClient.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)

	// verify 5: query segment info matches total row count
	checkWaitGroup := sync.WaitGroup{}
	checkWaitGroup.Add(1)
	go func() {
		defer checkWaitGroup.Done()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, time.Minute*2)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.Fail("check query segment info timeout")
				return
			default:
				querySegmentInfo, err := c.MilvusClient.GetQuerySegmentInfo(timeoutCtx, &milvuspb.GetQuerySegmentInfoRequest{
					DbName:         dbName,
					CollectionName: collectionName,
				})
				s.NoError(err)

				var queryRows int64
				for _, seg := range querySegmentInfo.Infos {
					queryRows += seg.NumRows
				}
				if queryRows == rowNum {
					return
				}
			}
			time.Sleep(time.Second * 3)
		}
	}()
	checkWaitGroup.Wait()

	log.Info("TestVectorClusteringCompaction succeed")
}

func TestVectorClusteringCompaction(t *testing.T) {
	suite.Run(t, new(VectorClusteringCompactionSuite))
}
