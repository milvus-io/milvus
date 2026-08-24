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
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type L2SingleCompactionSuite struct {
	integration.MiniClusterSuite
}

func (s *L2SingleCompactionSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.MixCompactionTriggerInterval.Key, "10")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.Key, "0")
	s.MiniClusterSuite.SetupSuite()
}

func (s *L2SingleCompactionSuite) TearDownSuite() {
	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.MixCompactionTriggerInterval.Key)
	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.Key)
	s.MiniClusterSuite.TearDownSuite()
}

func (s *L2SingleCompactionSuite) TestL2SingleCompaction() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim        = 128
		dbName     = "default"
		rowNum     = 10000
		deleteCnt  = 5000
		indexType  = integration.IndexFaissIvfFlat
		metricType = metric.L2
	)

	collectionName := "TestL2SingleCompaction" + funcutil.GenRandomStr()

	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            integration.Int64Field,
		IsPrimaryKey:    true,
		Description:     "",
		DataType:        schemapb.DataType_Int64,
		TypeParams:      nil,
		IndexParams:     nil,
		AutoID:          false,
		IsClusteringKey: true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      102,
		Name:         integration.FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		mlog.Warn(context.TODO(), "createCollectionStatus fail reason", mlog.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	mlog.Info(context.TODO(), "CreateCollection result", mlog.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	mlog.Info(context.TODO(), "ShowCollections result", mlog.Any("showCollectionsResp", showCollectionsResp))

	pkColumn := integration.NewInt64FieldData(integration.Int64Field, rowNum)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

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

	mlog.Info(context.TODO(), "Finish flush", mlog.FieldDbName(dbName), mlog.FieldCollectionName(collectionName))

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
	mlog.Info(context.TODO(), "Finish create index", mlog.FieldDbName(dbName), mlog.FieldCollectionName(collectionName))

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.NoError(err)
	s.WaitForLoad(ctx, collectionName)
	mlog.Info(context.TODO(), "Finish load", mlog.FieldDbName(dbName), mlog.FieldCollectionName(collectionName))

	compactReq := &milvuspb.ManualCompactionRequest{
		CollectionID:    showCollectionsResp.CollectionIds[0],
		MajorCompaction: true,
	}
	compactResp, err := c.MilvusClient.ManualCompaction(ctx, compactReq)
	s.NoError(err)
	mlog.Info(context.TODO(), "compact", mlog.Any("compactResp", compactResp))

	compacted := func() bool {
		resp, err := c.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compactResp.GetCompactionID(),
		})
		if err != nil {
			return false
		}
		return resp.GetState() == commonpb.CompactionState_Completed
	}
	for !compacted() {
		time.Sleep(3 * time.Second)
	}
	mlog.Info(context.TODO(), "compact done")

	// delete
	deleteResult, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("%s < %d", integration.Int64Field, deleteCnt),
	})
	err = merr.CheckRPCCall(deleteResult, err)
	s.NoError(err)

	// flush l0
	flushResp, err = c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	err = merr.CheckRPCCall(flushResp, err)
	s.NoError(err)
	flushTs, has = flushResp.GetCollFlushTs()[collectionName]
	s.True(has)
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

	// wait for l0 compaction completed
	showSegments := func() bool {
		segments, err := c.ShowSegments(collectionName)
		s.NoError(err)
		s.NotEmpty(segments)

		for _, segment := range segments {
			mlog.Info(context.TODO(), "ShowSegments result", mlog.Int64("id", segment.ID), mlog.String("state", segment.GetState().String()), mlog.String("level", segment.GetLevel().String()), mlog.Int64("numOfRows", segment.GetNumOfRows()))
		}
		flushed := lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
			return segment.GetState() == commonpb.SegmentState_Flushed
		})
		if len(flushed) == 1 &&
			flushed[0].GetLevel() == datapb.SegmentLevel_L1 &&
			flushed[0].GetNumOfRows() == rowNum {
			mlog.Info(context.TODO(), "l0 compaction done, wait for single compaction")
		}
		return len(flushed) == 1 &&
			flushed[0].GetLevel() == datapb.SegmentLevel_L1 &&
			flushed[0].GetNumOfRows() == rowNum-deleteCnt
	}
	for !showSegments() {
		select {
		case <-ctx.Done():
			s.Fail("waiting for compaction timeout")
			return
		case <-time.After(3 * time.Second):
		}
	}

	checkQuerySegmentInfo := func() bool {
		querySegmentInfo, err := c.MilvusClient.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		return len(querySegmentInfo.GetInfos()) == 1
	}

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
				if checkQuerySegmentInfo() {
					return
				}
			}
			time.Sleep(time.Second * 3)
		}
	}()

	checkWaitGroup.Wait()

	mlog.Info(context.TODO(), "TestL2SingleCompaction succeed")
}

func TestL2SingleCompaction(t *testing.T) {
	suite.Run(t, new(L2SingleCompactionSuite))
}
