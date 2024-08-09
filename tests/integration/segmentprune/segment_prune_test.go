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

package segmentprune

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type SegmentPruneSuite struct {
	integration.MiniClusterSuite
}

func (s *SegmentPruneSuite) TestPruneByStrField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim    = 128
		dbName = ""
		rowNum = 30000
	)
	c := s.Cluster

	collectionName := "TestSegmentPrune" + funcutil.GenRandomStr()

	// 0. set up params
	// 2000 rows for each segment, about 1MB.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, strconv.Itoa(1))
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key, strconv.Itoa(8))
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key)

	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, strconv.Itoa(102400))
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key, "1.0")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1.0")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)

	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.EnableSegmentPrune.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.EnableSegmentPrune.Key)

	// 1. create collection
	clusterKeyFieldName := integration.VarCharField
	ntps := []NameType{
		{
			name:  "pk",
			dType: schemapb.DataType_Int64,
		},
		{
			name:         clusterKeyFieldName,
			dType:        schemapb.DataType_VarChar,
			isClusterKey: true,
		},
		{
			name:  integration.FloatVecField,
			dType: schemapb.DataType_FloatVector,
			isVec: true,
		},
	}
	schema := ConstructScalarClusteringSchema(collectionName, dim, true, 30, ntps)
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
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))
	s.Equal(1, len(showCollectionsResp.CollectionIds))
	collectionID := showCollectionsResp.CollectionIds[0]

	// 3. prepare data
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	prefix := "test_segment_prune"
	clusteringColumn := integration.NewVarCharRandomFieldData(clusterKeyFieldName, rowNum, prefix, 0, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{clusteringColumn, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// 4. flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
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

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

	// 5. index
	indexType := integration.IndexFaissIvfFlat
	metricType := metric.L2
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      fVecColumn.FieldName,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricType),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, fVecColumn.FieldName)

	// 6. load
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

	// 7. manual compact
	compactReq := &milvuspb.ManualCompactionRequest{
		CollectionID:    showCollectionsResp.CollectionIds[0],
		MajorCompaction: true,
	}
	compactResp, err := c.Proxy.ManualCompaction(ctx, compactReq)
	s.NoError(err)
	log.Info("compact", zap.Any("compactResp", compactResp))

	compacted := func() bool {
		resp, err := c.Proxy.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
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
	desCollResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
		CollectionID:   0,
		TimeStamp:      0,
	})
	s.NoError(err)
	s.Equal(desCollResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	flushedSegmentsResp, err := c.DataCoord.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{
		CollectionID: desCollResp.GetCollectionID(),
		PartitionID:  -1,
	})
	s.NoError(err)
	s.Equal(flushedSegmentsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	totalRows := int64(0)
	segsInfoResp, err := c.DataCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs: flushedSegmentsResp.GetSegments(),
	})
	s.NoError(err)
	s.Equal(segsInfoResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	for _, segInfo := range segsInfoResp.GetInfos() {
		s.LessOrEqual(segInfo.GetNumOfRows(), int64(1024*1024/128))
		totalRows += segInfo.GetNumOfRows()
	}
	s.Equal(int64(rowNum), totalRows)
	log.Info("compact done")

	// 8. wait for load and partition stats updated
	req := &datapb.GetRecoveryInfoRequestV2{
		CollectionID: collectionID,
	}
	resp, err := c.DataCoord.GetRecoveryInfoV2(ctx, req)
	s.NoError(err)
	s.Equal(1, len(resp.Channels))
	channelInfo := resp.Channels[0]
	s.Equal(1, len(channelInfo.PartitionStatsVersions))
	var partitionStatsVersion int64 = 0
	var partitionID int64 = 0
	for partID, version := range channelInfo.PartitionStatsVersions {
		partitionID = partID
		partitionStatsVersion = version
	}
	s.True(partitionID > 0, fmt.Sprintf("partition ID is not set, got:%d", partitionID))
	s.True(partitionStatsVersion > 0, fmt.Sprintf("partitionStatVersion is not set, got:%d", partitionStatsVersion))

	partStatsSynced := func() bool {
		distReq := &querypb.GetDataDistributionRequest{}
		distResp, err := c.QueryNode.GetDataDistribution(ctx, distReq)
		s.NoError(err)
		leaderView := distResp.LeaderViews[0]
		return partitionStatsVersion == leaderView.PartitionStatsVersions[partitionID]
	}

	start := time.Now()
	for !partStatsSynced() {
		time.Sleep(3 * time.Second)
		if time.Since(start) > 30*time.Second {
			break
		}
	}
	s.True(partStatsSynced())

	// 9. search with expr
	vecType := schemapb.DataType_FloatVector
	nq := 10
	topk := 10
	roundDecimal := -1
	params := integration.GetSearchParams(indexType, metricType)

	exprs := []string{
		"varCharField == \"test_segment_prune11\"",
		"varCharField > \"test_segment_prune99\"",
		"varCharField <= \"test_segment_prune19\"",
		"\"test_segment_prune9\" <= varCharField <= \"test_segment_prune19\"",
		"\"test_segment_prune9\" <= varCharField and varCharField <= \"test_segment_prune19\"",
		"\"test_segment_prune9\" <= varCharField or varCharField <= \"test_segment_prune19\"",
	}

	for _, expr := range exprs {
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			fVecColumn.FieldName, vecType, nil, metricType, params, nq, dim, topk, roundDecimal)
		searchResult, err := c.Proxy.Search(ctx, searchReq)
		err = merr.CheckRPCCall(searchResult, err)
		s.NoError(err)
	}
}

type NameType struct {
	name         string
	dType        schemapb.DataType
	isClusterKey bool
	isVec        bool
}

func ConstructScalarClusteringSchema(collection string, dim int, autoID bool, maxLength int, nameDTypes []NameType) *schemapb.CollectionSchema {
	fieldID := int64(100)
	fields := make([]*schemapb.FieldSchema, 0)
	for _, ntp := range nameDTypes {
		isPk := fieldID == int64(100)
		field := &schemapb.FieldSchema{
			FieldID:         fieldID,
			Name:            ntp.name,
			DataType:        ntp.dType,
			IsPrimaryKey:    isPk,
			IsClusteringKey: ntp.isClusterKey,
		}
		if ntp.isVec {
			field.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: fmt.Sprintf("%d", dim),
				},
			}
		}
		if ntp.dType == schemapb.DataType_VarChar {
			field.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: fmt.Sprintf("%d", maxLength),
				},
			}
		}

		if isPk {
			field.AutoID = autoID
		}

		fieldID += 1
		fields = append(fields, field)
	}

	return &schemapb.CollectionSchema{
		Name:   collection,
		AutoID: autoID,
		Fields: fields,
	}
}

func TestSegmentPrune(t *testing.T) {
	suite.Run(t, new(SegmentPruneSuite))
}
