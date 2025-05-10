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

type ClusteringCompactionNullDataSuite struct {
	integration.MiniClusterSuite
}

func (s *ClusteringCompactionNullDataSuite) SetupSuite() {
	paramtable.Init()

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.TaskCheckInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.TaskScheduleInterval.Key, "100")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *ClusteringCompactionNullDataSuite) TestClusteringCompactionNullData() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 30000
	)

	collectionName := "TestClusteringCompactionNullData" + funcutil.GenRandomStr()

	// 2000 rows for each segment, about 1MB.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, strconv.Itoa(1))
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, strconv.Itoa(500*1024))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)

	paramtable.Get().Save(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key, strconv.Itoa(8))
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.Key)

	paramtable.Get().Save(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key, strconv.Itoa(102400))
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.BinLogMaxSize.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key, "1.0")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key, "1.0")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.Key)

	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            integration.Int64Field,
		IsPrimaryKey:    true,
		Description:     "",
		DataType:        schemapb.DataType_Int64,
		TypeParams:      nil,
		IndexParams:     nil,
		AutoID:          true,
		IsClusteringKey: false,
	}
	clusteringField := &schemapb.FieldSchema{
		FieldID:         101,
		Name:            "clustering",
		IsPrimaryKey:    false,
		Description:     "clustering key",
		DataType:        schemapb.DataType_Int64,
		IsClusteringKey: true,
		Nullable:        true,
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
		AutoID: true,
		Fields: []*schemapb.FieldSchema{pk, clusteringField, fVec},
	}

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

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	clusteringColumn := integration.NewInt64FieldDataNullableWithStart("clustering", rowNum, 1000)
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

	// flush
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

	indexType := integration.IndexFaissIvfFlat
	metricType := metric.L2
	vecType := schemapb.DataType_FloatVector

	// create index
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

	// load
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

	flushedSegmentsResp, err := c.MixCoord.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{
		CollectionID: desCollResp.GetCollectionID(),
		PartitionID:  -1,
	})
	s.NoError(err)
	s.Equal(flushedSegmentsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// 30000*(128*4+8+8) = 15.1MB/1MB = 15+1
	// The check is done every 100 lines written, so the size of each segment may be up to 99 lines larger.
	s.Contains([]int{15, 16}, len(flushedSegmentsResp.GetSegments()))
	log.Info("get flushed segments done", zap.Int64s("segments", flushedSegmentsResp.GetSegments()))
	totalRows := int64(0)
	segsInfoResp, err := c.MixCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs: flushedSegmentsResp.GetSegments(),
	})
	s.NoError(err)
	s.Equal(segsInfoResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	for _, segInfo := range segsInfoResp.GetInfos() {
		totalRows += segInfo.GetNumOfRows()
	}

	s.Equal(int64(rowNum), totalRows)

	log.Info("compact done")

	// search
	expr := "clustering > 0"
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(indexType, metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		fVecColumn.FieldName, vecType, []string{"clustering"}, metricType, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)

	checkWaitGroup := sync.WaitGroup{}

	checkQuerySegmentInfo := func() bool {
		querySegmentInfo, err := c.Proxy.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)

		var queryRows int64 = 0
		for _, seg := range querySegmentInfo.Infos {
			queryRows += seg.NumRows
		}

		return queryRows == rowNum
	}

	checkWaitGroup.Add(1)
	go func() {
		defer checkWaitGroup.Done()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, time.Minute*2)
		defer cancelFunc()

		for {
			select {
			case <-timeoutCtx.Done():
				s.FailNow("check query segment info timeout")
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
	log.Info("TestClusteringCompactionNullData succeed")
}

func TestClusteringCompactionNullData(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionNullDataSuite))
}
