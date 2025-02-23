package indexstat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type GetIndexStatisticsSuite struct {
	integration.MiniClusterSuite

	indexType  string
	metricType string
	vecType    schemapb.DataType
}

func (s *GetIndexStatisticsSuite) run() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestGetIndexStatistics"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
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
	s.Equal(true, has)
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

	// create index
	indexName := "_default"
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	getIndexStatisticsResponse, err := c.Proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
		CollectionName: collectionName,
		IndexName:      indexName,
	})
	s.NoError(err)
	indexInfos := getIndexStatisticsResponse.GetIndexDescriptions()
	s.Equal(1, len(indexInfos))
	s.Equal(int64(3000), indexInfos[0].IndexedRows)
	s.Equal(int64(3000), indexInfos[0].TotalRows)

	// skip second insert case for now
	// the result is not certain
	/*
		insertResult2, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.Equal(insertResult2.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

		_, err = c.proxy.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		segmentIDs2, has2 := flushResp.GetCollSegIDs()[collectionName]
		ids2 := segmentIDs2.GetData()
		s.NotEmpty(segmentIDs)
		s.Equal(true, has2)
		waitingForFlush(ctx, c, ids2)

			loadStatus, err := c.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
				DbName:         dbName,
				CollectionName: collectionName,
			})
			s.NoError(err)
			if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
				log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
			}
			s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
			waitingForLoad(ctx, c, collectionName)

			s.NoError(err)

		waitingForIndexBuilt(ctx,  collectionName, integration.FloatVecField)

		getIndexStatisticsResponse2, err := c.proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
			CollectionName: collectionName,
			IndexName:      indexName,
		})
		s.NoError(err)
		indexInfos2 := getIndexStatisticsResponse2.GetIndexDescriptions()
		s.Equal(1, len(indexInfos2))
		s.Equal(int64(6000), indexInfos2[0].IndexedRows)
		s.Equal(int64(6000), indexInfos2[0].TotalRows)
	*/

	log.Info("TestGetIndexStatistics succeed")
}

func (s *GetIndexStatisticsSuite) TestGetIndexStatistics_float() {
	s.indexType = integration.IndexFaissIvfFlat
	s.metricType = metric.L2
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func TestGetIndexStat(t *testing.T) {
	suite.Run(t, new(GetIndexStatisticsSuite))
}
