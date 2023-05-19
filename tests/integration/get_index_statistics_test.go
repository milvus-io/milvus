package integration

import (
	"context"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

type GetIndexStatisticsSuite struct {
	MiniClusterSuite
}

func (s *GetIndexStatisticsSuite) TestGetIndexStatistics() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestGetIndexStatistics"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := constructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
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

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	s.NotEmpty(segmentIDs)
	s.Equal(true, has)
	waitingForFlush(ctx, c, ids)

	// create index
	indexName := "_default"
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams:    constructIndexParam(dim, IndexFaissIvfFlat, distance.L2),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	waitingForIndexBuilt(ctx, c, s.T(), collectionName, floatVecField)

	getIndexStatisticsResponse, err := c.proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
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

		waitingForIndexBuilt(ctx, c, t, collectionName, floatVecField)

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
