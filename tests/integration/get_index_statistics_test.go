package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func TestGetIndexStatistics(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestGetIndexStatistics"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128
	rowNum := 3000

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)
	assert.Equal(t, insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	assert.NoError(t, err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	assert.NotEmpty(t, segmentIDs)

	if has && len(ids) > 0 {
		flushed := func() bool {
			resp, err := c.proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: ids,
			})
			if err != nil {
				//panic(errors.New("GetFlushState failed"))
				return false
			}
			return resp.GetFlushed()
		}
		for !flushed() {
			// respect context deadline/cancel
			select {
			case <-ctx.Done():
				panic(errors.New("deadline exceeded"))
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// create index
	indexName := "_default"
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
			{
				Key:   common.MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: strconv.Itoa(10),
			},
		},
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	getIndexStatisticsResponse, err := c.proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
		CollectionName: collectionName,
		IndexName:      indexName,
	})
	assert.NoError(t, err)
	indexInfos := getIndexStatisticsResponse.GetIndexDescriptions()
	assert.Equal(t, 1, len(indexInfos))
	assert.Equal(t, int64(0), indexInfos[0].IndexedRows)
	assert.Equal(t, int64(3000), indexInfos[0].TotalRows)

	insertResult2, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)

	_, err = c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	assert.NoError(t, err)
	segmentIDs2, has2 := flushResp.GetCollSegIDs()[collectionName]
	ids2 := segmentIDs2.GetData()
	assert.NotEmpty(t, segmentIDs)

	if has2 && len(ids2) > 0 {
		flushed := func() bool {
			resp, err := c.proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: ids2,
			})
			if err != nil {
				//panic(errors.New("GetFlushState failed"))
				return false
			}
			return resp.GetFlushed()
		}
		for !flushed() {
			// respect context deadline/cancel
			select {
			case <-ctx.Done():
				panic(errors.New("deadline exceeded"))
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	loadStatus, err := c.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	assert.Equal(t, commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	for {
		loadProgress, err := c.proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: collectionName,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		if loadProgress.GetProgress() == 100 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	assert.NoError(t, err)
	assert.Equal(t, insertResult2.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	getIndexStatisticsResponse2, err := c.proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
		CollectionName: collectionName,
		IndexName:      indexName,
	})
	assert.NoError(t, err)
	indexInfos2 := getIndexStatisticsResponse2.GetIndexDescriptions()
	assert.Equal(t, 1, len(indexInfos2))
	assert.Equal(t, int64(6000), indexInfos2[0].IndexedRows)
	assert.Equal(t, int64(6000), indexInfos2[0].TotalRows)

	log.Info("TestGetIndexStatistics succeed")
}
