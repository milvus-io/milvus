package integration

import (
	"context"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

type CreateCollectionConfig struct {
	DBName           string
	CollectionName   string
	ChannelNum       int
	SegmentNum       int
	RowNumPerSegment int
	Dim              int
	ReplicaNumber    int32
	ResourceGroups   []string
}

func (s *MiniClusterSuite) InsertAndFlush(ctx context.Context, dbName, collectionName string, rowNum, dim int) error {
	fVecColumn := NewFloatVectorFieldData(FloatVecField, rowNum, dim)
	hashKeys := GenerateHashKeys(rowNum)
	insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	if err != nil {
		return err
	}
	if !merr.Ok(insertResult.Status) {
		return merr.Error(insertResult.Status)
	}

	flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	if err != nil {
		return err
	}
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	if !has || segmentIDs == nil {
		return merr.Error(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IllegalArgument,
			Reason:    "failed to get segment IDs",
		})
	}
	ids := segmentIDs.GetData()
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	if !has {
		return merr.Error(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IllegalArgument,
			Reason:    "failed to get flush timestamp",
		})
	}
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)
	return nil
}

func (s *MiniClusterSuite) CreateCollectionWithConfiguration(ctx context.Context, cfg *CreateCollectionConfig) {
	schema := ConstructSchema(cfg.CollectionName, cfg.Dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	s.NotNil(marshaledSchema)

	createCollectionStatus, err := s.Cluster.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         cfg.DBName,
		CollectionName: cfg.CollectionName,
		Schema:         marshaledSchema,
		ShardsNum:      int32(cfg.ChannelNum),
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionReplicaNumber,
				Value: strconv.FormatInt(int64(cfg.ReplicaNumber), 10),
			},
			{
				Key:   common.CollectionResourceGroups,
				Value: strings.Join(cfg.ResourceGroups, ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := s.Cluster.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{DbName: cfg.DBName})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < cfg.SegmentNum; i++ {
		err = s.InsertAndFlush(ctx, cfg.DBName, cfg.CollectionName, cfg.RowNumPerSegment, cfg.Dim)
		s.NoError(err)
	}

	// create index
	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         cfg.DBName,
		CollectionName: cfg.CollectionName,
		FieldName:      FloatVecField,
		IndexName:      "_default",
		ExtraParams:    ConstructIndexParam(cfg.Dim, IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.True(merr.Ok(createIndexStatus))
	s.WaitForIndexBuiltWithDB(ctx, cfg.DBName, cfg.CollectionName, FloatVecField)
}
