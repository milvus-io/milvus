package integration

import (
	"context"
	"fmt"
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

type PrimaryKeyConfig struct {
	FieldName   string
	FieldType   schemapb.DataType
	NumChannels int
	StartPK     int64 // Starting PK value (default 1 if not specified)
}

// InsertAndFlush inserts data and flushes.
// Returns the next startPK for subsequent calls and any error.
func (s *MiniClusterSuite) InsertAndFlush(ctx context.Context, dbName, collectionName string, rowNum, dim int, pkConfig *PrimaryKeyConfig) (int64, error) {
	startPK := pkConfig.StartPK
	if startPK == 0 {
		startPK = 1 // Default to 1 if not specified
	}

	pkColumn, nextPK := GenerateChannelBalancedPrimaryKeys(pkConfig.FieldName, pkConfig.FieldType, rowNum, pkConfig.NumChannels, startPK)
	fVecColumn := NewFloatVectorFieldData(FloatVecField, rowNum, dim)
	insertResult, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn},
		NumRows:        uint32(rowNum),
	})
	if err != nil {
		return 0, err
	}
	if !merr.Ok(insertResult.Status) {
		return 0, merr.Error(insertResult.Status)
	}

	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	if err := merr.CheckRPCCall(flushResp.GetStatus(), err); err != nil {
		return 0, err
	}
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	if !has || segmentIDs == nil {
		return 0, merr.Error(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IllegalArgument,
			Reason:    "failed to get segment IDs",
		})
	}
	ids := segmentIDs.GetData()
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	if !has {
		return 0, merr.Error(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_IllegalArgument,
			Reason:    "failed to get flush timestamp",
		})
	}
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)
	return nextPK, nil
}

func (s *MiniClusterSuite) CreateCollectionWithConfiguration(ctx context.Context, cfg *CreateCollectionConfig) {
	schema := ConstructSchema(cfg.CollectionName, cfg.Dim, false)
	s.CreateCollection(ctx, cfg, schema)
}

func (s *MiniClusterSuite) CreateCollection(ctx context.Context, cfg *CreateCollectionConfig, schema *schemapb.CollectionSchema) {
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	s.NotNil(marshaledSchema)

	createCollectionStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
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
	showCollectionsResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{DbName: cfg.DBName})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	nextStartPK := int64(1)
	for i := 0; i < cfg.SegmentNum; i++ {
		var err error
		nextStartPK, err = s.InsertAndFlush(ctx, cfg.DBName, cfg.CollectionName, cfg.RowNumPerSegment, cfg.Dim, &PrimaryKeyConfig{
			FieldName:   Int64Field,
			FieldType:   schemapb.DataType_Int64,
			NumChannels: cfg.ChannelNum,
			StartPK:     nextStartPK,
		})
		s.NoError(err)
	}

	// create index
	createIndexStatus, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
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

func (s *MiniClusterSuite) DropAllCollections() {
	ctx := s.Cluster.GetContext()
	collections, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(collections.Status))

	for _, collection := range collections.CollectionNames {
		releaseStatus, err := s.Cluster.MilvusClient.ReleaseCollection(context.Background(), &milvuspb.ReleaseCollectionRequest{
			DbName:         "",
			CollectionName: collection,
		})
		if err := merr.CheckRPCCall(releaseStatus, err); err != nil {
			panic(fmt.Sprintf("failed to release collection %s", collection))
		}

		dropStatus, err := s.Cluster.MilvusClient.DropCollection(context.Background(), &milvuspb.DropCollectionRequest{
			DbName:         "",
			CollectionName: collection,
		})
		if err := merr.CheckRPCCall(dropStatus, err); err != nil {
			panic(fmt.Sprintf("failed to drop collection %s", collection))
		}
	}
}
