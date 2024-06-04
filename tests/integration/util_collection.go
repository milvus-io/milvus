package integration

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	proto "github.com/milvus-io/milvus/internal/util/protobr"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
)

type CreateCollectionConfig struct {
	DBName           string
	CollectionName   string
	ChannelNum       int
	SegmentNum       int
	RowNumPerSegment int
	Dim              int
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
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := s.Cluster.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < cfg.SegmentNum; i++ {
		fVecColumn := NewFloatVectorFieldData(FloatVecField, cfg.RowNumPerSegment, cfg.Dim)
		hashKeys := GenerateHashKeys(cfg.RowNumPerSegment)
		insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         cfg.DBName,
			CollectionName: cfg.CollectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(cfg.RowNumPerSegment),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.Status))

		flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          cfg.DBName,
			CollectionNames: []string{cfg.CollectionName},
		})
		s.NoError(err)
		segmentIDs, has := flushResp.GetCollSegIDs()[cfg.CollectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(segmentIDs)
		s.Require().True(has)
		flushTs, has := flushResp.GetCollFlushTs()[cfg.CollectionName]
		s.True(has)
		s.WaitForFlush(ctx, ids, flushTs, cfg.DBName, cfg.CollectionName)
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
	s.WaitForIndexBuilt(ctx, cfg.CollectionName, FloatVecField)
}
