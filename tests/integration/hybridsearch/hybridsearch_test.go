package hybridsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type HybridSearchSuite struct {
	integration.MiniClusterSuite
}

func (s *HybridSearchSuite) TestHybridSearch() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestHybridSearch"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := integration.ConstructSchema(collectionName, dim, true,
		&schemapb.FieldSchema{Name: integration.Int64Field, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		&schemapb.FieldSchema{Name: integration.FloatVecField, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
		&schemapb.FieldSchema{Name: integration.BinVecField, DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
		&schemapb.FieldSchema{Name: integration.SparseFloatVecField, DataType: schemapb.DataType_SparseFloatVector},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	bVecColumn := integration.NewBinaryVectorFieldData(integration.BinVecField, rowNum, dim)
	sparseVecColumn := integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn, bVecColumn, sparseVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))

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

	// load without index on vector fields
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Error(merr.Error(loadStatus))

	// create index for float vector
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default_float",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load with index on partial vector fields
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Error(merr.Error(loadStatus))

	// create index for binary vector
	createIndexStatus, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.BinVecField,
		IndexName:      "_default_binary",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissBinIvfFlat, metric.JACCARD),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}
	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, integration.BinVecField, "_default_binary")

	// load with index on partial vector fields
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Error(merr.Error(loadStatus))

	// create index for sparse float vector
	createIndexStatus, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default_sparse",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexSparseInvertedIndex, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}
	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, integration.SparseFloatVecField, "_default_sparse")

	// load with index on all vector fields
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 1
	topk := 10
	roundDecimal := -1

	fParams := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	bParams := integration.GetSearchParams(integration.IndexFaissBinIvfFlat, metric.L2)
	sParams := integration.GetSearchParams(integration.IndexSparseInvertedIndex, metric.IP)
	fSearchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, fParams, nq, dim, topk, roundDecimal)

	bSearchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.BinVecField, schemapb.DataType_BinaryVector, nil, metric.JACCARD, bParams, nq, dim, topk, roundDecimal)

	sSearchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.SparseFloatVecField, schemapb.DataType_SparseFloatVector, nil, metric.IP, sParams, nq, dim, topk, roundDecimal)
	hSearchReq := &milvuspb.HybridSearchRequest{
		Base:           nil,
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionNames: nil,
		Requests:       []*milvuspb.SearchRequest{fSearchReq, bSearchReq, sSearchReq},
		OutputFields:   []string{integration.FloatVecField, integration.BinVecField},
	}

	// rrf rank hybrid search
	rrfParams := make(map[string]float64)
	rrfParams[proxy.RRFParamsKey] = 60
	b, err := json.Marshal(rrfParams)
	s.NoError(err)
	hSearchReq.RankParams = []*commonpb.KeyValuePair{
		{Key: proxy.RankTypeKey, Value: "rrf"},
		{Key: proxy.RankParamsKey, Value: string(b)},
		{Key: proxy.LimitKey, Value: strconv.Itoa(topk)},
		{Key: proxy.RoundDecimalKey, Value: strconv.Itoa(roundDecimal)},
	}

	searchResult, err := c.Proxy.HybridSearch(ctx, hSearchReq)

	s.NoError(merr.CheckRPCCall(searchResult, err))

	// weighted rank hybrid search
	weightsParams := make(map[string][]float64)
	weightsParams[proxy.WeightsParamsKey] = []float64{0.5, 0.2, 0.1}
	b, err = json.Marshal(weightsParams)
	s.NoError(err)

	// create a new request preventing data race
	hSearchReq = &milvuspb.HybridSearchRequest{
		Base:           nil,
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionNames: nil,
		Requests:       []*milvuspb.SearchRequest{fSearchReq, bSearchReq, sSearchReq},
		OutputFields:   []string{integration.FloatVecField, integration.BinVecField, integration.SparseFloatVecField},
	}
	hSearchReq.RankParams = []*commonpb.KeyValuePair{
		{Key: proxy.RankTypeKey, Value: "weighted"},
		{Key: proxy.RankParamsKey, Value: string(b)},
		{Key: proxy.LimitKey, Value: strconv.Itoa(topk)},
	}

	searchResult, err = c.Proxy.HybridSearch(ctx, hSearchReq)

	s.NoError(merr.CheckRPCCall(searchResult, err))

	log.Info("TestHybridSearch succeed")
}

// this is special case to verify the correctness of hybrid search reduction
func (s *HybridSearchSuite) TestHybridSearchSingleSubReq() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestHybridSearch"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000

	schema := integration.ConstructSchema(collectionName, dim, true,
		&schemapb.FieldSchema{Name: integration.Int64Field, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		&schemapb.FieldSchema{Name: integration.FloatVecField, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

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
	s.True(merr.Ok(insertResult.GetStatus()))

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

	// load without index on vector fields
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Error(merr.Error(loadStatus))

	// create index for float vector
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default_float",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load with index on vector fields
	loadStatus, err = c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.NoError(merr.Error(loadStatus))
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 1
	topk := 10
	roundDecimal := -1

	fParams := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	fSearchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, fParams, nq, dim, topk, roundDecimal)

	hSearchReq := &milvuspb.HybridSearchRequest{
		Base:           nil,
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionNames: nil,
		Requests:       []*milvuspb.SearchRequest{fSearchReq},
		OutputFields:   []string{integration.FloatVecField},
	}

	// rrf rank hybrid search
	rrfParams := make(map[string]float64)
	rrfParams[proxy.RRFParamsKey] = 60
	b, err := json.Marshal(rrfParams)
	s.NoError(err)
	hSearchReq.RankParams = []*commonpb.KeyValuePair{
		{Key: proxy.RankTypeKey, Value: "rrf"},
		{Key: proxy.RankParamsKey, Value: string(b)},
		{Key: proxy.LimitKey, Value: strconv.Itoa(topk)},
		{Key: proxy.RoundDecimalKey, Value: strconv.Itoa(roundDecimal)},
	}

	searchResult, err := c.Proxy.HybridSearch(ctx, hSearchReq)

	s.NoError(merr.CheckRPCCall(searchResult, err))

	// weighted rank hybrid search
	weightsParams := make(map[string][]float64)
	weightsParams[proxy.WeightsParamsKey] = []float64{0.5}
	b, err = json.Marshal(weightsParams)
	s.NoError(err)

	// create a new request preventing data race
	hSearchReq = &milvuspb.HybridSearchRequest{
		Base:           nil,
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionNames: nil,
		Requests:       []*milvuspb.SearchRequest{fSearchReq},
		OutputFields:   []string{integration.FloatVecField},
	}
	hSearchReq.RankParams = []*commonpb.KeyValuePair{
		{Key: proxy.RankTypeKey, Value: "weighted"},
		{Key: proxy.RankParamsKey, Value: string(b)},
		{Key: proxy.LimitKey, Value: strconv.Itoa(topk)},
	}

	searchResult, err = c.Proxy.HybridSearch(ctx, hSearchReq)

	s.NoError(merr.CheckRPCCall(searchResult, err))
	s.Equal(topk, len(searchResult.GetResults().GetIds().GetIntId().Data))
	s.Equal(topk, len(searchResult.GetResults().GetScores()))
	s.Equal(int64(nq), searchResult.GetResults().GetNumQueries())
	log.Info("TestHybridSearchSingleSubRequest succeed")
}

func TestHybridSearch(t *testing.T) {
	suite.Run(t, new(HybridSearchSuite))
}
