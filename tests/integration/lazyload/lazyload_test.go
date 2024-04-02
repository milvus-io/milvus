package lazyload_test

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type LazyloadTestSuite struct {
	integration.MiniClusterSuite
}

func (s *LazyloadTestSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.LazyLoadEnabled.Key, "true")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *LazyloadTestSuite) TestLazyloadSearch() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestUpsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 300

	schema := integration.ConstructSchema(collectionName, dim, false)
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
	s.NoError(err)

	pkFieldData := integration.NewInt64FieldData(integration.Int64Field, rowNum)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	res, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)

	err = merr.Error(res.Status)
	s.NoError(err)

	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.True(merr.Ok(createIndexStatus))
	status, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(status)
	s.NoError(err)

	s.WaitForLoad(ctx, collectionName)
	req := integration.ConstructSearchRequest(dbName, collectionName, "", integration.FloatVecField, schemapb.DataType_FloatVector, []string{}, metric.L2, map[string]interface{}{}, 1, dim, 1, -1)
	searchRes, err := s.Cluster.Proxy.Search(ctx, req)
	s.NoError(err)
	err = merr.Error(searchRes.Status)
	s.NoError(err)
	log.Info("Search result", zap.Any("searchRes", searchRes))
}

func TestLazyLoad(t *testing.T) {
	suite.Run(t, new(LazyloadTestSuite))
}
