package integration

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestAliasOperations(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestAliasOperations"

	// create 2 collection
	dbName := ""
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128
	collectionName := prefix + funcutil.GenRandomStr()
	collectionName1 := collectionName + "1"
	collectionName2 := collectionName + "2"

	constructCollectionSchema := func(collName string) *schemapb.CollectionSchema {
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
			Name:        collName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema1 := constructCollectionSchema(collectionName1)
	marshaledSchema1, err := proto.Marshal(schema1)
	assert.NoError(t, err)
	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName1,
		Schema:         marshaledSchema1,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	log.Info("CreateCollection 1 result", zap.Any("createCollectionStatus", createCollectionStatus))

	schema2 := constructCollectionSchema(collectionName2)
	marshaledSchema2, err := proto.Marshal(schema2)
	assert.NoError(t, err)
	createCollectionStatus2, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName2,
		Schema:         marshaledSchema2,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	log.Info("CreateCollection 2 result", zap.Any("createCollectionStatus", createCollectionStatus2))

	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, 2, len(showCollectionsResp.CollectionNames))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	// create alias
	// alias11 -> collection1
	// alias12 -> collection1
	// alias21 -> collection2
	createAliasResp1, err := c.proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		CollectionName: collectionName1,
		Alias:          "alias11",
	})
	assert.NoError(t, err)
	assert.Equal(t, createAliasResp1.GetErrorCode(), commonpb.ErrorCode_Success)
	createAliasResp2, err := c.proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		CollectionName: collectionName1,
		Alias:          "alias12",
	})
	assert.NoError(t, err)
	assert.Equal(t, createAliasResp2.GetErrorCode(), commonpb.ErrorCode_Success)
	createAliasResp3, err := c.proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		CollectionName: collectionName2,
		Alias:          "alias21",
	})
	assert.NoError(t, err)
	assert.Equal(t, createAliasResp3.GetErrorCode(), commonpb.ErrorCode_Success)

	describeAliasResp1, err := c.proxy.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		Alias: "alias11",
	})
	assert.NoError(t, err)
	assert.Equal(t, describeAliasResp1.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, collectionName1, describeAliasResp1.GetCollection())
	log.Info("describeAliasResp1",
		zap.String("alias", describeAliasResp1.GetAlias()),
		zap.String("collection", describeAliasResp1.GetCollection()))

	describeAliasResp2, err := c.proxy.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		Alias: "alias12",
	})
	assert.NoError(t, err)
	assert.Equal(t, describeAliasResp2.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, collectionName1, describeAliasResp2.GetCollection())
	log.Info("describeAliasResp2",
		zap.String("alias", describeAliasResp2.GetAlias()),
		zap.String("collection", describeAliasResp2.GetCollection()))

	describeAliasResp3, err := c.proxy.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		Alias: "alias21",
	})
	assert.NoError(t, err)
	assert.Equal(t, describeAliasResp3.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, collectionName2, describeAliasResp3.GetCollection())
	log.Info("describeAliasResp3",
		zap.String("alias", describeAliasResp3.GetAlias()),
		zap.String("collection", describeAliasResp3.GetCollection()))

	listAliasesResp, err := c.proxy.ListAliases(ctx, &milvuspb.ListAliasRequest{})
	assert.NoError(t, err)
	assert.Equal(t, listAliasesResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, 3, len(listAliasesResp.Aliases))

	log.Info("listAliasesResp", zap.Strings("aliases", listAliasesResp.Aliases))

	dropAliasResp1, err := c.proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{
		Alias: "alias11",
	})
	assert.NoError(t, err)
	assert.Equal(t, dropAliasResp1.GetErrorCode(), commonpb.ErrorCode_Success)

	dropAliasResp3, err := c.proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{
		Alias: "alias21",
	})
	assert.NoError(t, err)
	assert.Equal(t, dropAliasResp3.GetErrorCode(), commonpb.ErrorCode_Success)

	listAliasesRespNew, err := c.proxy.ListAliases(ctx, &milvuspb.ListAliasRequest{})
	assert.NoError(t, err)
	assert.Equal(t, listAliasesRespNew.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, 1, len(listAliasesRespNew.Aliases))

	log.Info("listAliasesResp after drop", zap.Strings("aliases", listAliasesResp.Aliases))

	log.Info("TestAliasOperations succeed")
}
