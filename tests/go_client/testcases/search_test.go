package testcases

import (
	"testing"
	"time"

	clientv2 "github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
	"go.uber.org/zap"
)

func TestSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())
	log.Info("schema", zap.Any("schema", schema))

	insertParams := hp.NewInsertParams(schema, common.DefaultNb)
	hp.CollPrepare.InsertData(ctx, t, mc, insertParams, hp.TNewDataOption())

	// flush -> index -> load
	hp.CollPrepare.FlushData(ctx, t, mc, schema.CollectionName)
	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.NewIndexParams(schema))
	hp.CollPrepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	resSearch, err := mc.Search(ctx, clientv2.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)

	log.Info("search", zap.Any("resSearch", resSearch))
	log.Info("search", zap.Any("ids", resSearch[0].IDs))
	log.Info("search", zap.Any("scores", resSearch[0].Scores))
	id, _ := resSearch[0].IDs.GetAsInt64(0)
	log.Info("search", zap.Int64("ids", id))
}
