package shard

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func walFunctionRunnerKey(vchannel string) string {
	return "WAL-" + vchannel
}

func (impl *shardInterceptor) allocFunctionRunners(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) {
	key := walFunctionRunnerKey(vchannel)
	schemaVersion := function.LatestFunctionRunnerVersion
	if schema != nil {
		schemaVersion = schema.GetVersion()
	}
	if err := function.AllocFunctionRunners(collectionID, key, schema); err != nil {
		impl.shardManager.Logger().Warn("failed to allocate function runners",
			zap.Int64("collectionID", collectionID),
			zap.String("vchannel", vchannel),
			zap.String("key", key),
			zap.Int32("schemaVersion", schemaVersion),
			zap.Error(err))
	}
}

type collectionSchemaProvider interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

func (impl *shardInterceptor) materializeFunctionFields(ctx context.Context, insertMsg message.MutableInsertMessageV1, collectionID int64, schemaVersion int32) error {
	body := insertMsg.MustBody()
	changed, ok, err := function.TryMaterialize(collectionID, schemaVersion, body)
	if err != nil {
		return err
	}
	if ok {
		if changed {
			insertMsg.OverwriteBody(body)
		}
		return nil
	}

	changed, err = function.FillFunctionData(ctx, collectionID, nil, body)
	if err != nil {
		return err
	}
	if changed {
		insertMsg.OverwriteBody(body)
	}
	return nil
}
