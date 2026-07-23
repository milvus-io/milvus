package shard

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func walFunctionRunnerKey(vchannel string) string {
	return "WAL-" + vchannel
}

func (impl *shardInterceptor) allocFunctionRunners(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) {
	key := walFunctionRunnerKey(vchannel)
	if err := function.GetManager().Alloc(collectionID, key, schema); err != nil {
		var schemaVersion int32
		if schema != nil {
			schemaVersion = schema.GetVersion()
		}
		impl.shardManager.Logger().Warn(context.TODO(), "failed to allocate function runners",
			mlog.Int64("collectionID", collectionID),
			mlog.String("vchannel", vchannel),
			mlog.String("key", key),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
	}
}

func (impl *shardInterceptor) updateFunctionRunners(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) {
	key := walFunctionRunnerKey(vchannel)
	if err := function.GetManager().Update(collectionID, key, schema); err != nil {
		var schemaVersion int32
		if schema != nil {
			schemaVersion = schema.GetVersion()
		}
		impl.shardManager.Logger().Warn(context.TODO(), "failed to update function runners",
			mlog.Int64("collectionID", collectionID),
			mlog.String("vchannel", vchannel),
			mlog.String("key", key),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
	}
}

type collectionSchemaProvider interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

func (impl *shardInterceptor) materializeFunctionFields(
	ctx context.Context,
	insertMsg message.MutableInsertMessageV1,
	collectionID int64,
	schemaVersion int32,
) error {
	body := insertMsg.MustBody()
	changed, err := function.GetManager().Materialize(ctx, collectionID, walFunctionRunnerKey(insertMsg.VChannel()), schemaVersion, body)
	if err != nil {
		return err
	}
	if changed {
		insertMsg.OverwriteBody(body)
	}
	return nil
}
