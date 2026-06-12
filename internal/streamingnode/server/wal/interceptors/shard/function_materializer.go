package shard

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (impl *shardInterceptor) allocFunctionRunners(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) {
	if !function.HasEmbeddingFunctions(schema) {
		return
	}
	errCh := function.AllocFunctionRunners(collectionID, vchannel, schema)
	if errCh == nil {
		return
	}
	schemaVersion := int32(0)
	if schema != nil {
		schemaVersion = schema.GetVersion()
	}
	go func() {
		if err := <-errCh; err != nil {
			impl.shardManager.Logger().Warn(context.TODO(), "failed to allocate function runners",
				mlog.Int64("collectionID", collectionID),
				mlog.String("vchannel", vchannel),
				mlog.Int32("schemaVersion", schemaVersion),
				mlog.Err(err))
		}
	}()
}

func (impl *shardInterceptor) updateFunctionRunners(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) {
	if !function.HasEmbeddingFunctions(schema) {
		function.ReleaseFunctionRunners(collectionID, vchannel)
		return
	}
	errCh := function.UpdateFunctionRunners(collectionID, vchannel, schema)
	if errCh == nil {
		return
	}
	schemaVersion := int32(0)
	if schema != nil {
		schemaVersion = schema.GetVersion()
	}
	go func() {
		if err := <-errCh; err != nil {
			impl.shardManager.Logger().Warn(context.TODO(), "failed to update function runners",
				mlog.Int64("collectionID", collectionID),
				mlog.String("vchannel", vchannel),
				mlog.Int32("schemaVersion", schemaVersion),
				mlog.Err(err))
		}
	}()
}

type collectionSchemaProvider interface {
	GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo
}

type collectionSchemaGetter interface {
	GetCollectionSchema(collectionID int64, schemaVersion int32) (*schemapb.CollectionSchema, error)
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

	schemaGetter, ok := impl.shardManager.(collectionSchemaGetter)
	if !ok {
		return nil
	}

	schema, err := schemaGetter.GetCollectionSchema(collectionID, schemaVersion)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			return nil
		}
		return err
	}
	if !function.HasEmbeddingFunctions(schema) {
		return nil
	}

	changed, err = function.FillFunctionData(ctx, collectionID, schema, body)
	if err != nil {
		return err
	}
	if changed {
		insertMsg.OverwriteBody(body)
	}
	return nil
}
