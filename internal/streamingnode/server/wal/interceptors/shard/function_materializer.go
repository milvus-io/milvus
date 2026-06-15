package shard

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func (impl *shardInterceptor) allocFunctionRunners(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) {
	if !function.HasEmbeddingFunctions(schema) {
		return
	}
	errCh := function.AllocFunctionRunners(collectionID, vchannel, schema)
	if errCh == nil {
		return
	}
	go func() {
		if err := <-errCh; err != nil {
			impl.shardManager.Logger().Warn("failed to allocate function runners",
				zap.Int64("collectionID", collectionID),
				zap.String("vchannel", vchannel),
				zap.Int32("schemaVersion", 0),
				zap.Error(err))
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
	changed, ok, err := function.TryMaterialize(collectionID, 0, body)
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
