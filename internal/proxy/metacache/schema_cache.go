package metacache

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *MetaCache) GetCollectionInfo(ctx context.Context, database, name string, id int64) (*CollectionInfo, error) {
	return m.getCollectionMetadata(ctx, database, name, id)
}

// GetCollectionWriteInfo is retained for existing cache implementations. Schema
// selection belongs to CollectionInfo.GetWriteSchema, not to this metadata lookup.
func (m *MetaCache) GetCollectionWriteInfo(ctx context.Context, database, name string, id int64) (*CollectionInfo, error) {
	return m.GetCollectionInfo(ctx, database, name, id)
}

func (info *CollectionInfo) GetReadSchema(ctx context.Context) (*SchemaInfo, error) {
	channel, err := info.schemaChannel()
	if err != nil {
		return nil, err
	}
	return streaming.WAL().SchemaClient().GetSchemaVersion(ctx, streaming.GetSchemaVersionRequest{
		VChannel: channel, Version: info.ReadSchemaVersion,
	})
}

func (info *CollectionInfo) GetWriteSchema(ctx context.Context) (*SchemaInfo, error) {
	channel, err := info.schemaChannel()
	if err != nil {
		return nil, err
	}
	return streaming.WAL().SchemaClient().GetCurrentWriteSchema(ctx, streaming.GetCurrentWriteSchemaRequest{VChannel: channel})
}

func (info *CollectionInfo) schemaChannel() (string, error) {
	if len(info.VChannels) == 0 {
		return "", merr.WrapErrServiceNotReadyMsg("collection %d has no schema channel", info.CollID)
	}
	return info.VChannels[0], nil
}

// Refresh before publishing metadata, without depending on read-history availability.
func refreshSchema(ctx context.Context, info *CollectionInfo) error {
	channel, err := info.schemaChannel()
	if err != nil {
		return err
	}
	_, err = streaming.WAL().SchemaClient().GetCurrentWriteSchema(ctx, streaming.GetCurrentWriteSchemaRequest{
		VChannel: channel, BypassCache: true,
	})
	return err
}
