package metacache

import (
	"context"
	"maps"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetCollectionInfo retains the original facade while resolving the body only
// for this request. Metadata fills and write lookups never resolve a read body.
func (m *MetaCache) GetCollectionInfo(ctx context.Context, database, name string, id int64) (*CollectionInfo, error) {
	info, err := m.getCollectionMetadata(ctx, database, name, id)
	if err != nil {
		return nil, err
	}
	return m.collectionInfoWithSchema(ctx, info, false)
}

func (m *MetaCache) GetCollectionWriteInfo(ctx context.Context, database, name string, id int64) (*CollectionInfo, error) {
	info, err := m.getCollectionMetadata(ctx, database, name, id)
	if err != nil {
		return nil, err
	}
	return m.collectionInfoWithSchema(ctx, info, true)
}

func (m *MetaCache) collectionInfoWithSchema(ctx context.Context, info *CollectionInfo, write bool) (*CollectionInfo, error) {
	schema, err := resolveSchema(ctx, info, write)
	if err != nil {
		return nil, err
	}
	result := *info
	result.Schema = schema
	result.CollectionTTL = GetCollectionTTL(schema.GetProperties())
	return &result, nil
}

func resolveSchema(ctx context.Context, info *CollectionInfo, write bool) (*SchemaInfo, error) {
	if len(info.VChannels) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no schema channel", info.CollID)
	}
	client := streaming.WAL().SchemaClient()
	var snapshot *streaming.SchemaSnapshot
	var err error
	if write {
		snapshot, err = client.GetCurrentWriteSchema(ctx, streaming.GetCurrentWriteSchemaRequest{VChannel: info.VChannels[0]})
	} else {
		snapshot, err = client.GetSchemaVersion(ctx, streaming.GetSchemaVersionRequest{VChannel: info.VChannels[0], Version: info.ReadSchemaVersion})
	}
	if err != nil {
		return nil, err
	}
	schema, err := snapshot.UserSchema()
	if err != nil {
		return nil, err
	}
	return withCollectionMetadata(schema, info), nil
}

// Only the small protobuf envelope and properties belong to this operation.
// Reuse field nodes and derived indices; TTL remains bound to the selected
// version while other properties (notably timezone) follow current metadata.
func withCollectionMetadata(s *SchemaInfo, info *CollectionInfo) *SchemaInfo {
	props := common.KeyValuePairs(info.Properties).ToMap()
	versioned := common.KeyValuePairs(s.GetProperties()).ToMap()
	for _, key := range []string{common.CollectionTTLConfigKey, common.CollectionTTLFieldKey} {
		if value, ok := versioned[key]; ok {
			props[key] = value
		} else {
			delete(props, key)
		}
	}
	projected := s.GetProperties()
	if !maps.Equal(props, versioned) {
		projected = common.NewKeyValuePairs(props)
	}
	schema := schemautil.ShallowSchema(s.CollectionSchema)
	schema.Name = info.Name
	schema.DbName = info.DBName
	schema.Description = info.Description
	schema.ExternalSource = info.ExternalSource
	schema.ExternalSpec = info.ExternalSpec
	schema.Properties = projected
	view := *s
	view.CollectionSchema = schema
	view.SchemaHelper = s.SchemaHelper.WithMetadata(schema)
	return &view
}
