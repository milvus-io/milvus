package metacache

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type schemaIdentity struct {
	collectionID int64
	version      int32
}

// The read selector belongs to CollectionInfo's published metadata. A write
// selector is filled independently and expires with that same metadata entry.
// Immutable bodies can be shared by both selectors; history GC is a TODO.
type writeSchemaSelection struct {
	mu     sync.Mutex
	schema *SchemaInfo
}

func fetchCollectionSchema(ctx context.Context, vchannel string, version *int32) (*streaming.SchemaSnapshot, error) {
	client := streaming.WAL().SchemaClient()
	if version == nil {
		return client.GetCurrentWriteSchema(ctx, streaming.GetCurrentWriteSchemaRequest{VChannel: vchannel})
	}
	return client.GetSchemaVersion(ctx, streaming.GetSchemaVersionRequest{VChannel: vchannel, Version: *version})
}

func (m *MetaCache) resolveCollectionSchema(ctx context.Context, collectionID int64, channels []string, version *int32) (*SchemaInfo, error) {
	if version != nil {
		if body, ok := m.schemaBodies.Load(schemaIdentity{collectionID, *version}); ok {
			return body.(*SchemaInfo), nil
		}
	}
	if len(channels) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no schema channel", collectionID)
	}
	response, err := fetchCollectionSchema(ctx, channels[0], version)
	if err != nil {
		return nil, err
	}
	if response.Schema == nil || (version != nil && response.Schema.GetVersion() != *version) {
		return nil, merr.WrapErrServiceInternalMsg("streaming returned a mismatched collection schema")
	}
	// WAL schemas include RowID and Timestamp for segment encoding. Proxy
	// validates client rows against the user projection, just as it did with
	// DescribeCollection. Do not modify the immutable schema returned by SN.
	schema := proto.Clone(response.Schema).(*schemapb.CollectionSchema)
	fields := schema.Fields[:0]
	for _, field := range schema.Fields {
		if !common.IsSystemField(field.GetFieldID()) {
			fields = append(fields, field)
		}
	}
	schema.Fields = fields
	body, err := NewSchemaInfo(schema)
	if err != nil {
		return nil, err
	}
	stored, _ := m.schemaBodies.LoadOrStore(schemaIdentity{collectionID, body.GetVersion()}, body)
	return stored.(*SchemaInfo), nil
}

// GetCollectionWriteInfo resolves encoding/validation schema from the WAL write
// frontier, without replacing the published read selector in the metadata cache.
func (m *MetaCache) GetCollectionWriteInfo(ctx context.Context, database, name string, collectionID int64) (*CollectionInfo, error) {
	info, err := m.GetCollectionInfo(ctx, database, name, collectionID)
	if err != nil {
		return nil, err
	}
	selection := info.writeSchema
	selection.mu.Lock()
	defer selection.mu.Unlock()
	if selection.schema == nil {
		body, err := m.resolveCollectionSchema(ctx, info.CollID, info.VChannels, nil)
		if err != nil {
			return nil, err
		}
		selection.schema, err = body.withCollectionMetadata(info.Schema.CollectionSchema, info.Properties)
		if err != nil {
			return nil, err
		}
	}
	write := *info
	write.Schema = selection.schema
	write.CollectionTTL = GetCollectionTTL(selection.schema.GetProperties())
	return &write, nil
}

// Collection metadata can change without a column/schema revision. Overlay it
// on the request selector, never on the shared historical body. TTL remains
// bound to the selected schema, including an unpublished write-side version.
func (s *SchemaInfo) withCollectionMetadata(metadata *schemapb.CollectionSchema, properties []*commonpb.KeyValuePair) (*SchemaInfo, error) {
	props := common.CloneKeyValuePairs(properties).ToMap()
	versioned := common.CloneKeyValuePairs(s.GetProperties()).ToMap()
	for _, key := range []string{common.CollectionTTLConfigKey, common.CollectionTTLFieldKey} {
		if value, ok := versioned[key]; ok {
			props[key] = value
		} else {
			delete(props, key)
		}
	}
	projected := common.NewKeyValuePairs(props)
	if s.Name == metadata.GetName() && s.Description == metadata.GetDescription() &&
		s.ExternalSource == metadata.GetExternalSource() && s.ExternalSpec == metadata.GetExternalSpec() &&
		projected.Equal(s.GetProperties()) {
		return s, nil
	}
	schema := proto.Clone(s.CollectionSchema).(*schemapb.CollectionSchema)
	schema.Name = metadata.GetName()
	schema.Description = metadata.GetDescription()
	schema.ExternalSource = metadata.GetExternalSource()
	schema.ExternalSpec = metadata.GetExternalSpec()
	schema.Properties = projected
	return NewSchemaInfo(schema)
}
