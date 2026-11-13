package metacache

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetWriteSchema returns the cached current write schema, refreshing from SN on a miss. Unlike
// the published read schema in CollectionInfo, this schema includes system fields.
func (info *CollectionInfo) GetWriteSchema(ctx context.Context) (*SchemaInfo, error) {
	if len(info.VChannels) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no schema channel", info.CollID)
	}
	return streaming.WAL().SchemaClient().GetCurrentWriteSchema(ctx, streaming.GetCurrentWriteSchemaRequest{VChannel: info.VChannels[0]})
}

// loadCollectionSchema refreshes the published version on every MetaCache miss,
// including metadata-only changes that do not advance the schema version.
func loadCollectionSchema(ctx context.Context, collection *milvuspb.DescribeCollectionResponse) (*SchemaInfo, error) {
	if len(collection.GetVirtualChannelNames()) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no schema channel", collection.GetCollectionID())
	}
	full, err := streaming.WAL().SchemaClient().GetSchemaVersion(ctx, streaming.GetSchemaVersionRequest{
		VChannel: collection.GetVirtualChannelNames()[0], Version: collection.GetSchema().GetVersion(), BypassCache: true,
	})
	if err != nil {
		return nil, err
	}
	// Only the protobuf envelope and field slice are detached. Field nodes stay
	// shared and immutable; build the read indices once for this cache entry so
	// name lookup has the same visibility as field enumeration.
	schema := schemautil.ShallowSchema(full.CollectionSchema)
	schema.Fields = make([]*schemapb.FieldSchema, 0, len(full.GetFields()))
	for _, field := range full.GetFields() {
		if !common.IsSystemField(field.GetFieldID()) {
			schema.Fields = append(schema.Fields, field)
		}
	}
	return NewSchemaInfo(schema)
}
