package messageutil

import (
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// ApplyCollectionSchemaUpdates applies the schema projection of AlterCollection
// with the same field-mask semantics as the coordinator's collection model.
// A nil result means that none of the masks affects CollectionSchema. The
// returned schema owns its data; neither the previous schema nor the WAL body
// is mutated. Metadata-only updates keep the encoding version unchanged.
func ApplyCollectionSchemaUpdates(current *schemapb.CollectionSchema, header *message.AlterCollectionMessageHeader, body *message.AlterCollectionMessageBody) *schemapb.CollectionSchema {
	var schema *schemapb.CollectionSchema
	updates := body.GetUpdates()
	for _, path := range header.GetUpdateMask().GetPaths() {
		switch path {
		case message.FieldMaskDB, message.FieldMaskCollectionName,
			message.FieldMaskCollectionDescription, message.FieldMaskCollectionProperties,
			message.FieldMaskCollectionSchema, message.FieldMaskCollectionExternalSpec:
			if schema == nil {
				schema = proto.Clone(current).(*schemapb.CollectionSchema)
			}
		default:
			continue
		}
		switch path {
		case message.FieldMaskDB:
			schema.DbName = updates.GetDbName()
		case message.FieldMaskCollectionName:
			schema.Name = updates.GetCollectionName()
		case message.FieldMaskCollectionDescription:
			schema.Description = updates.GetDescription()
		case message.FieldMaskCollectionProperties:
			schema.Properties = updates.GetProperties()
		case message.FieldMaskCollectionSchema:
			next := updates.GetSchema()
			schema.AutoID = next.GetAutoID()
			schema.Fields = next.GetFields()
			schema.StructArrayFields = next.GetStructArrayFields()
			schema.Functions = next.GetFunctions()
			schema.EnableDynamicField = next.GetEnableDynamicField()
			schema.EnableNamespace = next.GetEnableNamespace()
			schema.Version = next.GetVersion()
			schema.FileResourceIds = next.GetFileResourceIds()
			schema.ExternalSource = next.GetExternalSource()
			schema.ExternalSpec = next.GetExternalSpec()
		case message.FieldMaskCollectionExternalSpec:
			// Legacy messages can carry only one half of the external tuple.
			if value := updates.GetSchema().GetExternalSource(); value != "" {
				schema.ExternalSource = value
			}
			if value := updates.GetSchema().GetExternalSpec(); value != "" {
				schema.ExternalSpec = value
			}
		}
	}
	if schema == nil {
		return nil
	}
	schema = proto.Clone(schema).(*schemapb.CollectionSchema)
	// This transient coordinator/DataCoord flag is not schema metadata.
	schema.Properties = slices.DeleteFunc(schema.Properties, func(property *commonpb.KeyValuePair) bool {
		return property.GetKey() == common.CollectionOnTruncatingKey
	})
	return schema
}
