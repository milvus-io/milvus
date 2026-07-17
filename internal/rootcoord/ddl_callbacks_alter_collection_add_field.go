package rootcoord

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastAlterCollectionForAddField broadcasts the put collection message for add field.
func (c *Core) broadcastAlterCollectionForAddField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// check if the collection is created.
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	// check if the field schema is illegal.
	fieldSchema := &schemapb.FieldSchema{}
	if err = proto.Unmarshal(req.Schema, fieldSchema); err != nil {
		return merr.Wrap(err, "failed to unmarshal field schema")
	}
	if fieldSchema.GetDataType() == schemapb.DataType_Text && fieldSchema.GetDefaultValue() != nil {
		return merr.WrapErrParameterInvalidMsg("default value is not supported when adding TEXT field, field name = %s", fieldSchema.GetName())
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return merr.Wrap(err, "failed to check field schema")
	}
	if fieldSchema.GetDataType() == schemapb.DataType_Timestamptz {
		timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, coll.Properties)
		if !exist {
			timezone = common.DefaultTimezone
		}
		if err := timestamptz.CheckAndRewriteTimestampTzDefaultValueForFieldSchema(fieldSchema, timezone); err != nil {
			return merr.WrapErrParameterInvalidErr(err, "invalid default value of field, name: %s", fieldSchema.Name)
		}
	}
	// check if the field already exists
	fieldNames := typeutil.NewSet[string]()
	for _, field := range coll.Fields {
		fieldNames.Insert(field.Name)
	}
	for _, structField := range coll.StructArrayFields {
		fieldNames.Insert(structField.Name)
		for _, field := range structField.Fields {
			fieldNames.Insert(field.Name)
			fieldNames.Insert(storedRootStructSubFieldName(structField.Name, field.Name))
		}
	}
	if fieldNames.Contain(fieldSchema.Name) {
		// TODO: idempotency check here.
		return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", fieldSchema.Name)
	}

	// build new collection schema.
	schema := coll.ToCollectionSchemaPB()
	// assign a new field id.
	fieldSchema.FieldID = maxAssignedFieldIDFromSchema(schema) + 1
	schema.Version = coll.SchemaVersion + 1
	schema.Fields = append(schema.Fields, fieldSchema)
	properties := updateMaxFieldIDProperty(coll.Properties, fieldSchema.GetFieldID())
	schema.Properties = properties
	if err := typeutil.ValidateExternalCollectionResolvedSchema(schema); err != nil {
		return err
	}
	if err := typeutil.ValidateTextRequiresStorageV3(schema, Params.CommonCfg.UseLoonFFI.GetAsBool()); err != nil {
		return merr.WrapErrParameterInvalidMsg("%s", err.Error())
	}
	if err := validateSchemaEvolution(coll, schema); err != nil {
		return err
	}

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	// broadcast the put collection v2 message.
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema, message.FieldMaskCollectionProperties},
			},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema:     schema,
				Properties: properties,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
