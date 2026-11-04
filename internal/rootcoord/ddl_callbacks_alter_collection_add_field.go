package rootcoord

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
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
	if err := refuseTextFieldDuringShardSplit(coll, fieldSchema); err != nil {
		return err
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
	addedFileResourceIds, err := c.prepareAlterCollectionAnalyzerFileResources(ctx, coll, schema)
	if err != nil {
		return err
	}

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
		WithBroadcast(coll.VirtualChannelNames).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		rollbackAlterCollectionAnalyzerFileResourceReservation(ctx, c.meta, coll.CollectionID, addedFileResourceIds, err)
		return err
	}
	return nil
}

// refuseTextFieldDuringShardSplit is asked by every path that can add a
// field: AddCollectionField and AlterCollectionSchema's add action. (A struct
// sub-field is an Array or ArrayOfVector, never TEXT, and AlterCollectionField
// never changes a field's type.)
//
// It refuses to add a TEXT field while a shard
// split of the collection is in flight: any shard of it is Splitting or
// Creating. A split moves the source's data by rewriting it, and the rewrite
// does not carry TEXT fields (LOB references), so a TEXT field added mid-split
// would make every remaining rewrite plan fail while the split, past its fence,
// can no longer abort.
//
// The request is valid; the collection is in a transient state that ends when
// the split adopts its targets. So the error is System and retriable
// (ServiceUnavailable), not an input error.
func refuseTextFieldDuringShardSplit(coll *model.Collection, field *schemapb.FieldSchema) error {
	if field.GetDataType() != schemapb.DataType_Text || !collectionHasShardSplitInFlight(coll) {
		return nil
	}
	return merr.WrapErrServiceUnavailable(fmt.Sprintf(
		"collection %d has a shard split in flight, a TEXT field %s can be added once it finishes",
		coll.CollectionID, field.GetName()))
}

// collectionHasShardSplitInFlight reports whether any shard of the collection
// is Splitting or Creating.
func collectionHasShardSplitInFlight(coll *model.Collection) bool {
	for _, shard := range coll.ShardInfos {
		if shard == nil {
			continue
		}
		if shard.State == schemapb.ShardState_ShardSplitting || shard.State == schemapb.ShardState_ShardCreating {
			return true
		}
	}
	return false
}
