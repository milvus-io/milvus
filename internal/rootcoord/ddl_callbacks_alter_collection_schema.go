package rootcoord

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastAlterCollectionForAddFunctionField broadcasts the put collection message for add function field.
func (c *Core) broadcastAlterCollectionSchema(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	// 1. check if the request is valid.
	action := req.GetAction()
	if action == nil {
		return merr.WrapErrParameterInvalidMsg("action is nil")
	}
	addRequest := action.GetAddRequest()
	if addRequest == nil {
		return merr.WrapErrParameterInvalidMsg("add_request is nil, only add operation is supported for now")
	}

	fieldInfos := addRequest.GetFieldInfos()
	funcSchemas := addRequest.GetFuncSchema()
	if len(funcSchemas) != 1 || funcSchemas[0] == nil {
		return merr.WrapErrParameterInvalidMsg("For now, exactly one function schema is supported in alter schema task")
	}
	functionSchema := funcSchemas[0]

	if len(fieldInfos) == 0 {
		return merr.WrapErrParameterInvalidMsg("fieldInfos is empty")
	}

	// 2. check if the field schemas are illegal.
	fieldSchemas := make([]*schemapb.FieldSchema, 0, len(fieldInfos))
	for _, fieldInfo := range fieldInfos {
		fieldSchema := fieldInfo.GetFieldSchema()
		if fieldSchema == nil {
			return merr.WrapErrParameterInvalidMsg("fieldSchema is nil in fieldInfos")
		}
		fieldSchemas = append(fieldSchemas, fieldSchema)
	}
	if err := checkFieldSchema(fieldSchemas); err != nil {
		return errors.Wrap(err, "failed to check field schema")
	}

	// 3. check if the fields already exist
	fieldNameSet := make(map[string]bool)
	for _, field := range coll.Fields {
		fieldNameSet[field.Name] = true
	}
	for _, fieldSchema := range fieldSchemas {
		if fieldNameSet[fieldSchema.Name] {
			return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", fieldSchema.Name)
		}
		fieldNameSet[fieldSchema.Name] = true
	}

	// 4. check if the function already exists
	for _, function := range coll.Functions {
		if function.Name == functionSchema.GetName() {
			return merr.WrapErrParameterInvalidMsg("function already exists, name: %s", functionSchema.GetName())
		}
	}

	// 5. assign new field and function ids.
	fieldIDStart := nextFieldID(coll)
	for i, fieldSchema := range fieldSchemas {
		fieldSchema.FieldID = fieldIDStart + int64(i)
	}
	functionSchema.Id = nextFunctionID(coll)
	name2id := make(map[string]int64)
	for _, field := range coll.Fields {
		name2id[field.Name] = field.FieldID
	}
	for _, fieldSchema := range fieldSchemas {
		name2id[fieldSchema.Name] = fieldSchema.FieldID
	}

	functionSchema.InputFieldIds = make([]int64, len(functionSchema.InputFieldNames))
	for idx, name := range functionSchema.InputFieldNames {
		fieldID, ok := name2id[name]
		if !ok {
			return fmt.Errorf("input field %s of function %s not found", name, functionSchema.GetName())
		}
		functionSchema.InputFieldIds[idx] = fieldID
	}

	functionSchema.OutputFieldIds = make([]int64, len(functionSchema.OutputFieldNames))
	for idx, name := range functionSchema.OutputFieldNames {
		fieldID, ok := name2id[name]
		if !ok {
			return fmt.Errorf("output field %s of function %s not found", name, functionSchema.GetName())
		}
		functionSchema.OutputFieldIds[idx] = fieldID
	}

	// 6. build new collection schema.
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: coll.EnableDynamicField,
		Properties:         coll.Properties,
		DbName:             coll.DBName,
		FileResourceIds:    coll.FileResourceIds,
		Version:            coll.SchemaVersion + 1,
		DoPhysicalBackfill: addRequest.GetDoPhysicalBackfill(),
	}
	schema.Fields = append(schema.Fields, fieldSchemas...)
	schema.Functions = append(schema.Functions, functionSchema)

	// 7. get cache expirations.
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema},
			},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: schema,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
