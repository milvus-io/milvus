// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastAlterCollectionSchema broadcasts the alter collection schema message to all channels.
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

	action := req.GetAction()
	if action == nil {
		return merr.WrapErrParameterInvalidMsg("action is nil")
	}

	switch action.GetOp().(type) {
	case *milvuspb.AlterCollectionSchemaRequest_Action_AddRequest:
		return c.broadcastAlterCollectionSchemaAdd(ctx, broadcaster, coll, req)
	case *milvuspb.AlterCollectionSchemaRequest_Action_DropRequest:
		return c.broadcastAlterCollectionSchemaDrop(ctx, broadcaster, coll, req)
	default:
		return merr.WrapErrParameterInvalidMsg("unknown action type in alter collection schema request")
	}
}

// broadcastAlterCollectionSchemaAdd handles AddRequest: adding function fields.
func (c *Core) broadcastAlterCollectionSchemaAdd(ctx context.Context, broadcaster broadcaster.BroadcastAPI, coll *model.Collection, req *milvuspb.AlterCollectionSchemaRequest) error {
	addRequest := req.GetAction().GetAddRequest()
	if addRequest == nil {
		return merr.WrapErrParameterInvalidMsg("add_request is nil")
	}

	fieldInfos := addRequest.GetFieldInfos()
	funcSchemas := addRequest.GetFuncSchema()
	if len(funcSchemas) != 1 || funcSchemas[0] == nil {
		return merr.WrapErrParameterInvalidMsg("For now, exactly one function schema is supported in alter schema task")
	}
	functionSchema := funcSchemas[0]

	// Physical backfill is currently only implemented for BM25 functions in the datanode
	// backfill_compactor. Proxy performs the same check; this is a defense-in-depth guard
	// for requests that bypass the Proxy (e.g. direct gRPC to RootCoord).
	if addRequest.GetDoPhysicalBackfill() && functionSchema.GetType() != schemapb.FunctionType_BM25 {
		return merr.WrapErrParameterInvalidMsg(
			"physical backfill is currently only supported for BM25 functions, got %s",
			functionSchema.GetType().String())
	}

	if len(fieldInfos) == 0 {
		return merr.WrapErrParameterInvalidMsg("fieldInfos is empty")
	}

	// Check field schemas.
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

	// Check fields don't already exist.
	fieldNameSet := make(map[string]struct{})
	for _, field := range coll.Fields {
		fieldNameSet[field.Name] = struct{}{}
	}
	for _, fieldSchema := range fieldSchemas {
		if _, ok := fieldNameSet[fieldSchema.GetName()]; ok {
			return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", fieldSchema.GetName())
		}
		fieldNameSet[fieldSchema.Name] = struct{}{}
	}

	// Check function doesn't already exist.
	for _, function := range coll.Functions {
		if function.Name == functionSchema.GetName() {
			return merr.WrapErrParameterInvalidMsg("function already exists, name: %s", functionSchema.GetName())
		}
	}

	// Assign new field and function IDs.
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
			return merr.WrapErrParameterInvalidMsg("input field %s of function %s not found", name, functionSchema.GetName())
		}
		functionSchema.InputFieldIds[idx] = fieldID
	}

	functionSchema.OutputFieldIds = make([]int64, len(functionSchema.OutputFieldNames))
	for idx, name := range functionSchema.OutputFieldNames {
		fieldID, ok := name2id[name]
		if !ok {
			return merr.WrapErrParameterInvalidMsg("output field %s of function %s not found", name, functionSchema.GetName())
		}
		functionSchema.OutputFieldIds[idx] = fieldID
	}

	// Build new collection schema.
	schema := coll.ToCollectionSchemaPB()
	schema.Version = coll.SchemaVersion + 1
	schema.DoPhysicalBackfill = addRequest.GetDoPhysicalBackfill()
	schema.Fields = append(schema.Fields, fieldSchemas...)
	schema.Functions = append(schema.Functions, functionSchema)

	// Broadcast.
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

// broadcastAlterCollectionSchemaDrop handles DropRequest: dropping fields or functions.
func (c *Core) broadcastAlterCollectionSchemaDrop(ctx context.Context, broadcaster broadcaster.BroadcastAPI, coll *model.Collection, req *milvuspb.AlterCollectionSchemaRequest) error {
	dropReq := req.GetAction().GetDropRequest()
	if dropReq == nil {
		return merr.WrapErrParameterInvalidMsg("drop_request is nil")
	}

	var schema *schemapb.CollectionSchema
	var properties []*commonpb.KeyValuePair
	var droppedFieldIds []int64
	var err error

	switch id := dropReq.GetIdentifier().(type) {
	case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FunctionName:
		schema, properties, droppedFieldIds, err = buildSchemaForDropFunction(coll, id.FunctionName)
	case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName:
		schema, properties, droppedFieldIds, err = buildSchemaForDropField(coll, id.FieldName, 0)
	case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldId:
		schema, properties, droppedFieldIds, err = buildSchemaForDropField(coll, "", id.FieldId)
	default:
		return merr.WrapErrParameterInvalidMsg("drop request must specify field_name, field_id, or function_name")
	}
	if err != nil {
		return err
	}

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
				Paths: []string{message.FieldMaskCollectionSchema, message.FieldMaskCollectionProperties},
			},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema:          schema,
				Properties:      properties,
				DroppedFieldIds: droppedFieldIds,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// buildSchemaForDropField builds the new schema, properties, and droppedFieldIds for dropping a field.
// It looks up the target by fieldName or fieldID across top-level Fields and StructArrayFields,
// removes it from the schema, and updates max_field_id. Dropping a sub-field of a struct array
// field is rejected (no symmetric add-sub-field support).
func buildSchemaForDropField(coll *model.Collection, fieldName string, fieldID int64) (
	schema *schemapb.CollectionSchema,
	properties []*commonpb.KeyValuePair,
	droppedFieldIds []int64,
	err error,
) {
	matchField := func(f *model.Field) bool {
		if fieldName != "" {
			return f.Name == fieldName
		}
		return fieldID > 0 && f.FieldID == fieldID
	}
	matchStruct := func(sf *model.StructArrayField) bool {
		if fieldName != "" {
			return sf.Name == fieldName
		}
		return fieldID > 0 && sf.FieldID == fieldID
	}

	// Top-level field path: remove from coll.Fields, droppedFieldIds has one ID.
	var droppedField *model.Field
	newFields := make([]*schemapb.FieldSchema, 0, len(coll.Fields))
	for _, f := range coll.Fields {
		if droppedField == nil && matchField(f) {
			droppedField = f
			continue
		}
		newFields = append(newFields, model.MarshalFieldModel(f))
	}
	if droppedField != nil {
		properties = updateMaxFieldIDProperty(coll.Properties, nextFieldID(coll)-1)
		schema = coll.ToCollectionSchemaPB()
		schema.Fields = newFields
		schema.Properties = properties
		schema.Version = coll.SchemaVersion + 1
		return schema, properties, []int64{droppedField.FieldID}, nil
	}

	// Struct array field path: remove the whole entry from StructArrayFields.
	// droppedFieldIds includes the struct ID plus every sub-field ID so that
	// index cascade (matched by FieldID) and segcore filtering (schema.has_field)
	// naturally cover every column that physically goes away.
	// Sub-field drops are already rejected at the proxy layer; if one reaches
	// here we fall through to the generic "field not found" tail.
	var droppedStruct *model.StructArrayField
	newStructs := make([]*schemapb.StructArrayFieldSchema, 0, len(coll.StructArrayFields))
	for _, s := range coll.StructArrayFields {
		if droppedStruct == nil && matchStruct(s) {
			droppedStruct = s
			continue
		}
		newStructs = append(newStructs, model.MarshalStructArrayFieldModel(s))
	}
	if droppedStruct != nil {
		droppedIDs := make([]int64, 0, 1+len(droppedStruct.Fields))
		droppedIDs = append(droppedIDs, droppedStruct.FieldID)
		for _, sub := range droppedStruct.Fields {
			droppedIDs = append(droppedIDs, sub.FieldID)
		}
		properties = updateMaxFieldIDProperty(coll.Properties, nextFieldID(coll)-1)
		schema = coll.ToCollectionSchemaPB()
		schema.StructArrayFields = newStructs
		schema.Properties = properties
		schema.Version = coll.SchemaVersion + 1
		return schema, properties, droppedIDs, nil
	}

	if fieldName != "" {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("field not found: %s", fieldName)
	}
	return nil, nil, nil, merr.WrapErrParameterInvalidMsg("field not found with id: %d", fieldID)
}

// buildSchemaForDropFunction builds the new schema for dropping a function and its output fields.
// It removes the function from Functions, removes all output fields from Fields, and updates max_field_id.
func buildSchemaForDropFunction(coll *model.Collection, functionName string) (
	schema *schemapb.CollectionSchema,
	properties []*commonpb.KeyValuePair,
	droppedFieldIds []int64,
	err error,
) {
	var targetFunc *model.Function
	for _, fn := range coll.Functions {
		if fn.Name == functionName {
			targetFunc = fn
			break
		}
	}
	if targetFunc == nil {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("function not found: %s", functionName)
	}

	outputFieldIDSet := make(map[int64]struct{}, len(targetFunc.OutputFieldIDs))
	for _, fid := range targetFunc.OutputFieldIDs {
		outputFieldIDSet[fid] = struct{}{}
	}
	droppedFieldIds = targetFunc.OutputFieldIDs

	maxFieldID := nextFieldID(coll) - 1
	properties = updateMaxFieldIDProperty(coll.Properties, maxFieldID)

	newFields := make([]*schemapb.FieldSchema, 0, len(coll.Fields))
	for _, field := range coll.Fields {
		if _, ok := outputFieldIDSet[field.FieldID]; !ok {
			newFields = append(newFields, model.MarshalFieldModel(field))
		}
	}

	newFunctions := make([]*schemapb.FunctionSchema, 0, len(coll.Functions)-1)
	for _, fn := range coll.Functions {
		if fn.Name != functionName {
			newFunctions = append(newFunctions, model.MarshalFunctionModel(fn))
		}
	}

	schema = coll.ToCollectionSchemaPB()
	schema.Fields = newFields
	schema.Functions = newFunctions
	schema.Properties = properties
	schema.Version = coll.SchemaVersion + 1

	return schema, properties, droppedFieldIds, nil
}

// updateMaxFieldIDProperty returns a new properties slice with max_field_id set.
// The original slice is not modified.
func updateMaxFieldIDProperty(properties []*commonpb.KeyValuePair, maxFieldID int64) []*commonpb.KeyValuePair {
	val := strconv.FormatInt(maxFieldID, 10)
	result := make([]*commonpb.KeyValuePair, 0, len(properties)+1)
	found := false
	for _, kv := range properties {
		if kv.Key == common.MaxFieldIDKey {
			result = append(result, &commonpb.KeyValuePair{
				Key:   common.MaxFieldIDKey,
				Value: val,
			})
			found = true
		} else {
			result = append(result, kv)
		}
	}
	if !found {
		result = append(result, &commonpb.KeyValuePair{
			Key:   common.MaxFieldIDKey,
			Value: val,
		})
	}
	return result
}
