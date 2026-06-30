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

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/function/validator"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastAlterCollectionSchema broadcasts the alter collection schema message to all channels.
func (c *Core) broadcastAlterCollectionSchema(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) error {
	action := req.GetAction()
	if action == nil {
		return merr.WrapErrParameterInvalidMsg("action is nil")
	}
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}
	if _, ok := action.GetOp().(*milvuspb.AlterCollectionSchemaRequest_Action_DropRequest); ok {
		if err := waitUntilSchemaDropReady(ctx); err != nil {
			return err
		}
	}

	broadcaster, err := c.startBroadcastWithCollectionLock(ctx, req.GetDbName(), coll.Name)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	coll, err = c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
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
	plan, err := schemautil.ParseAlterSchemaAddRequest(addRequest)
	if err != nil {
		return err
	}
	if plan.HasField() {
		if err := prepareAlterSchemaAddField(coll, plan); err != nil {
			return err
		}
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
		if fieldNames.Contain(plan.Field.GetName()) {
			return merr.WrapErrParameterInvalidMsg("field already exists, name: %s", plan.Field.GetName())
		}
	}
	if plan.HasFunction() {
		if err := schemautil.ValidateAlterSchemaAddFunctionPlan(plan); err != nil {
			return err
		}
		for _, function := range coll.Functions {
			if function.Name == plan.Function.GetName() {
				return merr.WrapErrParameterInvalidMsg("function already exists, name: %s", plan.Function.GetName())
			}
		}
	}

	schema, properties, err := buildAlterSchemaAddSchema(coll, plan)
	if err != nil {
		return err
	}
	if plan.HasFunction() {
		if err := validator.ValidateFunction(schema, plan.Function.GetName(), true); err != nil {
			return merr.Wrap(err, "invalid function schema")
		}
	}
	if err := typeutil.ValidateExternalCollectionResolvedSchema(schema); err != nil {
		return err
	}
	if err := typeutil.ValidateTextRequiresStorageV3(schema, Params.CommonCfg.UseLoonFFI.GetAsBool()); err != nil {
		return merr.WrapErrParameterInvalidMsg("%s", err.Error())
	}

	// bump_defence: register a gate for a newly-added FUNCTION OUTPUT field BEFORE the
	// dataView-changing broadcast. For the internal backfill the DDL itself auto-starts
	// the round (bump-schema compaction), so DDL == round start and old segments
	// (schema_version < V) must be protected from here until the compaction materializes
	// the field everywhere. A regular plain add_field (unbound) is born correctly-served
	// as NULL -> no gate. An external-collection field is NOT registered here either:
	// its round only starts when a refresh runs (the DDL starts nothing), so the gate is
	// registered by the refresh-apply path (datacoord applyFinishedJobSegments); until
	// then the field is uniformly absent and segcore rejects it cleanly.
	// NOTE: the F-in-load_fields guard is deferred; registering unconditionally for a
	// function output field over-protects (safe direction), never leaks a partial.
	if c.backfillGate != nil && plan.HasField() && plan.Field.GetIsFunctionOutput() {
		field := plan.Field
		v := schema.GetVersion()
		roundID, aerr := c.idAllocator.AllocOne()
		if aerr != nil {
			mlog.Warn(ctx, "failed to allocate bump_defence roundID; skip registration", mlog.Err(aerr))
		} else if err := c.backfillGate.RegisterWatermark(ctx, coll.CollectionID, roundID, []int64{field.GetFieldID()}, v); err != nil {
			mlog.Warn(ctx, "failed to register bump_defence for added field",
				mlog.FieldCollectionID(coll.CollectionID),
				mlog.Int64("fieldID", field.GetFieldID()), mlog.Err(err))
		}
	}

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

func prepareAlterSchemaAddField(coll *model.Collection, plan *schemautil.AlterSchemaAddPlan) error {
	if !plan.HasField() {
		return nil
	}

	fieldSchema := plan.Field
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return merr.Wrap(err, "failed to check field schema")
	}
	if fieldSchema.GetDataType() == schemapb.DataType_Timestamptz {
		timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, coll.Properties)
		if !exist {
			timezone = common.DefaultTimezone
		}
		if err := timestamptz.CheckAndRewriteTimestampTzDefaultValueForFieldSchema(fieldSchema, timezone); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid default value of field, name: %s, err: %w", fieldSchema.Name, err)
		}
	}

	return nil
}

func buildAlterSchemaAddSchema(coll *model.Collection, plan *schemautil.AlterSchemaAddPlan) (*schemapb.CollectionSchema, []*commonpb.KeyValuePair, error) {
	schema := coll.ToCollectionSchemaPB()
	name2id := make(map[string]int64, len(coll.Fields)+1)
	for _, field := range coll.Fields {
		name2id[field.Name] = field.FieldID
	}

	if plan.HasField() {
		plan.Field.FieldID = maxAssignedFieldIDFromSchema(schema) + 1
		name2id[plan.Field.GetName()] = plan.Field.GetFieldID()
	}
	if plan.HasFunction() {
		function := plan.Function
		function.Id = nextFunctionID(coll)
		function.InputFieldIds = make([]int64, len(function.InputFieldNames))
		for idx, name := range function.InputFieldNames {
			fieldID, ok := name2id[name]
			if !ok {
				return nil, nil, merr.WrapErrParameterInvalidMsg("input field %s of function %s not found", name, function.GetName())
			}
			function.InputFieldIds[idx] = fieldID
		}

		function.OutputFieldIds = make([]int64, len(function.OutputFieldNames))
		for idx, name := range function.OutputFieldNames {
			fieldID, ok := name2id[name]
			if !ok {
				return nil, nil, merr.WrapErrParameterInvalidMsg("output field %s of function %s not found", name, function.GetName())
			}
			if plan.Kind == schemautil.AlterSchemaAddFunction {
				for _, field := range coll.Fields {
					if field.Name == name && field.IsFunctionOutput {
						return nil, nil, merr.WrapErrParameterInvalidMsg("function output field %s is already of other functions", name)
					}
				}
			}
			function.OutputFieldIds[idx] = fieldID
		}
		schema.Functions = append(schema.Functions, function)
	}

	schema.Version = coll.SchemaVersion + 1
	switch plan.Kind {
	case schemautil.AlterSchemaAddField:
		plan.Field.IsFunctionOutput = false
	case schemautil.AlterSchemaAddFunctionField:
		plan.Field.IsFunctionOutput = true
	case schemautil.AlterSchemaAddFunction:
		for _, outputFieldName := range plan.Function.GetOutputFieldNames() {
			for _, field := range schema.Fields {
				if field.GetName() == outputFieldName {
					field.IsFunctionOutput = true
					break
				}
			}
		}
	}

	if plan.HasField() {
		schema.Fields = append(schema.Fields, plan.Field)
	}
	properties := updateMaxFieldIDProperty(coll.Properties, maxAssignedFieldIDFromSchema(schema))
	schema.Properties = properties
	return schema, properties, nil
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
		if dropReq.GetDropFunctionOutputFields() {
			schema, properties, droppedFieldIds, err = buildSchemaForDropFunctionField(coll, id.FunctionName)
		} else {
			schema, properties, droppedFieldIds, err = buildSchemaForDetachFunction(coll, id.FunctionName)
		}
	case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName:
		schema, properties, droppedFieldIds, err = buildSchemaForDropField(coll, id.FieldName, 0)
	case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldId:
		schema, properties, droppedFieldIds, err = buildSchemaForDropField(coll, "", id.FieldId)
	default:
		return merr.WrapErrParameterMissingMsg("drop request must specify field_name, field_id, or function_name")
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
			DroppedFieldIds:  droppedFieldIds,
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

	// Top-level field path: remove from fields.
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
		schema = coll.ToCollectionSchemaPB()
		maxFieldID := maxAssignedFieldIDFromSchema(schema)
		properties = updateMaxFieldIDProperty(coll.Properties, maxFieldID)
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
		schema = coll.ToCollectionSchemaPB()
		maxFieldID := maxAssignedFieldIDFromSchema(schema)
		properties = updateMaxFieldIDProperty(coll.Properties, maxFieldID)
		schema.StructArrayFields = newStructs
		schema.Properties = properties
		schema.Version = coll.SchemaVersion + 1
		droppedFieldIds = append(droppedFieldIds, droppedStruct.FieldID)
		for _, subField := range droppedStruct.Fields {
			droppedFieldIds = append(droppedFieldIds, subField.FieldID)
		}
		return schema, properties, droppedFieldIds, nil
	}

	if fieldName != "" {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("field not found: %s", fieldName)
	}
	return nil, nil, nil, merr.WrapErrParameterInvalidMsg("field not found with id: %d", fieldID)
}

func buildSchemaForDetachFunction(coll *model.Collection, functionName string) (
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
	if targetFunc.Type == schemapb.FunctionType_BM25 {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("BM25 function must be dropped with its output field in drop_function_field interface: %s", functionName)
	}

	outputFieldIDSet := make(map[int64]struct{}, len(targetFunc.OutputFieldIDs))
	for _, fid := range targetFunc.OutputFieldIDs {
		outputFieldIDSet[fid] = struct{}{}
	}

	newFields := make([]*schemapb.FieldSchema, 0, len(coll.Fields))
	for _, field := range coll.Fields {
		fieldSchema := model.MarshalFieldModel(field)
		if _, ok := outputFieldIDSet[field.FieldID]; ok {
			fieldSchema.IsFunctionOutput = false
		}
		newFields = append(newFields, fieldSchema)
	}

	newFunctions := make([]*schemapb.FunctionSchema, 0, len(coll.Functions)-1)
	for _, fn := range coll.Functions {
		if fn.Name != functionName {
			newFunctions = append(newFunctions, model.MarshalFunctionModel(fn))
		}
	}

	schema = coll.ToCollectionSchemaPB()
	properties = coll.Properties
	schema.Fields = newFields
	schema.Functions = newFunctions
	schema.Properties = properties
	schema.Version = coll.SchemaVersion + 1

	return schema, properties, nil, nil
}

func buildSchemaForDropFunctionField(coll *model.Collection, functionName string) (
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
	switch targetFunc.Type {
	case schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash:
	default:
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("only BM25 and MinHash functions support dropping output fields: %s", functionName)
	}

	droppedFieldIds = append(droppedFieldIds, targetFunc.OutputFieldIDs...)
	outputFieldIDSet := make(map[int64]struct{}, len(targetFunc.OutputFieldIDs))
	for _, fid := range targetFunc.OutputFieldIDs {
		outputFieldIDSet[fid] = struct{}{}
	}

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
	maxFieldID := maxAssignedFieldIDFromSchema(schema)
	properties = updateMaxFieldIDProperty(coll.Properties, maxFieldID)
	schema.Fields = newFields
	schema.Functions = newFunctions
	schema.Properties = properties
	schema.Version = coll.SchemaVersion + 1

	return schema, properties, droppedFieldIds, nil
}
