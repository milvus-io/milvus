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

	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/function/validator"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
		if err := schemautil.ValidateAlterSchemaAddFunctionPlan(plan, typeutil.IsExternalCollection(coll.ToCollectionSchemaPB())); err != nil {
			return err
		}
		if err := schemautil.CheckNoFunctionCascade(coll.ToCollectionSchemaPB().GetFunctions(), plan.Function); err != nil {
			return err
		}
		for _, function := range coll.Functions {
			if function.Name == plan.Function.GetName() {
				return merr.WrapErrParameterInvalidMsg("function already exists, name: %s", plan.Function.GetName())
			}
		}
	}

	assignedFieldID, sourceSchema, err := resolveAlterSchemaAddFieldID(coll, plan)
	if err != nil {
		return err
	}
	schema, properties, err := buildAlterSchemaAddSchema(coll, plan, assignedFieldID)
	if err != nil {
		return err
	}
	if sourceSchema != nil {
		if err := typeutil.ValidateMilvusTableSchemaIdentity(schema, sourceSchema, true); err != nil {
			return merr.Wrap(err, "milvus-table target schema does not match source snapshot schema")
		}
	}
	var allowedHistoricalFieldIDs []int64
	if sourceSchema != nil && plan.Kind == schemautil.AlterSchemaAddField {
		allowedHistoricalFieldIDs = []int64{assignedFieldID}
	}
	if err := validateSchemaEvolution(coll, schema, allowedHistoricalFieldIDs...); err != nil {
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

	// Materialize the bound index meta for the new function output field BEFORE the
	// broadcast, so the WAL message carries a complete, replay-deterministic index
	// definition and the ack callback stays a pure idempotent apply.
	var boundFieldIndexes []*indexpb.FieldIndex
	if plan.Kind == schemautil.AlterSchemaAddFunctionField {
		fieldIndex, err := c.prepareBoundFieldIndex(ctx, coll, plan)
		if err != nil {
			return err
		}
		boundFieldIndexes = append(boundFieldIndexes, fieldIndex)
	}

	// Broadcast.
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	addedFileResourceIds, err := c.prepareAlterCollectionAnalyzerFileResources(ctx, coll, schema)
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
				Schema:            schema,
				Properties:        properties,
				BoundFieldIndexes: boundFieldIndexes,
			},
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		rollbackAlterCollectionAnalyzerFileResourceReservation(ctx, c.meta, coll.CollectionID, addedFileResourceIds, err)
		return err
	}
	return nil
}

// prepareBoundFieldIndex materializes the index meta bound to the newly added
// function-output field, strictly BEFORE the DDL broadcast: index id/name are
// allocated here and serialized into the WAL message so that the ack-callback
// apply is a pure idempotent write (a replayed callback rebuilds the identical
// index), and every input-dependent rejection happens before anything commits.
func (c *Core) prepareBoundFieldIndex(ctx context.Context, coll *model.Collection, plan *schemautil.AlterSchemaAddPlan) (*indexpb.FieldIndex, error) {
	indexParamsMap, err := indexparamcheck.PrepareFunctionOutputIndexParams(
		plan.Function.GetType(), plan.Field.GetName(), plan.IndexExtraParams)
	if err != nil {
		return nil, err
	}
	// The index type must have a registered checker — an unknown type would pass
	// structural validation, get persisted via the ack callback, and then never
	// build. Proxy already rejects this, but the check must live pre-broadcast
	// for callers that reach rootcoord directly.
	indexType := indexParamsMap[common.IndexTypeKey]
	if checker, err := indexparamcheck.GetIndexCheckerMgrInstance().GetChecker(indexType); err != nil || indexparamcheck.IsHYBRIDChecker(checker) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"invalid index type %s for the bound index of function output field %q",
			indexType, plan.Field.GetName())
	}
	// Full field-aware validation (params size, dimension fill+match, data-type
	// compatibility, train params), identical to the create_index path — an index
	// that cannot build must never be persisted through the ack callback.
	if err := indexparamcheck.ValidateFieldIndexParams(plan.Field, indexParamsMap); err != nil {
		return nil, err
	}

	indexName := plan.IndexName
	if indexName == "" {
		indexName = plan.Field.GetName()
	}
	// Name-format rule, same as the proxy path — enforced here too for callers
	// that reach rootcoord directly.
	if err := indexparamcheck.ValidateIndexName(indexName); err != nil {
		return nil, err
	}
	// Reject index-name conflicts with existing indexes (the field itself is new,
	// so only cross-field name collisions are possible).
	resp, err := c.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: coll.CollectionID,
	})
	if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		if !merr.ErrIndexNotFound.Is(err) {
			return nil, merr.Wrap(err, "failed to list existing indexes for bound index preparation")
		}
	} else {
		for _, info := range resp.GetIndexInfos() {
			if info.GetIndexName() == indexName {
				return nil, merr.WrapErrParameterInvalidMsg("index name %s already exists in collection", indexName)
			}
		}
	}

	indexID, err := c.idAllocator.AllocOne()
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate index id for bound index")
	}
	createTime, err := c.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate timestamp for bound index")
	}

	indexParams := funcutil.Map2KeyValuePair(indexParamsMap)
	// Field type params minus per-field mmap/warmup keys, mirroring datacoord CreateIndex.
	typeParams := lo.Filter(plan.Field.GetTypeParams(), func(kv *commonpb.KeyValuePair, _ int) bool {
		return kv.GetKey() != common.MmapEnabledKey && kv.GetKey() != common.WarmupKey
	})
	index := &model.Index{
		CollectionID:    coll.CollectionID,
		FieldID:         plan.Field.GetFieldID(),
		IndexID:         indexID,
		IndexName:       indexName,
		TypeParams:      typeParams,
		IndexParams:     indexParams,
		CreateTime:      createTime,
		IsAutoIndex:     false,
		UserIndexParams: plan.IndexExtraParams,
	}
	if err := indexparamcheck.ValidateIndexParams(index); err != nil {
		return nil, err
	}
	return model.MarshalIndexModel(index), nil
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

// resolveAlterSchemaAddFieldID assigns source field IDs for milvus-table data
// columns and keeps target-only fields above both source and target high-water
// marks. Other collections use the normal collection high-water mark.
func resolveAlterSchemaAddFieldID(
	coll *model.Collection,
	plan *schemautil.AlterSchemaAddPlan,
) (int64, *schemapb.CollectionSchema, error) {
	if !plan.HasField() {
		return 0, nil, nil
	}

	assignedFieldID := maxAssignedFieldIDFromSchema(coll.ToCollectionSchemaPB()) + 1
	targetSchema := coll.ToCollectionSchemaPB()
	if !typeutil.NewStorageColumnResolver(targetSchema).IsMilvusTable() {
		return assignedFieldID, nil, nil
	}

	metadata, err := packed.ReadMilvusTableSnapshotMetadata(
		targetSchema.GetExternalSource(),
		targetSchema.GetExternalSpec(),
		createMilvusTableSnapshotStorageConfig(),
		packed.ExternalSpecContext{
			CollectionID: coll.CollectionID,
			Source:       targetSchema.GetExternalSource(),
			Spec:         targetSchema.GetExternalSpec(),
		},
	)
	if err != nil {
		return 0, nil, merr.Wrap(err, "read milvus-table snapshot metadata for schema alignment")
	}
	sourceSchema := metadata.GetCollection().GetSchema()
	if sourceSchema == nil {
		return 0, nil, merr.WrapErrParameterInvalidMsg("milvus-table snapshot metadata missing collection schema")
	}
	if typeutil.IsExternalCollection(sourceSchema) {
		return 0, nil, merr.WrapErrParameterInvalidMsg("milvus-table external collection cannot use an external collection snapshot as source")
	}
	if plan.Kind != schemautil.AlterSchemaAddField {
		return nextMilvusTableTargetOnlyFieldID(sourceSchema, targetSchema), sourceSchema, nil
	}

	sourceField := milvusTableSourceFieldsByName(sourceSchema)[plan.Field.GetExternalField()]
	if sourceField == nil {
		return 0, nil, merr.WrapErrParameterInvalidMsg(
			"milvus-table target field %q maps to missing source field %q",
			plan.Field.GetName(),
			plan.Field.GetExternalField(),
		)
	}
	for _, field := range coll.Fields {
		if field.FieldID == sourceField.GetFieldID() {
			return 0, nil, merr.WrapErrParameterInvalidMsg(
				"milvus-table source field %q uses field ID %d already assigned to target field %q",
				sourceField.GetName(),
				sourceField.GetFieldID(),
				field.Name,
			)
		}
	}
	return sourceField.GetFieldID(), sourceSchema, nil
}

func buildAlterSchemaAddSchema(coll *model.Collection, plan *schemautil.AlterSchemaAddPlan, assignedFieldID int64) (*schemapb.CollectionSchema, []*commonpb.KeyValuePair, error) {
	schema := coll.ToCollectionSchemaPB()
	name2id := make(map[string]int64, len(coll.Fields)+1)
	for _, field := range coll.Fields {
		name2id[field.Name] = field.FieldID
	}

	if plan.HasField() {
		plan.Field.FieldID = assignedFieldID
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
		if !dropReq.GetDropFunctionOutputFields() {
			return merr.WrapErrParameterInvalidMsg(
				"detaching a function without dropping its output field is not supported; drop_function always removes the function together with its output field: %s", id.FunctionName)
		}
		schema, properties, droppedFieldIds, err = buildSchemaForDropFunctionField(coll, id.FunctionName)
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
		rollbackAlterCollectionAnalyzerFileResourceReservation(ctx, c.meta, coll.CollectionID, addedFileResourceIds, err)
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
		// Mirror the proxy guard for direct-coord callers: a field a function
		// depends on must be dropped via the function DDL, not directly, else the
		// function is orphaned and its stored output invalidated.
		if fn, kind := functionReferencing(coll.Functions, droppedField.Name); fn != "" {
			return nil, nil, nil, merr.WrapErrParameterInvalidMsg("field is referenced by function %s as %s, drop function first", fn, kind)
		}
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
		for _, sub := range droppedStruct.Fields {
			if fn, kind := functionReferencing(coll.Functions, sub.Name); fn != "" {
				return nil, nil, nil, merr.WrapErrParameterInvalidMsg("cannot drop struct array field %s: sub-field %s is referenced by function %s as %s", droppedStruct.Name, sub.Name, fn, kind)
			}
		}
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

// resolveOutputFieldIDsFromNames re-derives a function's output field IDs from its
// OutputFieldNames against the current schema (the authoritative name->id mapping),
// and rejects if the persisted OutputFieldIDs disagree as a set. A drop deletes by
// field id, but the proxy guard authorizes by name; a stale/injected persisted id
// that no name resolves to (e.g. a primary key) would then be deleted past that
// guard. Re-resolving keeps authorization (name) and action (id) on one carrier.
// Read-only, unlike resolveFunctionFieldIDs which mutates the schema on add/alter.
func resolveOutputFieldIDsFromNames(fn *model.Function, fields []*model.Field) ([]int64, error) {
	nameToID := make(map[string]int64, len(fields))
	for _, f := range fields {
		nameToID[f.Name] = f.FieldID
	}
	resolved := make([]int64, 0, len(fn.OutputFieldNames))
	resolvedSet := make(map[int64]struct{}, len(fn.OutputFieldNames))
	for _, name := range fn.OutputFieldNames {
		id, ok := nameToID[name]
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("function %s output field %s not found in schema", fn.Name, name)
		}
		resolved = append(resolved, id)
		resolvedSet[id] = struct{}{}
	}
	persistedSet := make(map[int64]struct{}, len(fn.OutputFieldIDs))
	for _, id := range fn.OutputFieldIDs {
		persistedSet[id] = struct{}{}
	}
	mismatch := len(resolvedSet) != len(persistedSet)
	for id := range persistedSet {
		if _, ok := resolvedSet[id]; !ok {
			mismatch = true
			break
		}
	}
	if mismatch {
		return nil, merr.WrapErrParameterInvalidMsg(
			"function %s persisted output field ids %v do not align with output field names %v; metadata may be corrupt", fn.Name, fn.OutputFieldIDs, fn.OutputFieldNames)
	}
	return resolved, nil
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
	// Drop is uniform across function types (no backfill); unlike add_function_field
	// it is not type-restricted. Re-resolve the delete set from names so an injected
	// persisted id cannot be deleted past the name-based proxy guard.
	outputFieldIDs, err := resolveOutputFieldIDsFromNames(targetFunc, coll.Fields)
	if err != nil {
		return nil, nil, nil, err
	}
	droppedFieldIds = append(droppedFieldIds, outputFieldIDs...)
	outputFieldIDSet := make(map[int64]struct{}, len(outputFieldIDs))
	for _, fid := range outputFieldIDs {
		outputFieldIDSet[fid] = struct{}{}
	}

	// Mirror the proxy guard for direct-coord callers: dropping the output vector
	// field(s) must not leave the collection with no vector field.
	removedVectors := 0
	for _, field := range coll.Fields {
		if _, ok := outputFieldIDSet[field.FieldID]; ok && typeutil.IsVectorType(field.DataType) {
			removedVectors++
		}
	}
	if removedVectors > 0 && removedVectors >= len(typeutil.GetVectorFieldSchemas(coll.ToCollectionSchemaPB())) {
		return nil, nil, nil, merr.WrapErrParameterInvalidMsg("cannot drop function %s: it would leave no vector field in the collection", functionName)
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

// functionReferencing returns the name of the first function referencing fieldName
// and its role ("input"/"output"), or "" if none. A field a function depends on
// must not be dropped directly (mirrors the proxy validateDropField guard).
func functionReferencing(functions []*model.Function, fieldName string) (string, string) {
	for _, fn := range functions {
		for _, in := range fn.InputFieldNames {
			if in == fieldName {
				return fn.Name, "input"
			}
		}
		for _, out := range fn.OutputFieldNames {
			if out == fieldName {
				return fn.Name, "output"
			}
		}
	}
	return "", ""
}
