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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const externalCollectionFunctionMutationUnsupportedMsg = "external collection does not support altering or dropping functions"

func rejectExternalCollectionFunctionMutation(schema *schemapb.CollectionSchema) error {
	if typeutil.IsExternalCollection(schema) {
		return merr.WrapErrParameterInvalidMsg(externalCollectionFunctionMutationUnsupportedMsg)
	}
	return nil
}

func callAlterCollection(ctx context.Context, c *Core, broadcaster broadcaster.BroadcastAPI, oldColl *model.Collection, newColl *model.Collection, dbName string, collectionName string) error {
	// build new collection schema.
	schema := newColl.ToCollectionSchemaPB()
	schema.Version = newColl.SchemaVersion + 1
	if err := typeutil.ValidateExternalCollectionResolvedSchema(schema); err != nil {
		return err
	}
	if err := c.validateSchemaChange(ctx, oldColl, schema); err != nil {
		return err
	}

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, dbName, collectionName)
	if err != nil {
		return err
	}

	header := &messagespb.AlterCollectionMessageHeader{
		DbId:         newColl.DBID,
		CollectionId: newColl.CollectionID,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{message.FieldMaskCollectionSchema},
		},
		CacheExpirations: cacheExpirations,
	}
	body := &messagespb.AlterCollectionMessageBody{
		Updates: &messagespb.AlterCollectionMessageUpdates{
			Schema: schema,
		},
	}

	addedFileResourceIds, err := c.prepareAlterCollectionAnalyzerFileResources(ctx, oldColl, schema)
	if err != nil {
		return err
	}

	channels := make([]string, 0, len(newColl.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, newColl.VirtualChannelNames...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if err := c.broadcastValidatedSchemaChange(ctx, broadcaster, msg); err != nil {
		rollbackAlterCollectionAnalyzerFileResourceReservation(ctx, c.meta, newColl.CollectionID, addedFileResourceIds, err)
		return err
	}
	return nil
}

func alterFunctionGenNewCollection(ctx context.Context, fSchema *schemapb.FunctionSchema, collection *model.Collection) error {
	if fSchema == nil {
		return merr.WrapErrParameterInvalidMsg("function schema is empty")
	}
	var oldFuncSchema *model.Function
	newFuncs := []*model.Function{}
	for _, f := range collection.Functions {
		if f.Name == fSchema.Name {
			oldFuncSchema = f
		} else {
			newFuncs = append(newFuncs, f)
		}
	}
	if oldFuncSchema == nil {
		err := merr.WrapErrParameterInvalidMsg("function %s not exists", fSchema.Name)
		mlog.Error(ctx, "Alter function failed:", mlog.Err(err))
		return err
	}

	fSchema.Id = oldFuncSchema.ID

	fieldMapping := map[string]*model.Field{}
	for _, field := range collection.Fields {
		fieldMapping[field.Name] = field
	}

	// reset output field info
	for _, name := range oldFuncSchema.OutputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			return merr.WrapErrParameterInvalidMsg("old version function's output field %s not exists", name)
		}
		field.IsFunctionOutput = false
	}

	for _, name := range fSchema.InputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := merr.WrapErrParameterInvalidMsg("function's input field %s not exists", name)
			mlog.Error(ctx, "Incorrect function configuration:", mlog.Err(err))
			return err
		}
		fSchema.InputFieldIds = append(fSchema.InputFieldIds, field.FieldID)
	}
	for _, name := range fSchema.OutputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := merr.WrapErrParameterInvalidMsg("function's output field %s not exists", name)
			mlog.Error(ctx, "Incorrect function configuration:", mlog.Err(err))
			return err
		}
		if field.IsFunctionOutput {
			err := merr.WrapErrParameterInvalidMsg("function's output field %s is already of other functions", name)
			mlog.Error(ctx, "Incorrect function configuration: ", mlog.Err(err))
			return err
		}
		fSchema.OutputFieldIds = append(fSchema.OutputFieldIds, field.FieldID)
		field.IsFunctionOutput = true
	}
	newFunc := model.UnmarshalFunctionModel(fSchema)
	newFuncs = append(newFuncs, newFunc)
	collection.Functions = newFuncs
	return nil
}

func (c *Core) broadcastAlterCollectionForAlterFunction(ctx context.Context, req *milvuspb.AlterCollectionFunctionRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}
	if err := rejectExternalCollectionFunctionMutation(oldColl.ToCollectionSchemaPB()); err != nil {
		return err
	}

	newColl := oldColl.Clone()
	if err := alterFunctionGenNewCollection(ctx, req.FunctionSchema, newColl); err != nil {
		return err
	}

	return callAlterCollection(ctx, c, broadcaster, oldColl, newColl, req.GetDbName(), req.GetCollectionName())
}

func (c *Core) broadcastAlterCollectionForDropFunction(ctx context.Context, req *milvuspb.DropCollectionFunctionRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}
	if err := rejectExternalCollectionFunctionMutation(oldColl.ToCollectionSchemaPB()); err != nil {
		return err
	}

	var needDelFunc *model.Function
	for _, f := range oldColl.Functions {
		if f.Name == req.FunctionName {
			needDelFunc = f
			break
		}
	}
	if needDelFunc == nil {
		return nil
	}

	newColl := oldColl.Clone()

	newFuncs := []*model.Function{}
	for _, f := range newColl.Functions {
		if f.Name != needDelFunc.Name {
			newFuncs = append(newFuncs, f)
		}
	}
	newColl.Functions = newFuncs

	fieldMapping := map[int64]*model.Field{}
	for _, field := range newColl.Fields {
		fieldMapping[field.FieldID] = field
	}
	for _, id := range needDelFunc.OutputFieldIDs {
		field, exists := fieldMapping[id]
		if !exists {
			return merr.WrapErrServiceInternalMsg("function's output field %d not exists", id)
		}
		field.IsFunctionOutput = false
	}
	return callAlterCollection(ctx, c, broadcaster, oldColl, newColl, req.GetDbName(), req.GetCollectionName())
}

func (c *Core) broadcastAlterCollectionForAddFunction(ctx context.Context, req *milvuspb.AddCollectionFunctionRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	newColl := oldColl.Clone()
	fSchema := req.FunctionSchema
	if fSchema == nil {
		return merr.WrapErrParameterInvalidMsg("function schema is empty")
	}

	nextFunctionID := int64(StartOfUserFunctionID)
	for _, f := range newColl.Functions {
		if f.Name == fSchema.Name {
			return merr.WrapErrParameterInvalidMsg("function name already exists: %s", f.Name)
		}
		nextFunctionID = max(nextFunctionID, f.ID+1)
	}
	fSchema.Id = nextFunctionID

	fieldMapping := map[string]*model.Field{}
	for _, field := range newColl.Fields {
		fieldMapping[field.Name] = field
	}
	for _, name := range fSchema.InputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := merr.WrapErrParameterInvalidMsg("function's input field %s not exists", name)
			mlog.Error(ctx, "Incorrect function configuration:", mlog.Err(err))
			return err
		}
		fSchema.InputFieldIds = append(fSchema.InputFieldIds, field.FieldID)
	}
	for _, name := range fSchema.OutputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := merr.WrapErrParameterInvalidMsg("function's output field %s not exists", name)
			mlog.Error(ctx, "Incorrect function configuration:", mlog.Err(err))
			return err
		}
		if field.IsFunctionOutput {
			err := merr.WrapErrParameterInvalidMsg("function's output field %s is already of other functions", name)
			mlog.Error(ctx, "Incorrect function configuration: ", mlog.Err(err))
			return err
		}
		fSchema.OutputFieldIds = append(fSchema.OutputFieldIds, field.FieldID)
		field.IsFunctionOutput = true
	}
	newFunc := model.UnmarshalFunctionModel(fSchema)
	newColl.Functions = append(newColl.Functions, newFunc)
	return callAlterCollection(ctx, c, broadcaster, oldColl, newColl, req.GetDbName(), req.GetCollectionName())
}
