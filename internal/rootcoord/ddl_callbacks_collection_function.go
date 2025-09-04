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
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func callAlterCollection(ctx context.Context, c *Core, broadcaster broadcaster.BroadcastAPI, coll *model.Collection, dbName string, collectionName string) error {
	// build new collection schema.
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: coll.EnableDynamicField,
		Properties:         coll.Properties,
		Version:            coll.SchemaVersion + 1,
	}

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, dbName, collectionName)
	if err != nil {
		return err
	}

	header := &messagespb.AlterCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
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

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

func alterFunctionGenNewCollection(ctx context.Context, fSchema *schemapb.FunctionSchema, collection *model.Collection) error {
	if fSchema == nil {
		return fmt.Errorf("Function schema is empty")
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
		err := fmt.Errorf("Function %s not exists", fSchema.Name)
		log.Ctx(ctx).Error("Alter function failed:", zap.Error(err))
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
			return fmt.Errorf("Old version function's output field %s not exists", name)
		}
		field.IsFunctionOutput = false
	}

	for _, name := range fSchema.InputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := fmt.Errorf("function's input field %s not exists", name)
			log.Ctx(ctx).Error("Incorrect function configuration:", zap.Error(err))
			return err
		}
		fSchema.InputFieldIds = append(fSchema.InputFieldIds, field.FieldID)
	}
	for _, name := range fSchema.OutputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := fmt.Errorf("function's output field %s not exists", name)
			log.Ctx(ctx).Error("Incorrect function configuration:", zap.Error(err))
			return err
		}
		if field.IsFunctionOutput {
			err := fmt.Errorf("function's output field %s is already of other functions", name)
			log.Ctx(ctx).Error("Incorrect function configuration: ", zap.Error(err))
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

	oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	newColl := oldColl.Clone()
	if err := alterFunctionGenNewCollection(ctx, req.FunctionSchema, newColl); err != nil {
		return err
	}

	return callAlterCollection(ctx, c, broadcaster, newColl, req.GetDbName(), req.GetCollectionName())
}

func (c *Core) broadcastAlterCollectionForDropFunction(ctx context.Context, req *milvuspb.DropCollectionFunctionRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
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
			return fmt.Errorf("function's output field %d not exists", id)
		}
		field.IsFunctionOutput = false
	}
	return callAlterCollection(ctx, c, broadcaster, newColl, req.GetDbName(), req.GetCollectionName())
}

func (c *Core) broadcastAlterCollectionForAddFunction(ctx context.Context, req *milvuspb.AddCollectionFunctionRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	newColl := oldColl.Clone()
	fSchema := req.FunctionSchema
	if fSchema == nil {
		return merr.WrapErrParameterInvalidMsg("Function schema is empty")
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
			err := fmt.Errorf("function's input field %s not exists", name)
			log.Ctx(ctx).Error("Incorrect function configuration:", zap.Error(err))
			return err
		}
		fSchema.InputFieldIds = append(fSchema.InputFieldIds, field.FieldID)
	}
	for _, name := range fSchema.OutputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := fmt.Errorf("function's output field %s not exists", name)
			log.Ctx(ctx).Error("Incorrect function configuration:", zap.Error(err))
			return err
		}
		if field.IsFunctionOutput {
			err := fmt.Errorf("function's output field %s is already of other functions", name)
			log.Ctx(ctx).Error("Incorrect function configuration: ", zap.Error(err))
			return err
		}
		fSchema.OutputFieldIds = append(fSchema.OutputFieldIds, field.FieldID)
		field.IsFunctionOutput = true
	}
	newFunc := model.UnmarshalFunctionModel(fSchema)
	newColl.Functions = append(newColl.Functions, newFunc)
	return callAlterCollection(ctx, c, broadcaster, newColl, req.GetDbName(), req.GetCollectionName())
}
