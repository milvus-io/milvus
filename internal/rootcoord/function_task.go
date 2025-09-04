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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type addCollectionFunctionTask struct {
	baseTask
	Req *milvuspb.AddCollectionFunctionRequest

	functionSchema *schemapb.FunctionSchema
}

func (t *addCollectionFunctionTask) name() string {
	return "AddCollectionFunctionTask"
}

func (t *addCollectionFunctionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AddCollectionFunction); err != nil {
		return err
	}
	t.functionSchema = t.Req.FunctionSchema
	return nil
}

func (t *addCollectionFunctionTask) genNewCollection(ctx context.Context, fSchema *schemapb.FunctionSchema, collection *model.Collection) error {
	nextFunctionID := int64(StartOfUserFunctionID)
	for _, f := range collection.Functions {
		if f.Name == fSchema.Name {
			err := fmt.Errorf("function name already exists: %s", f.Name)
			log.Ctx(ctx).Error("Function name conflict",
				zap.String("functionName", f.Name),
				zap.Error(err))
			return err
		}
		nextFunctionID = max(nextFunctionID, f.ID+1)
	}
	fSchema.Id = nextFunctionID

	fieldMapping := map[string]*model.Field{}
	for _, field := range collection.Fields {
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
	collection.Functions = append(collection.Functions, newFunc)
	return nil
}

func (t *addCollectionFunctionTask) Execute(ctx context.Context) error {
	log := log.Ctx(ctx).With(
		zap.String("AddCollectionFunctionTask", t.Req.GetCollectionName()),
		zap.Int64("collectionID", t.Req.GetCollectionID()),
		zap.Uint64("ts", t.GetTs()))

	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Warn("get collection failed during add function",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}

	newColl := oldColl.Clone()
	if err := t.genNewCollection(ctx, t.functionSchema, newColl); err != nil {
		return err
	}
	ts := t.GetTs()
	t.Req.CollectionID = oldColl.CollectionID
	return executeCollectionFunctionTaskSteps(ctx, t.core, oldColl, newColl, t.Req.Base, commonpb.MsgType_AddCollectionFunction, ts)
}

func (t *addCollectionFunctionTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

type alterCollectionFunctionTask struct {
	baseTask
	Req *milvuspb.AlterCollectionFunctionRequest

	functionSchema *schemapb.FunctionSchema
}

func (t *alterCollectionFunctionTask) name() string {
	return "AlterCollectionFunctionTask"
}

func (t *alterCollectionFunctionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AlterCollectionFunction); err != nil {
		return err
	}
	t.functionSchema = t.Req.FunctionSchema
	return nil
}

func (t *alterCollectionFunctionTask) genNewCollection(ctx context.Context, fSchema *schemapb.FunctionSchema, collection *model.Collection) error {
	var oldFuncSchema *model.Function
	for _, f := range collection.Functions {
		if f.Name == fSchema.Name {
			oldFuncSchema = f
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
	collection.Functions = append(collection.Functions, newFunc)
	return nil
}

func (t *alterCollectionFunctionTask) Execute(ctx context.Context) error {
	log := log.Ctx(ctx).With(
		zap.String("AlterCollectionFunctionTask", t.Req.GetCollectionName()),
		zap.Int64("collectionID", t.Req.GetCollectionID()),
		zap.Uint64("ts", t.GetTs()))

	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Warn("get collection failed during alter function",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}

	newColl := oldColl.Clone()
	if err := t.genNewCollection(ctx, t.functionSchema, newColl); err != nil {
		return err
	}
	ts := t.GetTs()
	t.Req.CollectionID = oldColl.CollectionID
	return executeCollectionFunctionTaskSteps(ctx, t.core, oldColl, newColl, t.Req.Base, commonpb.MsgType_AlterCollectionFunction, ts)
}

func (t *alterCollectionFunctionTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

type dropCollectionFunctionTask struct {
	baseTask
	Req            *milvuspb.DropCollectionFunctionRequest
	functionSchema *schemapb.FunctionSchema
}

func (t *dropCollectionFunctionTask) name() string {
	return "DropCollectionFunctionTask"
}

func (t *dropCollectionFunctionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropCollectionFunction); err != nil {
		return err
	}
	return nil
}

func (t *dropCollectionFunctionTask) genNewCollection(ctx context.Context, needDelFunc *model.Function, collection *model.Collection) error {
	newFuncs := []*model.Function{}
	for _, f := range collection.Functions {
		if f.Name != needDelFunc.Name {
			newFuncs = append(newFuncs, f)
		}
	}
	collection.Functions = newFuncs

	fieldMapping := map[int64]*model.Field{}
	for _, field := range collection.Fields {
		fieldMapping[field.FieldID] = field
	}
	for _, id := range needDelFunc.OutputFieldIDs {
		field, exists := fieldMapping[id]
		if !exists {
			return fmt.Errorf("function's output field %d not exists", id)
		}
		field.IsFunctionOutput = false
	}
	return nil
}

func (t *dropCollectionFunctionTask) Execute(ctx context.Context) error {
	log := log.Ctx(ctx).With(
		zap.String("DropCollectionFunctionTask", t.Req.GetCollectionName()),
		zap.Int64("collectionID", t.Req.GetCollectionID()),
		zap.Uint64("ts", t.GetTs()))
	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Warn("get collection failed during drop function",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}

	var needDelFunc *model.Function
	for _, f := range oldColl.Functions {
		if f.Name == t.Req.FunctionName {
			needDelFunc = f
			break
		}
	}
	if needDelFunc == nil {
		return nil
	}

	newColl := oldColl.Clone()
	if err := t.genNewCollection(ctx, needDelFunc, newColl); err != nil {
		return err
	}
	ts := t.GetTs()
	t.Req.CollectionID = oldColl.CollectionID
	return executeCollectionFunctionTaskSteps(ctx, t.core, oldColl, newColl, t.Req.Base, commonpb.MsgType_DropCollectionFunction, ts)
}

func (t *dropCollectionFunctionTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeCollectionFunctionTaskSteps(ctx context.Context,
	core *Core,
	oldColl *model.Collection,
	newColl *model.Collection,
	baseMsg *commonpb.MsgBase,
	msgType commonpb.MsgType,
	ts Timestamp,
) error {
	redoTask := newBaseRedoTask(core.stepExecutor)

	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep:       baseStep{core: core},
		oldColl:        oldColl,
		newColl:        newColl,
		ts:             ts,
		fieldModify:    true,
		functionModify: true,
	})

	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: core},
		req: &milvuspb.AlterCollectionRequest{
			Base:           baseMsg,
			DbName:         oldColl.DBName,
			CollectionName: oldColl.Name,
			CollectionID:   oldColl.CollectionID,
		},
		core: core,
	})

	// Disable cache
	aliases := core.meta.ListAliasesByID(ctx, oldColl.CollectionID)
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          oldColl.DBName,
		collectionNames: append(aliases, oldColl.Name),
		collectionID:    oldColl.CollectionID,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(msgType)},
	})
	return redoTask.Execute(ctx)
}
