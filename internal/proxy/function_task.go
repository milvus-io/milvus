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

package proxy

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type addCollectionFunctionTask struct {
	baseTask
	Condition
	*milvuspb.AddCollectionFunctionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (t *addCollectionFunctionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *addCollectionFunctionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *addCollectionFunctionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *addCollectionFunctionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *addCollectionFunctionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *addCollectionFunctionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *addCollectionFunctionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *addCollectionFunctionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AddCollectionFunction
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *addCollectionFunctionTask) Name() string {
	return AddCollectionFunctionTask
}

func (t *addCollectionFunctionTask) PreExecute(ctx context.Context) error {
	if t.FunctionSchema == nil {
		return fmt.Errorf("Function Schema is empty")
	}

	if t.FunctionSchema.Type == schemapb.FunctionType_BM25 {
		return fmt.Errorf("Currently does not support adding BM25 function")
	}
	coll, err := getCollectionInfo(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		log.Ctx(t.ctx).Error("AddCollectionTask, get collection info failed",
			zap.String("dbName", t.GetDbName()),
			zap.String("collectionName", t.GetCollectionName()),
			zap.Error(err))
		return err
	}
	newColl := proto.Clone(coll.schema.CollectionSchema).(*schemapb.CollectionSchema)
	newColl.Functions = append(coll.schema.CollectionSchema.Functions, t.FunctionSchema)
	if err := validateFunction(newColl); err != nil {
		return err
	}
	return nil
}

func (t *addCollectionFunctionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.AddCollectionFunction(ctx, t.AddCollectionFunctionRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	return nil
}

func (t *addCollectionFunctionTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterCollectionFunctionTask struct {
	baseTask
	Condition
	*milvuspb.AlterCollectionFunctionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (t *alterCollectionFunctionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *alterCollectionFunctionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *alterCollectionFunctionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *alterCollectionFunctionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *alterCollectionFunctionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionFunctionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterCollectionFunctionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *alterCollectionFunctionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AlterCollectionFunction
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *alterCollectionFunctionTask) Name() string {
	return AlterCollectionFunctionTask
}

func (t *alterCollectionFunctionTask) PreExecute(ctx context.Context) error {
	if t.FunctionSchema == nil {
		return fmt.Errorf("Function Schema is empty")
	}
	if t.FunctionSchema.Type == schemapb.FunctionType_BM25 {
		return fmt.Errorf("Currently does not support alter BM25 function")
	}
	if t.FunctionName != t.FunctionSchema.Name {
		return fmt.Errorf("Invalid function config, name not match")
	}
	coll, err := getCollectionInfo(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		log.Ctx(t.ctx).Error("AddCollectionTask, get collection info failed",
			zap.String("dbName", t.GetDbName()),
			zap.String("collectionName", t.GetCollectionName()),
			zap.Error(err))
		return err
	}
	funcExist := false
	newFunctions := []*schemapb.FunctionSchema{}
	for _, fSchema := range coll.schema.Functions {
		if t.FunctionName == fSchema.Name {
			if fSchema.Type == schemapb.FunctionType_BM25 {
				return fmt.Errorf("Currently does not support alter BM25 function")
			}
			newFunctions = append(newFunctions, t.FunctionSchema)
			funcExist = true
		} else {
			newFunctions = append(newFunctions, fSchema)
		}
	}
	if !funcExist {
		return fmt.Errorf("Function %s not found", t.FunctionName)
	}

	newColl := proto.Clone(coll.schema.CollectionSchema).(*schemapb.CollectionSchema)
	newColl.Functions = newFunctions
	if err := validateFunction(newColl); err != nil {
		return err
	}
	return nil
}

func (t *alterCollectionFunctionTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.mixCoord.AlterCollectionFunction(ctx, t.AlterCollectionFunctionRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	return nil
}

func (t *alterCollectionFunctionTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropCollectionFunctionTask struct {
	baseTask
	Condition
	*milvuspb.DropCollectionFunctionRequest
	ctx      context.Context
	mixCoord types.MixCoordClient

	result  *commonpb.Status
	fSchema *schemapb.FunctionSchema
}

func (t *dropCollectionFunctionTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *dropCollectionFunctionTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *dropCollectionFunctionTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *dropCollectionFunctionTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *dropCollectionFunctionTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *dropCollectionFunctionTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *dropCollectionFunctionTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *dropCollectionFunctionTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DropCollectionFunction
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *dropCollectionFunctionTask) Name() string {
	return DropCollectionFunctionTask
}

func (t *dropCollectionFunctionTask) PreExecute(ctx context.Context) error {
	coll, err := getCollectionInfo(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		log.Ctx(t.ctx).Error("DropFunctionTask, get collection info failed",
			zap.String("dbName", t.GetDbName()),
			zap.String("collectionName", t.GetCollectionName()),
			zap.Error(err))
		return err
	}

	for _, f := range coll.schema.Functions {
		if f.Name == t.FunctionName {
			t.fSchema = f
			break
		}
	}
	return nil
}

func (t *dropCollectionFunctionTask) Execute(ctx context.Context) error {
	if t.fSchema == nil {
		return nil
	}

	if t.fSchema.Type == schemapb.FunctionType_BM25 {
		return fmt.Errorf("Currently does not support droping BM25 function")
	}

	var err error
	t.result, err = t.mixCoord.DropCollectionFunction(ctx, t.DropCollectionFunctionRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	return nil
}

func (t *dropCollectionFunctionTask) PostExecute(ctx context.Context) error {
	return nil
}

func getCollectionInfo(ctx context.Context, dbName string, collectionName string) (*collectionInfo, error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}
	coll, err := globalMetaCache.GetCollectionInfo(ctx, dbName, collectionName, collID)
	if err != nil {
		return nil, err
	}
	return coll, nil
}
