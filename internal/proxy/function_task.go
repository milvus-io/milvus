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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/function/validator"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const externalCollectionFunctionMutationUnsupportedMsg = "external collection does not support altering or dropping functions"

func rejectExternalCollectionFunctionMutation(schema *schemapb.CollectionSchema) error {
	if typeutil.IsExternalCollection(schema) {
		return merr.WrapErrParameterInvalidMsg(externalCollectionFunctionMutationUnsupportedMsg)
	}
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
		return merr.WrapErrParameterInvalidMsg("function schema is empty")
	}
	if t.FunctionSchema.Type == schemapb.FunctionType_BM25 {
		return merr.WrapErrParameterInvalidMsg("currently does not support alter BM25 function")
	}
	if t.FunctionName != t.FunctionSchema.Name {
		return merr.WrapErrParameterInvalidMsg("invalid function config, name not match")
	}
	coll, err := getCollectionInfo(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		mlog.Error(t.ctx, "AddCollectionTask, get collection info failed",
			mlog.String("dbName", t.GetDbName()),
			mlog.String("collectionName", t.GetCollectionName()),
			mlog.Err(err))
		return err
	}
	if err := rejectExternalCollectionFunctionMutation(coll.schema.CollectionSchema); err != nil {
		return err
	}
	funcExist := false
	newFunctions := []*schemapb.FunctionSchema{}
	for _, fSchema := range coll.schema.Functions {
		if t.FunctionName == fSchema.Name {
			if fSchema.Type == schemapb.FunctionType_BM25 {
				return merr.WrapErrParameterInvalidMsg("currently does not support alter BM25 function")
			}
			if err := validator.CheckFunctionAlterAllowed(fSchema, t.FunctionSchema); err != nil {
				return err
			}
			newFunctions = append(newFunctions, t.FunctionSchema)
			funcExist = true
		} else {
			newFunctions = append(newFunctions, fSchema)
		}
	}
	if !funcExist {
		return merr.WrapErrParameterInvalidMsg("function %s not found", t.FunctionName)
	}

	newColl := proto.Clone(coll.schema.CollectionSchema).(*schemapb.CollectionSchema)
	newColl.Functions = newFunctions
	if err := validator.ValidateFunction(newColl, t.FunctionName, false); err != nil {
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

func getCollectionInfo(ctx context.Context, dbName string, collectionName string) (*collectionInfo, error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}
	coll, err := globalMetaCache.GetCollectionInfo(ctx, dbName, collectionName, collID)
	if err != nil {
		return nil, err
	}
	coll.schema.DbName = dbName
	return coll, nil
}
