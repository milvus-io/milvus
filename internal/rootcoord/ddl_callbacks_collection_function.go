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
	"github.com/milvus-io/milvus/internal/util/function/validator"
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
		WithBroadcast(channels, alterCollectionBroadcastOptions(header)...).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		rollbackAlterCollectionAnalyzerFileResourceReservation(ctx, c.meta, newColl.CollectionID, addedFileResourceIds, err)
		return err
	}
	return nil
}

// resolveFunctionFieldIDs re-derives the function's input/output field IDs from
// its field names (the source of truth), discarding any client-supplied IDs — else
// a request could inject an unrelated field ID that a later drop_function_field
// would delete. It rejects unknown fields and output fields already owned by
// another function, and marks the resolved output fields as function outputs.
func resolveFunctionFieldIDs(ctx context.Context, fSchema *schemapb.FunctionSchema, fieldMapping map[string]*model.Field) error {
	fSchema.InputFieldIds = make([]int64, 0, len(fSchema.InputFieldNames))
	for _, name := range fSchema.InputFieldNames {
		field, exists := fieldMapping[name]
		if !exists {
			err := merr.WrapErrParameterInvalidMsg("function's input field %s not exists", name)
			mlog.Error(ctx, "Incorrect function configuration:", mlog.Err(err))
			return err
		}
		fSchema.InputFieldIds = append(fSchema.InputFieldIds, field.FieldID)
	}
	fSchema.OutputFieldIds = make([]int64, 0, len(fSchema.OutputFieldNames))
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

	if err := resolveFunctionFieldIDs(ctx, fSchema, fieldMapping); err != nil {
		return err
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

	// Only whitelisted params may be altered; function identity (type, name,
	// input/output fields) is immutable. Skip when the function is absent so the
	// mutation helper below emits the canonical "not exists" error.
	for _, fn := range oldColl.Functions {
		if fn.Name == req.GetFunctionSchema().GetName() {
			if err := validator.CheckFunctionAlterAllowed(model.MarshalFunctionModel(fn), req.GetFunctionSchema()); err != nil {
				return err
			}
			break
		}
	}

	newColl := oldColl.Clone()
	if err := alterFunctionGenNewCollection(ctx, req.FunctionSchema, newColl); err != nil {
		return err
	}

	return callAlterCollection(ctx, c, broadcaster, oldColl, newColl, req.GetDbName(), req.GetCollectionName())
}
