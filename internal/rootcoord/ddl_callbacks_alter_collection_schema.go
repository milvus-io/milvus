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

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
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

	// 6. build new collection schema.
	schema := coll.ToCollectionSchemaPB()
	// Version is incremented by 1. No CAS is needed here because Proxy's
	// checkSchemaVersionConsistency gate blocks new AlterCollectionSchema calls
	// until the previous backfill reaches 100% consistency, and DDL requests
	// are serialized through a single DDL queue — so concurrent schema changes
	// that could produce duplicate version numbers are impossible.
	schema.Version = coll.SchemaVersion + 1
	schema.DoPhysicalBackfill = addRequest.GetDoPhysicalBackfill()
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
