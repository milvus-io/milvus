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
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastAlterCollectionSchema broadcasts the alter collection schema message to all channels.
func (c *Core) broadcastAlterCollectionSchema(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) error {
	// startBroadcastWithAliasOrCollectionLock acquires a cluster-wide exclusive
	// resource-key lock on the collection (via StreamingCoord's broadcaster), which
	// already serializes concurrent AlterCollectionSchema / AddCollectionField
	// requests from different Proxy instances. The lock is alias-robust — the
	// collection name is pre-resolved before the lock is taken.
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

	if len(fieldInfos) == 0 {
		return merr.WrapErrParameterInvalidMsg("fieldInfos is empty")
	}

	// Schema version consistency re-check: the broadcast resource lock serializes
	// concurrent schema-change DDLs, but it does NOT guarantee that the previous
	// one's backfill has finished. Verify against DataCoord's authoritative state
	// that all segments have caught up to the current schema version before bumping
	// it again. Run after cheap request-shape validation so malformed requests do
	// not incur the DataCoord round-trip.
	if err := c.checkSchemaVersionConsistencyAtRootCoord(ctx, coll); err != nil {
		return err
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
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: coll.EnableDynamicField,
		Properties:         coll.Properties,
		DbName:             coll.DBName,
		FileResourceIds:    coll.FileResourceIds,
		// Version is incremented by 1. No CAS is needed here because Proxy's
		// checkSchemaVersionConsistency gate blocks new AlterCollectionSchema calls
		// until the previous backfill reaches 100% consistency, and DDL requests
		// are serialized through a single DDL queue — so concurrent schema changes
		// that could produce duplicate version numbers are impossible.
		Version:            coll.SchemaVersion + 1,
		DoPhysicalBackfill: addRequest.GetDoPhysicalBackfill(),
	}
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

// checkSchemaVersionConsistencyAtRootCoord is the authoritative, cluster-wide schema
// version consistency check. It is called from schema-change DDL handlers after
// acquiring the collection resource key lock and after cheap request-shape validation,
// and before bumping the schema version.
//
// Rationale for running this at RootCoord rather than only at Proxy: in multi-Proxy
// deployments, two concurrent requests routed to different Proxy instances each see
// their own empty per-Proxy alterSchemaInFlight map and each call the Proxy-layer
// consistency check independently. Both can pass before either has bumped the schema
// version, producing two serial schema bumps that overlap backfill. RootCoord is a
// single authoritative point, so its gate catches that race. The collection resource
// key lock in startBroadcastWithAliasOrCollectionLock provides the mutual exclusion;
// this function only adds the "wait for previous backfill to complete" semantics on
// top of it.
//
// The check itself mirrors Proxy's checkSchemaVersionConsistency:
//   - RPC DataCoord.GetCollectionStatistics
//   - Look for SchemaVersionConsistentSegmentsKey and SchemaVersionTotalSegmentsKey
//   - Absent keys → schema version 0 → trivially consistent → pass
//   - Equal counts → pass
//   - Otherwise → reject with ErrParameterInvalid
//
// Integer counts are used instead of a floating-point proportion to avoid the
// rounding hazard where e.g. 99999/100000 = 99.999% would format as "100.00" and
// falsely satisfy the gate.
func (c *Core) checkSchemaVersionConsistencyAtRootCoord(ctx context.Context, coll *model.Collection) error {
	// Defensive: callers should always pass a non-nil collection, but guard against
	// a future caller forgetting the invariant — the field accesses below would panic.
	if coll == nil {
		return merr.WrapErrParameterInvalidMsg("nil collection in schema version consistency check")
	}
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", coll.CollectionID),
		zap.String("collection", coll.Name))

	statsResp, err := c.mixCoord.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DbID:         coll.DBID,
		CollectionID: coll.CollectionID,
	})
	if err := merr.CheckRPCCall(statsResp, err); err != nil {
		log.Warn("failed to get collection statistics for schema version consistency check", zap.Error(err))
		return err
	}

	// Find schema_version_consistent_segments and schema_version_total_segments from Stats.
	// DataCoord emits these two integer keys only when the collection's schema version > 0.
	// Absent keys mean the schema version is still 0 — no function field has ever been
	// added — so there is no in-flight backfill and no DDL can be racing with one.
	// -1 is the "absent" sentinel; parsed values are always >= 0.
	consistent, total := -1, -1
	for _, stat := range statsResp.GetStats() {
		key := stat.GetKey()
		if key != common.SchemaVersionConsistentSegmentsKey && key != common.SchemaVersionTotalSegmentsKey {
			continue
		}
		v, err := strconv.Atoi(stat.GetValue())
		if err != nil || v < 0 {
			log.Warn("failed to parse schema version consistency stat",
				zap.String("key", key), zap.String("value", stat.GetValue()), zap.Error(err))
			return merr.WrapErrParameterInvalidMsg("invalid %s value: %s", key, stat.GetValue())
		}
		switch key {
		case common.SchemaVersionConsistentSegmentsKey:
			consistent = v
		case common.SchemaVersionTotalSegmentsKey:
			total = v
		}
	}

	if consistent == -1 && total == -1 {
		// Both keys absent: schema version is 0, no backfill has ever been triggered.
		return nil
	}
	if consistent == -1 || total == -1 {
		// Exactly one key present — should never happen since DataCoord emits both atomically.
		// Treat as a data corruption signal and block the DDL rather than silently passing.
		log.Warn("incomplete schema version consistency stats, blocking DDL",
			zap.Int("consistent", consistent), zap.Int("total", total))
		return merr.WrapErrParameterInvalidMsg("incomplete schema version consistency stats from DataCoord")
	}
	log.Info("checked schema version consistency",
		zap.Int("consistent", consistent), zap.Int("total", total))
	if consistent < total {
		return merr.WrapErrParameterInvalidMsg(
			"schema version consistency check failed: %d/%d segments have caught up; retry after backfill completes",
			consistent, total)
	}
	return nil
}
