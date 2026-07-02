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
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type createNamespaceTask struct {
	baseTask
	Condition
	*milvuspb.CreateNamespaceRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.CreateNamespaceResponse
}

func (t *createNamespaceTask) TraceCtx() context.Context { return t.ctx }
func (t *createNamespaceTask) ID() UniqueID              { return t.Base.MsgID }
func (t *createNamespaceTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *createNamespaceTask) Name() string              { return CreateNamespaceTaskName }
func (t *createNamespaceTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *createNamespaceTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *createNamespaceTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *createNamespaceTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *createNamespaceTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_CreatePartition
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *createNamespaceTask) PreExecute(ctx context.Context) error {
	if err := validateNamespaceCollection(ctx, t.GetDbName(), t.GetCollectionName()); err != nil {
		return err
	}
	return validatePartitionTag(t.GetNamespaceName(), true)
}

func (t *createNamespaceTask) Execute(ctx context.Context) error {
	resp, err := t.mixCoord.CreateNamespace(ctx, t.CreateNamespaceRequest)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		return err
	}
	status, err := t.mixCoord.SyncNewCreatedPartition(ctx, &querypb.SyncNewCreatedPartitionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_CreatePartition),
		),
		CollectionID: collectionID,
		PartitionID:  resp.GetPartitionID(),
	})
	if err = merr.CheckRPCCall(status, err); err != nil {
		return err
	}
	t.result = &milvuspb.CreateNamespaceResponse{
		Status:    merr.Success(),
		Namespace: resp.GetNamespace(),
	}
	return nil
}

func (t *createNamespaceTask) PostExecute(ctx context.Context) error { return nil }

type describeNamespaceTask struct {
	baseTask
	Condition
	*milvuspb.DescribeNamespaceRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeNamespaceResponse
}

func (t *describeNamespaceTask) TraceCtx() context.Context { return t.ctx }
func (t *describeNamespaceTask) ID() UniqueID              { return t.Base.MsgID }
func (t *describeNamespaceTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *describeNamespaceTask) Name() string              { return DescribeNamespaceTaskName }
func (t *describeNamespaceTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *describeNamespaceTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *describeNamespaceTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *describeNamespaceTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *describeNamespaceTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_ShowPartitions
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *describeNamespaceTask) PreExecute(ctx context.Context) error {
	if err := validateNamespaceCollection(ctx, t.GetDbName(), t.GetCollectionName()); err != nil {
		return err
	}
	return validatePartitionTag(t.GetNamespaceName(), true)
}

func (t *describeNamespaceTask) Execute(ctx context.Context) error {
	resp, err := t.mixCoord.DescribeNamespace(ctx, t.DescribeNamespaceRequest)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *describeNamespaceTask) PostExecute(ctx context.Context) error { return nil }

type listNamespacesTask struct {
	baseTask
	Condition
	*milvuspb.ListNamespacesRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListNamespacesResponse
}

func (t *listNamespacesTask) TraceCtx() context.Context { return t.ctx }
func (t *listNamespacesTask) ID() UniqueID              { return t.Base.MsgID }
func (t *listNamespacesTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *listNamespacesTask) Name() string              { return ListNamespacesTaskName }
func (t *listNamespacesTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *listNamespacesTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *listNamespacesTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *listNamespacesTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *listNamespacesTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_ShowPartitions
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *listNamespacesTask) PreExecute(ctx context.Context) error {
	return validateNamespaceCollection(ctx, t.GetDbName(), t.GetCollectionName())
}

func (t *listNamespacesTask) Execute(ctx context.Context) error {
	resp, err := t.mixCoord.ListNamespaces(ctx, t.ListNamespacesRequest)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *listNamespacesTask) PostExecute(ctx context.Context) error { return nil }

type dropNamespaceTask struct {
	baseTask
	Condition
	*milvuspb.DropNamespaceRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DropNamespaceResponse
}

func (t *dropNamespaceTask) TraceCtx() context.Context { return t.ctx }
func (t *dropNamespaceTask) ID() UniqueID              { return t.Base.MsgID }
func (t *dropNamespaceTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *dropNamespaceTask) Name() string              { return DropNamespaceTaskName }
func (t *dropNamespaceTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *dropNamespaceTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *dropNamespaceTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *dropNamespaceTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *dropNamespaceTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DropPartition
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *dropNamespaceTask) PreExecute(ctx context.Context) error {
	if err := validateNamespaceCollection(ctx, t.GetDbName(), t.GetCollectionName()); err != nil {
		return err
	}
	if err := validatePartitionTag(t.GetNamespaceName(), true); err != nil {
		return err
	}
	return checkNamespaceNotLoaded(ctx, t.mixCoord, t.GetDbName(), t.GetCollectionName(), t.GetNamespaceName())
}

func (t *dropNamespaceTask) Execute(ctx context.Context) error {
	resp, err := t.mixCoord.DropNamespace(ctx, t.DropNamespaceRequest)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *dropNamespaceTask) PostExecute(ctx context.Context) error { return nil }

type hasNamespaceTask struct {
	baseTask
	Condition
	*milvuspb.HasNamespaceRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.HasNamespaceResponse
}

func (t *hasNamespaceTask) TraceCtx() context.Context { return t.ctx }
func (t *hasNamespaceTask) ID() UniqueID              { return t.Base.MsgID }
func (t *hasNamespaceTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *hasNamespaceTask) Name() string              { return HasNamespaceTaskName }
func (t *hasNamespaceTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *hasNamespaceTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *hasNamespaceTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *hasNamespaceTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *hasNamespaceTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_HasPartition
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *hasNamespaceTask) PreExecute(ctx context.Context) error {
	if err := validateNamespaceCollection(ctx, t.GetDbName(), t.GetCollectionName()); err != nil {
		return err
	}
	return validatePartitionTag(t.GetNamespaceName(), true)
}

func (t *hasNamespaceTask) Execute(ctx context.Context) error {
	resp, err := t.mixCoord.HasNamespace(ctx, t.HasNamespaceRequest)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *hasNamespaceTask) PostExecute(ctx context.Context) error { return nil }

type getNamespaceStatsTask struct {
	baseTask
	Condition
	*milvuspb.GetNamespaceStatsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.GetNamespaceStatsResponse
}

func (t *getNamespaceStatsTask) TraceCtx() context.Context { return t.ctx }
func (t *getNamespaceStatsTask) ID() UniqueID              { return t.Base.MsgID }
func (t *getNamespaceStatsTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *getNamespaceStatsTask) Name() string              { return GetNamespaceStatsTaskName }
func (t *getNamespaceStatsTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *getNamespaceStatsTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *getNamespaceStatsTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *getNamespaceStatsTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *getNamespaceStatsTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *getNamespaceStatsTask) PreExecute(ctx context.Context) error {
	if t.GetExact() {
		return merr.WrapErrParameterInvalidMsg("exact namespace stats are not supported yet")
	}
	if err := validateNamespaceCollection(ctx, t.GetDbName(), t.GetCollectionName()); err != nil {
		return err
	}
	return validatePartitionTag(t.GetNamespaceName(), true)
}

func (t *getNamespaceStatsTask) Execute(ctx context.Context) error {
	collectionID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), t.GetCollectionName())
	if err != nil {
		return err
	}
	partitionID, err := globalMetaCache.GetPartitionID(ctx, t.GetDbName(), t.GetCollectionName(), t.GetNamespaceName())
	if err != nil {
		if errors.Is(err, merr.ErrPartitionNotFound) {
			return merr.WrapErrNamespaceNotFound(t.GetNamespaceName())
		}
		return err
	}
	if t.GetNamespaceName() == paramtable.Get().CommonCfg.DefaultPartitionName.GetValue() {
		return merr.WrapErrNamespaceNotFound(t.GetNamespaceName())
	}

	resp, err := t.mixCoord.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{
		Base: commonpbutil.UpdateMsgBase(
			t.Base,
			commonpbutil.WithMsgType(commonpb.MsgType_GetPartitionStatistics),
		),
		CollectionID: collectionID,
		PartitionIDs: []int64{partitionID},
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	t.result = &milvuspb.GetNamespaceStatsResponse{
		Status:        merr.Success(),
		NamespaceName: t.GetNamespaceName(),
		Stats:         namespaceStatsFromKVs(ctx, resp.GetStats()),
	}
	return nil
}

func (t *getNamespaceStatsTask) PostExecute(ctx context.Context) error { return nil }

func validateNamespaceCollection(ctx context.Context, dbName, collectionName string) error {
	if err := validateCollectionName(collectionName); err != nil {
		return err
	}
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, collectionName)
	if err != nil {
		return err
	}
	schema := collSchema.CollectionSchema
	if !schema.GetEnableNamespace() || !common.IsNamespaceModePartition(schema.GetProperties()...) {
		return merr.WrapErrParameterInvalidMsg("namespace APIs require collection with namespace enabled and namespace.mode=partition")
	}
	return nil
}

func checkNamespaceNotLoaded(ctx context.Context, mixCoord types.MixCoordClient, dbName, collectionName, namespaceName string) error {
	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	if err != nil {
		return err
	}
	partitionID, err := globalMetaCache.GetPartitionID(ctx, dbName, collectionName, namespaceName)
	if err != nil {
		if errors.Is(err, merr.ErrPartitionNotFound) || errors.Is(err, merr.ErrCollectionNotFound) || errors.Is(err, merr.ErrDatabaseNotFound) {
			return nil
		}
		return err
	}

	collLoaded, err := isCollectionLoaded(ctx, mixCoord, collectionID)
	if err != nil {
		return err
	}
	if !collLoaded {
		return nil
	}
	loaded, err := isPartitionLoaded(ctx, mixCoord, collectionID, partitionID)
	if err != nil {
		return err
	}
	if loaded {
		return merr.WrapErrParameterInvalidMsg("namespace cannot be dropped, namespace is loaded, please release it first")
	}
	return nil
}

func namespaceStatsFromKVs(ctx context.Context, stats []*commonpb.KeyValuePair) *milvuspb.NamespaceStats {
	namespaceStats := &milvuspb.NamespaceStats{
		Stats:           stats,
		EntityCountType: "approximate",
	}
	for _, stat := range stats {
		if stat.GetKey() != "row_count" {
			continue
		}
		count, err := strconv.ParseInt(stat.GetValue(), 10, 64)
		if err != nil {
			mlog.Warn(ctx, "failed to parse namespace row_count stat", mlog.String("value", stat.GetValue()), mlog.Err(err))
			continue
		}
		namespaceStats.ApproxEntityCount = count
		return namespaceStats
	}
	return namespaceStats
}
