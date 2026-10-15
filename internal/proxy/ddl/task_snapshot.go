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

package ddl

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	CreateSnapshotTaskName          = "CreateSnapshotTask"
	DropSnapshotTaskName            = "DropSnapshotTask"
	DescribeSnapshotTaskName        = "DescribeSnapshotTask"
	ListSnapshotsTaskName           = "ListSnapshotsTask"
	RestoreSnapshotTaskName         = "RestoreSnapshotTask"
	GetRestoreSnapshotStateTaskName = "GetRestoreSnapshotStateTask"
	ListRestoreSnapshotJobsTaskName = "ListRestoreSnapshotJobsTask"
	PinSnapshotDataTaskName         = "PinSnapshotDataTask"
	UnpinSnapshotDataTaskName       = "UnpinSnapshotDataTask"
)

// resolveCollectionNames resolves collection ID to (dbName, collectionName) via MetaCache.
// Returns empty strings on failure (best-effort for display purposes).
func resolveCollectionNames(ctx context.Context, metaCache Cache, collectionID int64) (string, string) {
	if collectionID == 0 {
		return "", ""
	}
	collInfo, err := metaCache.GetCollectionInfo(ctx, "", "", collectionID)
	if err != nil {
		mlog.Warn(ctx, "failed to resolve collection names from ID",
			mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return "", ""
	}
	return collInfo.DBName, collInfo.Schema.Name
}

type CreateSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.CreateSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID UniqueID
}

func (cst *CreateSnapshotTask) TraceCtx() context.Context {
	return cst.ctx
}

func (cst *CreateSnapshotTask) ID() UniqueID {
	return cst.req.GetBase().GetMsgID()
}

func (cst *CreateSnapshotTask) SetID(uid UniqueID) {
	cst.req.GetBase().MsgID = uid
}

func (cst *CreateSnapshotTask) Name() string {
	return CreateSnapshotTaskName
}

func (cst *CreateSnapshotTask) Type() commonpb.MsgType {
	return cst.req.GetBase().GetMsgType()
}

func (cst *CreateSnapshotTask) BeginTs() Timestamp {
	return cst.req.GetBase().GetTimestamp()
}

func (cst *CreateSnapshotTask) EndTs() Timestamp {
	return cst.req.GetBase().GetTimestamp()
}

func (cst *CreateSnapshotTask) SetTs(ts Timestamp) {
	cst.req.Base.Timestamp = ts
}

func (cst *CreateSnapshotTask) OnEnqueue() error {
	if cst.req.Base == nil {
		cst.req.Base = commonpbutil.NewMsgBase()
	}
	cst.req.Base.MsgType = commonpb.MsgType_CreateSnapshot
	cst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (cst *CreateSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(cst.req.GetName()); err != nil {
		return err
	}

	// Validate compaction protection duration
	maxCompactionProtectionSeconds := paramtable.Get().DataCoordCfg.SnapshotMaxCompactionProtectionSeconds.GetAsInt64()
	if cst.req.GetCompactionProtectionSeconds() < 0 {
		return merr.WrapErrParameterInvalidMsg("compaction_protection_seconds must be non-negative")
	}
	if cst.req.GetCompactionProtectionSeconds() > maxCompactionProtectionSeconds {
		return merr.WrapErrParameterInvalidMsg("compaction_protection_seconds must not exceed %d", maxCompactionProtectionSeconds)
	}

	collectionID, err := cst.GetMetaCache().GetCollectionID(ctx, cst.req.GetDbName(), cst.req.GetCollectionName())
	if err != nil {
		return err
	}
	cst.collectionID = collectionID

	return nil
}

func (cst *CreateSnapshotTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy create snapshot",
		mlog.String("snapshotName", cst.req.GetName()),
		mlog.FieldCollectionName(cst.req.GetCollectionName()),
		mlog.FieldCollectionID(cst.collectionID),
	)

	var err error
	cst.result, err = cst.mixCoord.CreateSnapshot(ctx, &datapb.CreateSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_CreateSnapshot),
		),
		Name:                        cst.req.GetName(),
		Description:                 cst.req.GetDescription(),
		CollectionId:                cst.collectionID,
		CompactionProtectionSeconds: cst.req.GetCompactionProtectionSeconds(),
	})
	if err = merr.CheckRPCCall(cst.result, err); err != nil {
		return err
	}
	return nil
}

func (cst *CreateSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.DropSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID UniqueID
}

func (dst *DropSnapshotTask) TraceCtx() context.Context {
	return dst.ctx
}

func (dst *DropSnapshotTask) ID() UniqueID {
	return dst.req.GetBase().GetMsgID()
}

func (dst *DropSnapshotTask) SetID(uid UniqueID) {
	dst.req.GetBase().MsgID = uid
}

func (dst *DropSnapshotTask) Name() string {
	return DropSnapshotTaskName
}

func (dst *DropSnapshotTask) Type() commonpb.MsgType {
	return dst.req.GetBase().GetMsgType()
}

func (dst *DropSnapshotTask) BeginTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *DropSnapshotTask) EndTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *DropSnapshotTask) SetTs(ts Timestamp) {
	dst.req.Base.Timestamp = ts
}

func (dst *DropSnapshotTask) OnEnqueue() error {
	if dst.req.Base == nil {
		dst.req.Base = commonpbutil.NewMsgBase()
	}
	dst.req.Base.MsgType = commonpb.MsgType_DropSnapshot
	dst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dst *DropSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(dst.req.GetName()); err != nil {
		return err
	}

	if dst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterMissingMsg("collection_name is required for drop snapshot")
	}
	collectionID, err := dst.GetMetaCache().GetCollectionID(ctx, dst.req.GetDbName(), dst.req.GetCollectionName())
	if err != nil {
		return err
	}
	dst.collectionID = collectionID

	return nil
}

func (dst *DropSnapshotTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy drop snapshot",
		mlog.String("snapshotName", dst.req.GetName()),
		mlog.FieldCollectionName(dst.req.GetCollectionName()),
		mlog.FieldCollectionID(dst.collectionID),
	)

	var err error
	dst.result, err = dst.mixCoord.DropSnapshot(ctx, &datapb.DropSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropSnapshot),
		),
		Name:         dst.req.GetName(),
		CollectionId: dst.collectionID,
	})
	if err = merr.CheckRPCCall(dst.result, err); err != nil {
		return err
	}
	return nil
}

func (dst *DropSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type DescribeSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.DescribeSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeSnapshotResponse

	collectionID UniqueID
}

func (dst *DescribeSnapshotTask) TraceCtx() context.Context {
	return dst.ctx
}

func (dst *DescribeSnapshotTask) ID() UniqueID {
	return dst.req.GetBase().GetMsgID()
}

func (dst *DescribeSnapshotTask) SetID(uid UniqueID) {
	dst.req.GetBase().MsgID = uid
}

func (dst *DescribeSnapshotTask) Name() string {
	return DescribeSnapshotTaskName
}

func (dst *DescribeSnapshotTask) Type() commonpb.MsgType {
	return dst.req.GetBase().GetMsgType()
}

func (dst *DescribeSnapshotTask) BeginTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *DescribeSnapshotTask) EndTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *DescribeSnapshotTask) SetTs(ts Timestamp) {
	dst.req.Base.Timestamp = ts
}

func (dst *DescribeSnapshotTask) OnEnqueue() error {
	if dst.req.Base == nil {
		dst.req.Base = commonpbutil.NewMsgBase()
	}
	dst.req.Base.MsgType = commonpb.MsgType_DescribeSnapshot
	dst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dst *DescribeSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(dst.req.GetName()); err != nil {
		return err
	}

	if dst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterMissingMsg("collection_name is required for describe snapshot")
	}
	collectionID, err := dst.GetMetaCache().GetCollectionID(ctx, dst.req.GetDbName(), dst.req.GetCollectionName())
	if err != nil {
		return err
	}
	dst.collectionID = collectionID

	return nil
}

func (dst *DescribeSnapshotTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy describe snapshot",
		mlog.String("snapshotName", dst.req.GetName()),
	)

	result, err := dst.mixCoord.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeSnapshot),
		),
		Name:                  dst.req.GetName(),
		CollectionId:          dst.collectionID,
		IncludeCollectionInfo: false,
	})
	if err = merr.CheckRPCCall(result, err); err != nil {
		dst.result = &milvuspb.DescribeSnapshotResponse{
			Status: merr.Status(err),
		}
		return err
	}

	snapshotInfo := result.GetSnapshotInfo()

	collectionName, err := dst.GetMetaCache().GetCollectionName(ctx, "", snapshotInfo.GetCollectionId())
	if err != nil {
		mlog.Warn(ctx, "DescribeSnapshot fail to get collection name",
			mlog.Err(err))
		return err
	}
	var partitionNames []string
	for _, partitionID := range snapshotInfo.GetPartitionIds() {
		partitionName, err := dst.GetMetaCache().GetPartitionName(ctx, "", collectionName, partitionID)
		if err != nil {
			mlog.Warn(ctx, "DescribeSnapshot fail to get partition name",
				mlog.FieldPartitionID(partitionID),
				mlog.Err(err))
		}
		partitionNames = append(partitionNames, partitionName)
	}

	dst.result = &milvuspb.DescribeSnapshotResponse{
		Status:         result.GetStatus(),
		Name:           snapshotInfo.GetName(),
		Description:    snapshotInfo.GetDescription(),
		CreateTs:       snapshotInfo.GetCreateTs(),
		CollectionName: collectionName,
		PartitionNames: partitionNames,
		S3Location:     snapshotInfo.GetS3Location(),
	}

	return nil
}

func (dst *DescribeSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type ListSnapshotsTask struct {
	baseTask
	Condition
	req      *milvuspb.ListSnapshotsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListSnapshotsResponse

	collectionID UniqueID
	dbID         UniqueID
}

func (lst *ListSnapshotsTask) TraceCtx() context.Context {
	return lst.ctx
}

func (lst *ListSnapshotsTask) ID() UniqueID {
	return lst.req.GetBase().GetMsgID()
}

func (lst *ListSnapshotsTask) SetID(uid UniqueID) {
	lst.req.GetBase().MsgID = uid
}

func (lst *ListSnapshotsTask) Name() string {
	return ListSnapshotsTaskName
}

func (lst *ListSnapshotsTask) Type() commonpb.MsgType {
	return lst.req.GetBase().GetMsgType()
}

func (lst *ListSnapshotsTask) BeginTs() Timestamp {
	return lst.req.GetBase().GetTimestamp()
}

func (lst *ListSnapshotsTask) EndTs() Timestamp {
	return lst.req.GetBase().GetTimestamp()
}

func (lst *ListSnapshotsTask) SetTs(ts Timestamp) {
	lst.req.Base.Timestamp = ts
}

func (lst *ListSnapshotsTask) OnEnqueue() error {
	if lst.req.Base == nil {
		lst.req.Base = commonpbutil.NewMsgBase()
	}
	lst.req.Base.MsgType = commonpb.MsgType_ListSnapshots
	lst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (lst *ListSnapshotsTask) PreExecute(ctx context.Context) error {
	// Resolve database ID for db-level filtering
	if lst.req.GetDbName() != "" {
		dbInfo, err := lst.GetMetaCache().GetDatabaseInfo(ctx, lst.req.GetDbName())
		if err != nil {
			return err
		}
		lst.dbID = dbInfo.DBID
	}

	if lst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterMissingMsg("collection_name is required for ListSnapshots")
	}

	collectionID, err := lst.GetMetaCache().GetCollectionID(ctx, lst.req.GetDbName(), lst.req.GetCollectionName())
	if err != nil {
		return err
	}
	lst.collectionID = collectionID

	return nil
}

func (lst *ListSnapshotsTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy list snapshots",
		mlog.FieldCollectionName(lst.req.GetCollectionName()),
		mlog.FieldCollectionID(lst.collectionID),
	)

	result, err := lst.mixCoord.ListSnapshots(ctx, &datapb.ListSnapshotsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ListSnapshots),
		),
		CollectionId: lst.collectionID,
		DbId:         lst.dbID,
	})
	if err = merr.CheckRPCCall(result, err); err != nil {
		lst.result = &milvuspb.ListSnapshotsResponse{
			Status: merr.Status(err),
		}
		return err
	}

	lst.result = &milvuspb.ListSnapshotsResponse{
		Status:    result.GetStatus(),
		Snapshots: result.GetSnapshots(),
	}

	return nil
}

func (lst *ListSnapshotsTask) PostExecute(ctx context.Context) error {
	return nil
}

type RestoreSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.RestoreSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.RestoreSnapshotResponse

	collectionID UniqueID // source collection ID for per-collection snapshot lookup
}

func (rst *RestoreSnapshotTask) TraceCtx() context.Context {
	return rst.ctx
}

func (rst *RestoreSnapshotTask) ID() UniqueID {
	return rst.req.GetBase().GetMsgID()
}

func (rst *RestoreSnapshotTask) SetID(uid UniqueID) {
	rst.req.GetBase().MsgID = uid
}

func (rst *RestoreSnapshotTask) Name() string {
	return RestoreSnapshotTaskName
}

func (rst *RestoreSnapshotTask) Type() commonpb.MsgType {
	return rst.req.GetBase().GetMsgType()
}

func (rst *RestoreSnapshotTask) BeginTs() Timestamp {
	return rst.req.GetBase().GetTimestamp()
}

func (rst *RestoreSnapshotTask) EndTs() Timestamp {
	return rst.req.GetBase().GetTimestamp()
}

func (rst *RestoreSnapshotTask) SetTs(ts Timestamp) {
	rst.req.Base.Timestamp = ts
}

func (rst *RestoreSnapshotTask) OnEnqueue() error {
	if rst.req.Base == nil {
		rst.req.Base = commonpbutil.NewMsgBase()
	}
	rst.req.Base.MsgType = commonpb.MsgType_RestoreSnapshot
	rst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (rst *RestoreSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(rst.req.GetName()); err != nil {
		return err
	}

	// Validate source collection name
	if rst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterMissingMsg("collection_name is required for restore snapshot")
	}

	// Validate target collection name (required, cheap checks before RPC)
	if rst.req.GetTargetCollectionName() == "" {
		return merr.WrapErrParameterMissingMsg("target_collection_name is required for restore snapshot")
	}
	if err := ValidateCollectionName(rst.req.GetTargetCollectionName()); err != nil {
		return err
	}

	// Resolve source collection ID for per-collection snapshot lookup (RPC call)
	collectionID, err := rst.GetMetaCache().GetCollectionID(ctx, rst.req.GetDbName(), rst.req.GetCollectionName())
	if err != nil {
		return err
	}
	rst.collectionID = collectionID

	return nil
}

func (rst *RestoreSnapshotTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy restore snapshot",
		mlog.String("snapshotName", rst.req.GetName()),
		mlog.String("sourceCollection", rst.req.GetCollectionName()),
		mlog.String("sourceDb", rst.req.GetDbName()),
		mlog.String("targetCollection", rst.req.GetTargetCollectionName()),
		mlog.String("targetDb", rst.req.GetTargetDbName()),
	)

	// Delegate directly to DataCoord which handles the entire restore process
	resp, err := rst.mixCoord.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RestoreSnapshot),
		),
		Name:                 rst.req.GetName(),
		TargetDbName:         rst.req.GetTargetDbName(),
		TargetCollectionName: rst.req.GetTargetCollectionName(),
		SourceCollectionId:   rst.collectionID,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(ctx, "RestoreSnapshot failed",
			mlog.Err(err))
		rst.result = &milvuspb.RestoreSnapshotResponse{Status: merr.Status(err)}
		return err
	}
	rst.result = &milvuspb.RestoreSnapshotResponse{
		Status: merr.Success(),
		JobId:  resp.GetJobId(),
	}
	return nil
}

func (rst *RestoreSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type GetRestoreSnapshotStateTask struct {
	baseTask
	Condition
	req      *milvuspb.GetRestoreSnapshotStateRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.GetRestoreSnapshotStateResponse
}

func (grst *GetRestoreSnapshotStateTask) TraceCtx() context.Context {
	return grst.ctx
}

func (grst *GetRestoreSnapshotStateTask) ID() UniqueID {
	return grst.req.GetBase().GetMsgID()
}

func (grst *GetRestoreSnapshotStateTask) SetID(uid UniqueID) {
	grst.req.GetBase().MsgID = uid
}

func (grst *GetRestoreSnapshotStateTask) Name() string {
	return GetRestoreSnapshotStateTaskName
}

func (grst *GetRestoreSnapshotStateTask) Type() commonpb.MsgType {
	return grst.req.GetBase().GetMsgType()
}

func (grst *GetRestoreSnapshotStateTask) BeginTs() Timestamp {
	return grst.req.GetBase().GetTimestamp()
}

func (grst *GetRestoreSnapshotStateTask) EndTs() Timestamp {
	return grst.req.GetBase().GetTimestamp()
}

func (grst *GetRestoreSnapshotStateTask) SetTs(ts Timestamp) {
	grst.req.Base.Timestamp = ts
}

func (grst *GetRestoreSnapshotStateTask) OnEnqueue() error {
	if grst.req.Base == nil {
		grst.req.Base = commonpbutil.NewMsgBase()
	}
	grst.req.Base.MsgType = commonpb.MsgType_GetRestoreSnapshotState
	grst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (grst *GetRestoreSnapshotStateTask) PreExecute(ctx context.Context) error {
	// No additional validation needed for get restore snapshot state
	return nil
}

func (grst *GetRestoreSnapshotStateTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy get restore snapshot state",
		mlog.FieldJobID(grst.req.GetJobId()),
	)

	result, err := grst.mixCoord.GetRestoreSnapshotState(ctx, &datapb.GetRestoreSnapshotStateRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Undefined),
		),
		JobId: grst.req.GetJobId(),
	})
	if err = merr.CheckRPCCall(result, err); err != nil {
		grst.result = &milvuspb.GetRestoreSnapshotStateResponse{
			Status: merr.Status(err),
		}
		return err
	}

	// Convert datapb.RestoreSnapshotInfo to milvuspb.RestoreSnapshotInfo
	info := result.GetInfo()
	var milvusInfo *milvuspb.RestoreSnapshotInfo
	if info != nil {
		dbName, collectionName := resolveCollectionNames(ctx, grst.GetMetaCache(), info.GetCollectionId())
		milvusInfo = &milvuspb.RestoreSnapshotInfo{
			JobId:          info.GetJobId(),
			SnapshotName:   info.GetSnapshotName(),
			DbName:         dbName,
			CollectionName: collectionName,
			State:          milvuspb.RestoreSnapshotState(info.GetState()),
			Progress:       info.GetProgress(),
			Reason:         info.GetReason(),
			StartTime:      info.GetStartTime(),
			TimeCost:       info.GetTimeCost(),
		}
	}

	grst.result = &milvuspb.GetRestoreSnapshotStateResponse{
		Status: result.GetStatus(),
		Info:   milvusInfo,
	}

	return nil
}

func (grst *GetRestoreSnapshotStateTask) PostExecute(ctx context.Context) error {
	return nil
}

type ListRestoreSnapshotJobsTask struct {
	baseTask
	Condition
	req      *milvuspb.ListRestoreSnapshotJobsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListRestoreSnapshotJobsResponse

	collectionID UniqueID
	dbID         UniqueID
}

func (lrst *ListRestoreSnapshotJobsTask) TraceCtx() context.Context {
	return lrst.ctx
}

func (lrst *ListRestoreSnapshotJobsTask) ID() UniqueID {
	return lrst.req.GetBase().GetMsgID()
}

func (lrst *ListRestoreSnapshotJobsTask) SetID(uid UniqueID) {
	lrst.req.GetBase().MsgID = uid
}

func (lrst *ListRestoreSnapshotJobsTask) Name() string {
	return ListRestoreSnapshotJobsTaskName
}

func (lrst *ListRestoreSnapshotJobsTask) Type() commonpb.MsgType {
	return lrst.req.GetBase().GetMsgType()
}

func (lrst *ListRestoreSnapshotJobsTask) BeginTs() Timestamp {
	return lrst.req.GetBase().GetTimestamp()
}

func (lrst *ListRestoreSnapshotJobsTask) EndTs() Timestamp {
	return lrst.req.GetBase().GetTimestamp()
}

func (lrst *ListRestoreSnapshotJobsTask) SetTs(ts Timestamp) {
	lrst.req.Base.Timestamp = ts
}

func (lrst *ListRestoreSnapshotJobsTask) OnEnqueue() error {
	if lrst.req.Base == nil {
		lrst.req.Base = commonpbutil.NewMsgBase()
	}
	lrst.req.Base.MsgType = commonpb.MsgType_ListRestoreSnapshotJobs
	lrst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (lrst *ListRestoreSnapshotJobsTask) PreExecute(ctx context.Context) error {
	// Resolve database ID for db-level filtering
	if lrst.req.GetDbName() != "" {
		dbInfo, err := lrst.GetMetaCache().GetDatabaseInfo(ctx, lrst.req.GetDbName())
		if err != nil {
			return err
		}
		lrst.dbID = dbInfo.DBID
	}

	if lrst.req.GetCollectionName() != "" {
		collectionID, err := lrst.GetMetaCache().GetCollectionID(ctx, lrst.req.GetDbName(), lrst.req.GetCollectionName())
		if err != nil {
			return err
		}
		lrst.collectionID = collectionID
	}
	return nil
}

func (lrst *ListRestoreSnapshotJobsTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy list restore snapshot jobs",
		mlog.FieldCollectionName(lrst.req.GetCollectionName()),
		mlog.FieldCollectionID(lrst.collectionID),
	)

	result, err := lrst.mixCoord.ListRestoreSnapshotJobs(ctx, &datapb.ListRestoreSnapshotJobsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Undefined),
		),
		CollectionId: lrst.collectionID,
		DbId:         lrst.dbID,
	})
	if err = merr.CheckRPCCall(result, err); err != nil {
		lrst.result = &milvuspb.ListRestoreSnapshotJobsResponse{
			Status: merr.Status(err),
		}
		return err
	}

	// Convert datapb.RestoreSnapshotInfo to milvuspb.RestoreSnapshotInfo
	jobs := result.GetJobs()
	milvusJobs := make([]*milvuspb.RestoreSnapshotInfo, 0, len(jobs))
	for _, job := range jobs {
		dbName, collectionName := resolveCollectionNames(ctx, lrst.GetMetaCache(), job.GetCollectionId())
		milvusJobs = append(milvusJobs, &milvuspb.RestoreSnapshotInfo{
			JobId:          job.GetJobId(),
			SnapshotName:   job.GetSnapshotName(),
			DbName:         dbName,
			CollectionName: collectionName,
			State:          milvuspb.RestoreSnapshotState(job.GetState()),
			Progress:       job.GetProgress(),
			Reason:         job.GetReason(),
			StartTime:      job.GetStartTime(),
			TimeCost:       job.GetTimeCost(),
		})
	}

	lrst.result = &milvuspb.ListRestoreSnapshotJobsResponse{
		Status: result.GetStatus(),
		Jobs:   milvusJobs,
	}

	return nil
}

func (lrst *ListRestoreSnapshotJobsTask) PostExecute(ctx context.Context) error {
	return nil
}

// PinSnapshotDataTask pins snapshot data to prevent GC from cleaning up segments
// referenced by a snapshot. Accepts milvuspb request from user, resolves
// collection_name to collection_id, and forwards to DataCoord.
type PinSnapshotDataTask struct {
	baseTask
	Condition
	req      *milvuspb.PinSnapshotDataRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.PinSnapshotDataResponse

	collectionID UniqueID
}

func (pst *PinSnapshotDataTask) TraceCtx() context.Context {
	return pst.ctx
}

func (pst *PinSnapshotDataTask) ID() UniqueID {
	return pst.req.GetBase().GetMsgID()
}

func (pst *PinSnapshotDataTask) SetID(uid UniqueID) {
	pst.req.GetBase().MsgID = uid
}

func (pst *PinSnapshotDataTask) Name() string {
	return PinSnapshotDataTaskName
}

func (pst *PinSnapshotDataTask) Type() commonpb.MsgType {
	return pst.req.GetBase().GetMsgType()
}

func (pst *PinSnapshotDataTask) BeginTs() Timestamp {
	return pst.req.GetBase().GetTimestamp()
}

func (pst *PinSnapshotDataTask) EndTs() Timestamp {
	return pst.req.GetBase().GetTimestamp()
}

func (pst *PinSnapshotDataTask) SetTs(ts Timestamp) {
	pst.req.Base.Timestamp = ts
}

func (pst *PinSnapshotDataTask) OnEnqueue() error {
	if pst.req.Base == nil {
		pst.req.Base = commonpbutil.NewMsgBase()
	}
	pst.req.Base.MsgType = commonpb.MsgType_PinSnapshotData
	pst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

const maxPinTTLSeconds = 30 * 24 * 3600 // 30 days

func (pst *PinSnapshotDataTask) PreExecute(ctx context.Context) error {
	if err := ValidateSnapshotName(pst.req.GetName()); err != nil {
		return err
	}

	if pst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterMissingMsg("collection_name is required for pin snapshot data")
	}

	if pst.req.GetTtlSeconds() < 0 {
		return merr.WrapErrParameterInvalidMsg("ttl_seconds must be non-negative")
	}
	if pst.req.GetTtlSeconds() > maxPinTTLSeconds {
		return merr.WrapErrParameterInvalidMsg("ttl_seconds exceeds maximum of %d (30 days)", maxPinTTLSeconds)
	}

	collectionID, err := pst.GetMetaCache().GetCollectionID(ctx, pst.req.GetDbName(), pst.req.GetCollectionName())
	if err != nil {
		return err
	}
	pst.collectionID = collectionID

	return nil
}

func (pst *PinSnapshotDataTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy pin snapshot data",
		mlog.String("snapshotName", pst.req.GetName()),
		mlog.FieldCollectionName(pst.req.GetCollectionName()),
		mlog.FieldCollectionID(pst.collectionID),
	)

	resp, err := pst.mixCoord.PinSnapshotData(ctx, &datapb.PinSnapshotDataRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_PinSnapshotData),
		),
		Name:         pst.req.GetName(),
		CollectionId: pst.collectionID,
		TtlSeconds:   pst.req.GetTtlSeconds(),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		pst.result = &milvuspb.PinSnapshotDataResponse{
			Status: merr.Status(err),
		}
		return err
	}
	pst.result = &milvuspb.PinSnapshotDataResponse{
		Status: merr.Success(),
		PinId:  resp.GetPinId(),
	}
	return nil
}

func (pst *PinSnapshotDataTask) PostExecute(ctx context.Context) error {
	return nil
}

// UnpinSnapshotDataTask unpins previously pinned snapshot data, allowing GC
// to clean up segments if no other pins reference them.
type UnpinSnapshotDataTask struct {
	baseTask
	Condition
	req      *milvuspb.UnpinSnapshotDataRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (ust *UnpinSnapshotDataTask) TraceCtx() context.Context {
	return ust.ctx
}

func (ust *UnpinSnapshotDataTask) ID() UniqueID {
	return ust.req.GetBase().GetMsgID()
}

func (ust *UnpinSnapshotDataTask) SetID(uid UniqueID) {
	ust.req.GetBase().MsgID = uid
}

func (ust *UnpinSnapshotDataTask) Name() string {
	return UnpinSnapshotDataTaskName
}

func (ust *UnpinSnapshotDataTask) Type() commonpb.MsgType {
	return ust.req.GetBase().GetMsgType()
}

func (ust *UnpinSnapshotDataTask) BeginTs() Timestamp {
	return ust.req.GetBase().GetTimestamp()
}

func (ust *UnpinSnapshotDataTask) EndTs() Timestamp {
	return ust.req.GetBase().GetTimestamp()
}

func (ust *UnpinSnapshotDataTask) SetTs(ts Timestamp) {
	ust.req.Base.Timestamp = ts
}

func (ust *UnpinSnapshotDataTask) OnEnqueue() error {
	if ust.req.Base == nil {
		ust.req.Base = commonpbutil.NewMsgBase()
	}
	ust.req.Base.MsgType = commonpb.MsgType_UnpinSnapshotData
	ust.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ust *UnpinSnapshotDataTask) PreExecute(ctx context.Context) error {
	if ust.req.GetPinId() == 0 {
		return merr.WrapErrParameterMissingMsg("pin_id is required for unpin snapshot data")
	}
	return nil
}

func (ust *UnpinSnapshotDataTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy unpin snapshot data",
		mlog.Int64("pinID", ust.req.GetPinId()),
	)

	var err error
	ust.result, err = ust.mixCoord.UnpinSnapshotData(ctx, &datapb.UnpinSnapshotDataRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_UnpinSnapshotData),
		),
		PinId: ust.req.GetPinId(),
	})
	if err = merr.CheckRPCCall(ust.result, err); err != nil {
		return err
	}
	return nil
}

func (ust *UnpinSnapshotDataTask) PostExecute(ctx context.Context) error {
	return nil
}
