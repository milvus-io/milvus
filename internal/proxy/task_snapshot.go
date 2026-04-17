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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
func resolveCollectionNames(ctx context.Context, collectionID int64) (string, string) {
	if collectionID == 0 {
		return "", ""
	}
	collInfo, err := globalMetaCache.GetCollectionInfo(ctx, "", "", collectionID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to resolve collection names from ID",
			zap.Int64("collectionID", collectionID), zap.Error(err))
		return "", ""
	}
	return collInfo.dbName, collInfo.schema.Name
}

type createSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.CreateSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID UniqueID
}

func (cst *createSnapshotTask) TraceCtx() context.Context {
	return cst.ctx
}

func (cst *createSnapshotTask) ID() UniqueID {
	return cst.req.GetBase().GetMsgID()
}

func (cst *createSnapshotTask) SetID(uid UniqueID) {
	cst.req.GetBase().MsgID = uid
}

func (cst *createSnapshotTask) Name() string {
	return CreateSnapshotTaskName
}

func (cst *createSnapshotTask) Type() commonpb.MsgType {
	return cst.req.GetBase().GetMsgType()
}

func (cst *createSnapshotTask) BeginTs() Timestamp {
	return cst.req.GetBase().GetTimestamp()
}

func (cst *createSnapshotTask) EndTs() Timestamp {
	return cst.req.GetBase().GetTimestamp()
}

func (cst *createSnapshotTask) SetTs(ts Timestamp) {
	cst.req.Base.Timestamp = ts
}

func (cst *createSnapshotTask) OnEnqueue() error {
	if cst.req.Base == nil {
		cst.req.Base = commonpbutil.NewMsgBase()
	}
	cst.req.Base.MsgType = commonpb.MsgType_CreateSnapshot
	cst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (cst *createSnapshotTask) PreExecute(ctx context.Context) error {
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

	collectionID, err := globalMetaCache.GetCollectionID(ctx, cst.req.GetDbName(), cst.req.GetCollectionName())
	if err != nil {
		return err
	}
	cst.collectionID = collectionID

	return nil
}

func (cst *createSnapshotTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy create snapshot",
		zap.String("snapshotName", cst.req.GetName()),
		zap.String("collectionName", cst.req.GetCollectionName()),
		zap.Int64("collectionID", cst.collectionID),
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

func (cst *createSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.DropSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID UniqueID
}

func (dst *dropSnapshotTask) TraceCtx() context.Context {
	return dst.ctx
}

func (dst *dropSnapshotTask) ID() UniqueID {
	return dst.req.GetBase().GetMsgID()
}

func (dst *dropSnapshotTask) SetID(uid UniqueID) {
	dst.req.GetBase().MsgID = uid
}

func (dst *dropSnapshotTask) Name() string {
	return DropSnapshotTaskName
}

func (dst *dropSnapshotTask) Type() commonpb.MsgType {
	return dst.req.GetBase().GetMsgType()
}

func (dst *dropSnapshotTask) BeginTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *dropSnapshotTask) EndTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *dropSnapshotTask) SetTs(ts Timestamp) {
	dst.req.Base.Timestamp = ts
}

func (dst *dropSnapshotTask) OnEnqueue() error {
	if dst.req.Base == nil {
		dst.req.Base = commonpbutil.NewMsgBase()
	}
	dst.req.Base.MsgType = commonpb.MsgType_DropSnapshot
	dst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dst *dropSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(dst.req.GetName()); err != nil {
		return err
	}

	if dst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("collection_name is required for drop snapshot")
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, dst.req.GetDbName(), dst.req.GetCollectionName())
	if err != nil {
		return err
	}
	dst.collectionID = collectionID

	return nil
}

func (dst *dropSnapshotTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy drop snapshot",
		zap.String("snapshotName", dst.req.GetName()),
		zap.String("collectionName", dst.req.GetCollectionName()),
		zap.Int64("collectionID", dst.collectionID),
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

func (dst *dropSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.DescribeSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeSnapshotResponse

	collectionID UniqueID
}

func (dst *describeSnapshotTask) TraceCtx() context.Context {
	return dst.ctx
}

func (dst *describeSnapshotTask) ID() UniqueID {
	return dst.req.GetBase().GetMsgID()
}

func (dst *describeSnapshotTask) SetID(uid UniqueID) {
	dst.req.GetBase().MsgID = uid
}

func (dst *describeSnapshotTask) Name() string {
	return DescribeSnapshotTaskName
}

func (dst *describeSnapshotTask) Type() commonpb.MsgType {
	return dst.req.GetBase().GetMsgType()
}

func (dst *describeSnapshotTask) BeginTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *describeSnapshotTask) EndTs() Timestamp {
	return dst.req.GetBase().GetTimestamp()
}

func (dst *describeSnapshotTask) SetTs(ts Timestamp) {
	dst.req.Base.Timestamp = ts
}

func (dst *describeSnapshotTask) OnEnqueue() error {
	if dst.req.Base == nil {
		dst.req.Base = commonpbutil.NewMsgBase()
	}
	dst.req.Base.MsgType = commonpb.MsgType_DescribeSnapshot
	dst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (dst *describeSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(dst.req.GetName()); err != nil {
		return err
	}

	if dst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("collection_name is required for describe snapshot")
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, dst.req.GetDbName(), dst.req.GetCollectionName())
	if err != nil {
		return err
	}
	dst.collectionID = collectionID

	return nil
}

func (dst *describeSnapshotTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy describe snapshot",
		zap.String("snapshotName", dst.req.GetName()),
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

	collectionName, err := globalMetaCache.GetCollectionName(ctx, "", snapshotInfo.GetCollectionId())
	if err != nil {
		log.Ctx(ctx).Warn("DescribeSnapshot fail to get collection name",
			zap.Error(err))
		return err
	}
	var partitionNames []string
	for _, partitionID := range snapshotInfo.GetPartitionIds() {
		partitionName, err := globalMetaCache.GetPartitionName(ctx, "", collectionName, partitionID)
		if err != nil {
			log.Ctx(ctx).Warn("DescribeSnapshot fail to get partition name",
				zap.Int64("partitionID", partitionID),
				zap.Error(err))
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

func (dst *describeSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type listSnapshotsTask struct {
	baseTask
	Condition
	req      *milvuspb.ListSnapshotsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListSnapshotsResponse

	collectionID UniqueID
	dbID         UniqueID
}

func (lst *listSnapshotsTask) TraceCtx() context.Context {
	return lst.ctx
}

func (lst *listSnapshotsTask) ID() UniqueID {
	return lst.req.GetBase().GetMsgID()
}

func (lst *listSnapshotsTask) SetID(uid UniqueID) {
	lst.req.GetBase().MsgID = uid
}

func (lst *listSnapshotsTask) Name() string {
	return ListSnapshotsTaskName
}

func (lst *listSnapshotsTask) Type() commonpb.MsgType {
	return lst.req.GetBase().GetMsgType()
}

func (lst *listSnapshotsTask) BeginTs() Timestamp {
	return lst.req.GetBase().GetTimestamp()
}

func (lst *listSnapshotsTask) EndTs() Timestamp {
	return lst.req.GetBase().GetTimestamp()
}

func (lst *listSnapshotsTask) SetTs(ts Timestamp) {
	lst.req.Base.Timestamp = ts
}

func (lst *listSnapshotsTask) OnEnqueue() error {
	if lst.req.Base == nil {
		lst.req.Base = commonpbutil.NewMsgBase()
	}
	lst.req.Base.MsgType = commonpb.MsgType_ListSnapshots
	lst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (lst *listSnapshotsTask) PreExecute(ctx context.Context) error {
	// Resolve database ID for db-level filtering
	if lst.req.GetDbName() != "" {
		dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, lst.req.GetDbName())
		if err != nil {
			return err
		}
		lst.dbID = dbInfo.dbID
	}

	if lst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("collection_name is required for ListSnapshots")
	}

	collectionID, err := globalMetaCache.GetCollectionID(ctx, lst.req.GetDbName(), lst.req.GetCollectionName())
	if err != nil {
		return err
	}
	lst.collectionID = collectionID

	return nil
}

func (lst *listSnapshotsTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy list snapshots",
		zap.String("collectionName", lst.req.GetCollectionName()),
		zap.Int64("collectionID", lst.collectionID),
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

func (lst *listSnapshotsTask) PostExecute(ctx context.Context) error {
	return nil
}

type restoreSnapshotTask struct {
	baseTask
	Condition
	req      *milvuspb.RestoreSnapshotRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.RestoreSnapshotResponse

	collectionID UniqueID // source collection ID for per-collection snapshot lookup
}

func (rst *restoreSnapshotTask) TraceCtx() context.Context {
	return rst.ctx
}

func (rst *restoreSnapshotTask) ID() UniqueID {
	return rst.req.GetBase().GetMsgID()
}

func (rst *restoreSnapshotTask) SetID(uid UniqueID) {
	rst.req.GetBase().MsgID = uid
}

func (rst *restoreSnapshotTask) Name() string {
	return RestoreSnapshotTaskName
}

func (rst *restoreSnapshotTask) Type() commonpb.MsgType {
	return rst.req.GetBase().GetMsgType()
}

func (rst *restoreSnapshotTask) BeginTs() Timestamp {
	return rst.req.GetBase().GetTimestamp()
}

func (rst *restoreSnapshotTask) EndTs() Timestamp {
	return rst.req.GetBase().GetTimestamp()
}

func (rst *restoreSnapshotTask) SetTs(ts Timestamp) {
	rst.req.Base.Timestamp = ts
}

func (rst *restoreSnapshotTask) OnEnqueue() error {
	if rst.req.Base == nil {
		rst.req.Base = commonpbutil.NewMsgBase()
	}
	rst.req.Base.MsgType = commonpb.MsgType_RestoreSnapshot
	rst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (rst *restoreSnapshotTask) PreExecute(ctx context.Context) error {
	// Validate snapshot_name using standard naming rules
	if err := ValidateSnapshotName(rst.req.GetName()); err != nil {
		return err
	}

	// Validate source collection name
	if rst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("collection_name is required for restore snapshot")
	}

	// Validate target collection name (required, cheap checks before RPC)
	if rst.req.GetTargetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("target_collection_name is required for restore snapshot")
	}
	if err := ValidateCollectionName(rst.req.GetTargetCollectionName()); err != nil {
		return err
	}

	// Resolve source collection ID for per-collection snapshot lookup (RPC call)
	collectionID, err := globalMetaCache.GetCollectionID(ctx, rst.req.GetDbName(), rst.req.GetCollectionName())
	if err != nil {
		return err
	}
	rst.collectionID = collectionID

	return nil
}

func (rst *restoreSnapshotTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy restore snapshot",
		zap.String("snapshotName", rst.req.GetName()),
		zap.String("sourceCollection", rst.req.GetCollectionName()),
		zap.String("sourceDb", rst.req.GetDbName()),
		zap.String("targetCollection", rst.req.GetTargetCollectionName()),
		zap.String("targetDb", rst.req.GetTargetDbName()),
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
		log.Ctx(ctx).Warn("RestoreSnapshot failed",
			zap.Error(err))
		rst.result = &milvuspb.RestoreSnapshotResponse{Status: merr.Status(err)}
		return err
	}
	rst.result = &milvuspb.RestoreSnapshotResponse{
		Status: merr.Success(),
		JobId:  resp.GetJobId(),
	}
	return nil
}

func (rst *restoreSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}

type getRestoreSnapshotStateTask struct {
	baseTask
	Condition
	req      *milvuspb.GetRestoreSnapshotStateRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.GetRestoreSnapshotStateResponse
}

func (grst *getRestoreSnapshotStateTask) TraceCtx() context.Context {
	return grst.ctx
}

func (grst *getRestoreSnapshotStateTask) ID() UniqueID {
	return grst.req.GetBase().GetMsgID()
}

func (grst *getRestoreSnapshotStateTask) SetID(uid UniqueID) {
	grst.req.GetBase().MsgID = uid
}

func (grst *getRestoreSnapshotStateTask) Name() string {
	return GetRestoreSnapshotStateTaskName
}

func (grst *getRestoreSnapshotStateTask) Type() commonpb.MsgType {
	return grst.req.GetBase().GetMsgType()
}

func (grst *getRestoreSnapshotStateTask) BeginTs() Timestamp {
	return grst.req.GetBase().GetTimestamp()
}

func (grst *getRestoreSnapshotStateTask) EndTs() Timestamp {
	return grst.req.GetBase().GetTimestamp()
}

func (grst *getRestoreSnapshotStateTask) SetTs(ts Timestamp) {
	grst.req.Base.Timestamp = ts
}

func (grst *getRestoreSnapshotStateTask) OnEnqueue() error {
	if grst.req.Base == nil {
		grst.req.Base = commonpbutil.NewMsgBase()
	}
	grst.req.Base.MsgType = commonpb.MsgType_GetRestoreSnapshotState
	grst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (grst *getRestoreSnapshotStateTask) PreExecute(ctx context.Context) error {
	// No additional validation needed for get restore snapshot state
	return nil
}

func (grst *getRestoreSnapshotStateTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy get restore snapshot state",
		zap.Int64("jobID", grst.req.GetJobId()),
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
		dbName, collectionName := resolveCollectionNames(ctx, info.GetCollectionId())
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

func (grst *getRestoreSnapshotStateTask) PostExecute(ctx context.Context) error {
	return nil
}

type listRestoreSnapshotJobsTask struct {
	baseTask
	Condition
	req      *milvuspb.ListRestoreSnapshotJobsRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListRestoreSnapshotJobsResponse

	collectionID UniqueID
	dbID         UniqueID
}

func (lrst *listRestoreSnapshotJobsTask) TraceCtx() context.Context {
	return lrst.ctx
}

func (lrst *listRestoreSnapshotJobsTask) ID() UniqueID {
	return lrst.req.GetBase().GetMsgID()
}

func (lrst *listRestoreSnapshotJobsTask) SetID(uid UniqueID) {
	lrst.req.GetBase().MsgID = uid
}

func (lrst *listRestoreSnapshotJobsTask) Name() string {
	return ListRestoreSnapshotJobsTaskName
}

func (lrst *listRestoreSnapshotJobsTask) Type() commonpb.MsgType {
	return lrst.req.GetBase().GetMsgType()
}

func (lrst *listRestoreSnapshotJobsTask) BeginTs() Timestamp {
	return lrst.req.GetBase().GetTimestamp()
}

func (lrst *listRestoreSnapshotJobsTask) EndTs() Timestamp {
	return lrst.req.GetBase().GetTimestamp()
}

func (lrst *listRestoreSnapshotJobsTask) SetTs(ts Timestamp) {
	lrst.req.Base.Timestamp = ts
}

func (lrst *listRestoreSnapshotJobsTask) OnEnqueue() error {
	if lrst.req.Base == nil {
		lrst.req.Base = commonpbutil.NewMsgBase()
	}
	lrst.req.Base.MsgType = commonpb.MsgType_ListRestoreSnapshotJobs
	lrst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (lrst *listRestoreSnapshotJobsTask) PreExecute(ctx context.Context) error {
	// Resolve database ID for db-level filtering
	if lrst.req.GetDbName() != "" {
		dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, lrst.req.GetDbName())
		if err != nil {
			return err
		}
		lrst.dbID = dbInfo.dbID
	}

	if lrst.req.GetCollectionName() != "" {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, lrst.req.GetDbName(), lrst.req.GetCollectionName())
		if err != nil {
			return err
		}
		lrst.collectionID = collectionID
	}
	return nil
}

func (lrst *listRestoreSnapshotJobsTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy list restore snapshot jobs",
		zap.String("collectionName", lrst.req.GetCollectionName()),
		zap.Int64("collectionID", lrst.collectionID),
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
		dbName, collectionName := resolveCollectionNames(ctx, job.GetCollectionId())
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

func (lrst *listRestoreSnapshotJobsTask) PostExecute(ctx context.Context) error {
	return nil
}

// pinSnapshotDataTask pins snapshot data to prevent GC from cleaning up segments
// referenced by a snapshot. Accepts milvuspb request from user, resolves
// collection_name to collection_id, and forwards to DataCoord.
type pinSnapshotDataTask struct {
	baseTask
	Condition
	req      *milvuspb.PinSnapshotDataRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.PinSnapshotDataResponse

	collectionID UniqueID
}

func (pst *pinSnapshotDataTask) TraceCtx() context.Context {
	return pst.ctx
}

func (pst *pinSnapshotDataTask) ID() UniqueID {
	return pst.req.GetBase().GetMsgID()
}

func (pst *pinSnapshotDataTask) SetID(uid UniqueID) {
	pst.req.GetBase().MsgID = uid
}

func (pst *pinSnapshotDataTask) Name() string {
	return PinSnapshotDataTaskName
}

func (pst *pinSnapshotDataTask) Type() commonpb.MsgType {
	return pst.req.GetBase().GetMsgType()
}

func (pst *pinSnapshotDataTask) BeginTs() Timestamp {
	return pst.req.GetBase().GetTimestamp()
}

func (pst *pinSnapshotDataTask) EndTs() Timestamp {
	return pst.req.GetBase().GetTimestamp()
}

func (pst *pinSnapshotDataTask) SetTs(ts Timestamp) {
	pst.req.Base.Timestamp = ts
}

func (pst *pinSnapshotDataTask) OnEnqueue() error {
	if pst.req.Base == nil {
		pst.req.Base = commonpbutil.NewMsgBase()
	}
	pst.req.Base.MsgType = commonpb.MsgType_PinSnapshotData
	pst.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

const maxPinTTLSeconds = 30 * 24 * 3600 // 30 days

func (pst *pinSnapshotDataTask) PreExecute(ctx context.Context) error {
	if err := ValidateSnapshotName(pst.req.GetName()); err != nil {
		return err
	}

	if pst.req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("collection_name is required for pin snapshot data")
	}

	if pst.req.GetTtlSeconds() < 0 {
		return merr.WrapErrParameterInvalidMsg("ttl_seconds must be non-negative")
	}
	if pst.req.GetTtlSeconds() > maxPinTTLSeconds {
		return merr.WrapErrParameterInvalidMsg("ttl_seconds exceeds maximum of %d (30 days)", maxPinTTLSeconds)
	}

	collectionID, err := globalMetaCache.GetCollectionID(ctx, pst.req.GetDbName(), pst.req.GetCollectionName())
	if err != nil {
		return err
	}
	pst.collectionID = collectionID

	return nil
}

func (pst *pinSnapshotDataTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy pin snapshot data",
		zap.String("snapshotName", pst.req.GetName()),
		zap.String("collectionName", pst.req.GetCollectionName()),
		zap.Int64("collectionID", pst.collectionID),
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

func (pst *pinSnapshotDataTask) PostExecute(ctx context.Context) error {
	return nil
}

// unpinSnapshotDataTask unpins previously pinned snapshot data, allowing GC
// to clean up segments if no other pins reference them.
type unpinSnapshotDataTask struct {
	baseTask
	Condition
	req      *milvuspb.UnpinSnapshotDataRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (ust *unpinSnapshotDataTask) TraceCtx() context.Context {
	return ust.ctx
}

func (ust *unpinSnapshotDataTask) ID() UniqueID {
	return ust.req.GetBase().GetMsgID()
}

func (ust *unpinSnapshotDataTask) SetID(uid UniqueID) {
	ust.req.GetBase().MsgID = uid
}

func (ust *unpinSnapshotDataTask) Name() string {
	return UnpinSnapshotDataTaskName
}

func (ust *unpinSnapshotDataTask) Type() commonpb.MsgType {
	return ust.req.GetBase().GetMsgType()
}

func (ust *unpinSnapshotDataTask) BeginTs() Timestamp {
	return ust.req.GetBase().GetTimestamp()
}

func (ust *unpinSnapshotDataTask) EndTs() Timestamp {
	return ust.req.GetBase().GetTimestamp()
}

func (ust *unpinSnapshotDataTask) SetTs(ts Timestamp) {
	ust.req.Base.Timestamp = ts
}

func (ust *unpinSnapshotDataTask) OnEnqueue() error {
	if ust.req.Base == nil {
		ust.req.Base = commonpbutil.NewMsgBase()
	}
	ust.req.Base.MsgType = commonpb.MsgType_UnpinSnapshotData
	ust.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ust *unpinSnapshotDataTask) PreExecute(ctx context.Context) error {
	if ust.req.GetPinId() == 0 {
		return merr.WrapErrParameterInvalidMsg("pin_id is required for unpin snapshot data")
	}
	return nil
}

func (ust *unpinSnapshotDataTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy unpin snapshot data",
		zap.Int64("pinID", ust.req.GetPinId()),
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

func (ust *unpinSnapshotDataTask) PostExecute(ctx context.Context) error {
	return nil
}
