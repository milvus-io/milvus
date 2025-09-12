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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	CreateSnapshotTaskName   = "CreateSnapshotTask"
	DropSnapshotTaskName     = "DropSnapshotTask"
	DescribeSnapshotTaskName = "DescribeSnapshotTask"
	ListSnapshotsTaskName    = "ListSnapshotsTask"
	RestoreSnapshotTaskName  = "RestoreSnapshotTask"
)

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
		Name:         cst.req.GetName(),
		Description:  cst.req.GetDescription(),
		CollectionId: cst.collectionID,
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
	// No additional validation needed for drop snapshot
	return nil
}

func (dst *dropSnapshotTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy drop snapshot",
		zap.String("snapshotName", dst.req.GetName()),
	)

	var err error
	dst.result, err = dst.mixCoord.DropSnapshot(ctx, &datapb.DropSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropSnapshot),
		),
		Name: dst.req.GetName(),
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
	// No additional validation needed for describe snapshot
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
		Name: dst.req.GetName(),
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
	result   *commonpb.Status
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
	// No additional validation needed for restore snapshot pre-execute
	return nil
}

func (rst *restoreSnapshotTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("proxy restore snapshot",
		zap.String("snapshotName", rst.req.GetName()),
		zap.String("collectionName", rst.req.GetCollectionName()),
		zap.String("dbName", rst.req.GetDbName()),
	)

	// First, get snapshot detail info
	resp, err := rst.mixCoord.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeSnapshot),
		),
		Name: rst.req.GetName(),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		rst.result = merr.Status(err)
		return err
	}

	var collectionCreated bool
	var status *commonpb.Status
	defer func() {
		if !merr.Ok(status) && collectionCreated {
			result, err := rst.mixCoord.DropCollection(ctx, &milvuspb.DropCollectionRequest{
				CollectionName: rst.req.GetCollectionName(),
			})
			if merr.CheckRPCCall(result, err) != nil {
				log.Ctx(ctx).Warn("failed to drop collection after RestoreSnapshot fail",
					zap.Error(err))
			}
		}
	}()

	var createdPartitionNames []string
	defer func() {
		if !merr.Ok(status) && len(createdPartitionNames) > 0 {
			for _, partitionName := range createdPartitionNames {
				result, err := rst.mixCoord.DropPartition(ctx, &milvuspb.DropPartitionRequest{
					CollectionName: rst.req.GetCollectionName(),
					PartitionName:  partitionName,
				})
				if merr.CheckRPCCall(result, err) != nil {
					log.Ctx(ctx).Warn("failed to drop partition after RestoreSnapshot fail",
						zap.Error(err))
				}
			}
		}
	}()

	// Create collection and partition
	collInfo := resp.GetCollectionInfo()
	collInfo.Schema.Name = rst.req.GetCollectionName()
	fields := make([]*schemapb.FieldSchema, 0)
	for _, field := range collInfo.Schema.Fields {
		if field.GetName() != "$meta" && field.GetName() != "Timestamp" && field.GetName() != "RowID" {
			fields = append(fields, field)
		}
	}
	collInfo.Schema.Fields = fields
	schema, err := proto.Marshal(collInfo.GetSchema())
	if err != nil {
		log.Ctx(ctx).Warn("RestoreSnapshot fail to marshal schema",
			zap.String("collectionName", rst.req.GetCollectionName()),
			zap.Error(err))
		status = merr.Status(err)
		rst.result = status
		return err
	}

	result, err := rst.mixCoord.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_CreateCollection),
		),
		CollectionName:   rst.req.GetCollectionName(),
		DbName:           rst.req.GetDbName(),
		Schema:           schema,
		ShardsNum:        int32(collInfo.GetNumShards()),
		ConsistencyLevel: collInfo.GetConsistencyLevel(),
		Properties:       collInfo.Schema.GetProperties(),
	})
	if err = merr.CheckRPCCall(result, err); err != nil {
		log.Ctx(ctx).Warn("RestoreSnapshot fail to create collection",
			zap.Error(err))
		status = merr.Status(err)
		rst.result = status
		return err
	}
	collectionCreated = true

	partitionMap, err := globalMetaCache.GetPartitions(ctx, rst.req.GetDbName(), rst.req.GetCollectionName())
	if err != nil {
		log.Ctx(ctx).Warn("RestoreSnapshot fail to get partitions",
			zap.Error(err))
		status = merr.Status(err)
		rst.result = status
		return err
	}

	for partitionName := range partitionMap {
		if partitionName == "_default" {
			continue
		}
		result, err := rst.mixCoord.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			CollectionName: rst.req.GetCollectionName(),
			PartitionName:  partitionName,
		})
		if err = merr.CheckRPCCall(result, err); err != nil {
			log.Ctx(ctx).Warn("RestoreSnapshot fail to create partition",
				zap.String("partitionName", partitionName),
				zap.Error(err))
			status = merr.Status(err)
			rst.result = status
			return err
		}
		createdPartitionNames = append(createdPartitionNames, partitionName)
	}

	// Create indexes
	for _, indexInfo := range resp.GetIndexInfos() {
		result, err := rst.mixCoord.CreateIndex(ctx, &indexpb.CreateIndexRequest{
			CollectionID:    resp.GetSnapshotInfo().GetCollectionId(),
			FieldID:         indexInfo.GetFieldID(),
			IndexName:       indexInfo.GetIndexName(),
			TypeParams:      indexInfo.GetTypeParams(),
			IndexParams:     indexInfo.GetIndexParams(),
			Timestamp:       rst.BeginTs(),
			IsAutoIndex:     indexInfo.GetIsAutoIndex(),
			UserIndexParams: indexInfo.GetUserIndexParams(),
		})
		if err = merr.CheckRPCCall(result, err); err != nil {
			log.Ctx(ctx).Warn("RestoreSnapshot fail to create index",
				zap.String("indexName", indexInfo.GetIndexName()),
				zap.Int64("fieldID", indexInfo.GetFieldID()),
				zap.Error(err))
			status = merr.Status(err)
			rst.result = status
			return err
		}
	}

	// Restore data
	collectionID, err := globalMetaCache.GetCollectionID(ctx, "", rst.req.GetCollectionName())
	if err != nil {
		log.Ctx(ctx).Warn("RestoreSnapshot fail to get collection ID for restore data",
			zap.String("collectionName", rst.req.GetCollectionName()),
			zap.Error(err))
		status = merr.Status(err)
		rst.result = status
		return err
	}

	rst.result, err = rst.mixCoord.RestoreSnapshot(ctx, &datapb.RestoreSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RestoreSnapshot),
		),
		Name:         rst.req.GetName(),
		CollectionId: collectionID,
		RewriteData:  rst.req.GetRewriteData(),
	})
	if err = merr.CheckRPCCall(rst.result, err); err != nil {
		log.Ctx(ctx).Warn("RestoreSnapshot fail to restore data",
			zap.Error(err))
		status = rst.result
		return err
	}

	return nil
}

func (rst *restoreSnapshotTask) PostExecute(ctx context.Context) error {
	return nil
}
