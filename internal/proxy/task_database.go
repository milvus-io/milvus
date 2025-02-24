package proxy

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type createDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.CreateDatabaseRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status

	replicateMsgStream msgstream.MsgStream
}

func (cdt *createDatabaseTask) TraceCtx() context.Context {
	return cdt.ctx
}

func (cdt *createDatabaseTask) ID() UniqueID {
	return cdt.Base.MsgID
}

func (cdt *createDatabaseTask) SetID(uid UniqueID) {
	cdt.Base.MsgID = uid
}

func (cdt *createDatabaseTask) Name() string {
	return CreateDatabaseTaskName
}

func (cdt *createDatabaseTask) Type() commonpb.MsgType {
	return cdt.Base.MsgType
}

func (cdt *createDatabaseTask) BeginTs() Timestamp {
	return cdt.Base.Timestamp
}

func (cdt *createDatabaseTask) EndTs() Timestamp {
	return cdt.Base.Timestamp
}

func (cdt *createDatabaseTask) SetTs(ts Timestamp) {
	cdt.Base.Timestamp = ts
}

func (cdt *createDatabaseTask) OnEnqueue() error {
	if cdt.Base == nil {
		cdt.Base = commonpbutil.NewMsgBase()
	}
	cdt.Base.MsgType = commonpb.MsgType_CreateDatabase
	cdt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (cdt *createDatabaseTask) PreExecute(ctx context.Context) error {
	return ValidateDatabaseName(cdt.GetDbName())
}

func (cdt *createDatabaseTask) Execute(ctx context.Context) error {
	var err error
	cdt.result, err = cdt.rootCoord.CreateDatabase(ctx, cdt.CreateDatabaseRequest)
	err = merr.CheckRPCCall(cdt.result, err)
	if err == nil {
		SendReplicateMessagePack(ctx, cdt.replicateMsgStream, cdt.CreateDatabaseRequest)
	}
	return err
}

func (cdt *createDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.DropDatabaseRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status

	replicateMsgStream msgstream.MsgStream
}

func (ddt *dropDatabaseTask) TraceCtx() context.Context {
	return ddt.ctx
}

func (ddt *dropDatabaseTask) ID() UniqueID {
	return ddt.Base.MsgID
}

func (ddt *dropDatabaseTask) SetID(uid UniqueID) {
	ddt.Base.MsgID = uid
}

func (ddt *dropDatabaseTask) Name() string {
	return DropCollectionTaskName
}

func (ddt *dropDatabaseTask) Type() commonpb.MsgType {
	return ddt.Base.MsgType
}

func (ddt *dropDatabaseTask) BeginTs() Timestamp {
	return ddt.Base.Timestamp
}

func (ddt *dropDatabaseTask) EndTs() Timestamp {
	return ddt.Base.Timestamp
}

func (ddt *dropDatabaseTask) SetTs(ts Timestamp) {
	ddt.Base.Timestamp = ts
}

func (ddt *dropDatabaseTask) OnEnqueue() error {
	if ddt.Base == nil {
		ddt.Base = commonpbutil.NewMsgBase()
	}
	ddt.Base.MsgType = commonpb.MsgType_DropDatabase
	ddt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ddt *dropDatabaseTask) PreExecute(ctx context.Context) error {
	return ValidateDatabaseName(ddt.GetDbName())
}

func (ddt *dropDatabaseTask) Execute(ctx context.Context) error {
	var err error
	ddt.result, err = ddt.rootCoord.DropDatabase(ctx, ddt.DropDatabaseRequest)

	err = merr.CheckRPCCall(ddt.result, err)
	if err == nil {
		globalMetaCache.RemoveDatabase(ctx, ddt.DbName)
		SendReplicateMessagePack(ctx, ddt.replicateMsgStream, ddt.DropDatabaseRequest)
	}
	return err
}

func (ddt *dropDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type listDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.ListDatabasesRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.ListDatabasesResponse
}

func (ldt *listDatabaseTask) TraceCtx() context.Context {
	return ldt.ctx
}

func (ldt *listDatabaseTask) ID() UniqueID {
	return ldt.Base.MsgID
}

func (ldt *listDatabaseTask) SetID(uid UniqueID) {
	ldt.Base.MsgID = uid
}

func (ldt *listDatabaseTask) Name() string {
	return ListDatabaseTaskName
}

func (ldt *listDatabaseTask) Type() commonpb.MsgType {
	return ldt.Base.MsgType
}

func (ldt *listDatabaseTask) BeginTs() Timestamp {
	return ldt.Base.Timestamp
}

func (ldt *listDatabaseTask) EndTs() Timestamp {
	return ldt.Base.Timestamp
}

func (ldt *listDatabaseTask) SetTs(ts Timestamp) {
	ldt.Base.Timestamp = ts
}

func (ldt *listDatabaseTask) OnEnqueue() error {
	ldt.Base = commonpbutil.NewMsgBase()
	ldt.Base.MsgType = commonpb.MsgType_ListDatabases
	ldt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ldt *listDatabaseTask) PreExecute(ctx context.Context) error {
	return nil
}

func (ldt *listDatabaseTask) Execute(ctx context.Context) error {
	var err error
	ctx = AppendUserInfoForRPC(ctx)
	ldt.result, err = ldt.rootCoord.ListDatabases(ctx, ldt.ListDatabasesRequest)
	return merr.CheckRPCCall(ldt.result, err)
}

func (ldt *listDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.AlterDatabaseRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *commonpb.Status

	replicateMsgStream msgstream.MsgStream
}

func (t *alterDatabaseTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *alterDatabaseTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *alterDatabaseTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *alterDatabaseTask) Name() string {
	return AlterDatabaseTaskName
}

func (t *alterDatabaseTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *alterDatabaseTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterDatabaseTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *alterDatabaseTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *alterDatabaseTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AlterDatabase
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *alterDatabaseTask) PreExecute(ctx context.Context) error {
	_, ok := common.GetReplicateID(t.Properties)
	if ok {
		return merr.WrapErrParameterInvalidMsg("can't set the replicate id property in alter database request")
	}
	endTS, ok := common.GetReplicateEndTS(t.Properties)
	if !ok { // not exist replicate end ts property
		return nil
	}
	cacheInfo, err := globalMetaCache.GetDatabaseInfo(ctx, t.DbName)
	if err != nil {
		return err
	}
	oldReplicateEnable, _ := common.IsReplicateEnabled(cacheInfo.properties)
	if !oldReplicateEnable { // old replicate enable is false
		return merr.WrapErrParameterInvalidMsg("can't set the replicate end ts property in alter database request when db replicate is disabled")
	}
	allocResp, err := t.rootCoord.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{
		Count:          1,
		BlockTimestamp: endTS,
	})
	if err = merr.CheckRPCCall(allocResp, err); err != nil {
		return merr.WrapErrServiceInternal("alloc timestamp failed", err.Error())
	}
	if allocResp.GetTimestamp() <= endTS {
		return merr.WrapErrServiceInternal("alter database: alloc timestamp failed, timestamp is not greater than endTS",
			fmt.Sprintf("timestamp = %d, endTS = %d", allocResp.GetTimestamp(), endTS))
	}

	return nil
}

func (t *alterDatabaseTask) Execute(ctx context.Context) error {
	var err error

	req := &rootcoordpb.AlterDatabaseRequest{
		Base:       t.AlterDatabaseRequest.GetBase(),
		DbName:     t.AlterDatabaseRequest.GetDbName(),
		DbId:       t.AlterDatabaseRequest.GetDbId(),
		Properties: t.AlterDatabaseRequest.GetProperties(),
		DeleteKeys: t.AlterDatabaseRequest.GetDeleteKeys(),
	}

	ret, err := t.rootCoord.AlterDatabase(ctx, req)
	err = merr.CheckRPCCall(ret, err)
	if err != nil {
		return err
	}

	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.AlterDatabaseRequest)
	t.result = ret
	return nil
}

func (t *alterDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.DescribeDatabaseRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.DescribeDatabaseResponse
}

func (t *describeDatabaseTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *describeDatabaseTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *describeDatabaseTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *describeDatabaseTask) Name() string {
	return AlterDatabaseTaskName
}

func (t *describeDatabaseTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *describeDatabaseTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *describeDatabaseTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *describeDatabaseTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *describeDatabaseTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DescribeDatabase
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *describeDatabaseTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *describeDatabaseTask) Execute(ctx context.Context) error {
	req := &rootcoordpb.DescribeDatabaseRequest{
		Base:   t.DescribeDatabaseRequest.GetBase(),
		DbName: t.DescribeDatabaseRequest.GetDbName(),
	}
	ret, err := t.rootCoord.DescribeDatabase(ctx, req)
	if err != nil {
		log.Ctx(ctx).Warn("DescribeDatabase failed", zap.Error(err))
		return err
	}

	if err := merr.CheckRPCCall(ret, err); err != nil {
		log.Ctx(ctx).Warn("DescribeDatabase failed", zap.Error(err))
		return err
	}

	t.result = &milvuspb.DescribeDatabaseResponse{
		Status:           ret.GetStatus(),
		DbName:           ret.GetDbName(),
		DbID:             ret.GetDbID(),
		CreatedTimestamp: ret.GetCreatedTimestamp(),
		Properties:       ret.GetProperties(),
	}
	return nil
}

func (t *describeDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}
