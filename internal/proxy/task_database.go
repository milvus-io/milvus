package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type createDatabaseTask struct {
	Condition
	*milvuspb.CreateDatabaseRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
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
	cdt.Base = commonpbutil.NewMsgBase()
	cdt.Base.MsgType = commonpb.MsgType_CreateDatabase
	cdt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (cdt *createDatabaseTask) PreExecute(ctx context.Context) error {
	return ValidateDatabaseName(cdt.GetDbName())
}

func (cdt *createDatabaseTask) Execute(ctx context.Context) error {
	var err error
	cdt.result = &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	cdt.result, err = cdt.rootCoord.CreateDatabase(ctx, cdt.CreateDatabaseRequest)
	return err
}

func (cdt *createDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropDatabaseTask struct {
	Condition
	*milvuspb.DropDatabaseRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
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
	ddt.Base = commonpbutil.NewMsgBase()
	ddt.Base.MsgType = commonpb.MsgType_DropDatabase
	ddt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ddt *dropDatabaseTask) PreExecute(ctx context.Context) error {
	return ValidateDatabaseName(ddt.GetDbName())
}

func (ddt *dropDatabaseTask) Execute(ctx context.Context) error {
	var err error
	ddt.result = &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	ddt.result, err = ddt.rootCoord.DropDatabase(ctx, ddt.DropDatabaseRequest)

	if ddt.result != nil && ddt.result.ErrorCode == commonpb.ErrorCode_Success {
		globalMetaCache.RemoveDatabase(ctx, ddt.DbName)
	}
	return err
}

func (ddt *dropDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type listDatabaseTask struct {
	Condition
	*milvuspb.ListDatabasesRequest
	ctx       context.Context
	rootCoord types.RootCoord
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
	ldt.result = &milvuspb.ListDatabasesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	ldt.result, err = ldt.rootCoord.ListDatabases(ctx, ldt.ListDatabasesRequest)
	return err
}

func (ldt *listDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}
