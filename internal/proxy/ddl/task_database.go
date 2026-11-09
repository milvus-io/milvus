package ddl

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
)

type CreateDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.CreateDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (cdt *CreateDatabaseTask) TraceCtx() context.Context {
	return cdt.ctx
}

func (cdt *CreateDatabaseTask) ID() UniqueID {
	return cdt.Base.MsgID
}

func (cdt *CreateDatabaseTask) SetID(uid UniqueID) {
	cdt.Base.MsgID = uid
}

func (cdt *CreateDatabaseTask) Name() string {
	return CreateDatabaseTaskName
}

func (cdt *CreateDatabaseTask) Type() commonpb.MsgType {
	return cdt.Base.MsgType
}

func (cdt *CreateDatabaseTask) BeginTs() Timestamp {
	return cdt.Base.Timestamp
}

func (cdt *CreateDatabaseTask) EndTs() Timestamp {
	return cdt.Base.Timestamp
}

func (cdt *CreateDatabaseTask) SetTs(ts Timestamp) {
	cdt.Base.Timestamp = ts
}

func (cdt *CreateDatabaseTask) OnEnqueue() error {
	if cdt.Base == nil {
		cdt.Base = commonpbutil.NewMsgBase()
	}
	cdt.Base.MsgType = commonpb.MsgType_CreateDatabase
	cdt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (cdt *CreateDatabaseTask) PreExecute(ctx context.Context) error {
	err := ValidateDatabaseName(cdt.GetDbName())
	if err != nil {
		return err
	}
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, cdt.GetProperties())
	if exist && !timestamptz.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}
	return nil
}

func (cdt *CreateDatabaseTask) Execute(ctx context.Context) error {
	var err error
	cdt.result, err = cdt.mixCoord.CreateDatabase(ctx, cdt.CreateDatabaseRequest)
	err = merr.CheckRPCCall(cdt.result, err)
	return err
}

func (cdt *CreateDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.DropDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (ddt *DropDatabaseTask) TraceCtx() context.Context {
	return ddt.ctx
}

func (ddt *DropDatabaseTask) ID() UniqueID {
	return ddt.Base.MsgID
}

func (ddt *DropDatabaseTask) SetID(uid UniqueID) {
	ddt.Base.MsgID = uid
}

func (ddt *DropDatabaseTask) Name() string {
	return DropCollectionTaskName
}

func (ddt *DropDatabaseTask) Type() commonpb.MsgType {
	return ddt.Base.MsgType
}

func (ddt *DropDatabaseTask) BeginTs() Timestamp {
	return ddt.Base.Timestamp
}

func (ddt *DropDatabaseTask) EndTs() Timestamp {
	return ddt.Base.Timestamp
}

func (ddt *DropDatabaseTask) SetTs(ts Timestamp) {
	ddt.Base.Timestamp = ts
}

func (ddt *DropDatabaseTask) OnEnqueue() error {
	if ddt.Base == nil {
		ddt.Base = commonpbutil.NewMsgBase()
	}
	ddt.Base.MsgType = commonpb.MsgType_DropDatabase
	ddt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ddt *DropDatabaseTask) PreExecute(ctx context.Context) error {
	return ValidateDatabaseName(ddt.GetDbName())
}

func (ddt *DropDatabaseTask) Execute(ctx context.Context) error {
	var err error
	ddt.result, err = ddt.mixCoord.DropDatabase(ctx, ddt.DropDatabaseRequest)

	err = merr.CheckRPCCall(ddt.result, err)
	if err == nil {
		// Local best-effort cleanup on the issuing proxy; the authoritative
		// eviction is the DropDatabase broadcast handled in
		// InvalidateCollectionMetaCache.
		ddt.GetMetaCache().RemoveDatabase(ctx, ddt.DbName)
	}
	return err
}

func (ddt *DropDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type ListDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.ListDatabasesRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListDatabasesResponse
}

func (ldt *ListDatabaseTask) TraceCtx() context.Context {
	return ldt.ctx
}

func (ldt *ListDatabaseTask) ID() UniqueID {
	return ldt.Base.MsgID
}

func (ldt *ListDatabaseTask) SetID(uid UniqueID) {
	ldt.Base.MsgID = uid
}

func (ldt *ListDatabaseTask) Name() string {
	return ListDatabaseTaskName
}

func (ldt *ListDatabaseTask) Type() commonpb.MsgType {
	return ldt.Base.MsgType
}

func (ldt *ListDatabaseTask) BeginTs() Timestamp {
	return ldt.Base.Timestamp
}

func (ldt *ListDatabaseTask) EndTs() Timestamp {
	return ldt.Base.Timestamp
}

func (ldt *ListDatabaseTask) SetTs(ts Timestamp) {
	ldt.Base.Timestamp = ts
}

func (ldt *ListDatabaseTask) OnEnqueue() error {
	ldt.Base = commonpbutil.NewMsgBase()
	ldt.Base.MsgType = commonpb.MsgType_ListDatabases
	ldt.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (ldt *ListDatabaseTask) PreExecute(ctx context.Context) error {
	return nil
}

func (ldt *ListDatabaseTask) Execute(ctx context.Context) error {
	var err error
	ctx = AppendUserInfoForRPC(ctx)
	ldt.result, err = ldt.mixCoord.ListDatabases(ctx, ldt.ListDatabasesRequest)
	return merr.CheckRPCCall(ldt.result, err)
}

func (ldt *ListDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type AlterDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.AlterDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
}

func (t *AlterDatabaseTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *AlterDatabaseTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *AlterDatabaseTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *AlterDatabaseTask) Name() string {
	return AlterDatabaseTaskName
}

func (t *AlterDatabaseTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *AlterDatabaseTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *AlterDatabaseTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *AlterDatabaseTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *AlterDatabaseTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AlterDatabase
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *AlterDatabaseTask) PreExecute(ctx context.Context) error {
	if len(t.GetProperties()) > 0 {
		// Check the validation of timezone
		userDefinedTimezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, t.Properties)
		if exist && !timestamptz.IsTimezoneValid(userDefinedTimezone) {
			return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", userDefinedTimezone)
		}
	}

	return nil
}

func (t *AlterDatabaseTask) Execute(ctx context.Context) error {
	var err error

	req := &rootcoordpb.AlterDatabaseRequest{
		Base:       t.GetBase(),
		DbName:     t.GetDbName(),
		DbId:       t.GetDbId(),
		Properties: t.GetProperties(),
		DeleteKeys: t.GetDeleteKeys(),
	}

	ret, err := t.mixCoord.AlterDatabase(ctx, req)
	err = merr.CheckRPCCall(ret, err)
	if err != nil {
		return err
	}
	t.result = ret
	return nil
}

func (t *AlterDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type DescribeDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.DescribeDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeDatabaseResponse
}

func (t *DescribeDatabaseTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *DescribeDatabaseTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *DescribeDatabaseTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *DescribeDatabaseTask) Name() string {
	return AlterDatabaseTaskName
}

func (t *DescribeDatabaseTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *DescribeDatabaseTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DescribeDatabaseTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DescribeDatabaseTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *DescribeDatabaseTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DescribeDatabase
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *DescribeDatabaseTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *DescribeDatabaseTask) Execute(ctx context.Context) error {
	req := &rootcoordpb.DescribeDatabaseRequest{
		Base:   t.GetBase(),
		DbName: t.GetDbName(),
	}

	ctx = AppendUserInfoForRPC(ctx)
	ret, err := t.mixCoord.DescribeDatabase(ctx, req)
	if err != nil {
		mlog.Warn(ctx, "DescribeDatabase failed", mlog.Err(err))
		return err
	}

	if err := merr.CheckRPCCall(ret, err); err != nil {
		mlog.Warn(ctx, "DescribeDatabase failed", mlog.Err(err))
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

func (t *DescribeDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}
