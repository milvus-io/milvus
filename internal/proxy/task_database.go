package proxy

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

type createDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.CreateDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	err := ValidateDatabaseName(cdt.GetDbName())
	if err != nil {
		return err
	}
	tz, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, cdt.GetProperties())
	if exist && !timestamptz.IsTimezoneValid(tz) {
		return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", tz)
	}

	// A database that already exists must let rootcoord's own CreateDatabase
	// answer (idempotent success or a genuine conflict), not a quota
	// rejection: an admission check run ahead of this would turn a harmless
	// retry against an instance already at its cap into ResourceExhausted.
	//
	// Unlike createCollectionTask.PreExecute's checkCreateCollectionAdmission
	// (admission first, existence lookup only on rejection), this check stays
	// existence-first: CheckDatabase/HasDatabase is a local map lookup with no
	// RPC fallback on a miss, so there is no coordinator round trip to move
	// off the common path here.
	//
	// The whole block is gated on the admission capability being installed.
	// admissionChecker() is resolved once, through c, and that same value
	// drives both the gate and the check itself -- there is no second
	// consultation of extension.Caps() hiding in a wrapper function, because
	// this path calls the checker directly rather than through one. With no
	// provider installed PreExecute performs neither the existence lookup nor
	// the admission call, reaching exactly the statements it reached before
	// this task existed: installing nothing must change nothing, not even a
	// local map read.
	//
	// Admission first, existence only on rejection - the same order and for
	// the same reason as createCollectionTask: the existence probe that makes
	// an idempotent retry pass costs a coordinator round trip on a cache miss,
	// and a local HasDatabase peek is not a substitute (it only knows
	// databases whose collections this proxy has already cached, so an empty
	// or un-cached database would still be refused). GetDatabaseInfo carries
	// the RPC fallback, so a genuine retry of an existing database gets
	// rootcoord's own already-exists answer instead of ResourceExhausted.
	if c := admissionChecker(); c != nil {
		if err := c.CheckCreateDatabase(ctx, mixCoordAdmissionClient{cdt.mixCoord}); err != nil {
			if _, lookupErr := globalMetaCache.GetDatabaseInfo(ctx, cdt.GetDbName()); lookupErr != nil {
				return err
			}
		}
	}

	return nil
}

func (cdt *createDatabaseTask) Execute(ctx context.Context) error {
	var err error
	cdt.result, err = cdt.mixCoord.CreateDatabase(ctx, cdt.CreateDatabaseRequest)
	err = merr.CheckRPCCall(cdt.result, err)
	return err
}

func (cdt *createDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type dropDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.DropDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	ddt.result, err = ddt.mixCoord.DropDatabase(ctx, ddt.DropDatabaseRequest)

	err = merr.CheckRPCCall(ddt.result, err)
	if err == nil {
		// Local best-effort cleanup on the issuing proxy; the authoritative
		// eviction is the DropDatabase broadcast handled in
		// InvalidateCollectionMetaCache.
		globalMetaCache.RemoveDatabase(ctx, ddt.DbName)
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
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.ListDatabasesResponse
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
	ldt.result, err = ldt.mixCoord.ListDatabases(ctx, ldt.ListDatabasesRequest)
	return merr.CheckRPCCall(ldt.result, err)
}

func (ldt *listDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type alterDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.AlterDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status
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
	if len(t.GetProperties()) > 0 {
		// Check the validation of timezone
		userDefinedTimezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, t.Properties)
		if exist && !timestamptz.IsTimezoneValid(userDefinedTimezone) {
			return merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", userDefinedTimezone)
		}
	}

	return nil
}

func (t *alterDatabaseTask) Execute(ctx context.Context) error {
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

func (t *alterDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}

type describeDatabaseTask struct {
	baseTask
	Condition
	*milvuspb.DescribeDatabaseRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.DescribeDatabaseResponse
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

func (t *describeDatabaseTask) PostExecute(ctx context.Context) error {
	return nil
}
