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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (node *Proxy) compactionTargetSubject(ctx context.Context) (rbacSubject, error) {
	var subject rbacSubject
	if authorizationDisabled() {
		subject.bypass = true
	} else {
		// The shared parser includes the credential in some parse errors.
		if _, _, err := contextutil.GetAuthInfoFromContext(ctx); err != nil {
			return subject, merr.ErrNeedAuthenticate
		}
		var err error
		subject, err = getCurrentUserRBACSubject(ctx)
		if err != nil {
			return subject, err
		}
	}
	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() || !Params.DataCoordCfg.EnableTargetBasedCompaction.GetAsBool() {
		return subject, merr.WrapErrOperationNotSupportedMsg("target-based compaction is disabled")
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return subject, err
	}
	if node.mixCoord == nil {
		return subject, merr.WrapErrServiceNotReadyMsg("compaction target coordinator is initializing")
	}
	return subject, ctx.Err()
}

func authorizeTargetResource(subject rbacSubject, resource *datapb.CompactionTargetResource) error {
	if resource == nil {
		return merr.WrapErrServiceUnavailableMsg("compaction target resource identity is unavailable")
	}
	if subject.bypass {
		return nil
	}
	db, name := resource.GetDbName(), resource.GetCollectionName()
	objectType := commonpb.ObjectType_Collection.String()
	privilege := commonpb.ObjectPrivilege_PrivilegeCompaction.String()
	if resource.GetCollectionId() == 0 || resource.GetDeleted() {
		// Match the existing cluster-admin capability used for WAL reads.
		db, name = util.AnyWord, util.AnyWord
		objectType = commonpb.ObjectType_Global.String()
		privilege = commonpb.ObjectPrivilege_PrivilegeManageOwnership.String()
	}
	for _, role := range subject.roleNames {
		allowed, err := isRolePermitted(role, db, objectType, name, privilege)
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
	}
	return merr.WrapErrPrivilegeNotPermitted("not allowed to access compaction target")
}

func (node *Proxy) describeTargetCollection(ctx context.Context, db, name string, id int64) (*datapb.CompactionTargetResource, bool, error) {
	resp, err := node.mixCoord.DescribeCollection(describeCollectionMetadataContext(ctx), &milvuspb.DescribeCollectionRequest{
		Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
		DbName: db, CollectionName: name, CollectionID: id,
	})
	if err := checkTargetRPC(resp.GetStatus(), err); err != nil {
		return nil, false, err
	}
	if resp.GetCollectionID() <= 0 || (id != 0 && resp.GetCollectionID() != id) ||
		resp.GetDbName() == "" || resp.GetSchema().GetName() == "" ||
		resp.GetUpdateTimestampStr() != strconv.FormatUint(resp.GetUpdateTimestamp(), 10) {
		return nil, false, merr.WrapErrServiceUnavailableMsg("compaction target collection identity is unavailable")
	}
	dbID := resp.GetDbId()
	if dbID == util.NonDBID {
		dbID = util.DefaultDBID
	}
	return &datapb.CompactionTargetResource{
		CollectionId: resp.GetCollectionID(), DbId: dbID, DbName: resp.GetDbName(),
		CollectionName: resp.GetSchema().GetName(), Revision: resp.GetUpdateTimestamp(),
	}, typeutil.IsExternalCollection(resp.GetSchema()), nil
}

func checkTargetRPC(response *commonpb.Status, err error) error {
	if err != nil {
		return err
	}
	if response == nil {
		return merr.WrapErrServiceUnavailableMsg("compaction target coordinator response has no status")
	}
	return merr.Error(response)
}

func (node *Proxy) CreateCompactionTarget(ctx context.Context, req *milvuspb.CreateCompactionTargetRequest) (*milvuspb.CreateCompactionTargetResponse, error) {
	fail := func(err error) (*milvuspb.CreateCompactionTargetResponse, error) {
		return &milvuspb.CreateCompactionTargetResponse{Status: merr.Status(err)}, nil
	}
	subject, err := node.compactionTargetSubject(ctx)
	if err != nil {
		return fail(err)
	}
	if req.GetCollectionName() == "" {
		return fail(merr.WrapErrParameterInvalidMsg("collection_name is required"))
	}
	if err := validateCollectionName(req.GetCollectionName()); err != nil {
		return fail(err)
	}
	if req.GetRewrite() == nil && req.GetOptimize().GetInputTargetSizeBytes() <= 0 {
		return fail(merr.WrapErrParameterInvalidMsg("a rewrite or a positive input_target_size_bytes is required"))
	}
	if len(req.ProtoReflect().GetUnknown()) != 0 {
		return fail(merr.WrapErrParameterInvalidMsg("unsupported compaction target request fields"))
	}
	for _, id := range req.GetRewrite().GetSegmentIds() {
		if id <= 0 {
			return fail(merr.WrapErrParameterInvalidMsg("segment IDs must be positive"))
		}
	}

	db := req.GetDbName()
	if db == "" {
		db = util.DefaultDBName
	}
	collection, external, err := node.describeTargetCollection(ctx, db, req.GetCollectionName(), 0)
	if err != nil {
		return fail(err)
	}
	if err := authorizeTargetResource(subject, collection); err != nil {
		return fail(err)
	}
	current, _, err := node.describeTargetCollection(ctx, "", "", collection.GetCollectionId())
	if err != nil {
		if errors.IsAny(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound) {
			return fail(merr.WrapErrServiceUnavailableMsg("collection changed during compaction target authorization"))
		}
		return fail(err)
	}
	if !proto.Equal(collection, current) {
		return fail(merr.WrapErrServiceUnavailableMsg("collection changed during compaction target authorization"))
	}
	if external {
		return fail(merr.WrapErrParameterInvalidMsg("compaction is not supported for external collection"))
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	forwarded := &datapb.CreateCompactionTargetRequest{Base: req.GetBase(), Collection: collection}
	if rewrite := req.GetRewrite(); rewrite != nil {
		forwarded.Target = &datapb.CreateCompactionTargetRequest_Rewrite{
			Rewrite: &datapb.CompactionTargetRewrite{RootSegmentIds: rewrite.GetSegmentIds()},
		}
	} else {
		forwarded.Target = &datapb.CreateCompactionTargetRequest_InputTargetSizeBytes{
			InputTargetSizeBytes: req.GetOptimize().GetInputTargetSizeBytes(),
		}
	}
	response, err := node.mixCoord.CreateCompactionTarget(ctx, forwarded)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return nil, err
		}
		if merr.IsMilvusError(err) {
			return fail(err)
		}
		return fail(merr.Wrap(merr.ErrCompactionTargetCreateOutcomeUnknown, "no reliable coordinator response"))
	}
	st := response.GetStatus()
	optimize := response.GetOptimize()
	if st != nil && !merr.Ok(st) && response.GetTargetId() == 0 && response.GetTotalRows() == 0 && optimize == nil {
		return &milvuspb.CreateCompactionTargetResponse{Status: st}, nil
	}
	if st == nil || !merr.Ok(st) || response.GetTargetId() <= 0 || response.GetTotalRows() < 0 ||
		(req.GetRewrite() != nil && optimize != nil) ||
		(req.GetOptimize() != nil && (optimize.GetInputTargetSizeBytes() != req.GetOptimize().GetInputTargetSizeBytes() || optimize.GetActualTargetSizeBytes() <= 0)) {
		return fail(merr.Wrap(merr.ErrCompactionTargetCreateOutcomeUnknown, "inconsistent coordinator response"))
	}
	result := &milvuspb.CreateCompactionTargetResponse{
		Status: st, TargetId: response.GetTargetId(), TotalRows: response.GetTotalRows(),
	}
	if optimize != nil {
		result.Optimize = &milvuspb.OptimizeTargetCreateResult{
			InputTargetSizeBytes:  optimize.GetInputTargetSizeBytes(),
			ActualTargetSizeBytes: optimize.GetActualTargetSizeBytes(),
		}
	}
	return result, nil
}

func (node *Proxy) targetBinding(ctx context.Context, base *commonpb.MsgBase, id int64) (*datapb.CompactionTargetResource, error) {
	response, err := node.mixCoord.GetCompactionTargetBinding(ctx, &datapb.GetCompactionTargetBindingRequest{Base: base, TargetId: id})
	if err := checkTargetRPC(response.GetStatus(), err); err != nil {
		return nil, err
	}
	if response.GetResource() == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("compaction target binding is unavailable")
	}
	return response.GetResource(), nil
}

func (node *Proxy) recheckTargetResource(ctx context.Context, resource *datapb.CompactionTargetResource) error {
	if resource.GetCollectionId() == 0 || resource.GetDeleted() {
		// DataCoord confirmed a stable-ID deletion from authoritative lifecycle
		// metadata. This authorization uses a cluster-scoped grant.
		return nil
	}
	current, _, err := node.describeTargetCollection(ctx, "", "", resource.GetCollectionId())
	if err != nil {
		if errors.IsAny(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound) {
			return merr.WrapErrServiceUnavailableMsg("compaction target resource changed during authorization")
		}
		return err
	}
	if !proto.Equal(current, resource) {
		return merr.WrapErrServiceUnavailableMsg("compaction target resource changed during authorization")
	}
	return nil
}

func (node *Proxy) DescribeCompactionTarget(ctx context.Context, req *milvuspb.DescribeCompactionTargetRequest) (*milvuspb.DescribeCompactionTargetResponse, error) {
	fail := func(err error) (*milvuspb.DescribeCompactionTargetResponse, error) {
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return &milvuspb.DescribeCompactionTargetResponse{Status: merr.Status(err)}, nil
	}
	subject, err := node.compactionTargetSubject(ctx)
	if err != nil {
		return fail(err)
	}
	if req.GetTargetId() <= 0 {
		return fail(merr.WrapErrParameterInvalidMsg("target_id must be positive"))
	}
	binding, err := node.targetBinding(ctx, req.GetBase(), req.GetTargetId())
	if err != nil {
		return fail(err)
	}
	if err := authorizeTargetResource(subject, binding); err != nil {
		return fail(err)
	}
	response, err := node.mixCoord.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
		Base: req.GetBase(), TargetId: req.GetTargetId(), Collection: binding,
	})
	if err := checkTargetRPC(response.GetStatus(), err); err != nil {
		return fail(err)
	}
	target := response.GetTarget()
	if target.GetTargetId() != req.GetTargetId() || !proto.Equal(binding, target.GetResource()) {
		return fail(merr.WrapErrServiceUnavailableMsg("compaction target description is inconsistent"))
	}
	if err := node.recheckTargetResource(ctx, binding); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return &milvuspb.DescribeCompactionTargetResponse{Status: merr.Success(), Target: publicCompactionTarget(target)}, nil
}

func (node *Proxy) ListCompactionTargets(ctx context.Context, req *milvuspb.ListCompactionTargetsRequest) (*milvuspb.ListCompactionTargetsResponse, error) {
	fail := func(err error) (*milvuspb.ListCompactionTargetsResponse, error) {
		if ctx.Err() != nil {
			return nil, status.Errorf(status.FromContextError(ctx.Err()).Code(), "list compaction targets exceeded its request budget; narrow the filters")
		}
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return &milvuspb.ListCompactionTargetsResponse{Status: merr.Status(err)}, nil
	}
	subject, err := node.compactionTargetSubject(ctx)
	if err != nil {
		return fail(err)
	}
	forwarded := &datapb.ListCompactionTargetsRequest{
		Base: req.GetBase(), DbName: req.GetDbName(), IncludeSystem: req.GetIncludeSystem(),
	}
	switch filter := req.GetCollectionFilter().(type) {
	case *milvuspb.ListCompactionTargetsRequest_CollectionName:
		if err := validateCollectionName(filter.CollectionName); err != nil {
			return fail(err)
		}
		forwarded.CollectionFilter = &datapb.ListCompactionTargetsRequest_CollectionName{CollectionName: filter.CollectionName}
	case *milvuspb.ListCompactionTargetsRequest_CollectionId:
		if filter.CollectionId <= 0 {
			return fail(merr.WrapErrParameterInvalidMsg("collection_id must be positive"))
		}
		forwarded.CollectionFilter = &datapb.ListCompactionTargetsRequest_CollectionId{CollectionId: filter.CollectionId}
	}
	for _, state := range req.GetStates() {
		if state != milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_ACTIVE && state != milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_INACTIVE {
			return fail(merr.WrapErrParameterInvalidMsg("unknown compaction target state %d", state))
		}
		forwarded.States = append(forwarded.States, datapb.TargetState(state))
	}

	response, err := node.mixCoord.ListCompactionTargets(ctx, forwarded)
	if err := checkTargetRPC(response.GetStatus(), err); err != nil {
		return fail(err)
	}
	denied := make(map[int64]bool)
	verified := make(map[int64]*datapb.CompactionTargetResource)
	result := &milvuspb.ListCompactionTargetsResponse{Status: merr.Success()}
	for _, target := range response.GetTargets() {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		resource := target.GetResource()
		if target.GetTargetId() <= 0 || resource == nil {
			return fail(merr.WrapErrServiceUnavailableMsg("compaction target list is inconsistent"))
		}
		id := resource.GetCollectionId()
		if denied[id] {
			continue
		}
		if previous, ok := verified[id]; ok {
			if !proto.Equal(previous, resource) {
				return fail(merr.WrapErrServiceUnavailableMsg("compaction target list contains inconsistent collection identities"))
			}
		} else {
			err := authorizeTargetResource(subject, resource)
			if errors.Is(err, merr.ErrPrivilegeNotPermitted) {
				denied[id] = true
				continue
			}
			if err != nil {
				return fail(err)
			}
			if err := node.recheckTargetResource(ctx, resource); err != nil {
				return fail(err)
			}
			verified[id] = resource
		}
		result.Targets = append(result.Targets, publicCompactionTarget(target))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if limit := Params.ProxyGrpcServerCfg.ServerMaxSendSize.GetAsInt(); limit > 0 && proto.Size(result) > limit {
		return nil, status.Error(codes.ResourceExhausted, "list compaction targets result is too large; narrow the filters")
	}
	return result, nil
}

func (node *Proxy) DropCompactionTarget(ctx context.Context, req *milvuspb.DropCompactionTargetRequest) (*milvuspb.DropCompactionTargetResponse, error) {
	fail := func(err error) (*milvuspb.DropCompactionTargetResponse, error) {
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return &milvuspb.DropCompactionTargetResponse{Status: merr.Status(err)}, nil
	}
	subject, err := node.compactionTargetSubject(ctx)
	if err != nil {
		return fail(err)
	}
	if req.GetTargetId() <= 0 {
		return fail(merr.WrapErrParameterInvalidMsg("target_id must be positive"))
	}
	binding, err := node.targetBinding(ctx, req.GetBase(), req.GetTargetId())
	if err != nil {
		return fail(err)
	}
	if err := authorizeTargetResource(subject, binding); err != nil {
		return fail(err)
	}
	if err := node.recheckTargetResource(ctx, binding); err != nil {
		return fail(err)
	}
	response, err := node.mixCoord.DropCompactionTarget(ctx, &datapb.DropCompactionTargetRequest{
		Base: req.GetBase(), TargetId: req.GetTargetId(), Collection: binding,
	})
	if err := checkTargetRPC(response.GetStatus(), err); err != nil {
		return fail(err)
	}
	return &milvuspb.DropCompactionTargetResponse{Status: response.GetStatus()}, nil
}

func publicCompactionTarget(target *datapb.CompactionTargetReadModel) *milvuspb.CompactionTargetInfo {
	resource := target.GetResource()
	public := &milvuspb.CompactionTargetInfo{
		TargetId: target.GetTargetId(), CollectionId: resource.GetCollectionId(), DbId: resource.GetDbId(),
		DbName: resource.GetDbName(), CollectionName: resource.GetCollectionName(),
		State: milvuspb.CompactionTargetState(target.GetState()), CreatedAt: target.GetCreatedAt(),
		StoppedAt: target.GetStoppedAt(), StopReason: target.GetStopReason(), TotalRows: target.TotalRows,
		RemainingRows: target.RemainingRows, CheckedAt: target.GetCheckedAt(), Message: target.GetMessage(),
	}
	switch {
	case target.GetIntent() == datapb.TargetIntent_INTENT_REWRITE && target.GetRewrite() != nil:
		public.Target = &milvuspb.CompactionTargetInfo_Rewrite{Rewrite: &milvuspb.RewriteTargetSpec{SegmentIds: target.GetRewrite().GetRootSegmentIds()}}
	case target.GetIntent() == datapb.TargetIntent_INTENT_OPTIMIZE && target.GetOptimize() != nil:
		public.Target = &milvuspb.CompactionTargetInfo_Optimize{Optimize: &milvuspb.OptimizeTargetInfo{
			InputTargetSizeBytes: target.GetOptimize().GetInputTargetSizeBytes(), ActualTargetSizeBytes: target.GetOptimize().GetActualTargetSizeBytes(),
		}}
	case target.GetIntent() == datapb.TargetIntent_INTENT_SORT && target.Enabled != nil:
		public.Target = &milvuspb.CompactionTargetInfo_Other{Other: &milvuspb.OtherTargetSpec{
			Condition: "SORT", ClusterWide: true, Parameters: []*commonpb.KeyValuePair{
				{Key: "enabled", Value: strconv.FormatBool(target.GetEnabled())},
			},
		}}
	default:
		public.Target = &milvuspb.CompactionTargetInfo_Other{Other: &milvuspb.OtherTargetSpec{
			Condition: target.GetIntent().String(), ClusterWide: resource.GetCollectionId() == 0,
		}}
	}
	return public
}
