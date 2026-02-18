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
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// requireAdminForRLS checks that the current user is root or has the admin role.
// RLS policy management is an admin-only operation.
func requireAdminForRLS(ctx context.Context) error {
	if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		return nil
	}
	username, err := GetCurUserFromContext(ctx)
	if err != nil || username == "" {
		return merr.WrapErrPrivilegeNotPermitted("RLS management requires authentication")
	}
	if username == util.UserRoot {
		return nil
	}
	roles, err := GetRole(username)
	if err != nil {
		return merr.WrapErrPrivilegeNotPermitted("failed to get roles for RLS authorization")
	}
	for _, role := range roles {
		if role == util.RoleAdmin {
			return nil
		}
	}
	return merr.WrapErrPrivilegeNotPermitted(
		fmt.Sprintf("user '%s' is not authorized to manage RLS policies (requires root or admin role)", username))
}

// CreateRowPolicy creates a new row-level security policy for a collection.
func (node *Proxy) CreateRowPolicy(ctx context.Context, req *milvuspb.CreateRowPolicyRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateRowPolicy")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("dbName", req.GetDbName()),
		zap.String("policyName", req.GetPolicyName()),
		zap.String("collectionName", req.GetCollectionName()),
	)

	log.Info("received CreateRowPolicy request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("CreateRowPolicy failed, proxy not healthy")
		return merr.Status(err), nil
	}

	if err := requireAdminForRLS(ctx); err != nil {
		log.Warn("CreateRowPolicy authorization failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Convert milvuspb request to messagespb request
	internalReq := convertMilvuspbToInternalCreateRowPolicy(req)
	resp, err := node.mixCoord.CreateRowPolicy(ctx, internalReq)
	if err != nil {
		log.Warn("CreateRowPolicy failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Refresh local RLS cache after successful policy creation
	if resp.GetErrorCode() == commonpb.ErrorCode_Success {
		node.refreshRLSCache(ctx, req.GetDbName(), req.GetCollectionName())
	}

	log.Info("CreateRowPolicy done")
	return resp, nil
}

// DropRowPolicy removes an existing row-level security policy.
func (node *Proxy) DropRowPolicy(ctx context.Context, req *milvuspb.DropRowPolicyRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropRowPolicy")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("dbName", req.GetDbName()),
		zap.String("collectionName", req.GetCollectionName()),
		zap.String("policyName", req.GetPolicyName()),
	)

	log.Info("received DropRowPolicy request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DropRowPolicy failed, proxy not healthy")
		return merr.Status(err), nil
	}

	if err := requireAdminForRLS(ctx); err != nil {
		log.Warn("DropRowPolicy authorization failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Convert milvuspb request to messagespb request
	internalReq := convertMilvuspbToInternalDropRowPolicy(req)
	resp, err := node.mixCoord.DropRowPolicy(ctx, internalReq)
	if err != nil {
		log.Warn("DropRowPolicy failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Refresh local RLS cache after successful policy removal
	if resp.GetErrorCode() == commonpb.ErrorCode_Success {
		node.refreshRLSCache(ctx, req.GetDbName(), req.GetCollectionName())
	}

	log.Info("DropRowPolicy done")
	return resp, nil
}

// ListRowPolicies lists all row-level security policies for a collection.
func (node *Proxy) ListRowPolicies(ctx context.Context, req *milvuspb.ListRowPoliciesRequest) (*milvuspb.ListRowPoliciesResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListRowPolicies")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("dbName", req.GetDbName()),
		zap.String("collectionName", req.GetCollectionName()),
	)

	log.Info("received ListRowPolicies request")

	failResp := &milvuspb.ListRowPoliciesResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		failResp.Status = merr.Status(err)
		log.Warn("ListRowPolicies failed, proxy not healthy")
		return failResp, nil
	}

	// Convert milvuspb request to messagespb request
	internalReq := convertMilvuspbToInternalListRowPolicies(req)
	resp, err := node.mixCoord.ListRowPolicies(ctx, internalReq)
	if err != nil {
		log.Warn("ListRowPolicies failed", zap.Error(err))
		failResp.Status = merr.Status(err)
		return failResp, nil
	}

	// Convert messagespb response to milvuspb response
	result := convertInternalToMilvuspbListRowPoliciesResponse(resp, req.GetDbName(), req.GetCollectionName())

	log.Info("ListRowPolicies done", zap.Int("policyCount", len(result.GetPolicies())))
	return result, nil
}

// AddUserTags adds tags to a user (used in RLS policies).
func (node *Proxy) AddUserTags(ctx context.Context, req *milvuspb.AddUserTagsRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AddUserTags")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("userName", req.GetUserName()),
		zap.Int("tagCount", len(req.GetTags())),
	)

	log.Info("received AddUserTags request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("AddUserTags failed, proxy not healthy")
		return merr.Status(err), nil
	}

	if err := requireAdminForRLS(ctx); err != nil {
		log.Warn("AddUserTags authorization failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Convert milvuspb request to messagespb request
	internalReq := &messagespb.SetUserTagsRequest{
		Base:     req.GetBase(),
		UserName: req.GetUserName(),
		Tags:     req.GetTags(),
	}

	resp, err := node.mixCoord.SetUserTags(ctx, internalReq)
	if err != nil {
		log.Warn("AddUserTags failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Refresh local RLS cache after successful tag update
	if resp.GetErrorCode() == commonpb.ErrorCode_Success {
		node.refreshRLSUserTags(ctx, req.GetUserName())
	}

	log.Info("AddUserTags done")
	return resp, nil
}

// GetUserTags retrieves tags for a user.
func (node *Proxy) GetUserTags(ctx context.Context, req *milvuspb.GetUserTagsRequest) (*milvuspb.GetUserTagsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetUserTags")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("userName", req.GetUserName()),
	)

	log.Info("received GetUserTags request")

	failResp := &milvuspb.GetUserTagsResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		failResp.Status = merr.Status(err)
		log.Warn("GetUserTags failed, proxy not healthy")
		return failResp, nil
	}

	// Convert milvuspb request to messagespb request
	internalReq := &messagespb.GetUserTagsRequest{
		Base:     req.GetBase(),
		UserName: req.GetUserName(),
	}

	resp, err := node.mixCoord.GetUserTags(ctx, internalReq)
	if err != nil {
		log.Warn("GetUserTags failed", zap.Error(err))
		failResp.Status = merr.Status(err)
		return failResp, nil
	}

	// Convert messagespb response to milvuspb response
	result := &milvuspb.GetUserTagsResponse{
		Status: resp.GetStatus(),
		Tags:   resp.GetTags(),
	}

	log.Info("GetUserTags done", zap.Int("tagCount", len(result.GetTags())))
	return result, nil
}

// DeleteUserTags removes specific tags from a user.
func (node *Proxy) DeleteUserTags(ctx context.Context, req *milvuspb.DeleteUserTagsRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DeleteUserTags")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("userName", req.GetUserName()),
		zap.Strings("tagKeys", req.GetTagKeys()),
	)

	log.Info("received DeleteUserTags request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DeleteUserTags failed, proxy not healthy")
		return merr.Status(err), nil
	}

	if err := requireAdminForRLS(ctx); err != nil {
		log.Warn("DeleteUserTags authorization failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Delete each tag key one by one through the internal API
	for _, tagKey := range req.GetTagKeys() {
		internalReq := &messagespb.DeleteUserTagRequest{
			Base:     req.GetBase(),
			UserName: req.GetUserName(),
			TagKey:   tagKey,
		}

		resp, err := node.mixCoord.DeleteUserTag(ctx, internalReq)
		if err != nil {
			log.Warn("DeleteUserTags failed", zap.Error(err), zap.String("tagKey", tagKey))
			return merr.Status(err), nil
		}
		if resp.GetErrorCode() != commonpb.ErrorCode_Success {
			return resp, nil
		}
	}

	// Refresh local RLS cache after successful tag deletion
	node.refreshRLSUserTags(ctx, req.GetUserName())

	log.Info("DeleteUserTags done")
	return merr.Success(), nil
}

// ListUsersWithTag lists all users that have a specific tag.
func (node *Proxy) ListUsersWithTag(ctx context.Context, req *milvuspb.ListUsersWithTagRequest) (*milvuspb.ListUsersWithTagResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListUsersWithTag")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("tagKey", req.GetTagKey()),
		zap.String("tagValue", req.GetTagValue()),
	)

	log.Info("received ListUsersWithTag request")

	failResp := &milvuspb.ListUsersWithTagResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		failResp.Status = merr.Status(err)
		log.Warn("ListUsersWithTag failed, proxy not healthy")
		return failResp, nil
	}

	// Convert milvuspb request to messagespb request
	internalReq := &messagespb.ListUsersWithTagRequest{
		Base:     req.GetBase(),
		TagKey:   req.GetTagKey(),
		TagValue: req.GetTagValue(),
	}

	resp, err := node.mixCoord.ListUsersWithTag(ctx, internalReq)
	if err != nil {
		log.Warn("ListUsersWithTag failed", zap.Error(err))
		failResp.Status = merr.Status(err)
		return failResp, nil
	}

	// Convert messagespb response to milvuspb response
	result := &milvuspb.ListUsersWithTagResponse{
		Status:    resp.GetStatus(),
		UserNames: resp.GetUsers(),
	}

	log.Info("ListUsersWithTag done", zap.Int("userCount", len(result.GetUserNames())))
	return result, nil
}

// RefreshRLSCache triggers a refresh of the RLS cache.
func (node *Proxy) RefreshRLSCache(ctx context.Context, req *messagespb.RefreshRLSCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RefreshRLSCache")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("dbName", req.GetDbName()),
		zap.String("collectionName", req.GetCollectionName()),
	)

	log.Info("received RefreshRLSCache request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("RefreshRLSCache failed, proxy not healthy")
		return merr.Status(err), nil
	}

	// Forward to rootcoord for cluster-wide refresh
	resp, err := node.mixCoord.RefreshRLSCache(ctx, req)
	if err != nil {
		log.Warn("RefreshRLSCache failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Also refresh local cache
	node.refreshRLSCache(ctx, req.GetDbName(), req.GetCollectionName())

	log.Info("RefreshRLSCache done")
	return resp, nil
}

// refreshRLSCache refreshes the local RLS cache for a collection.
func (node *Proxy) refreshRLSCache(ctx context.Context, dbName, collectionName string) {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil || rlsMgr.GetCache() == nil {
		return
	}

	// Get database and collection IDs
	dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, dbName)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get database info for RLS cache refresh",
			zap.String("dbName", dbName),
			zap.Error(err))
		return
	}

	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get collection ID for RLS cache refresh",
			zap.String("collectionName", collectionName),
			zap.Error(err))
		return
	}

	// Fetch fresh policies from rootcoord
	listReq := &messagespb.ListRowPoliciesRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	listResp, err := node.mixCoord.ListRowPolicies(ctx, listReq)
	if err != nil {
		log.Ctx(ctx).Warn("failed to fetch policies for RLS cache refresh",
			zap.Error(err))
		return
	}
	if listResp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("failed to fetch policies for RLS cache refresh: non-success status",
			zap.String("reason", listResp.GetStatus().GetReason()))
		return
	}

	// Convert proto policies to model policies
	policies := make([]*model.RLSPolicy, 0, len(listResp.GetPolicies()))
	for _, proto := range listResp.GetPolicies() {
		policy := convertProtoToModelRLSPolicy(proto, collectionID, dbInfo.dbID)
		if policy != nil {
			policies = append(policies, policy)
		}
	}

	// Update the local cache
	cache := rlsMgr.GetCache()
	cache.UpdatePolicies(dbInfo.dbID, collectionID, policies)

	log.Ctx(ctx).Info("RLS cache refreshed",
		zap.String("dbName", dbName),
		zap.String("collectionName", collectionName),
		zap.Int("policyCount", len(policies)))
}

// refreshRLSUserTags refreshes the local RLS cache for user tags.
func (node *Proxy) refreshRLSUserTags(ctx context.Context, userName string) {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil || rlsMgr.GetCache() == nil {
		return
	}

	// Fetch fresh tags from rootcoord
	getReq := &messagespb.GetUserTagsRequest{
		UserName: userName,
	}
	getResp, err := node.mixCoord.GetUserTags(ctx, getReq)
	if err != nil {
		log.Ctx(ctx).Warn("failed to fetch user tags for RLS cache refresh",
			zap.String("userName", userName),
			zap.Error(err))
		return
	}
	if getResp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Ctx(ctx).Warn("failed to fetch user tags for RLS cache refresh: non-success status",
			zap.String("userName", userName),
			zap.String("reason", getResp.GetStatus().GetReason()))
		return
	}

	// Update the local cache
	cache := rlsMgr.GetCache()
	cache.SetUserTags(userName, getResp.GetTags())

	log.Ctx(ctx).Info("RLS user tags refreshed",
		zap.String("userName", userName),
		zap.Int("tagCount", len(getResp.GetTags())))
}

// convertProtoToModelRLSPolicy converts a protobuf RLSPolicy to a model.RLSPolicy.
func convertProtoToModelRLSPolicy(proto *messagespb.RLSPolicy, collectionID, dbID int64) *model.RLSPolicy {
	if proto == nil {
		return nil
	}

	policyType := model.RLSPolicyTypePermissive
	if proto.GetPolicyType() == messagespb.RLSPolicyType_RESTRICTIVE {
		policyType = model.RLSPolicyTypeRestrictive
	}

	return &model.RLSPolicy{
		PolicyName:   proto.GetPolicyName(),
		CollectionID: collectionID,
		DBID:         dbID,
		PolicyType:   policyType,
		Actions:      proto.GetActions(),
		Roles:        proto.GetRoles(),
		UsingExpr:    proto.GetUsingExpr(),
		CheckExpr:    proto.GetCheckExpr(),
	}
}

// convertMilvuspbToInternalCreateRowPolicy converts milvuspb.CreateRowPolicyRequest to messagespb.CreateRowPolicyRequest
func convertMilvuspbToInternalCreateRowPolicy(req *milvuspb.CreateRowPolicyRequest) *messagespb.CreateRowPolicyRequest {
	if req == nil {
		return nil
	}

	// Convert actions from milvuspb to string slice
	actions := make([]string, 0, len(req.GetActions()))
	for _, action := range req.GetActions() {
		switch action {
		case milvuspb.RowPolicyAction_Query:
			actions = append(actions, string(model.RLSActionQuery))
		case milvuspb.RowPolicyAction_Search:
			actions = append(actions, string(model.RLSActionSearch))
		case milvuspb.RowPolicyAction_Insert:
			actions = append(actions, string(model.RLSActionInsert))
		case milvuspb.RowPolicyAction_Delete:
			actions = append(actions, string(model.RLSActionDelete))
		case milvuspb.RowPolicyAction_Upsert:
			actions = append(actions, string(model.RLSActionUpsert))
		default:
			// Ignore unknown enum values; rootcoord model validation will reject empty/invalid result.
		}
	}

	// Default to PERMISSIVE policy type.
	// NOTE: milvuspb.CreateRowPolicyRequest does not yet carry a PolicyType field in the
	// current proto version, so we always fall back to PERMISSIVE here.  When the proto is
	// updated to include a RowPolicyType field this block should be replaced with:
	//   policyType := messagespb.RLSPolicyType_PERMISSIVE
	//   if req.GetPolicyType() == milvuspb.RowPolicyType_RESTRICTIVE {
	//       policyType = messagespb.RLSPolicyType_RESTRICTIVE
	//   }
	policyType := messagespb.RLSPolicyType_PERMISSIVE

	return &messagespb.CreateRowPolicyRequest{
		Base:   req.GetBase(),
		DbName: req.GetDbName(),
		Policy: &messagespb.RLSPolicy{
			PolicyName:     req.GetPolicyName(),
			CollectionName: req.GetCollectionName(),
			PolicyType:     policyType,
			Actions:        actions,
			Roles:          req.GetRoles(),
			UsingExpr:      req.GetUsingExpr(),
			CheckExpr:      req.GetCheckExpr(),
			Description:    req.GetDescription(),
		},
	}
}

// convertMilvuspbToInternalDropRowPolicy converts milvuspb.DropRowPolicyRequest to messagespb.DropRowPolicyRequest
func convertMilvuspbToInternalDropRowPolicy(req *milvuspb.DropRowPolicyRequest) *messagespb.DropRowPolicyRequest {
	if req == nil {
		return nil
	}

	return &messagespb.DropRowPolicyRequest{
		Base:           req.GetBase(),
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PolicyName:     req.GetPolicyName(),
	}
}

// convertMilvuspbToInternalListRowPolicies converts milvuspb.ListRowPoliciesRequest to messagespb.ListRowPoliciesRequest
func convertMilvuspbToInternalListRowPolicies(req *milvuspb.ListRowPoliciesRequest) *messagespb.ListRowPoliciesRequest {
	if req == nil {
		return nil
	}

	return &messagespb.ListRowPoliciesRequest{
		Base:           req.GetBase(),
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
	}
}

// convertInternalToMilvuspbListRowPoliciesResponse converts messagespb.ListRowPoliciesResponse to milvuspb.ListRowPoliciesResponse
func convertInternalToMilvuspbListRowPoliciesResponse(resp *messagespb.ListRowPoliciesResponse, dbName, collectionName string) *milvuspb.ListRowPoliciesResponse {
	if resp == nil {
		return &milvuspb.ListRowPoliciesResponse{}
	}

	policies := make([]*milvuspb.RowPolicy, 0, len(resp.GetPolicies()))
	for _, internalPolicy := range resp.GetPolicies() {
		policy := convertInternalPolicyToMilvuspbPolicy(internalPolicy)
		if policy != nil {
			policies = append(policies, policy)
		}
	}

	return &milvuspb.ListRowPoliciesResponse{
		Status:         resp.GetStatus(),
		Policies:       policies,
		DbName:         dbName,
		CollectionName: collectionName,
	}
}

// convertInternalPolicyToMilvuspbPolicy converts messagespb.RLSPolicy to milvuspb.RowPolicy
func convertInternalPolicyToMilvuspbPolicy(internal *messagespb.RLSPolicy) *milvuspb.RowPolicy {
	if internal == nil {
		return nil
	}

	// Convert string actions to milvuspb.RowPolicyAction
	actions := make([]milvuspb.RowPolicyAction, 0, len(internal.GetActions()))
	for _, action := range internal.GetActions() {
		switch action {
		case "Query", "query":
			actions = append(actions, milvuspb.RowPolicyAction_Query)
		case "Search", "search":
			actions = append(actions, milvuspb.RowPolicyAction_Search)
		case "Insert", "insert":
			actions = append(actions, milvuspb.RowPolicyAction_Insert)
		case "Delete", "delete":
			actions = append(actions, milvuspb.RowPolicyAction_Delete)
		case "Upsert", "upsert":
			actions = append(actions, milvuspb.RowPolicyAction_Upsert)
		}
	}

	return &milvuspb.RowPolicy{
		PolicyName:  internal.GetPolicyName(),
		Actions:     actions,
		Roles:       internal.GetRoles(),
		UsingExpr:   internal.GetUsingExpr(),
		CheckExpr:   internal.GetCheckExpr(),
		Description: internal.GetDescription(),
		CreatedAt:   internal.GetCreatedAt(),
	}
}
