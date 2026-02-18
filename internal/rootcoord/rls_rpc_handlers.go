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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

// CreateRowPolicy creates a new RLS policy for a collection
func (c *Core) CreateRowPolicy(ctx context.Context, in *milvuspb.CreateRowPolicyRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()),
		zap.String("policyName", in.GetPolicy().GetPolicyName()),
	)
	logger.Info("received request to create RLS policy")

	policy := in.GetPolicy()
	if policy == nil {
		return merr.Status(merr.WrapErrParameterInvalid("valid policy", "nil", "policy cannot be nil")), nil
	}

	req := &CreateRLSPolicyRequest{
		DbName:         in.GetDbName(),
		CollectionName: policy.GetCollectionName(),
		PolicyName:     policy.GetPolicyName(),
		PolicyType:     int32(policy.GetPolicyType()),
		Actions:        policy.GetActions(),
		Roles:          policy.GetRoles(),
		UsingExpr:      policy.GetUsingExpr(),
		CheckExpr:      policy.GetCheckExpr(),
		Description:    policy.GetDescription(),
	}

	task := &createRowPolicyTask{
		baseTask: newBaseTask(ctx, c),
		Req:      req,
	}

	if err := task.Prepare(ctx); err != nil {
		logger.Error("failed to prepare create row policy task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := c.scheduler.AddTask(task); err != nil {
		logger.Error("failed to enqueue create row policy task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := task.WaitToFinish(); err != nil {
		logger.Error("failed to create RLS policy", zap.Error(err))
		return merr.Status(err), nil
	}

	logger.Info("RLS policy created successfully")
	return merr.Success(), nil
}

// DropRowPolicy removes an RLS policy from a collection
func (c *Core) DropRowPolicy(ctx context.Context, in *milvuspb.DropRowPolicyRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()),
		zap.String("collectionName", in.GetCollectionName()),
		zap.String("policyName", in.GetPolicyName()),
	)
	logger.Info("received request to drop RLS policy")

	req := &DropRLSPolicyRequest{
		DbName:         in.GetDbName(),
		CollectionName: in.GetCollectionName(),
		PolicyName:     in.GetPolicyName(),
	}

	task := &dropRowPolicyTask{
		baseTask: newBaseTask(ctx, c),
		Req:      req,
	}

	if err := task.Prepare(ctx); err != nil {
		logger.Error("failed to prepare drop row policy task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := c.scheduler.AddTask(task); err != nil {
		logger.Error("failed to enqueue drop row policy task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := task.WaitToFinish(); err != nil {
		logger.Error("failed to drop RLS policy", zap.Error(err))
		return merr.Status(err), nil
	}

	logger.Info("RLS policy dropped successfully")
	return merr.Success(), nil
}

// ListRowPolicies lists all RLS policies for a collection
func (c *Core) ListRowPolicies(ctx context.Context, in *milvuspb.ListRowPoliciesRequest) (*milvuspb.ListRowPoliciesResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ListRowPoliciesResponse{
			Status: merr.Status(err),
		}, nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("dbName", in.GetDbName()),
		zap.String("collectionName", in.GetCollectionName()),
	)
	logger.Info("received request to list RLS policies")

	// Get collection metadata
	collMeta, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		logger.Error("failed to get collection", zap.Error(err))
		return &milvuspb.ListRowPoliciesResponse{
			Status: merr.Status(err),
		}, nil
	}

	// List policies from metastore
	policies, err := c.meta.ListRLSPolicies(ctx, collMeta.DBID, collMeta.CollectionID)
	if err != nil {
		logger.Error("failed to list RLS policies", zap.Error(err))
		return &milvuspb.ListRowPoliciesResponse{
			Status: merr.Status(err),
		}, nil
	}

	// Convert to protobuf
	pbPolicies := make([]*milvuspb.RLSPolicy, 0, len(policies))
	for _, policy := range policies {
		pbPolicies = append(pbPolicies, &milvuspb.RLSPolicy{
			PolicyName:     policy.PolicyName,
			CollectionName: in.GetCollectionName(),
			CollectionId:   policy.CollectionID,
			PolicyType:     milvuspb.RLSPolicyType(policy.PolicyType),
			Actions:        policy.Actions,
			Roles:          policy.Roles,
			UsingExpr:      policy.UsingExpr,
			CheckExpr:      policy.CheckExpr,
			Description:    policy.Description,
			CreatedAt:      int64(policy.CreatedAt),
		})
	}

	logger.Info("listed RLS policies successfully", zap.Int("count", len(pbPolicies)))
	return &milvuspb.ListRowPoliciesResponse{
		Status:   merr.Success(),
		Policies: pbPolicies,
	}, nil
}

// SetUserTags sets or updates tags for a user
func (c *Core) SetUserTags(ctx context.Context, in *milvuspb.SetUserTagsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("userName", in.GetUserName()),
		zap.Int("numTags", len(in.GetTags())),
	)
	logger.Info("received request to set user tags")

	req := &SetUserTagsRequest{
		UserName: in.GetUserName(),
		Tags:     in.GetTags(),
	}

	task := &setUserTagsTask{
		baseTask: newBaseTask(ctx, c),
		Req:      req,
	}

	if err := task.Prepare(ctx); err != nil {
		logger.Error("failed to prepare set user tags task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := c.scheduler.AddTask(task); err != nil {
		logger.Error("failed to enqueue set user tags task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := task.WaitToFinish(); err != nil {
		logger.Error("failed to set user tags", zap.Error(err))
		return merr.Status(err), nil
	}

	logger.Info("user tags set successfully")
	return merr.Success(), nil
}

// GetUserTags retrieves tags for a user
func (c *Core) GetUserTags(ctx context.Context, in *milvuspb.GetUserTagsRequest) (*milvuspb.GetUserTagsResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.GetUserTagsResponse{
			Status: merr.Status(err),
		}, nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("userName", in.GetUserName()),
	)
	logger.Info("received request to get user tags")

	// Get user tags from metastore
	userTags, err := c.meta.GetUserTags(ctx, in.GetUserName())
	if err != nil {
		logger.Warn("failed to get user tags", zap.Error(err))
		// Return empty tags if not found
		return &milvuspb.GetUserTagsResponse{
			Status: merr.Success(),
			Tags:   make(map[string]string),
		}, nil
	}

	logger.Info("retrieved user tags successfully", zap.Int("numTags", len(userTags.Tags)))
	return &milvuspb.GetUserTagsResponse{
		Status: merr.Success(),
		Tags:   userTags.Tags,
	}, nil
}

// DeleteUserTag removes a specific tag from a user
func (c *Core) DeleteUserTag(ctx context.Context, in *milvuspb.DeleteUserTagRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("userName", in.GetUserName()),
		zap.String("tagKey", in.GetTagKey()),
	)
	logger.Info("received request to delete user tag")

	req := &DeleteUserTagRequest{
		UserName: in.GetUserName(),
		TagKey:   in.GetTagKey(),
	}

	task := &deleteUserTagTask{
		baseTask: newBaseTask(ctx, c),
		Req:      req,
	}

	if err := task.Prepare(ctx); err != nil {
		logger.Error("failed to prepare delete user tag task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := c.scheduler.AddTask(task); err != nil {
		logger.Error("failed to enqueue delete user tag task", zap.Error(err))
		return merr.Status(err), nil
	}

	if err := task.WaitToFinish(); err != nil {
		logger.Error("failed to delete user tag", zap.Error(err))
		return merr.Status(err), nil
	}

	logger.Info("user tag deleted successfully")
	return merr.Success(), nil
}

// ListUsersWithTag lists all users that have a specific tag value
func (c *Core) ListUsersWithTag(ctx context.Context, in *milvuspb.ListUsersWithTagRequest) (*milvuspb.ListUsersWithTagResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &milvuspb.ListUsersWithTagResponse{
			Status: merr.Status(err),
		}, nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("tagKey", in.GetTagKey()),
		zap.String("tagValue", in.GetTagValue()),
	)
	logger.Info("received request to list users with tag")

	// List users from metastore
	users, err := c.meta.ListUsersWithTag(ctx, in.GetTagKey(), in.GetTagValue())
	if err != nil {
		logger.Error("failed to list users with tag", zap.Error(err))
		return &milvuspb.ListUsersWithTagResponse{
			Status: merr.Status(err),
		}, nil
	}

	logger.Info("listed users with tag successfully", zap.Int("count", len(users)))
	return &milvuspb.ListUsersWithTagResponse{
		Status: merr.Success(),
		Users:  users,
	}, nil
}

// RefreshRLSCache broadcasts a cache refresh to all proxies
func (c *Core) RefreshRLSCache(ctx context.Context, in *milvuspb.RefreshRLSCacheRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("role", typeutil.RootCoordRole),
		zap.String("opType", in.GetOpType().String()),
	)
	logger.Info("received request to refresh RLS cache")

	// In a real implementation, this would broadcast to all proxies
	// For now, we just log the operation
	logger.Info("RLS cache refresh requested",
		zap.String("dbName", in.GetDbName()),
		zap.String("collectionName", in.GetCollectionName()),
		zap.String("policyName", in.GetPolicyName()),
		zap.String("userName", in.GetUserName()),
	)

	return merr.Success(), nil
}
