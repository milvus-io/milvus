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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfCreateRole(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if create role")
	}

	msg := message.NewAlterRoleMessageBuilderV2().
		WithHeader(&message.AlterRoleMessageHeader{
			RoleEntity: in.GetEntity(),
		}).
		WithBody(&message.AlterRoleMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// alterRoleV2AckCallback is the ack callback function for the AlterRoleMessageV2 message.
func (c *DDLCallback) alterRoleV2AckCallback(ctx context.Context, result message.BroadcastResultAlterRoleMessageV2) error {
	return c.meta.CreateRole(ctx, util.DefaultTenant, result.Message.Header().RoleEntity)
}

func (c *Core) broadcastDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfDropRole(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if drop role")
	}

	msg := message.NewDropRoleMessageBuilderV2().
		WithHeader(&message.DropRoleMessageHeader{
			RoleName: in.RoleName,
		}).
		WithBody(&message.DropRoleMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// dropRoleV2AckCallback is the ack callback function for the DropRoleMessageV2 message.
func (c *DDLCallback) dropRoleV2AckCallback(ctx context.Context, result message.BroadcastResultDropRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	msg := result.Message
	err := c.meta.DropRole(ctx, util.DefaultTenant, msg.Header().RoleName)
	if err != nil {
		return errors.Wrap(err, "failed to drop role")
	}
	if err := c.meta.DropGrant(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: msg.Header().RoleName}); err != nil {
		return errors.Wrap(err, "failed to drop grant")
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDropRole),
		OpKey:  msg.Header().RoleName,
	}); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}

func (c *Core) broadcastOperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfOperateUserRole(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if operate user role")
	}

	var msg message.BroadcastMutableMessage
	switch in.Type {
	case milvuspb.OperateUserRoleType_AddUserToRole:
		msg = message.NewAlterUserRoleMessageBuilderV2().
			WithHeader(&message.AlterUserRoleMessageHeader{
				RoleBinding: &message.RoleBinding{
					UserEntity: &milvuspb.UserEntity{Name: in.Username},
					RoleEntity: &milvuspb.RoleEntity{Name: in.RoleName},
				},
			}).
			WithBody(&message.AlterUserRoleMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	case milvuspb.OperateUserRoleType_RemoveUserFromRole:
		msg = message.NewDropUserRoleMessageBuilderV2().
			WithHeader(&message.DropUserRoleMessageHeader{
				RoleBinding: &message.RoleBinding{
					UserEntity: &milvuspb.UserEntity{Name: in.Username},
					RoleEntity: &milvuspb.RoleEntity{Name: in.RoleName},
				},
			}).
			WithBody(&message.DropUserRoleMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	default:
		return errors.New("invalid operate user role type")
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) alterUserRoleV2AckCallback(ctx context.Context, result message.BroadcastResultAlterUserRoleMessageV2) error {
	header := result.Message.Header()
	if err := c.meta.OperateUserRole(ctx, util.DefaultTenant, header.RoleBinding.UserEntity, header.RoleBinding.RoleEntity, milvuspb.OperateUserRoleType_AddUserToRole); err != nil {
		return errors.Wrap(err, "failed to operate user role")
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheAddUserToRole),
		OpKey:  funcutil.EncodeUserRoleCache(header.RoleBinding.UserEntity.Name, header.RoleBinding.RoleEntity.Name),
	}); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}

func (c *DDLCallback) dropUserRoleV2AckCallback(ctx context.Context, result message.BroadcastResultDropUserRoleMessageV2) error {
	header := result.Message.Header()
	if err := c.meta.OperateUserRole(ctx, util.DefaultTenant, header.RoleBinding.UserEntity, header.RoleBinding.RoleEntity, milvuspb.OperateUserRoleType_RemoveUserFromRole); err != nil {
		return errors.Wrap(err, "failed to operate user role")
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheRemoveUserFromRole),
		OpKey:  funcutil.EncodeUserRoleCache(header.RoleBinding.UserEntity.Name, header.RoleBinding.RoleEntity.Name),
	}); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}
