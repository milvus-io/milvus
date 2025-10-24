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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func (c *Core) broadcastOperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.operatePrivilegeCommonCheck(ctx, in); err != nil {
		return errors.Wrap(err, "failed to operate privilege common check")
	}
	privName := in.Entity.Grantor.Privilege.Name
	switch in.Version {
	case "v2":
		if err := c.isValidPrivilegeV2(ctx, privName); err != nil {
			return err
		}
		if err := c.validatePrivilegeGroupParams(ctx, privName, in.Entity.DbName, in.Entity.ObjectName); err != nil {
			return err
		}
		// set up object type for metastore, to be compatible with v1 version
		in.Entity.Object.Name = util.GetObjectType(privName)
	default:
		if err := c.isValidPrivilege(ctx, privName, in.Entity.Object.Name); err != nil {
			return err
		}
		// set up object name if it is global object type and not built in privilege group
		if in.Entity.Object.Name == commonpb.ObjectType_Global.String() && !util.IsBuiltinPrivilegeGroup(in.Entity.Grantor.Privilege.Name) {
			in.Entity.ObjectName = util.AnyWord
		}
	}

	var msg message.BroadcastMutableMessage
	switch in.Type {
	case milvuspb.OperatePrivilegeType_Grant:
		msg = message.NewAlterPrivilegeMessageBuilderV2().
			WithHeader(&message.AlterPrivilegeMessageHeader{
				Entity: in.Entity,
			}).
			WithBody(&message.AlterPrivilegeMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	case milvuspb.OperatePrivilegeType_Revoke:
		msg = message.NewDropPrivilegeMessageBuilderV2().
			WithHeader(&message.DropPrivilegeMessageHeader{
				Entity: in.Entity,
			}).
			WithBody(&message.DropPrivilegeMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	default:
		return errors.New("invalid operate privilege type")
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) alterPrivilegeV2AckCallback(ctx context.Context, result message.BroadcastResultAlterPrivilegeMessageV2) error {
	return executeOperatePrivilegeTaskSteps(ctx, c.Core, result.Message.Header().Entity, milvuspb.OperatePrivilegeType_Grant)
}

func (c *DDLCallback) dropPrivilegeV2AckCallback(ctx context.Context, result message.BroadcastResultDropPrivilegeMessageV2) error {
	return executeOperatePrivilegeTaskSteps(ctx, c.Core, result.Message.Header().Entity, milvuspb.OperatePrivilegeType_Revoke)
}

func (c *Core) broadcastCreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest) error {
	in.GroupName = strings.TrimSpace(in.GroupName)
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfPrivilegeGroupCreatable(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if privilege group creatable")
	}

	msg := message.NewAlterPrivilegeGroupMessageBuilderV2().
		WithHeader(&message.AlterPrivilegeGroupMessageHeader{
			PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
				GroupName: in.GroupName,
			},
		}).
		WithBody(&message.AlterPrivilegeGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastOperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfPrivilegeGroupAlterable(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if privilege group alterable")
	}

	var msg message.BroadcastMutableMessage
	switch in.Type {
	case milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup:
		msg = message.NewAlterPrivilegeGroupMessageBuilderV2().
			WithHeader(&message.AlterPrivilegeGroupMessageHeader{
				PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
					GroupName:  in.GroupName,
					Privileges: in.Privileges,
				},
			}).
			WithBody(&message.AlterPrivilegeGroupMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	case milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup:
		msg = message.NewDropPrivilegeGroupMessageBuilderV2().
			WithHeader(&message.DropPrivilegeGroupMessageHeader{
				PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
					GroupName:  in.GroupName,
					Privileges: in.Privileges,
				},
			}).
			WithBody(&message.DropPrivilegeGroupMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	default:
		return errors.New("invalid operate privilege group type")
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) alterPrivilegeGroupV2AckCallback(ctx context.Context, result message.BroadcastResultAlterPrivilegeGroupMessageV2) error {
	if len(result.Message.Header().PrivilegeGroupInfo.Privileges) == 0 {
		return c.meta.CreatePrivilegeGroup(ctx, result.Message.Header().PrivilegeGroupInfo.GroupName)
	}
	return executeOperatePrivilegeGroupTaskSteps(ctx, c.Core, result.Message.Header().PrivilegeGroupInfo, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup)
}

func (c *Core) broadcastDropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfPrivilegeGroupDropable(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if privilege group dropable")
	}

	msg := message.NewDropPrivilegeGroupMessageBuilderV2().
		WithHeader(&message.DropPrivilegeGroupMessageHeader{
			PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
				GroupName: in.GroupName,
			},
		}).
		WithBody(&message.DropPrivilegeGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) dropPrivilegeGroupV2AckCallback(ctx context.Context, result message.BroadcastResultDropPrivilegeGroupMessageV2) error {
	if len(result.Message.Header().PrivilegeGroupInfo.Privileges) == 0 {
		return c.meta.DropPrivilegeGroup(ctx, result.Message.Header().PrivilegeGroupInfo.GroupName)
	}
	return executeOperatePrivilegeGroupTaskSteps(ctx, c.Core, result.Message.Header().PrivilegeGroupInfo, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
}
