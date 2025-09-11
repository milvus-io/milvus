package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

func (c *Core) broadcastCreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.checkCreateRole(ctx, in); err != nil {
		return err
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

// checkCreateRole check if the role can be created.
func (c *Core) checkCreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) error {
	if in.GetEntity().GetName() == "" {
		return errors.New("role name is empty")
	}
	results, err := c.meta.SelectRole(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		log.Ctx(ctx).Warn("fail to list roles", zap.Error(err))
		return err
	}
	if len(results) >= Params.ProxyCfg.MaxRoleNum.GetAsInt() {
		errMsg := "unable to create role because the number of roles has reached the limit"
		log.Ctx(ctx).Warn(errMsg, zap.Int("max_role_num", Params.ProxyCfg.MaxRoleNum.GetAsInt()))
		return errors.New(errMsg)
	}
	for _, result := range results {
		if result.GetRole().GetName() == in.GetEntity().GetName() {
			log.Ctx(ctx).Info("role already exists", zap.String("role", in.GetEntity().GetName()))
			return common.NewIgnorableError(errors.Newf("role [%s] already exists", in.GetEntity().GetName()))
		}
	}
	return nil
}

// alterRoleV2AckCallback is the ack callback function for the AlterRoleMessageV2 message.
func (c *DDLCallback) alterRoleV2AckCallback(ctx context.Context, result message.BroadcastResultAlterRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	return c.meta.CreateRole(ctx, util.DefaultTenant, result.Message.Header().RoleEntity)
}

func (c *Core) broadcastDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.checkDropRole(ctx, in); err != nil {
		return err
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

// checkDropRole check if the role can be dropped.
func (c *Core) checkDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	if util.IsBuiltinRole(in.GetRoleName()) {
		return merr.WrapErrPrivilegeNotPermitted("the role[%s] is a builtin role, which can't be dropped", in.GetRoleName())
	}

	if in.GetRoleName() == "" {
		return errors.New("role name is empty")
	}

	if _, err := c.meta.SelectRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.GetRoleName()}, false); err != nil {
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
		return errors.New(errMsg)
	}
	if in.GetForceDrop() {
		return nil
	}

	grantEntities, err := c.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
		Role:   &milvuspb.RoleEntity{Name: in.GetRoleName()},
		DbName: "*",
	})
	if err != nil {
		return err
	}
	if len(grantEntities) != 0 {
		errMsg := "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"
		return errors.New(errMsg)
	}
	return nil
}

// dropRoleV2AckCallback is the ack callback function for the DropRoleMessageV2 message.
func (c *DDLCallback) dropRoleV2AckCallback(ctx context.Context, result message.BroadcastResultDropRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	msg := result.Message
	err := c.meta.DropRole(ctx, util.DefaultTenant, msg.Header().RoleName)
	if err != nil {
		log.Ctx(ctx).Warn("drop role mata data failed", zap.String("role_name", msg.Header().RoleName), zap.Error(err))
		return err
	}
	if err := c.meta.DropGrant(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: msg.Header().RoleName}); err != nil {
		log.Ctx(ctx).Warn("drop the privilege list failed for the role", zap.String("role_name", msg.Header().RoleName), zap.Error(err))
		return err
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDropRole),
		OpKey:  msg.Header().RoleName,
	}); err != nil {
		log.Ctx(ctx).Warn("delete user role cache failed for the role", zap.String("role_name", msg.Header().RoleName), zap.Error(err))
		return err
	}
	return nil
}

func (c *Core) broadcastOperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest) error {
	if funcutil.IsEmptyString(in.Username) {
		return errors.New("username in the user entity is empty")
	}
	if funcutil.IsEmptyString(in.RoleName) {
		return errors.New("role name in the role entity is empty")
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if _, err := c.meta.SelectRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.RoleName}, false); err != nil {
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
		return errors.New(errMsg)
	}
	if in.Type != milvuspb.OperateUserRoleType_RemoveUserFromRole {
		if _, err := c.meta.SelectUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: in.Username}, false); err != nil {
			errMsg := "not found the user, maybe the user isn't existed or internal system error"
			return errors.New(errMsg)
		}
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
	username := result.Message.Header().RoleBinding.UserEntity.Name
	roleName := result.Message.Header().RoleBinding.RoleEntity.Name
	return executeOperateUserRoleTaskSteps(ctx, c.Core, username, roleName, milvuspb.OperateUserRoleType_AddUserToRole)
}

func (c *DDLCallback) dropUserRoleV2AckCallback(ctx context.Context, result message.BroadcastResultDropUserRoleMessageV2) error {
	username := result.Message.Header().RoleBinding.UserEntity.Name
	roleName := result.Message.Header().RoleBinding.RoleEntity.Name
	return executeOperateUserRoleTaskSteps(ctx, c.Core, username, roleName, milvuspb.OperateUserRoleType_RemoveUserFromRole)
}
