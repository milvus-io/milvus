package rootcoord

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.checkCreateCredential(ctx, credInfo); err != nil {
		return err
	}

	msg := message.NewAlterUserMessageBuilderV2().
		WithHeader(&message.AlterUserMessageHeader{
			UserEntity: &milvuspb.UserEntity{Name: credInfo.Username},
		}).
		WithBody(&message.AlterUserMessageBody{
			CredentialInfo: credInfo,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// checkCreateCredential check if the credential can be created.
func (c *Core) checkCreateCredential(ctx context.Context, in *internalpb.CredentialInfo) error {
	if in.GetUsername() == "" {
		return errors.New("username is empty")
	}

	// check if the number of roles has reached the limit.
	resp, err := c.meta.ListCredentialUsernames(ctx)
	if err != nil {
		return err
	}
	if len(resp.Usernames) >= Params.ProxyCfg.MaxUserNum.GetAsInt() {
		errMsg := "unable to add user because the number of users has reached the limit"
		log.Ctx(ctx).Error(errMsg, zap.Int("max_user_num", Params.ProxyCfg.MaxUserNum.GetAsInt()))
		return errors.New(errMsg)
	}

	// check if the username already exists.
	for _, username := range resp.Usernames {
		if username == in.GetUsername() {
			return fmt.Errorf("user already exists: %s", in.GetUsername())
		}
	}
	return nil
}

func (c *Core) broadcastUpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.checkUpdateCredential(ctx, credInfo); err != nil {
		return err
	}

	msg := message.NewAlterUserMessageBuilderV2().
		WithHeader(&message.AlterUserMessageHeader{
			UserEntity: &milvuspb.UserEntity{Name: credInfo.Username},
		}).
		WithBody(&message.AlterUserMessageBody{
			CredentialInfo: credInfo,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// checkUpdateCredential check if the credential can be updated.
func (c *Core) checkUpdateCredential(ctx context.Context, in *internalpb.CredentialInfo) error {
	if in.GetUsername() == "" {
		return errors.New("username is empty")
	}
	// check if the number of credential exists.
	if _, err := c.meta.GetCredential(ctx, in.GetUsername()); err != nil {
		return err
	}
	return nil
}

// alterUserV2AckCallback is the ack callback function for the AlterUserMessageV2 message.
func (c *DDLCallback) alterUserV2AckCallback(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error {
	// insert to db
	if err := c.meta.AlterCredential(ctx, result); err != nil {
		return err
	}
	// update proxy's local cache
	if err := c.UpdateCredCache(ctx, result.Message.MustBody().CredentialInfo); err != nil {
		return err
	}
	return nil
}

func (c *Core) broadcastDropCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest) error {
	if in.Username == "" {
		return errors.New("username is empty")
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// TODO: check if the credential can be dropped.

	msg := message.NewDropUserMessageBuilderV2().
		WithHeader(&message.DropUserMessageHeader{
			UserName: in.Username,
		}).
		WithBody(&message.DropUserMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// dropUserV2AckCallback is the ack callback function for the DeleteCredential message
func (c *DDLCallback) dropUserV2AckCallback(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error {
	// There should always be only one message in the msgs slice.
	if err := c.meta.DeleteCredential(ctx, result); err != nil {
		return err
	}
	if err := c.ExpireCredCache(ctx, result.Message.Header().UserName); err != nil {
		return err
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDeleteUser),
		OpKey:  result.Message.Header().UserName,
	}); err != nil {
		return err
	}
	return nil
}

func (c *Core) broadcastRestoreRBACV2(ctx context.Context, meta *milvuspb.RBACMeta) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	msg := message.NewRestoreRBACMessageBuilderV2().
		WithHeader(&message.RestoreRBACMessageHeader{}).
		WithBody(&message.RestoreRBACMessageBody{
			RbacMeta: meta,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) restoreRBACV2AckCallback(ctx context.Context, result message.BroadcastResultRestoreRBACMessageV2) error {
	return executeRestoreRBACTaskSteps(ctx, c.Core, result.Message.MustBody().RbacMeta)
}
