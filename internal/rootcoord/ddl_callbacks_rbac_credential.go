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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastAlterUserForCreateCredential broadcasts the alter user message for create credential.
func (c *Core) broadcastAlterUserForCreateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	credInfo.Username = strings.TrimSpace(credInfo.Username)
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAddCredential(ctx, credInfo); err != nil {
		return errors.Wrap(err, "failed to check if add credential")
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

// broadcastAlterUserForUpdateCredential broadcasts the alter user message for update credential.
func (c *Core) broadcastAlterUserForUpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	credInfo.Username = strings.TrimSpace(credInfo.Username)
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfUpdateCredential(ctx, credInfo); err != nil {
		return errors.Wrap(err, "failed to check if update credential")
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

// alterUserV2AckCallback is the ack callback function for the AlterUserMessageV2 message.
func (c *DDLCallback) alterUserV2AckCallback(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error {
	// insert to db
	if err := c.meta.AlterCredential(ctx, result); err != nil {
		return errors.Wrap(err, "failed to alter credential")
	}
	// update proxy's local cache
	if err := c.UpdateCredCache(ctx, result.Message.MustBody().CredentialInfo); err != nil {
		return errors.Wrap(err, "failed to update cred cache")
	}
	return nil
}

// broadcastDropUserForDeleteCredential broadcasts the drop user message for delete credential.
func (c *Core) broadcastDropUserForDeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest) error {
	in.Username = strings.TrimSpace(in.Username)
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfDeleteCredential(ctx, in); err != nil {
		return errors.Wrap(err, "failed to check if delete credential")
	}

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
	if err := c.meta.DeleteCredential(ctx, result); err != nil {
		return errors.Wrap(err, "failed to delete credential")
	}
	if err := c.ExpireCredCache(ctx, result.Message.Header().UserName); err != nil {
		return errors.Wrap(err, "failed to expire cred cache")
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDeleteUser),
		OpKey:  result.Message.Header().UserName,
	}); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}
