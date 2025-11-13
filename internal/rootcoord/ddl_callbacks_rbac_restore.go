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
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastRestoreRBACV2(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) error {
	broadcaster, err := startBroadcastWithRBACLock(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfRBACRestorable(ctx, req); err != nil {
		return errors.Wrap(err, "failed to check if rbac restorable")
	}

	msg := message.NewRestoreRBACMessageBuilderV2().
		WithHeader(&message.RestoreRBACMessageHeader{}).
		WithBody(&message.RestoreRBACMessageBody{
			RbacMeta: req.GetRBACMeta(),
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) restoreRBACV2AckCallback(ctx context.Context, result message.BroadcastResultRestoreRBACMessageV2) error {
	meta := result.Message.MustBody().RbacMeta
	if err := c.meta.RestoreRBAC(ctx, util.DefaultTenant, meta); err != nil {
		return errors.Wrap(err, "failed to restore rbac meta data")
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheRefresh),
	}); err != nil {
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	return nil
}
