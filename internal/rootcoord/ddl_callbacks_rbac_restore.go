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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	restoreErr := c.meta.RestoreRBAC(ctx, util.DefaultTenant, meta)
	// On partial skip, on-disk state is already consistent and all non-skipped
	// grants have been applied — the proxy cache still needs to be refreshed
	// so the persisted grants take effect. Surface the skip error to the caller
	// afterwards so they can decide whether to retry.
	partialSkip := IsRestoreRBACPartialSkip(restoreErr)
	if restoreErr != nil && !partialSkip {
		return errors.Wrap(restoreErr, "failed to restore rbac meta data")
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheRefresh),
	}); err != nil {
		if partialSkip {
			log.Ctx(ctx).Warn("restore rbac partially skipped and failed to refresh policy cache",
				zap.Error(restoreErr), zap.Error(err))
		}
		return errors.Wrap(err, "failed to refresh policy info cache")
	}
	if partialSkip {
		return errors.Wrap(restoreErr, "rbac restore partially applied")
	}
	return nil
}
