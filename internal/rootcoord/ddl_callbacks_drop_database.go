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
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastDropDatabase(ctx context.Context, req *milvuspb.DropDatabaseRequest) error {
	req.DbName = strings.TrimSpace(req.DbName)
	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.GetDbName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfDatabaseDroppable(ctx, req); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return errors.Wrap(err, "failed to get database name")
	}

	msg := message.NewDropDatabaseMessageBuilderV2().
		WithHeader(&message.DropDatabaseMessageHeader{
			DbName: req.GetDbName(),
			DbId:   db.ID,
		}).
		WithBody(&message.DropDatabaseMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) dropDatabaseV1AckCallback(ctx context.Context, result message.BroadcastResultDropDatabaseMessageV2) error {
	header := result.Message.Header()
	if err := c.meta.DropDatabase(ctx, header.DbName, result.GetControlChannelResult().TimeTick); err != nil {
		return errors.Wrap(err, "failed to drop database")
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMMsgType(commonpb.MsgType_DropDatabase),
		),
		result.GetControlChannelResult().TimeTick)
}
