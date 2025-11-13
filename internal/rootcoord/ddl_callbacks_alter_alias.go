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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) error {
	req.DbName = strings.TrimSpace(req.DbName)
	req.Alias = strings.TrimSpace(req.Alias)
	req.CollectionName = strings.TrimSpace(req.CollectionName)
	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.GetDbName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAliasCreatable(ctx, req.GetDbName(), req.GetAlias(), req.GetCollectionName()); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	collection, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(&message.AlterAliasMessageHeader{
			DbId:           db.ID,
			DbName:         req.GetDbName(),
			CollectionId:   collection.CollectionID,
			Alias:          req.GetAlias(),
			CollectionName: req.GetCollectionName(),
		}).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastAlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) error {
	req.DbName = strings.TrimSpace(req.DbName)
	req.Alias = strings.TrimSpace(req.Alias)
	req.CollectionName = strings.TrimSpace(req.CollectionName)
	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.GetDbName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAliasAlterable(ctx, req.GetDbName(), req.GetAlias(), req.GetCollectionName()); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	collection, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(&message.AlterAliasMessageHeader{
			DbId:           db.ID,
			DbName:         req.GetDbName(),
			CollectionId:   collection.CollectionID,
			Alias:          req.GetAlias(),
			CollectionName: req.GetCollectionName(),
		}).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) alterAliasV2AckCallback(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error {
	if err := c.meta.AlterAlias(ctx, result); err != nil {
		return err
	}
	return c.ExpireCaches(ctx,
		ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(result.Message.Header().DbName),
			ce.OptLPCMCollectionName(result.Message.Header().Alias),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias)),
		result.GetControlChannelResult().TimeTick)
}
