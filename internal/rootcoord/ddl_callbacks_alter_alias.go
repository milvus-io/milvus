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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	collection, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
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
	collection, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	// Resolve the alias's CURRENT (pre-alter) target under the same database
	// lock and carry it in the header, so the cache expiration can evict the old
	// target by id. The proxy cannot always resolve the old target itself: a
	// concurrent describe of the new target may have re-pointed its alias
	// resolution before the expiration arrives, which would leave the old
	// target's cached Aliases list permanently stale. Computing this at
	// broadcast time (not in the ack callback) keeps message replay
	// deterministic. Best-effort: 0 means unknown.
	var oldCollectionID int64
	if oldName, err := c.meta.DescribeAlias(ctx, req.GetDbName(), req.GetAlias(), typeutil.MaxTimestamp); err == nil && oldName != "" {
		if oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), oldName, typeutil.MaxTimestamp, false); err == nil {
			oldCollectionID = oldColl.CollectionID
		}
	}

	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(&message.AlterAliasMessageHeader{
			DbId:            db.ID,
			DbName:          req.GetDbName(),
			CollectionId:    collection.CollectionID,
			Alias:           req.GetAlias(),
			CollectionName:  req.GetCollectionName(),
			OldCollectionId: oldCollectionID,
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
	header := result.Message.Header()
	builder := ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(header.DbName),
		ce.OptLPCMCollectionName(header.Alias),
		// Forward the new target's collection id so the proxy also evicts the
		// canonical entry (and id index) of the collection now gaining the
		// alias, which the proxy cannot resolve from the alias name (the alias
		// did not point at it when it was cached). Without this, an id-only
		// Describe of the new target would serve a stale Aliases list.
		ce.OptLPCMCollectionID(header.CollectionId),
		ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias))
	if header.OldCollectionId != 0 && header.OldCollectionId != header.CollectionId {
		// Evict the OLD target by id too (see broadcastAlterAlias). A second
		// expiration entry with no collection name: the proxy's handler then
		// only runs the id eviction.
		builder = builder.WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMCollectionID(header.OldCollectionId),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias))
	}
	return c.ExpireCaches(ctx, builder)
}
