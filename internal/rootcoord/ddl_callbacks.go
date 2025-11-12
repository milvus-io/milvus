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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(core *Core) {
	ddlCallback := &DDLCallback{
		Core: core,
	}

	ddlCallback.registerCollectionCallbacks()
	ddlCallback.registerPartitionCallbacks()
	ddlCallback.registerRBACCallbacks()
	ddlCallback.registerDatabaseCallbacks()
	ddlCallback.registerAliasCallbacks()
}

// registerRBACCallbacks registers the rbac callbacks.
func (c *DDLCallback) registerRBACCallbacks() {
	registry.RegisterAlterUserV2AckCallback(c.alterUserV2AckCallback)
	registry.RegisterDropUserV2AckCallback(c.dropUserV2AckCallback)
	registry.RegisterAlterRoleV2AckCallback(c.alterRoleV2AckCallback)
	registry.RegisterDropRoleV2AckCallback(c.dropRoleV2AckCallback)
	registry.RegisterAlterUserRoleV2AckCallback(c.alterUserRoleV2AckCallback)
	registry.RegisterDropUserRoleV2AckCallback(c.dropUserRoleV2AckCallback)
	registry.RegisterAlterPrivilegeV2AckCallback(c.alterPrivilegeV2AckCallback)
	registry.RegisterDropPrivilegeV2AckCallback(c.dropPrivilegeV2AckCallback)
	registry.RegisterAlterPrivilegeGroupV2AckCallback(c.alterPrivilegeGroupV2AckCallback)
	registry.RegisterDropPrivilegeGroupV2AckCallback(c.dropPrivilegeGroupV2AckCallback)
	registry.RegisterRestoreRBACV2AckCallback(c.restoreRBACV2AckCallback)
}

// registerDatabaseCallbacks registers the database callbacks.
func (c *DDLCallback) registerDatabaseCallbacks() {
	registry.RegisterCreateDatabaseV2AckCallback(c.createDatabaseV1AckCallback)
	registry.RegisterAlterDatabaseV2AckCallback(c.alterDatabaseV1AckCallback)
	registry.RegisterDropDatabaseV2AckCallback(c.dropDatabaseV1AckCallback)
}

// registerAliasCallbacks registers the alias callbacks.
func (c *DDLCallback) registerAliasCallbacks() {
	registry.RegisterAlterAliasV2AckCallback(c.alterAliasV2AckCallback)
	registry.RegisterDropAliasV2AckCallback(c.dropAliasV2AckCallback)
}

// registerCollectionCallbacks registers the collection callbacks.
func (c *DDLCallback) registerCollectionCallbacks() {
	registry.RegisterCreateCollectionV1AckCallback(c.createCollectionV1AckCallback)
	registry.RegisterAlterCollectionV2AckCallback(c.alterCollectionV2AckCallback)
	registry.RegisterDropCollectionV1AckCallback(c.dropCollectionV1AckCallback)
}

// registerPartitionCallbacks registers the partition callbacks.
func (c *DDLCallback) registerPartitionCallbacks() {
	registry.RegisterCreatePartitionV1AckCallback(c.createPartitionV1AckCallback)
	registry.RegisterDropPartitionV1AckCallback(c.dropPartitionV1AckCallback)
}

// DDLCallback is the callback of ddl.
type DDLCallback struct {
	*Core
}

// CacheExpirationsGetter is the getter of cache expirations.
type CacheExpirationsGetter interface {
	GetCacheExpirations() *message.CacheExpirations
}

// ExpireCaches handles the cache
func (c *DDLCallback) ExpireCaches(ctx context.Context, expirations any, timetick uint64) error {
	var cacheExpirations *message.CacheExpirations
	if g, ok := expirations.(CacheExpirationsGetter); ok {
		cacheExpirations = g.GetCacheExpirations()
	} else if g, ok := expirations.(*message.CacheExpirations); ok {
		cacheExpirations = g
	} else if g, ok := expirations.(*ce.CacheExpirationsBuilder); ok {
		cacheExpirations = g.Build()
	} else {
		panic(fmt.Sprintf("invalid getter type: %T", expirations))
	}
	for _, cacheExpiration := range cacheExpirations.CacheExpirations {
		if err := c.expireCache(ctx, cacheExpiration, timetick); err != nil {
			return err
		}
	}
	return nil
}

func (c *DDLCallback) expireCache(ctx context.Context, cacheExpiration *message.CacheExpiration, timetick uint64) error {
	switch cacheExpiration.Cache.(type) {
	case *messagespb.CacheExpiration_LegacyProxyCollectionMetaCache:
		legacyProxyCollectionMetaCache := cacheExpiration.GetLegacyProxyCollectionMetaCache()
		return c.Core.ExpireMetaCache(ctx, legacyProxyCollectionMetaCache.DbName, []string{legacyProxyCollectionMetaCache.CollectionName}, legacyProxyCollectionMetaCache.CollectionId, legacyProxyCollectionMetaCache.PartitionName, timetick, proxyutil.SetMsgType(legacyProxyCollectionMetaCache.MsgType))
	}
	return nil
}

// startBroadcastWithRBACLock starts a broadcast for rbac.
func startBroadcastWithRBACLock(ctx context.Context) (broadcaster.BroadcastAPI, error) {
	api, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return nil, errors.Wrap(err, "failed to start broadcast with rbac lock")
	}
	return api, nil
}

// startBroadcastWithDatabaseLock starts a broadcast with database lock.
func startBroadcastWithDatabaseLock(ctx context.Context, dbName string) (broadcaster.BroadcastAPI, error) {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveDBNameResourceKey(dbName))
	if err != nil {
		return nil, errors.Wrap(err, "failed to start broadcast with database lock")
	}
	return broadcaster, nil
}

// startBroadcastWithCollectionLock starts a broadcast with collection lock.
// CreateCollection and DropCollection can only be called with collection name itself, not alias.
// So it's safe to use collection name directly for those API.
func (*Core) startBroadcastWithCollectionLock(ctx context.Context, dbName string, collectionName string) (broadcaster.BroadcastAPI, error) {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start broadcast with collection lock")
	}
	return broadcaster, nil
}

// startBroadcastWithAliasOrCollectionLock starts a broadcast with alias or collection lock.
// Some API like AlterCollection can be called with alias or collection name,
// so we need to get the real collection name to add resource key lock.
func (c *Core) startBroadcastWithAliasOrCollectionLock(ctx context.Context, dbName string, collectionNameOrAlias string) (broadcaster.BroadcastAPI, error) {
	coll, err := c.meta.GetCollectionByName(ctx, dbName, collectionNameOrAlias, typeutil.MaxTimestamp)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get collection by name")
	}
	return c.startBroadcastWithCollectionLock(ctx, dbName, coll.Name)
}
