package rootcoord

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
)

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(core *Core) {
	ddlCallback := &DDLCallback{
		Core: core,
	}
	// RBAC
	ddlCallback.registerRBACCallbacks()
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
