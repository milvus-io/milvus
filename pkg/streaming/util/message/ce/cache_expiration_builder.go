package ce

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func NewBuilder() *CacheExpirationsBuilder {
	return &CacheExpirationsBuilder{
		cacheExpirations: &message.CacheExpirations{
			CacheExpirations: make([]*message.CacheExpiration, 0, 1),
		},
	}
}

type CacheExpirationsBuilder struct {
	cacheExpirations *message.CacheExpirations
}

func (b *CacheExpirationsBuilder) WithLegacyProxyCollectionMetaCache(opts ...OptLegacyProxyCollectionMetaCache) *CacheExpirationsBuilder {
	lpcmc := &message.LegacyProxyCollectionMetaCache{}
	for _, opt := range opts {
		opt(lpcmc)
	}
	b.cacheExpirations.CacheExpirations = append(b.cacheExpirations.CacheExpirations, &message.CacheExpiration{
		Cache: &messagespb.CacheExpiration_LegacyProxyCollectionMetaCache{
			LegacyProxyCollectionMetaCache: lpcmc,
		},
	})
	return b
}

func (b *CacheExpirationsBuilder) Build() *message.CacheExpirations {
	return b.cacheExpirations
}
