package ce

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestCacheExpirationsBuilder(t *testing.T) {
	builder := NewBuilder()

	ce := builder.WithLegacyProxyCollectionMetaCache(
		OptLPCMDBName("test"),
		OptLPCMCollectionID(1),
		OptLPCMPartitionName("test"),
		OptLPCMMsgType(commonpb.MsgType_CreateCollection),
		OptLPCMCollectionName("test")).
		WithLegacyProxyCollectionMetaCache(
			OptLPCMDBName("test2"),
			OptLPCMCollectionID(2),
			OptLPCMPartitionName("test2p"),
			OptLPCMMsgType(commonpb.MsgType_AlterCollection),
			OptLPCMCollectionName("test2c"),
		).Build()

	assert.Equal(t, ce.CacheExpirations[0].GetLegacyProxyCollectionMetaCache().DbName, "test")
	assert.Equal(t, ce.CacheExpirations[0].GetLegacyProxyCollectionMetaCache().CollectionId, int64(1))
	assert.Equal(t, ce.CacheExpirations[0].GetLegacyProxyCollectionMetaCache().PartitionName, "test")
	assert.Equal(t, ce.CacheExpirations[0].GetLegacyProxyCollectionMetaCache().MsgType, commonpb.MsgType_CreateCollection)
	assert.Equal(t, ce.CacheExpirations[0].GetLegacyProxyCollectionMetaCache().CollectionName, "test")
	assert.Equal(t, ce.CacheExpirations[1].GetLegacyProxyCollectionMetaCache().DbName, "test2")
	assert.Equal(t, ce.CacheExpirations[1].GetLegacyProxyCollectionMetaCache().CollectionId, int64(2))
	assert.Equal(t, ce.CacheExpirations[1].GetLegacyProxyCollectionMetaCache().PartitionName, "test2p")
	assert.Equal(t, ce.CacheExpirations[1].GetLegacyProxyCollectionMetaCache().MsgType, commonpb.MsgType_AlterCollection)
	assert.Equal(t, ce.CacheExpirations[1].GetLegacyProxyCollectionMetaCache().CollectionName, "test2c")
}
