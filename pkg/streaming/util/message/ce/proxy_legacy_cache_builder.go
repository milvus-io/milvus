package ce

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

type OptLegacyProxyCollectionMetaCache func(*messagespb.LegacyProxyCollectionMetaCache)

func OptLPCMPartitionName(partitionName string) OptLegacyProxyCollectionMetaCache {
	return func(lpcmc *messagespb.LegacyProxyCollectionMetaCache) {
		lpcmc.PartitionName = partitionName
	}
}

func OptLPCMMsgType(msgType commonpb.MsgType) OptLegacyProxyCollectionMetaCache {
	return func(lpcmc *messagespb.LegacyProxyCollectionMetaCache) {
		lpcmc.MsgType = msgType
	}
}

func OptLPCMCollectionName(collectionName string) OptLegacyProxyCollectionMetaCache {
	return func(lpcmc *messagespb.LegacyProxyCollectionMetaCache) {
		lpcmc.CollectionName = collectionName
	}
}

func OptLPCMCollectionID(collectionID int64) OptLegacyProxyCollectionMetaCache {
	return func(lpcmc *messagespb.LegacyProxyCollectionMetaCache) {
		lpcmc.CollectionId = collectionID
	}
}

func OptLPCMDBName(dbName string) OptLegacyProxyCollectionMetaCache {
	return func(lpcmc *messagespb.LegacyProxyCollectionMetaCache) {
		lpcmc.DbName = dbName
	}
}
