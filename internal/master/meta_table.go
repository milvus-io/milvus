package master

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

type metaTable struct {
	client     kv.Base                          // client of a reliable kv service, i.e. etcd client
	rootPath   string                           // this metaTable's working root path on the reliable kv service
	tenantMeta map[int64]etcdpb.TenantMeta      // tenant id to tenant meta
	proxyMeta  map[int64]etcdpb.ProxyMeta       // proxy id to proxy meta
	collMeta   map[int64]etcdpb.CollectionMeta // collection id to collection meta
	segMeta    map[int64]etcdpb.SegmentMeta     // segment id to segment meta
}

func (mt *metaTable) GetCollectionByName(collectionName string) (*etcdpb.CollectionMeta, error) {
	for _, v := range mt.collMeta {
		if v.Schema.Name == collectionName {
			return &v, nil
		}
	}

	return nil, errors.New("Cannot found collection: " + collectionName)
}

func (mt *metaTable) HasPartition(partitionTag, collectionName string) bool {
	var hasPartition = false

	for _, v := range mt.collMeta {
		if v.Schema.Name == collectionName {
			for _, tag := range v.PartitionTags {
				if tag == partitionTag {
					hasPartition = true
				}
			}
		}
	}

	return hasPartition
}

func (mt *metaTable) DeletePartition(partitionTag, collectionName string) error {
	var tmpPartitionTags = make([]string, 0)
	var hasPartition = false

	for _, v := range mt.collMeta {
		if v.Schema.Name == collectionName {
			for _, tag := range v.PartitionTags {
				if tag == partitionTag {
					hasPartition = true
				} else {
					tmpPartitionTags = append(tmpPartitionTags, tag)
				}
			}
			if !hasPartition {
				return errors.New("Cannot found partition: " + partitionTag + " in collection: " + collectionName)
			} else {
				v.PartitionTags = tmpPartitionTags
				return nil
			}
		}
	}

	return errors.New("Cannot found collection: " + collectionName)
}
