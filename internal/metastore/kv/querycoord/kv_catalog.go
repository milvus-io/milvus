package querycoord

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
)

var ErrInvalidKey = errors.New("invalid load info key")

const (
	CollectionLoadInfoPrefix = "querycoord-collection-loadinfo"
	PartitionLoadInfoPrefix  = "querycoord-partition-loadinfo"
	ReplicaPrefix            = "querycoord-replica"
	CollectionMetaPrefixV1   = "queryCoord-collectionMeta"
	ReplicaMetaPrefixV1      = "queryCoord-ReplicaMeta"
	ResourceGroupPrefix      = "queryCoord-ResourceGroup"
)

type Catalog struct {
	cli    kv.MetaKv
	Col2Db *StrCache
}

func NewCatalog(cli kv.MetaKv) Catalog {
	return Catalog{
		cli:    cli,
		Col2Db: NewStrCache(cli),
	}
}

func (s Catalog) SaveCollection(collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error {
	dbID := s.Col2Db.get(fmt.Sprintf("%d", collection.GetCollectionID()))
	k := EncodeCollectionLoadInfoKey(collection.GetCollectionID(), s.cli.GetType(), dbID)
	v, err := proto.Marshal(collection)
	if err != nil {
		return err
	}
	err = s.cli.Save(k, string(v))
	if err != nil {
		return err
	}
	return s.SavePartition(partitions...)
}

func (s Catalog) SavePartition(info ...*querypb.PartitionLoadInfo) error {
	for _, partition := range info {
		dbID := s.Col2Db.get(fmt.Sprintf("%d", partition.GetCollectionID()))
		k := EncodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID(), s.cli.GetType(), dbID)
		v, err := proto.Marshal(partition)
		if err != nil {
			return err
		}
		err = s.cli.Save(k, string(v))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s Catalog) SaveReplica(replica *querypb.Replica) error {
	dbID := s.Col2Db.get(fmt.Sprintf("%d", replica.GetCollectionID()))

	key := encodeReplicaKey(replica.GetCollectionID(), replica.GetID(), s.cli.GetType(), dbID)
	value, err := proto.Marshal(replica)
	if err != nil {
		return err
	}
	return s.cli.Save(key, string(value))
}

func (s Catalog) SaveResourceGroup(rgs ...*querypb.ResourceGroup) error {
	ret := make(map[string]string)
	for _, rg := range rgs {
		key := encodeResourceGroupKey(rg.GetName())
		value, err := proto.Marshal(rg)
		if err != nil {
			return err
		}

		ret[key] = string(value)
	}

	return s.cli.MultiSave(ret)
}

func (s Catalog) RemoveResourceGroup(rgName string) error {
	key := encodeResourceGroupKey(rgName)
	return s.cli.Remove(key)
}

func (s Catalog) GetCollections() ([]*querypb.CollectionLoadInfo, error) {
	_, values, err := s.cli.LoadWithPrefix(CollectionLoadInfoPrefix)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.CollectionLoadInfo, 0, len(values))
	for _, v := range values {
		info := querypb.CollectionLoadInfo{}
		if err := proto.Unmarshal([]byte(v), &info); err != nil {
			return nil, err
		}
		ret = append(ret, &info)
	}

	return ret, nil
}

func (s Catalog) GetPartitions() (map[int64][]*querypb.PartitionLoadInfo, error) {
	_, values, err := s.cli.LoadWithPrefix(PartitionLoadInfoPrefix)
	if err != nil {
		return nil, err
	}
	ret := make(map[int64][]*querypb.PartitionLoadInfo)
	for _, v := range values {
		info := querypb.PartitionLoadInfo{}
		if err := proto.Unmarshal([]byte(v), &info); err != nil {
			return nil, err
		}
		ret[info.GetCollectionID()] = append(ret[info.GetCollectionID()], &info)
	}

	return ret, nil
}

func (s Catalog) GetReplicas() ([]*querypb.Replica, error) {
	_, values, err := s.cli.LoadWithPrefix(ReplicaPrefix)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.Replica, 0, len(values))
	for _, v := range values {
		info := querypb.Replica{}
		if err := proto.Unmarshal([]byte(v), &info); err != nil {
			return nil, err
		}
		ret = append(ret, &info)
	}

	replicasV1, err := s.getReplicasFromV1()
	if err != nil {
		return nil, err
	}
	ret = append(ret, replicasV1...)

	return ret, nil
}

func (s Catalog) getReplicasFromV1() ([]*querypb.Replica, error) {
	_, replicaValues, err := s.cli.LoadWithPrefix(ReplicaMetaPrefixV1)
	if err != nil {
		return nil, err
	}

	ret := make([]*querypb.Replica, 0, len(replicaValues))
	for _, value := range replicaValues {
		replicaInfo := milvuspb.ReplicaInfo{}
		err = proto.Unmarshal([]byte(value), &replicaInfo)
		if err != nil {
			return nil, err
		}

		ret = append(ret, &querypb.Replica{
			ID:           replicaInfo.GetReplicaID(),
			CollectionID: replicaInfo.GetCollectionID(),
			Nodes:        replicaInfo.GetNodeIds(),
		})
	}
	return ret, nil
}

func (s Catalog) GetResourceGroups() ([]*querypb.ResourceGroup, error) {
	_, rgs, err := s.cli.LoadWithPrefix(ResourceGroupPrefix)
	if err != nil {
		return nil, err
	}

	ret := make([]*querypb.ResourceGroup, 0, len(rgs))
	for _, value := range rgs {
		rg := &querypb.ResourceGroup{}
		err := proto.Unmarshal([]byte(value), rg)
		if err != nil {
			return nil, err
		}

		ret = append(ret, rg)
	}
	return ret, nil
}

func (s Catalog) ReleaseCollection(collection int64) error {
	// obtain partitions of this collection
	_, values, err := s.cli.LoadWithPrefix(fmt.Sprintf("%s/%d", PartitionLoadInfoPrefix, collection))
	if err != nil {
		return err
	}
	partitions := make([]*querypb.PartitionLoadInfo, 0)
	for _, v := range values {
		info := querypb.PartitionLoadInfo{}
		if err = proto.Unmarshal([]byte(v), &info); err != nil {
			return err
		}
		partitions = append(partitions, &info)
	}
	// remove collection and obtained partitions
	keys := lo.Map(partitions, func(partition *querypb.PartitionLoadInfo, _ int) string {
		dbID := s.Col2Db.get(fmt.Sprintf("%d", partition.GetCollectionID()))
		return EncodePartitionLoadInfoKey(collection, partition.GetPartitionID(), s.cli.GetType(), dbID)
	})
	dbID := s.Col2Db.get(fmt.Sprintf("%d", collection))
	k := EncodeCollectionLoadInfoKey(collection, s.cli.GetType(), dbID)
	keys = append(keys, k)
	return s.cli.MultiRemove(keys)
}

func (s Catalog) ReleasePartition(collection int64, partitions ...int64) error {
	keys := lo.Map(partitions, func(partition int64, _ int) string {
		dbID := s.Col2Db.get(fmt.Sprintf("%d", collection))
		return EncodePartitionLoadInfoKey(collection, partition, s.cli.GetType(), dbID)
	})
	return s.cli.MultiRemove(keys)
}

func (s Catalog) ReleaseReplicas(collectionID int64) error {
	dbID := s.Col2Db.get(fmt.Sprintf("%d", collectionID))
	key := encodeCollectionReplicaKey(collectionID, s.cli.GetType(), dbID)
	return s.cli.RemoveWithPrefix(key)
}

func (s Catalog) ReleaseReplica(collection, replica int64) error {
	dbID := s.Col2Db.get(fmt.Sprintf("%d", collection))

	key := encodeReplicaKey(collection, replica, s.cli.GetType(), dbID)
	return s.cli.Remove(key)
}

func EncodeCollectionLoadInfoKey(collection int64, metatype string, dbID string) string {
	if metatype == util.MetaStoreTypeEtcd {
		return fmt.Sprintf("%s/%s/%d", CollectionLoadInfoPrefix, dbID, collection)
	}
	return fmt.Sprintf("%s/%d", CollectionLoadInfoPrefix, collection)
}

func EncodePartitionLoadInfoKey(collection, partition int64, metatype string, dbID string) string {
	if metatype == util.MetaStoreTypeEtcd {
		return fmt.Sprintf("%s/%s/%d/%d", PartitionLoadInfoPrefix, dbID, collection, partition)
	}
	return fmt.Sprintf("%s/%d/%d", PartitionLoadInfoPrefix, collection, partition)
}

func encodeReplicaKey(collection, replica int64, metatype string, dbID string) string {
	if metatype == util.MetaStoreTypeEtcd {
		return fmt.Sprintf("%s/%s/%d/%d", ReplicaPrefix, dbID, collection, replica)
	}
	return fmt.Sprintf("%s/%d/%d", ReplicaPrefix, collection, replica)

}

func encodeCollectionReplicaKey(collection int64, metatype string, dbID string) string {
	if metatype == util.MetaStoreTypeEtcd {
		return fmt.Sprintf("%s/%s/%d", ReplicaPrefix, dbID, collection)
	}
	return fmt.Sprintf("%s/%d", ReplicaPrefix, collection)
}

func encodeResourceGroupKey(rgName string) string {
	return fmt.Sprintf("%s/%s", ResourceGroupPrefix, rgName)
}

type StrCache struct {
	mu    sync.Mutex
	kv    kv.MetaKv
	cache map[string]string
}

func NewStrCache(kv kv.MetaKv) *StrCache {
	new_c := &StrCache{
		cache: make(map[string]string),
		kv:    kv,
	}
	new_c.update()
	return new_c
}

func (c *StrCache) get(key string) string {
	log.Warn("Getting col-db for: ", zap.String("col_id", key))
	c.mu.Lock()
	defer c.mu.Unlock()
	val, ok := c.cache[key]
	if ok == false {
		c.update()
		val, ok = c.cache[key]
		if ok == false {
			panic("Unable to find ColID -> DBID map")
		}
		return val
	}
	return val
}

func (c *StrCache) update() {
	// If we need to grab the nondb collections
	new_cache := map[string]string{}
	keys, _, err := c.kv.LoadWithPrefix(rootcoord.CollectionMetaPrefix)
	if err != nil {
		return
	}
	for _, key := range keys {
		split_key := strings.Split(key, "/")
		if len(split_key) == 3 { // Current keys in ColMetaPre are 3 long
			colID := split_key[len(split_key)-1]
			new_cache[colID] = fmt.Sprintf("%d", util.NonDBID)
		} else {
			log.Warn("unclean split for old keys", zap.Int("len", len(split_key)))
		}
	}

	keys, _, err = c.kv.LoadWithPrefix(rootcoord.CollectionInfoMetaPrefix)
	if err != nil {
		return
	}
	for _, key := range keys {
		split_key := strings.Split(key, "/")
		if len(split_key) == 7 { // Current keys in ColInfoMetaPre are 6 long
			colID := split_key[len(split_key)-1]
			dbID := split_key[len(split_key)-2]
			new_cache[colID] = dbID
		} else {
			log.Warn("unclean split for new keys", zap.Int("len", len(split_key)))
		}
	}
	c.cache = new_cache
}
