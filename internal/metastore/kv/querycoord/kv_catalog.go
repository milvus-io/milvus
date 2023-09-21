package querycoord

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	cli kv.MetaKv
}

func NewCatalog(cli kv.MetaKv) Catalog {
	return Catalog{
		cli: cli,
	}
}

func (s Catalog) SaveCollection(collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error {
	k := EncodeCollectionLoadInfoKey(collection.GetCollectionID())
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
		k := EncodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID())
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
	key := encodeReplicaKey(replica.GetCollectionID(), replica.GetID())
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
		return EncodePartitionLoadInfoKey(collection, partition.GetPartitionID())
	})
	k := EncodeCollectionLoadInfoKey(collection)
	keys = append(keys, k)
	return s.cli.MultiRemove(keys)
}

func (s Catalog) ReleasePartition(collection int64, partitions ...int64) error {
	keys := lo.Map(partitions, func(partition int64, _ int) string {
		return EncodePartitionLoadInfoKey(collection, partition)
	})
	return s.cli.MultiRemove(keys)
}

func (s Catalog) ReleaseReplicas(collectionID int64) error {
	key := encodeCollectionReplicaKey(collectionID)
	return s.cli.RemoveWithPrefix(key)
}

func (s Catalog) ReleaseReplica(collection, replica int64) error {
	key := encodeReplicaKey(collection, replica)
	return s.cli.Remove(key)
}

func EncodeCollectionLoadInfoKey(collection int64) string {
	return fmt.Sprintf("%s/%d", CollectionLoadInfoPrefix, collection)
}

func EncodePartitionLoadInfoKey(collection, partition int64) string {
	return fmt.Sprintf("%s/%d/%d", PartitionLoadInfoPrefix, collection, partition)
}

func encodeReplicaKey(collection, replica int64) string {
	return fmt.Sprintf("%s/%d/%d", ReplicaPrefix, collection, replica)
}

func encodeCollectionReplicaKey(collection int64) string {
	return fmt.Sprintf("%s/%d", ReplicaPrefix, collection)
}

func encodeResourceGroupKey(rgName string) string {
	return fmt.Sprintf("%s/%s", ResourceGroupPrefix, rgName)
}
