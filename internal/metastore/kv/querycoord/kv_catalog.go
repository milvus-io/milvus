package querycoord

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util"
)

var (
	ErrInvalidKey = errors.New("invalid load info key")
)

const (
	CollectionLoadInfoPrefix = "querycoord-collection-loadinfo"
	PartitionLoadInfoPrefix  = "querycoord-partition-loadinfo"
	ReplicaPrefix            = "querycoord-replica"
	CollectionMetaPrefixV1   = "queryCoord-collectionMeta"
	ReplicaMetaPrefixV1      = "queryCoord-ReplicaMeta"
)

type WatchStoreChan = clientv3.WatchChan

type Catalog struct {
	cli kv.MetaKv
}

func NewCatalog(cli kv.MetaKv) Catalog {
	return Catalog{
		cli: cli,
	}
}

func (s Catalog) SaveCollection(info *querypb.CollectionLoadInfo) error {
	k := EncodeCollectionLoadInfoKey(info.GetCollectionID())
	v, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return s.cli.Save(k, string(v))
}

func (s Catalog) SavePartition(info ...*querypb.PartitionLoadInfo) error {
	kvs := make(map[string]string)
	for _, partition := range info {
		key := EncodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID())
		value, err := proto.Marshal(partition)
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}
	return s.cli.MultiSave(kvs)
}

func (s Catalog) SaveReplica(replica *querypb.Replica) error {
	key := EncodeReplicaKey(replica.GetCollectionID(), replica.GetID())
	value, err := proto.Marshal(replica)
	if err != nil {
		return err
	}
	return s.cli.Save(key, string(value))
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

func (s Catalog) ReleaseCollection(id int64) error {
	k := EncodeCollectionLoadInfoKey(id)
	return s.cli.Remove(k)
}

func (s Catalog) ReleasePartition(collection int64, partitions ...int64) error {
	keys := lo.Map(partitions, func(partition int64, _ int) string {
		return EncodePartitionLoadInfoKey(collection, partition)
	})
	return s.cli.MultiRemove(keys)
}

func (s Catalog) ReleaseReplicas(collectionID int64) error {
	key := EncodeCollectionReplicaKey(collectionID)
	return s.cli.RemoveWithPrefix(key)
}

func (s Catalog) ReleaseReplica(collection, replica int64) error {
	key := EncodeReplicaKey(collection, replica)
	return s.cli.Remove(key)
}

func (s Catalog) RemoveHandoffEvent(info *querypb.SegmentInfo) error {
	key := EncodeHandoffEventKey(info.CollectionID, info.PartitionID, info.SegmentID)
	return s.cli.Remove(key)
}

func EncodeCollectionLoadInfoKey(collection int64) string {
	return fmt.Sprintf("%s/%d", CollectionLoadInfoPrefix, collection)
}

func EncodePartitionLoadInfoKey(collection, partition int64) string {
	return fmt.Sprintf("%s/%d/%d", PartitionLoadInfoPrefix, collection, partition)
}

func EncodeReplicaKey(collection, replica int64) string {
	return fmt.Sprintf("%s/%d/%d", ReplicaPrefix, collection, replica)
}

func EncodeCollectionReplicaKey(collection int64) string {
	return fmt.Sprintf("%s/%d", ReplicaPrefix, collection)
}

func EncodeHandoffEventKey(collection, partition, segment int64) string {
	return fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, collection, partition, segment)
}
