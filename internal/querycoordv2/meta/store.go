package meta

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
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

// Store is used to save and get from object storage.
type Store interface {
	metastore.QueryCoordCatalog
	WatchHandoffEvent(revision int64) WatchStoreChan
	LoadHandoffWithRevision() ([]string, []string, int64, error)
}

type metaStore struct {
	cli kv.MetaKv
}

func NewMetaStore(cli kv.MetaKv) metaStore {
	return metaStore{
		cli: cli,
	}
}

func (s metaStore) SaveCollection(info *querypb.CollectionLoadInfo) error {
	k := encodeCollectionLoadInfoKey(info.GetCollectionID())
	v, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return s.cli.Save(k, string(v))
}

func (s metaStore) SavePartition(info ...*querypb.PartitionLoadInfo) error {
	kvs := make(map[string]string)
	for _, partition := range info {
		key := encodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID())
		value, err := proto.Marshal(partition)
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}
	return s.cli.MultiSave(kvs)
}

func (s metaStore) SaveReplica(replica *querypb.Replica) error {
	key := encodeReplicaKey(replica.GetCollectionID(), replica.GetID())
	value, err := proto.Marshal(replica)
	if err != nil {
		return err
	}
	return s.cli.Save(key, string(value))
}

func (s metaStore) GetCollections() ([]*querypb.CollectionLoadInfo, error) {
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

	collectionsV1, err := s.getCollectionsFromV1()
	if err != nil {
		return nil, err
	}
	ret = append(ret, collectionsV1...)

	return ret, nil
}

// getCollectionsFromV1 recovers collections from 2.1 meta store
func (s metaStore) getCollectionsFromV1() ([]*querypb.CollectionLoadInfo, error) {
	_, collectionValues, err := s.cli.LoadWithPrefix(CollectionMetaPrefixV1)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.CollectionLoadInfo, 0, len(collectionValues))
	for _, value := range collectionValues {
		collectionInfo := querypb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &collectionInfo)
		if err != nil {
			return nil, err
		}
		if collectionInfo.LoadType != querypb.LoadType_LoadCollection {
			continue
		}
		ret = append(ret, &querypb.CollectionLoadInfo{
			CollectionID:       collectionInfo.GetCollectionID(),
			ReleasedPartitions: collectionInfo.GetReleasedPartitionIDs(),
			ReplicaNumber:      collectionInfo.GetReplicaNumber(),
			Status:             querypb.LoadStatus_Loaded,
		})
	}
	return ret, nil
}

func (s metaStore) GetPartitions() (map[int64][]*querypb.PartitionLoadInfo, error) {
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

	partitionsV1, err := s.getPartitionsFromV1()
	if err != nil {
		return nil, err
	}
	for _, partition := range partitionsV1 {
		ret[partition.GetCollectionID()] = append(ret[partition.GetCollectionID()], partition)
	}

	return ret, nil
}

// getCollectionsFromV1 recovers collections from 2.1 meta store
func (s metaStore) getPartitionsFromV1() ([]*querypb.PartitionLoadInfo, error) {
	_, collectionValues, err := s.cli.LoadWithPrefix(CollectionMetaPrefixV1)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.PartitionLoadInfo, 0, len(collectionValues))
	for _, value := range collectionValues {
		collectionInfo := querypb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &collectionInfo)
		if err != nil {
			return nil, err
		}
		if collectionInfo.LoadType != querypb.LoadType_LoadPartition {
			continue
		}

		for _, partition := range collectionInfo.GetPartitionIDs() {
			ret = append(ret, &querypb.PartitionLoadInfo{
				CollectionID:  collectionInfo.GetCollectionID(),
				PartitionID:   partition,
				ReplicaNumber: collectionInfo.GetReplicaNumber(),
				Status:        querypb.LoadStatus_Loaded,
			})
		}
	}
	return ret, nil
}

func (s metaStore) GetReplicas() ([]*querypb.Replica, error) {
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

func (s metaStore) getReplicasFromV1() ([]*querypb.Replica, error) {
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

func (s metaStore) ReleaseCollection(id int64) error {
	k := encodeCollectionLoadInfoKey(id)
	return s.cli.Remove(k)
}

func (s metaStore) ReleasePartition(collection int64, partitions ...int64) error {
	keys := lo.Map(partitions, func(partition int64, _ int) string {
		return encodePartitionLoadInfoKey(collection, partition)
	})
	return s.cli.MultiRemove(keys)
}

func (s metaStore) ReleaseReplicas(collectionID int64) error {
	key := encodeCollectionReplicaKey(collectionID)
	return s.cli.RemoveWithPrefix(key)
}

func (s metaStore) ReleaseReplica(collection, replica int64) error {
	key := encodeReplicaKey(collection, replica)
	return s.cli.Remove(key)
}

func (s metaStore) WatchHandoffEvent(revision int64) WatchStoreChan {
	return s.cli.WatchWithRevision(util.HandoffSegmentPrefix, revision)
}

func (s metaStore) RemoveHandoffEvent(info *querypb.SegmentInfo) error {
	key := encodeHandoffEventKey(info.CollectionID, info.PartitionID, info.SegmentID)
	return s.cli.Remove(key)
}

func (s metaStore) LoadHandoffWithRevision() ([]string, []string, int64, error) {
	return s.cli.LoadWithRevision(util.HandoffSegmentPrefix)
}

func encodeCollectionLoadInfoKey(collection int64) string {
	return fmt.Sprintf("%s/%d", CollectionLoadInfoPrefix, collection)
}

func encodePartitionLoadInfoKey(collection, partition int64) string {
	return fmt.Sprintf("%s/%d/%d", PartitionLoadInfoPrefix, collection, partition)
}

func encodeReplicaKey(collection, replica int64) string {
	return fmt.Sprintf("%s/%d/%d", ReplicaPrefix, collection, replica)
}

func encodeCollectionReplicaKey(collection int64) string {
	return fmt.Sprintf("%s/%d", ReplicaPrefix, collection)
}

func encodeHandoffEventKey(collection, partition, segment int64) string {
	return fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, collection, partition, segment)
}
