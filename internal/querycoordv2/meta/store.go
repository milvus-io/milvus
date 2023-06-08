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

package meta

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	ResourceGroupPrefix      = "queryCoord-ResourceGroup"
)

type WatchStoreChan = clientv3.WatchChan

// Store is used to save and get from object storage.
type Store interface {
	metastore.QueryCoordCatalog
}

type metaStore struct {
	cli kv.MetaKv
}

func NewMetaStore(cli kv.MetaKv) metaStore {
	return metaStore{
		cli: cli,
	}
}

func (s metaStore) SaveCollection(collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error {
	k := encodeCollectionLoadInfoKey(collection.GetCollectionID())
	v, err := proto.Marshal(collection)
	if err != nil {
		return err
	}
	kvs := make(map[string]string)
	for _, partition := range partitions {
		key := encodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID())
		value, err := proto.Marshal(partition)
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}
	kvs[k] = string(v)
	return s.cli.MultiSave(kvs)
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

func (s metaStore) SaveResourceGroup(rgs ...*querypb.ResourceGroup) error {
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

func (s metaStore) RemoveResourceGroup(rgName string) error {
	key := encodeResourceGroupKey(rgName)
	return s.cli.Remove(key)
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

func (s metaStore) GetResourceGroups() ([]*querypb.ResourceGroup, error) {
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

func (s metaStore) ReleaseCollection(collection int64) error {
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
		return encodePartitionLoadInfoKey(collection, partition.GetPartitionID())
	})
	k := encodeCollectionLoadInfoKey(collection)
	keys = append(keys, k)
	return s.cli.MultiRemove(keys)
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

func encodeResourceGroupKey(rgName string) string {
	return fmt.Sprintf("%s/%s", ResourceGroupPrefix, rgName)
}
