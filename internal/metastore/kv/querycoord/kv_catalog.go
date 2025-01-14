package querycoord

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/compressor"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var ErrInvalidKey = errors.New("invalid load info key")

const (
	CollectionLoadInfoPrefix = "querycoord-collection-loadinfo"
	PartitionLoadInfoPrefix  = "querycoord-partition-loadinfo"
	ReplicaPrefix            = "querycoord-replica"
	CollectionMetaPrefixV1   = "queryCoord-collectionMeta"
	ReplicaMetaPrefixV1      = "queryCoord-ReplicaMeta"
	ResourceGroupPrefix      = "queryCoord-ResourceGroup"

	MetaOpsBatchSize       = 128
	CollectionTargetPrefix = "queryCoord-Collection-Target"
)

type Catalog struct {
	cli            kv.MetaKv
	paginationSize int
}

func NewCatalog(cli kv.MetaKv) Catalog {
	return Catalog{
		cli:            cli,
		paginationSize: paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt(),
	}
}

func (s Catalog) SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error {
	k := EncodeCollectionLoadInfoKey(collection.GetCollectionID())
	v, err := proto.Marshal(collection)
	if err != nil {
		return err
	}
	err = s.cli.Save(ctx, k, string(v))
	if err != nil {
		return err
	}
	return s.SavePartition(ctx, partitions...)
}

func (s Catalog) SavePartition(ctx context.Context, info ...*querypb.PartitionLoadInfo) error {
	for _, partition := range info {
		k := EncodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID())
		v, err := proto.Marshal(partition)
		if err != nil {
			return err
		}
		err = s.cli.Save(ctx, k, string(v))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s Catalog) SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error {
	kvs := make(map[string]string)
	for _, replica := range replicas {
		key := encodeReplicaKey(replica.GetCollectionID(), replica.GetID())
		value, err := proto.Marshal(replica)
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}
	return s.cli.MultiSave(ctx, kvs)
}

func (s Catalog) SaveResourceGroup(ctx context.Context, rgs ...*querypb.ResourceGroup) error {
	ret := make(map[string]string)
	for _, rg := range rgs {
		key := encodeResourceGroupKey(rg.GetName())
		value, err := proto.Marshal(rg)
		if err != nil {
			return err
		}

		ret[key] = string(value)
	}

	return s.cli.MultiSave(ctx, ret)
}

func (s Catalog) RemoveResourceGroup(ctx context.Context, rgName string) error {
	key := encodeResourceGroupKey(rgName)
	return s.cli.Remove(ctx, key)
}

func (s Catalog) GetCollections(ctx context.Context) ([]*querypb.CollectionLoadInfo, error) {
	ret := make([]*querypb.CollectionLoadInfo, 0)
	applyFn := func(key []byte, value []byte) error {
		info := querypb.CollectionLoadInfo{}
		if err := proto.Unmarshal(value, &info); err != nil {
			return err
		}
		ret = append(ret, &info)
		return nil
	}

	err := s.cli.WalkWithPrefix(ctx, CollectionLoadInfoPrefix, s.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (s Catalog) GetPartitions(ctx context.Context) (map[int64][]*querypb.PartitionLoadInfo, error) {
	ret := make(map[int64][]*querypb.PartitionLoadInfo)
	applyFn := func(key []byte, value []byte) error {
		info := querypb.PartitionLoadInfo{}
		if err := proto.Unmarshal(value, &info); err != nil {
			return err
		}
		ret[info.GetCollectionID()] = append(ret[info.GetCollectionID()], &info)
		return nil
	}

	err := s.cli.WalkWithPrefix(ctx, PartitionLoadInfoPrefix, s.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (s Catalog) GetReplicas(ctx context.Context) ([]*querypb.Replica, error) {
	ret := make([]*querypb.Replica, 0)
	applyFn := func(key []byte, value []byte) error {
		info := querypb.Replica{}
		if err := proto.Unmarshal(value, &info); err != nil {
			return err
		}
		ret = append(ret, &info)
		return nil
	}

	err := s.cli.WalkWithPrefix(ctx, ReplicaPrefix, s.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	replicasV1, err := s.getReplicasFromV1(ctx)
	if err != nil {
		return nil, err
	}
	ret = append(ret, replicasV1...)

	return ret, nil
}

func (s Catalog) getReplicasFromV1(ctx context.Context) ([]*querypb.Replica, error) {
	_, replicaValues, err := s.cli.LoadWithPrefix(ctx, ReplicaMetaPrefixV1)
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

func (s Catalog) GetResourceGroups(ctx context.Context) ([]*querypb.ResourceGroup, error) {
	_, rgs, err := s.cli.LoadWithPrefix(ctx, ResourceGroupPrefix)
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

func (s Catalog) ReleaseCollection(ctx context.Context, collection int64) error {
	// remove collection and obtained partitions
	collectionKey := EncodeCollectionLoadInfoKey(collection)
	err := s.cli.Remove(ctx, collectionKey)
	if err != nil {
		return err
	}
	partitionsPrefix := fmt.Sprintf("%s/%d", PartitionLoadInfoPrefix, collection)
	return s.cli.RemoveWithPrefix(ctx, partitionsPrefix)
}

func (s Catalog) ReleasePartition(ctx context.Context, collection int64, partitions ...int64) error {
	keys := lo.Map(partitions, func(partition int64, _ int) string {
		return EncodePartitionLoadInfoKey(collection, partition)
	})
	if len(partitions) >= MetaOpsBatchSize {
		index := 0
		for index < len(partitions) {
			endIndex := index + MetaOpsBatchSize
			if endIndex > len(partitions) {
				endIndex = len(partitions)
			}
			err := s.cli.MultiRemove(ctx, keys[index:endIndex])
			if err != nil {
				return err
			}
			index = endIndex
		}
		return nil
	}
	return s.cli.MultiRemove(ctx, keys)
}

func (s Catalog) ReleaseReplicas(ctx context.Context, collectionID int64) error {
	key := encodeCollectionReplicaKey(collectionID)
	return s.cli.RemoveWithPrefix(ctx, key)
}

func (s Catalog) ReleaseReplica(ctx context.Context, collection int64, replicas ...int64) error {
	keys := lo.Map(replicas, func(replica int64, _ int) string {
		return encodeReplicaKey(collection, replica)
	})
	if len(replicas) >= MetaOpsBatchSize {
		index := 0
		for index < len(replicas) {
			endIndex := index + MetaOpsBatchSize
			if endIndex > len(replicas) {
				endIndex = len(replicas)
			}
			err := s.cli.MultiRemove(ctx, keys[index:endIndex])
			if err != nil {
				return err
			}
			index = endIndex
		}
		return nil
	}
	return s.cli.MultiRemove(ctx, keys)
}

func (s Catalog) SaveCollectionTargets(ctx context.Context, targets ...*querypb.CollectionTarget) error {
	kvs := make(map[string]string)
	for _, target := range targets {
		k := encodeCollectionTargetKey(target.GetCollectionID())
		v, err := proto.Marshal(target)
		if err != nil {
			return err
		}

		// only compress data when size is larger than 1MB
		compressLevel := zstd.SpeedFastest
		if len(v) > 1024*1024 {
			compressLevel = zstd.SpeedBetterCompression
		}
		var compressed bytes.Buffer
		compressor.ZstdCompress(bytes.NewReader(v), io.Writer(&compressed), zstd.WithEncoderLevel(compressLevel))
		kvs[k] = compressed.String()
	}

	// to reduce the target size, we do compress before write to etcd
	err := s.cli.MultiSave(ctx, kvs)
	if err != nil {
		return err
	}
	return nil
}

func (s Catalog) RemoveCollectionTarget(ctx context.Context, collectionID int64) error {
	k := encodeCollectionTargetKey(collectionID)
	return s.cli.Remove(ctx, k)
}

func (s Catalog) GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error) {
	ret := make(map[int64]*querypb.CollectionTarget)
	applyFn := func(key []byte, value []byte) error {
		var decompressed bytes.Buffer
		compressor.ZstdDecompress(bytes.NewReader(value), io.Writer(&decompressed))
		target := &querypb.CollectionTarget{}
		if err := proto.Unmarshal(decompressed.Bytes(), target); err != nil {
			// recover target from meta is a optimize policy, skip when failure happens
			log.Warn("failed to unmarshal collection target", zap.String("key", string(key)), zap.Error(err))
			return nil
		}
		ret[target.GetCollectionID()] = target
		return nil
	}

	err := s.cli.WalkWithPrefix(ctx, CollectionTargetPrefix, s.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return ret, nil
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

func encodeCollectionTargetKey(collection int64) string {
	return fmt.Sprintf("%s/%d", CollectionTargetPrefix, collection)
}
