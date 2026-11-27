package querycoord

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/compressor"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// SaveCollection writes the collection load info and the given partitions in
// one transaction when they fit within the store's op limit. Otherwise the
// collection is written first and the partitions follow in chunks.
func (s Catalog) SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error {
	v, err := proto.Marshal(collection)
	if err != nil {
		return err
	}
	b := txn.New()
	b.Save(EncodeCollectionLoadInfoKey(collection.GetCollectionID()), string(v))
	for _, partition := range partitions {
		k, v, err := encodePartitionLoadInfo(partition)
		if err != nil {
			return err
		}
		b.Save(k, v)
	}
	return txn.Commit(ctx, s.cli, b)
}

func (s Catalog) SavePartition(ctx context.Context, info ...*querypb.PartitionLoadInfo) error {
	kvs := make(map[string]string)
	for _, partition := range info {
		k, v, err := encodePartitionLoadInfo(partition)
		if err != nil {
			return err
		}
		kvs[k] = v
		if len(kvs) >= MetaOpsBatchSize {
			err := s.cli.MultiSave(ctx, kvs)
			if err != nil {
				return err
			}
			kvs = make(map[string]string)
		}
	}
	if len(kvs) > 0 {
		return s.cli.MultiSave(ctx, kvs)
	}
	return nil
}

func encodePartitionLoadInfo(partition *querypb.PartitionLoadInfo) (string, string, error) {
	v, err := proto.Marshal(partition)
	if err != nil {
		return "", "", err
	}
	return EncodePartitionLoadInfoKey(partition.GetCollectionID(), partition.GetPartitionID()), string(v), nil
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

	err := s.cli.WalkWithPrefix(ctx, CollectionLoadInfoPrefix+"/", s.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// GetPartitions walks the partition prefix once instead of reading the prefix
// of each collection. A key belongs to a requested collection exactly when
// that collection's prefix matches it; other keys are skipped undecoded. Every
// requested collection gets an entry, and the error is that of the first
// requested collection with a value that does not decode.
func (s Catalog) GetPartitions(ctx context.Context, collectionIDs []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
	result := make(map[int64][]*querypb.PartitionLoadInfo, len(collectionIDs))
	if len(collectionIDs) == 0 {
		return result, nil
	}
	requested := make(map[string]int64, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		requested[strconv.FormatInt(collectionID, 10)] = collectionID
		result[collectionID] = make([]*querypb.PartitionLoadInfo, 0)
	}
	invalid := make(map[int64]error)
	prefix := s.cli.GetPath(PartitionLoadInfoPrefix + "/")
	err := s.cli.WalkWithPrefix(ctx, PartitionLoadInfoPrefix+"/", s.paginationSize, func(key []byte, value []byte) error {
		segment, _, ok := strings.Cut(strings.TrimPrefix(string(key), prefix), "/")
		collectionID, found := requested[segment]
		if !ok || !found || invalid[collectionID] != nil {
			return nil
		}
		info := &querypb.PartitionLoadInfo{}
		if err := proto.Unmarshal(value, info); err != nil {
			invalid[collectionID] = err
			return nil
		}
		result[collectionID] = append(result[collectionID], info)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, collectionID := range collectionIDs {
		if err := invalid[collectionID]; err != nil {
			return nil, err
		}
	}
	return result, nil
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

	err := s.cli.WalkWithPrefix(ctx, ReplicaPrefix+"/", s.paginationSize, applyFn)
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
	_, replicaValues, err := s.cli.LoadWithPrefix(ctx, ReplicaMetaPrefixV1+"/")
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
	_, rgs, err := s.cli.LoadWithPrefix(ctx, ResourceGroupPrefix+"/")
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
	partitionsPrefix := fmt.Sprintf("%s/%d/", PartitionLoadInfoPrefix, collection)
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
	key := encodeCollectionReplicaPrefix(collectionID)
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

func (s Catalog) RemoveCollectionTargets(ctx context.Context) error {
	return s.cli.RemoveWithPrefix(ctx, CollectionTargetPrefix)
}

func (s Catalog) GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error) {
	ret := make(map[int64]*querypb.CollectionTarget)
	applyFn := func(key []byte, value []byte) error {
		var decompressed bytes.Buffer
		compressor.ZstdDecompress(bytes.NewReader(value), io.Writer(&decompressed))
		target := &querypb.CollectionTarget{}
		if err := proto.Unmarshal(decompressed.Bytes(), target); err != nil {
			// recover target from meta is a optimize policy, skip when failure happens
			mlog.Warn(ctx, "failed to unmarshal collection target", mlog.String("key", string(key)), mlog.Err(err))
			return nil
		}
		ret[target.GetCollectionID()] = target
		return nil
	}

	err := s.cli.WalkWithPrefix(ctx, CollectionTargetPrefix+"/", s.paginationSize, applyFn)
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

func EncodePartitionLoadInfoPrefix(collection int64) string {
	return fmt.Sprintf("%s/%d/", PartitionLoadInfoPrefix, collection)
}

func encodeReplicaKey(collection, replica int64) string {
	return fmt.Sprintf("%s/%d/%d", ReplicaPrefix, collection, replica)
}

func encodeCollectionReplicaPrefix(collection int64) string {
	return fmt.Sprintf("%s/%d/", ReplicaPrefix, collection)
}

func encodeResourceGroupKey(rgName string) string {
	return fmt.Sprintf("%s/%s", ResourceGroupPrefix, rgName)
}

func encodeCollectionTargetKey(collection int64) string {
	return fmt.Sprintf("%s/%d", CollectionTargetPrefix, collection)
}
