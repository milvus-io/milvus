package migration

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/util/etcd"

	"github.com/blang/semver/v4"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcd220 implements Backend.
type etcd220 struct {
	Backend
	opt     *backendOpt
	txn     kv.MetaKv
	etcdCli *clientv3.Client
}

func newEtcd220(opt *backendOpt) (*etcd220, error) {
	etcdCli, err := etcd.GetEtcdClient(&opt.config.EtcdCfg)
	if err != nil {
		return nil, err
	}
	txn := etcdkv.NewEtcdKV(etcdCli, opt.config.EtcdCfg.MetaRootPath)
	b := &etcd220{opt: opt, etcdCli: etcdCli, txn: txn}
	return b, nil
}

func lineSaveTo(key string) {
	fmt.Printf("save to %s\n", key)
}

func (b etcd220) saveAliases(aliases aliasesMeta, dryRun bool) error {
	for alias := range aliases {
		// save to newly path.
		key := rootcoord.BuildAliasKey(alias)
		aliasInfo := aliases[alias]
		if dryRun {
			lineSaveTo(key)
			continue
		}
		if aliasInfo == nil {
			// save a tombstone.
			if err := b.txn.Save(key, string(rootcoord.ConstructTombstone())); err != nil {
				return err
			}
		} else {
			aliasPb := model.MarshalAliasModel(aliasInfo)
			marshaledAliasPb, err := proto.Marshal(aliasPb)
			if err != nil {
				return err
			}
			if err := b.txn.Save(key, string(marshaledAliasPb)); err != nil {
				return err
			}
		}
		lineSaveTo(key)
	}
	return nil
}

func (b etcd220) saveTtAliases(ttAliases ttAliasesMeta, dryRun bool) error {
	for alias := range ttAliases {
		for ts := range ttAliases[alias] {
			// save to newly path.
			akey := rootcoord.BuildAliasKey(alias)
			key := rootcoord.ComposeSnapshotKey(rootcoord.SnapshotPrefix, akey, rootcoord.SnapshotsSep, ts)
			aliasInfo := ttAliases[alias][ts]
			if dryRun {
				lineSaveTo(key)
				continue
			}
			if aliasInfo == nil {
				// save a tombstone.
				if err := b.txn.Save(key, string(rootcoord.ConstructTombstone())); err != nil {
					return err
				}
			} else {
				aliasPb := model.MarshalAliasModel(aliasInfo)
				marshaledAliasPb, err := proto.Marshal(aliasPb)
				if err != nil {
					return err
				}
				if err := b.txn.Save(key, string(marshaledAliasPb)); err != nil {
					return err
				}
			}
			lineSaveTo(key)
		}

	}
	return nil
}

func (b etcd220) saveFields(fields fieldsMeta, dryRun bool) error {
	// TODO: not necessary now.
	return nil
}

func (b etcd220) saveTtFields(ttFields ttFieldsMeta, dryRun bool) error {
	// TODO: not necessary now.
	return nil
}

func (b etcd220) savePartitions(partitions partitionsMeta, dryRun bool) error {
	// TODO: not necessary now.
	return nil
}

func (b etcd220) saveTtPartitions(ttPartitions ttPartitionsMeta, dryRun bool) error {
	// TODO: not necessary now.
	return nil
}

func (b etcd220) saveCollections(collections collectionsMeta, sourceVersion semver.Version, dryRun bool) error {
	opts := make([]model.Option, 0)
	if sourceVersion.LTE(version210) {
		opts = append(opts, model.WithFields())
		opts = append(opts, model.WithPartitions())
	}

	for collectionID := range collections {
		key := rootcoord.BuildCollectionKey(collectionID)
		collection := collections[collectionID]
		if dryRun {
			lineSaveTo(key)
			continue
		}
		if collection == nil {
			// save a tombstone.
			if err := b.txn.Save(key, string(rootcoord.ConstructTombstone())); err != nil {
				return err
			}
		} else {
			collectionPb := model.MarshalCollectionModelWithOption(collection, opts...)
			marshaledCollectionPb, err := proto.Marshal(collectionPb)
			if err != nil {
				return err
			}
			if err := b.txn.Save(key, string(marshaledCollectionPb)); err != nil {
				return err
			}
		}
		lineSaveTo(key)
	}
	return nil
}

func (b etcd220) saveTtCollections(ttCollections ttCollectionsMeta, sourceVersion semver.Version, dryRun bool) error {
	opts := make([]model.Option, 0)
	if sourceVersion.LTE(version210) {
		opts = append(opts, model.WithFields())
		opts = append(opts, model.WithPartitions())
	}

	for collectionID := range ttCollections {
		for ts := range ttCollections[collectionID] {
			ckey := rootcoord.BuildCollectionKey(collectionID)
			key := rootcoord.ComposeSnapshotKey(rootcoord.SnapshotPrefix, ckey, rootcoord.SnapshotsSep, ts)
			if dryRun {
				lineSaveTo(key)
				continue
			}
			collection := ttCollections[collectionID][ts]
			if collection == nil {
				// save a tombstone.
				if err := b.txn.Save(key, string(rootcoord.ConstructTombstone())); err != nil {
					return err
				}
			} else {
				collectionPb := model.MarshalCollectionModelWithOption(collection, opts...)
				marshaledCollectionPb, err := proto.Marshal(collectionPb)
				if err != nil {
					return err
				}
				if err := b.txn.Save(key, string(marshaledCollectionPb)); err != nil {
					return err
				}
			}
			lineSaveTo(key)
		}
	}
	return nil
}

func (b etcd220) saveCollectionIndexes(collectionIndexes collectionIndexesMeta, dryRun bool) error {
	for collectionID := range collectionIndexes {
		for indexID := range collectionIndexes[collectionID] {
			key := indexcoord.BuildIndexKey(collectionID, indexID)
			if dryRun {
				lineSaveTo(key)
				continue
			}
			index := collectionIndexes[collectionID][indexID]
			indexPb := model.MarshalIndexModel(index)
			marshaledIndexPb, err := proto.Marshal(indexPb)
			if err != nil {
				return err
			}
			if err := b.txn.Save(key, string(marshaledIndexPb)); err != nil {
				return err
			}
			lineSaveTo(key)
		}
	}
	return nil
}

func (b etcd220) saveSegmentIndexes(segmentIndexes segmentIndexesMeta, dryRun bool) error {
	for segmentID := range segmentIndexes {
		for indexID := range segmentIndexes[segmentID] {
			segmentIndex := segmentIndexes[segmentID][indexID]
			key := indexcoord.BuildSegmentIndexKey(segmentIndex.CollectionID, segmentIndex.PartitionID, segmentID, segmentIndex.BuildID)
			if dryRun {
				lineSaveTo(key)
				continue
			}
			segmentIndexPb := model.MarshalSegmentIndexModel(segmentIndex)
			marshaledSegmentIndexPb, err := proto.Marshal(segmentIndexPb)
			if err != nil {
				return err
			}
			if err := b.txn.Save(key, string(marshaledSegmentIndexPb)); err != nil {
				return err
			}
			lineSaveTo(key)
		}
	}
	return nil
}

func (b etcd220) Save(meta *Meta, dryRun bool) error {
	if err := b.saveAliases(meta.aliases, dryRun); err != nil {
		return err
	}
	if err := b.saveTtAliases(meta.ttAliases, dryRun); err != nil {
		return err
	}
	if err := b.saveFields(meta.fields, dryRun); err != nil {
		return err
	}
	if err := b.saveTtFields(meta.ttFields, dryRun); err != nil {
		return err
	}
	if err := b.savePartitions(meta.partitions, dryRun); err != nil {
		return err
	}
	if err := b.saveTtPartitions(meta.ttPartitions, dryRun); err != nil {
		return err
	}
	if err := b.saveCollections(meta.collections, meta.version, dryRun); err != nil {
		return err
	}
	if err := b.saveTtCollections(meta.ttCollections, meta.version, dryRun); err != nil {
		return err
	}
	if err := b.saveCollectionIndexes(meta.collectionIndexes, dryRun); err != nil {
		return err
	}
	if err := b.saveSegmentIndexes(meta.segmentIndexes, dryRun); err != nil {
		return err
	}
	return nil
}
