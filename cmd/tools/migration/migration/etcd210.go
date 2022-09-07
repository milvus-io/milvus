package migration

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/util/etcd"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcd210 implements Backend.
type etcd210 struct {
	Backend
	opt     *backendOpt
	txn     kv.MetaKv
	etcdCli *clientv3.Client
}

func newEtcd210(opt *backendOpt) (*etcd210, error) {
	etcdCli, err := etcd.GetEtcdClient(&opt.config.EtcdCfg)
	if err != nil {
		return nil, err
	}
	txn := etcdkv.NewEtcdKV(etcdCli, opt.config.EtcdCfg.MetaRootPath)
	b := &etcd210{opt: opt, etcdCli: etcdCli, txn: txn}
	return b, nil
}

func lineBackupTo(key string) {
	fmt.Printf("backup to %s\n", key)
}

func (b etcd210) backupWithPrefix(prefix string, dryRun bool) error {
	keys, values, err := b.txn.LoadWithPrefix(prefix)
	if err != nil {
		return err
	}
	if len(keys) != len(values) {
		return fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		k := constructBackupKey(etcd210BackupPrefix, keys[i])
		if !dryRun {
			if err := b.txn.Save(k, values[i]); err != nil {
				return err
			}
		}
		lineBackupTo(k)
	}
	return nil
}

func (b etcd210) Backup(dryRun bool) error {
	// aliases.
	if err := b.backupWithPrefix(rootcoord.CollectionAliasMetaPrefix, dryRun); err != nil {
		return err
	}
	if err := b.backupWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionAliasMetaPrefix), dryRun); err != nil {
		return err
	}
	// collections.
	if err := b.backupWithPrefix(rootcoord.CollectionMetaPrefix, dryRun); err != nil {
		return err
	}
	if err := b.backupWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionMetaPrefix), dryRun); err != nil {
		return err
	}
	// segment index.
	if err := b.backupWithPrefix(rootcoord.SegmentIndexPrefixBefore220, dryRun); err != nil {
		return err
	}
	if err := b.backupWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.SegmentIndexPrefixBefore220), dryRun); err != nil {
		return err
	}
	// index (index params).
	if err := b.backupWithPrefix(rootcoord.IndexMetaBefore220Prefix, dryRun); err != nil {
		return err
	}
	if err := b.backupWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.IndexMetaBefore220Prefix), dryRun); err != nil {
		return err
	}
	return nil
}

func getFileName(path string) string {
	got := strings.Split(path, "/")
	l := len(got)
	return got[l-1]
}

func (b etcd210) loadAliasesByPrefix(prefix string) (aliasesMeta, ttAliasesMeta, error) {
	aliases := make(aliasesMeta)
	ttAliases := make(ttAliasesMeta)
	keys, values, err := b.txn.LoadWithPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(values) {
		return nil, nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		tsKey := keys[i]
		tsValue := values[i]
		valueIsTombstone := rootcoord.IsTombstone(tsValue)
		var aliasInfo *model.Alias
		if valueIsTombstone {
			aliasInfo = nil
		} else {
			var aliasPb pb.CollectionInfo
			if err := proto.Unmarshal([]byte(tsValue), &aliasPb); err != nil {
				return nil, nil, err
			}
			aliasInfo = &model.Alias{
				Name:         aliasPb.GetSchema().GetName(),
				CollectionID: aliasPb.GetID(),
				State:        pb.AliasState_AliasCreated,
			}
		}
		key, ts, err := SplitBySeparator(tsKey)
		if err != nil && !isErrNotOfTsKey(err) {
			return nil, nil, err
		} else if err != nil && isErrNotOfTsKey(err) {
			aliases.addAlias(getFileName(tsKey), aliasInfo)
		} else {
			ttAliases.addAlias(getFileName(key), aliasInfo, ts)
		}
	}
	return aliases, ttAliases, nil
}

func (b etcd210) loadAliases() (aliasesMeta, ttAliasesMeta, error) {
	aliases := make(aliasesMeta)
	ttAliases := make(ttAliasesMeta)
	aliases1, ttAliases1, err := b.loadAliasesByPrefix(rootcoord.CollectionAliasMetaPrefix)
	if err != nil {
		return nil, nil, err
	}
	aliases2, ttAliases2, err := b.loadAliasesByPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionAliasMetaPrefix))
	if err != nil {
		return nil, nil, err
	}
	aliases.merge(aliases1)
	aliases.merge(aliases2)
	ttAliases.merge(ttAliases1)
	ttAliases.merge(ttAliases2)
	return aliases, ttAliases, nil
}

func (b etcd210) processCollections(keys, values []string) (collectionsMeta, ttCollectionsMeta, collectionIndexesMeta, error) {
	collections := make(collectionsMeta)
	ttCollections := make(ttCollectionsMeta)
	collectionIndexes := make(collectionIndexesMeta)
	// collection_id -> index_id -> latest change ts
	collectionIndexesTs := make(map[UniqueID]map[UniqueID]Timestamp)
	insertAnIndexesRecord := func(collectionID UniqueID, fieldIndexes []*pb.FieldIndexInfo, ts Timestamp) {
		for _, indexes := range fieldIndexes {
			indexID := indexes.GetIndexID()
			_, collExist := collectionIndexesTs[collectionID]
			if !collExist {
				collectionIndexesTs[collectionID] = map[UniqueID]Timestamp{
					indexID: ts,
				}
				collectionIndexes[collectionID] = map[UniqueID]*model.Index{
					indexID: {
						TenantID:     "", // todo
						CollectionID: collectionID,
						FieldID:      indexes.GetFiledID(),
						IndexID:      indexID,

						// below five fields will be filled by pb.IndexInfo.
						IndexName:   "",
						IsDeleted:   false,
						CreateTime:  0,
						TypeParams:  nil,
						IndexParams: nil,
					},
				}
			} else {
				indexTs, indexExist := collectionIndexesTs[collectionID][indexID]
				if !indexExist || indexTs <= ts {
					collectionIndexesTs[collectionID][indexID] = ts
					collectionIndexes[collectionID][indexID] = &model.Index{
						TenantID:     "", // todo
						CollectionID: collectionID,
						FieldID:      indexes.GetFiledID(),
						IndexID:      indexID,

						// below five fields will be filled by pb.IndexInfo.
						IndexName:   "",
						IsDeleted:   false,
						CreateTime:  0,
						TypeParams:  nil,
						IndexParams: nil,
					}
				}
			}
		}
	}
	if len(keys) != len(values) {
		return nil, nil, nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		tsKey := keys[i]
		tsValue := values[i]

		// ugly here, since alias and collections have same prefix.
		if strings.Contains(tsKey, rootcoord.CollectionAliasMetaPrefix) {
			continue
		}

		valueIsTombstone := rootcoord.IsTombstone(tsValue)
		var coll *model.Collection
		var fieldIndexes []*pb.FieldIndexInfo
		if valueIsTombstone {
			coll = nil
			fieldIndexes = nil
		} else {
			var collPb pb.CollectionInfo
			if err := proto.Unmarshal([]byte(tsValue), &collPb); err != nil {
				return nil, nil, nil, err
			}
			coll = model.UnmarshalCollectionModel(&collPb)
			fieldIndexes = collPb.GetFieldIndexes()
		}
		key, ts, err := SplitBySeparator(tsKey)
		if err != nil && !isErrNotOfTsKey(err) {
			return nil, nil, nil, err
		} else if err != nil && isErrNotOfTsKey(err) {
			collectionID, err := strconv.Atoi(getFileName(tsKey))
			if err != nil {
				return nil, nil, nil, err
			}
			collections.addCollection(UniqueID(collectionID), coll)
			insertAnIndexesRecord(UniqueID(collectionID), fieldIndexes, typeutil.MaxTimestamp)
		} else {
			collectionID, err := strconv.Atoi(getFileName(key))
			if err != nil {
				return nil, nil, nil, err
			}
			ttCollections.addCollection(UniqueID(collectionID), coll, ts)
			insertAnIndexesRecord(UniqueID(collectionID), fieldIndexes, ts)
		}
	}
	return collections, ttCollections, collectionIndexes, nil
}

func (b etcd210) loadHistoricalCollectionAndFieldIndexes() (collectionsMeta, ttCollectionsMeta, collectionIndexesMeta, error) {
	keys, values, err := b.txn.LoadWithPrefix(rootcoord.CollectionMetaPrefix)
	if err != nil {
		return nil, nil, nil, err
	}
	return b.processCollections(keys, values)
}

func (b etcd210) loadLatestCollectionAndFieldIndexes() (collectionsMeta, ttCollectionsMeta, collectionIndexesMeta, error) {
	keys, values, err := b.txn.LoadWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionMetaPrefix))
	if err != nil {
		return nil, nil, nil, err
	}
	return b.processCollections(keys, values)
}

func (b etcd210) loadCollectionAndFieldIndexes() (collectionsMeta, ttCollectionsMeta, collectionIndexesMeta, error) {
	collections := make(collectionsMeta)
	ttCollections := make(ttCollectionsMeta)
	collectionIndexes := make(collectionIndexesMeta)
	collections1, ttCollections1, collectionIndexes1, err := b.loadHistoricalCollectionAndFieldIndexes()
	if err != nil {
		return nil, nil, nil, err
	}
	collections2, ttCollections2, collectionIndexes2, err := b.loadLatestCollectionAndFieldIndexes()
	if err != nil {
		return nil, nil, nil, err
	}
	collections.merge(collections1)
	ttCollections.merge(ttCollections2)
	collectionIndexes.merge(collectionIndexes1)
	// latest should override historical.
	collections.merge(collections2)
	ttCollections.merge(ttCollections1)
	collectionIndexes.merge(collectionIndexes2)
	return collections, ttCollections, collectionIndexes, nil
}

func (b etcd210) fillIndexInfos(collectionIndexes collectionIndexesMeta) error {
	for collectionID := range collectionIndexes {
		indexes := collectionIndexes[collectionID]
		for indexID := range indexes {
			ck := strconv.Itoa(int(collectionID))
			ik := strconv.Itoa(int(indexID))
			value, err := b.txn.Load(path.Join(rootcoord.IndexMetaBefore220Prefix, ck, ik))
			if err != nil {
				return err
			}
			var pbIndexInfo pb.IndexInfo
			if err := proto.Unmarshal([]byte(value), &pbIndexInfo); err != nil {
				return err
			}
			index := collectionIndexes[collectionID][indexID]
			collectionIndexes[collectionID][indexID] = &model.Index{
				TenantID:     index.TenantID,
				CollectionID: index.CollectionID,
				FieldID:      index.FieldID,
				IndexID:      index.IndexID,
				IndexName:    pbIndexInfo.GetIndexName(),
				IsDeleted:    pbIndexInfo.GetDeleted(),
				CreateTime:   pbIndexInfo.GetCreateTime(),
				TypeParams:   nil, // strange here. why newer version has type params?
				IndexParams:  pbIndexInfo.GetIndexParams(),
			}
		}
	}
	return nil
}

func (b etcd210) loadSegmentIndexes() (segmentIndexesMeta, error) {
	segmentIndexes := make(segmentIndexesMeta)
	keys, values, err := b.txn.LoadWithPrefix(rootcoord.SegmentIndexPrefixBefore220)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		value := values[i]
		var pbSegmentIndex pb.SegmentIndexInfo
		if err := proto.Unmarshal([]byte(value), &pbSegmentIndex); err != nil {
			return nil, err
		}
		segmentIndexes.addRecord(&pbSegmentIndex)
	}
	return segmentIndexes, nil
}

func (b etcd210) Load() (*Meta, error) {
	aliases, ttAliases, err := b.loadAliases()
	if err != nil {
		return nil, err
	}
	collections, ttCollections, collectionIndexes, err := b.loadCollectionAndFieldIndexes()
	if err != nil {
		return nil, err
	}
	if err := b.fillIndexInfos(collectionIndexes); err != nil {
		return nil, err
	}
	segmentIndexes, err := b.loadSegmentIndexes()
	if err != nil {
		return nil, err
	}
	return &Meta{
		ttAliases:         ttAliases,
		aliases:           aliases,
		collections:       collections,
		ttCollections:     ttCollections,
		collectionIndexes: collectionIndexes,
		segmentIndexes:    segmentIndexes,

		// we have compatibility logic inside rootcoord.
		partitions:   make(partitionsMeta),
		ttPartitions: make(ttPartitionsMeta),
		fields:       make(fieldsMeta),
		ttFields:     make(ttFieldsMeta),

		version: version210,
	}, nil
}

func (b etcd210) cleanWithPrefix(prefix string) error {
	return b.txn.RemoveWithPrefix(prefix)
}

func (b etcd210) Clean() error {
	// aliases.
	if err := b.cleanWithPrefix(rootcoord.CollectionAliasMetaPrefix); err != nil {
		return err
	}
	if err := b.cleanWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionAliasMetaPrefix)); err != nil {
		return err
	}
	// collections.
	if err := b.cleanWithPrefix(rootcoord.CollectionMetaPrefix); err != nil {
		return err
	}
	if err := b.cleanWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionMetaPrefix)); err != nil {
		return err
	}
	// segment index.
	if err := b.cleanWithPrefix(rootcoord.SegmentIndexPrefixBefore220); err != nil {
		return err
	}
	if err := b.cleanWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.SegmentIndexPrefixBefore220)); err != nil {
		return err
	}
	// index (index params).
	if err := b.cleanWithPrefix(rootcoord.IndexMetaBefore220Prefix); err != nil {
		return err
	}
	if err := b.cleanWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.IndexMetaBefore220Prefix)); err != nil {
		return err
	}
	// useless ugly code, I'm amazed why this information is also snapshotted.
	if err := b.cleanWithPrefix(rootcoord.DDMsgSendPrefix); err != nil {
		return err
	}
	if err := b.cleanWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.DDMsgSendPrefix)); err != nil {
		return err
	}
	// useless ugly code, I'm amazed why this information is also snapshotted.
	if err := b.cleanWithPrefix(rootcoord.DDOperationPrefix); err != nil {
		return err
	}
	if err := b.cleanWithPrefix(path.Join(rootcoord.SnapshotPrefix, rootcoord.DDOperationPrefix)); err != nil {
		return err
	}
	return nil
}
