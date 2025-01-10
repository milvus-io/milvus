package backend

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/cmd/tools/migration/console"
	"github.com/milvus-io/milvus/cmd/tools/migration/legacy"
	"github.com/milvus-io/milvus/cmd/tools/migration/legacy/legacypb"
	"github.com/milvus-io/milvus/cmd/tools/migration/meta"
	"github.com/milvus-io/milvus/cmd/tools/migration/utils"
	"github.com/milvus-io/milvus/cmd/tools/migration/versions"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	pb "github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// etcd210 implements Backend.
type etcd210 struct {
	Backend
	*etcdBasedBackend
}

func newEtcd210(cfg *configs.MilvusConfig) (*etcd210, error) {
	etcdBackend, err := newEtcdBasedBackend(cfg)
	if err != nil {
		return nil, err
	}
	return &etcd210{etcdBasedBackend: etcdBackend}, nil
}

func (b etcd210) loadTtAliases() (meta.TtAliasesMeta210, error) {
	ttAliases := make(meta.TtAliasesMeta210)
	prefix := path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionAliasMetaPrefix210)
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		tsKey := keys[i]
		tsValue := values[i]
		valueIsTombstone := rootcoord.IsTombstone(tsValue)
		aliasInfo := &pb.CollectionInfo{} // alias stored in collection info.
		if valueIsTombstone {
			aliasInfo = nil
		} else {
			if err := proto.Unmarshal([]byte(tsValue), aliasInfo); err != nil {
				return nil, err
			}
		}
		key, ts, err := utils.SplitBySeparator(tsKey)
		if err != nil {
			return nil, err
		}
		ttAliases.AddAlias(utils.GetFileName(key), aliasInfo, ts)
	}
	return ttAliases, nil
}

func (b etcd210) loadAliases() (meta.AliasesMeta210, error) {
	aliases := make(meta.AliasesMeta210)
	prefix := rootcoord.CollectionAliasMetaPrefix210
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		key := keys[i]
		value := values[i]
		valueIsTombstone := rootcoord.IsTombstone(value)
		aliasInfo := &pb.CollectionInfo{} // alias stored in collection info.
		if valueIsTombstone {
			aliasInfo = nil
		} else {
			if err := proto.Unmarshal([]byte(value), aliasInfo); err != nil {
				return nil, err
			}
		}
		aliases.AddAlias(utils.GetFileName(key), aliasInfo)
	}
	return aliases, nil
}

func (b etcd210) loadTtCollections() (meta.TtCollectionsMeta210, error) {
	ttCollections := make(meta.TtCollectionsMeta210)
	prefix := path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionMetaPrefix)
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		tsKey := keys[i]
		tsValue := values[i]

		// ugly here, since alias and collections have same prefix.
		if strings.Contains(tsKey, rootcoord.CollectionAliasMetaPrefix210) {
			continue
		}

		valueIsTombstone := rootcoord.IsTombstone(tsValue)
		coll := &pb.CollectionInfo{}
		if valueIsTombstone {
			coll = nil
		} else {
			if err := proto.Unmarshal([]byte(tsValue), coll); err != nil {
				return nil, err
			}
		}
		key, ts, err := utils.SplitBySeparator(tsKey)
		if err != nil {
			return nil, err
		}
		collectionID, err := strconv.Atoi(utils.GetFileName(key))
		if err != nil {
			return nil, err
		}
		ttCollections.AddCollection(typeutil.UniqueID(collectionID), coll, ts)
	}
	return ttCollections, nil
}

func (b etcd210) loadCollections() (meta.CollectionsMeta210, error) {
	collections := make(meta.CollectionsMeta210)
	prefix := rootcoord.CollectionMetaPrefix
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		key := keys[i]
		value := values[i]

		// ugly here, since alias and collections have same prefix.
		if strings.Contains(key, rootcoord.CollectionAliasMetaPrefix210) {
			continue
		}

		valueIsTombstone := rootcoord.IsTombstone(value)
		coll := &pb.CollectionInfo{}
		if valueIsTombstone {
			coll = nil
		} else {
			if err := proto.Unmarshal([]byte(value), coll); err != nil {
				return nil, err
			}
		}
		collectionID, err := strconv.Atoi(utils.GetFileName(key))
		if err != nil {
			return nil, err
		}
		collections.AddCollection(typeutil.UniqueID(collectionID), coll)
	}
	return collections, nil
}

func parseCollectionIndexKey(key string) (collectionID, indexID typeutil.UniqueID, err error) {
	ss := strings.Split(key, "/")
	l := len(ss)
	if l < 2 {
		return 0, 0, fmt.Errorf("failed to parse collection index key: %s", key)
	}
	index, err := strconv.Atoi(ss[l-1])
	if err != nil {
		return 0, 0, err
	}
	collection, err := strconv.Atoi(ss[l-2])
	if err != nil {
		return 0, 0, err
	}
	return typeutil.UniqueID(collection), typeutil.UniqueID(index), nil
}

func (b etcd210) loadCollectionIndexes() (meta.CollectionIndexesMeta210, error) {
	collectionIndexes := make(meta.CollectionIndexesMeta210)
	prefix := legacy.IndexMetaBefore220Prefix
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		key := keys[i]
		value := values[i]

		index := &pb.IndexInfo{}
		if err := proto.Unmarshal([]byte(value), index); err != nil {
			return nil, err
		}
		collectionID, indexID, err := parseCollectionIndexKey(key)
		if err != nil {
			return nil, err
		}
		collectionIndexes.AddIndex(collectionID, indexID, index)
	}
	return collectionIndexes, nil
}

func (b etcd210) loadSegmentIndexes() (meta.SegmentIndexesMeta210, error) {
	segmentIndexes := make(meta.SegmentIndexesMeta210)
	prefix := legacy.SegmentIndexPrefixBefore220
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		value := values[i]

		index := &pb.SegmentIndexInfo{}
		if err := proto.Unmarshal([]byte(value), index); err != nil {
			return nil, err
		}
		segmentIndexes.AddIndex(index.GetSegmentID(), index.GetIndexID(), index)
	}
	return segmentIndexes, nil
}

func (b etcd210) loadIndexBuildMeta() (meta.IndexBuildMeta210, error) {
	indexBuildMeta := make(meta.IndexBuildMeta210)
	prefix := legacy.IndexBuildPrefixBefore220
	keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length mismatch")
	}
	l := len(keys)
	for i := 0; i < l; i++ {
		value := values[i]

		record := &legacypb.IndexMeta{}
		if err := proto.Unmarshal([]byte(value), record); err != nil {
			return nil, err
		}
		indexBuildMeta.AddRecord(record.GetIndexBuildID(), record)
	}
	return indexBuildMeta, nil
}

func (b etcd210) loadLastDDLRecords() (meta.LastDDLRecords, error) {
	records := make(meta.LastDDLRecords)
	prefixes := []string{
		legacy.DDOperationPrefixBefore220,
		legacy.DDMsgSendPrefixBefore220,
		path.Join(rootcoord.SnapshotPrefix, legacy.DDOperationPrefixBefore220),
		path.Join(rootcoord.SnapshotPrefix, legacy.DDMsgSendPrefixBefore220),
	}
	for _, prefix := range prefixes {
		keys, values, err := b.txn.LoadWithPrefix(context.TODO(), prefix)
		if err != nil {
			return nil, err
		}
		if len(keys) != len(values) {
			return nil, fmt.Errorf("length mismatch")
		}
		for i, k := range keys {
			records.AddRecord(k, values[i])
		}
	}
	return records, nil
}

func (b etcd210) loadLoadInfos() (meta.CollectionLoadInfo210, error) {
	loadInfo := make(meta.CollectionLoadInfo210)
	_, collectionValues, err := b.txn.LoadWithPrefix(context.TODO(), legacy.CollectionLoadMetaPrefixV1)
	if err != nil {
		return nil, err
	}
	for _, value := range collectionValues {
		collectionInfo := querypb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &collectionInfo)
		if err != nil {
			return nil, err
		}
		if collectionInfo.InMemoryPercentage < 100 {
			continue
		}
		loadInfo[collectionInfo.CollectionID] = &model.CollectionLoadInfo{
			CollectionID:         collectionInfo.GetCollectionID(),
			PartitionIDs:         collectionInfo.GetPartitionIDs(),
			ReleasedPartitionIDs: collectionInfo.GetReleasedPartitionIDs(),
			LoadType:             collectionInfo.GetLoadType(),
			LoadPercentage:       100,
			Status:               querypb.LoadStatus_Loaded,
			ReplicaNumber:        collectionInfo.GetReplicaNumber(),
			FieldIndexID:         make(map[int64]int64),
		}
	}
	return loadInfo, nil
}

func (b etcd210) Load() (*meta.Meta, error) {
	ttCollections, err := b.loadTtCollections()
	if err != nil {
		return nil, err
	}
	collections, err := b.loadCollections()
	if err != nil {
		return nil, err
	}
	ttAliases, err := b.loadTtAliases()
	if err != nil {
		return nil, err
	}
	aliases, err := b.loadAliases()
	if err != nil {
		return nil, err
	}
	collectionIndexes, err := b.loadCollectionIndexes()
	if err != nil {
		return nil, err
	}
	segmentIndexes, err := b.loadSegmentIndexes()
	if err != nil {
		return nil, err
	}
	indexBuildMeta, err := b.loadIndexBuildMeta()
	if err != nil {
		return nil, err
	}
	lastDdlRecords, err := b.loadLastDDLRecords()
	if err != nil {
		return nil, err
	}
	loadInfos, err := b.loadLoadInfos()
	if err != nil {
		return nil, err
	}
	return &meta.Meta{
		Version: versions.Version210,
		Meta210: &meta.All210{
			TtCollections:       ttCollections,
			Collections:         collections,
			TtAliases:           ttAliases,
			Aliases:             aliases,
			CollectionIndexes:   collectionIndexes,
			SegmentIndexes:      segmentIndexes,
			IndexBuildMeta:      indexBuildMeta,
			LastDDLRecords:      lastDdlRecords,
			CollectionLoadInfos: loadInfos,
		},
	}, nil
}

func lineCleanPrefix(prefix string) {
	fmt.Printf("prefix %s will be removed!\n", prefix)
}

func (b etcd210) Clean() error {
	prefixes := []string{
		rootcoord.CollectionMetaPrefix,
		path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionMetaPrefix),

		rootcoord.CollectionAliasMetaPrefix210,
		path.Join(rootcoord.SnapshotPrefix, rootcoord.CollectionAliasMetaPrefix210),

		legacy.SegmentIndexPrefixBefore220,

		legacy.IndexMetaBefore220Prefix,

		legacy.IndexBuildPrefixBefore220,

		legacy.DDMsgSendPrefixBefore220,
		path.Join(rootcoord.SnapshotPrefix, legacy.DDMsgSendPrefixBefore220),
		legacy.DDOperationPrefixBefore220,
		path.Join(rootcoord.SnapshotPrefix, legacy.DDOperationPrefixBefore220),
	}
	for _, prefix := range prefixes {
		if err := b.CleanWithPrefix(prefix); err != nil {
			return err
		}
		lineCleanPrefix(prefix)
	}
	return nil
}

func (b etcd210) Backup(meta *meta.Meta, backupFile string) error {
	saves := meta.Meta210.GenerateSaves()
	codec := NewBackupCodec()
	var instance, metaPath string
	metaRootPath := b.cfg.EtcdCfg.MetaRootPath.GetValue()
	parts := strings.Split(metaRootPath, "/")
	if len(parts) > 1 {
		metaPath = parts[len(parts)-1]
		instance = path.Join(parts[:len(parts)-1]...)
	} else {
		instance = metaRootPath
	}
	header := &BackupHeader{
		Version:   int32(BackupHeaderVersionV1),
		Instance:  instance,
		MetaPath:  metaPath,
		Entries:   int64(len(saves)),
		Component: "",
		Extra:     nil,
	}
	backup, err := codec.Serialize(header, saves)
	if err != nil {
		return err
	}
	console.Warning(fmt.Sprintf("backup to: %s", backupFile))
	return storage.WriteFile(backupFile, backup, 0o600)
}

func (b etcd210) BackupV2(file string) error {
	var instance, metaPath string
	metaRootPath := b.cfg.EtcdCfg.MetaRootPath.GetValue()
	parts := strings.Split(metaRootPath, "/")
	if len(parts) > 1 {
		metaPath = parts[len(parts)-1]
		instance = path.Join(parts[:len(parts)-1]...)
	} else {
		instance = metaRootPath
	}

	ctx := context.Background()
	// TODO: optimize this if memory consumption is too large.
	saves := make(map[string]string)
	cntResp, err := b.etcdCli.Get(ctx, metaRootPath, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return err
	}

	opts := []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithRev(cntResp.Header.Revision), clientv3.WithLimit(1)}
	currentKey := metaRootPath
	for i := 0; int64(i) < cntResp.Count; i++ {
		resp, err := b.etcdCli.Get(ctx, currentKey, opts...)
		if err != nil {
			return err
		}
		for _, kv := range resp.Kvs {
			currentKey = string(append(kv.Key, 0))
			if kv.Lease != 0 {
				console.Warning(fmt.Sprintf("lease key won't be backuped: %s, lease id: %d", kv.Key, kv.Lease))
				continue
			}
			saves[string(kv.Key)] = string(kv.Value)
		}
	}

	header := &BackupHeader{
		Version:   int32(BackupHeaderVersionV1),
		Instance:  instance,
		MetaPath:  metaPath,
		Entries:   int64(len(saves)),
		Component: "",
		Extra:     newBackupHeaderExtra(setEntryIncludeRootPath(true)).ToJSONBytes(),
	}

	codec := NewBackupCodec()
	backup, err := codec.Serialize(header, saves)
	if err != nil {
		return err
	}

	console.Warning(fmt.Sprintf("backup to: %s", file))
	return storage.WriteFile(file, backup, 0o600)
}

func (b etcd210) Restore(backupFile string) error {
	backup, err := storage.ReadFile(backupFile)
	if err != nil {
		return err
	}
	codec := NewBackupCodec()
	header, saves, err := codec.DeSerialize(backup)
	if err != nil {
		return err
	}
	entryIncludeRootPath := GetExtra(header.Extra).EntryIncludeRootPath
	getRealKey := func(key string) string {
		if entryIncludeRootPath {
			return key
		}
		return path.Join(header.Instance, header.MetaPath, key)
	}
	ctx := context.Background()
	for k, v := range saves {
		if _, err := b.etcdCli.Put(ctx, getRealKey(k), v); err != nil {
			return err
		}
	}
	return nil
}
