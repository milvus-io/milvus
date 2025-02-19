package backend

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/cmd/tools/migration/meta"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

// etcd220 implements Backend.
type etcd220 struct {
	Backend
	*etcdBasedBackend
}

func newEtcd220(cfg *configs.MilvusConfig) (*etcd220, error) {
	etcdBackend, err := newEtcdBasedBackend(cfg)
	if err != nil {
		return nil, err
	}
	return &etcd220{etcdBasedBackend: etcdBackend}, nil
}

func lineSaveTo(key string) {
	fmt.Printf("save to %s\n", key)
}

func printSaves(saves map[string]string) {
	for k := range saves {
		lineSaveTo(k)
	}
}

func (b etcd220) save(saves map[string]string) error {
	for k, v := range saves {
		if err := b.txn.Save(context.TODO(), k, v); err != nil {
			return err
		}
	}
	return nil
}

func (b etcd220) Save(metas *meta.Meta) error {
	{
		saves, err := metas.Meta220.TtCollections.GenerateSaves(metas.SourceVersion)
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.Collections.GenerateSaves(metas.SourceVersion)
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.TtAliases.GenerateSaves()
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.Aliases.GenerateSaves()
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.CollectionIndexes.GenerateSaves()
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.SegmentIndexes.GenerateSaves()
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.CollectionLoadInfos.GenerateSaves()
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}
	{
		saves, err := metas.Meta220.PartitionLoadInfos.GenerateSaves()
		if err != nil {
			return err
		}
		if err := b.save(saves); err != nil {
			return err
		}
	}

	return nil
}

func (b etcd220) Clean() error {
	prefixes := []string{
		rootcoord.CollectionMetaPrefix,
		rootcoord.PartitionMetaPrefix,
		rootcoord.FieldMetaPrefix,
		rootcoord.AliasMetaPrefix,

		rootcoord.SnapshotPrefix,

		util.FieldIndexPrefix,
		util.SegmentIndexPrefix,

		querycoord.CollectionLoadInfoPrefix,
		querycoord.PartitionLoadInfoPrefix,
	}
	for _, prefix := range prefixes {
		if err := b.CleanWithPrefix(prefix); err != nil {
			return err
		}
		lineCleanPrefix(prefix)
	}
	return nil
}
