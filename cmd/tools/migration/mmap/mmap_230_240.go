package mmap

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

// In Milvus 2.3.x, querynode.MmapDirPath is used to enable mmap and save mmap files.
// In Milvus 2.4.x, mmap is enabled by setting collection properties and altering index.
// querynode.MmapDirPath is only used to save mmap files.
// Therefore, we need to read configs from 2.3.x and modify meta data if necessary.
type MmapMigration struct {
	rootcoordMeta    rootcoord.IMetaTable
	tsoAllocator     tso.Allocator
	datacoordCatalog metastore.DataCoordCatalog
}

func (m *MmapMigration) Migrate(ctx context.Context) {
	m.MigrateRootCoordCollection(ctx)
	m.MigrateIndexCoordCollection(ctx)
}

func updateOrAddMmapKey(kv []*commonpb.KeyValuePair, key, value string) []*commonpb.KeyValuePair {
	for _, pair := range kv {
		if pair.Key == key {
			pair.Value = value
			return kv
		}
	}
	return append(kv, &commonpb.KeyValuePair{Key: key, Value: value})
}

func (m *MmapMigration) MigrateRootCoordCollection(ctx context.Context) {
	ts, err := m.tsoAllocator.GenerateTSO(1)
	if err != nil {
		panic(err)
	}
	db2Colls := m.rootcoordMeta.ListAllAvailCollections(ctx)
	for did, collIds := range db2Colls {
		db, err := m.rootcoordMeta.GetDatabaseByID(ctx, did, ts)
		if err != nil {
			panic(err)
		}
		for _, cid := range collIds {
			collection, err := m.rootcoordMeta.GetCollectionByID(ctx, db.Name, cid, ts, false)
			if err != nil {
				panic(err)
			}
			newColl := collection.Clone()

			newColl.Properties = updateOrAddMmapKey(newColl.Properties, common.MmapEnabledKey, "true")
			fmt.Printf("migrate collection %v, %s\n", collection.CollectionID, collection.Name)

			if err := m.rootcoordMeta.AlterCollection(ctx, collection, newColl, ts); err != nil {
				panic(err)
			}
		}
	}
}

func (m *MmapMigration) MigrateIndexCoordCollection(ctx context.Context) {
	// load field indexes
	fieldIndexes, err := m.datacoordCatalog.ListIndexes(ctx)
	if err != nil {
		panic(err)
	}

	getIndexType := func(indexParams []*commonpb.KeyValuePair) string {
		for _, param := range indexParams {
			if param.Key == common.IndexTypeKey {
				return param.Value
			}
		}
		return "invalid"
	}

	alteredIndexes := make([]*model.Index, 0)
	for _, index := range fieldIndexes {
		if !vecindexmgr.GetVecIndexMgrInstance().IsMMapSupported(getIndexType(index.IndexParams)) {
			continue
		}
		fmt.Printf("migrate index, collection:%v, indexId: %v, indexName: %s\n", index.CollectionID, index.IndexID, index.IndexName)
		newIndex := model.CloneIndex(index)

		newIndex.UserIndexParams = updateOrAddMmapKey(newIndex.UserIndexParams, common.MmapEnabledKey, "true")
		newIndex.IndexParams = updateOrAddMmapKey(newIndex.IndexParams, common.MmapEnabledKey, "true")
		alteredIndexes = append(alteredIndexes, newIndex)
	}

	if err := m.datacoordCatalog.AlterIndexes(ctx, alteredIndexes); err != nil {
		panic(err)
	}
}

func NewMmapMigration(rootcoordMeta rootcoord.IMetaTable, tsoAllocator tso.Allocator, datacoordCatalog metastore.DataCoordCatalog) *MmapMigration {
	return &MmapMigration{
		rootcoordMeta:    rootcoordMeta,
		tsoAllocator:     tsoAllocator,
		datacoordCatalog: datacoordCatalog,
	}
}
