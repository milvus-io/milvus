package qvresource

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type queryViewCollectionRuntimeManager struct {
	meta        qnview.QueryViewLoadMetadataProvider
	collections qvCollectionManager
}

func newQueryViewCollectionRuntimeManager(meta qnview.QueryViewLoadMetadataProvider, collections qvCollectionManager) *queryViewCollectionRuntimeManager {
	return &queryViewCollectionRuntimeManager{
		meta:        meta,
		collections: collections,
	}
}

func (m *queryViewCollectionRuntimeManager) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, error) {
	if view == nil {
		return nil, fmt.Errorf("query view is nil")
	}
	pb := view.IntoProto()
	meta := pb.GetMeta()
	collection, err := m.meta.DescribeCollection(ctx, meta.GetCollectionId())
	if err != nil {
		return nil, err
	}
	if collection == nil || collection.GetSchema() == nil {
		return nil, fmt.Errorf("collection metadata is incomplete")
	}
	if err := m.collections.PutOrRef(
		meta.GetCollectionId(),
		collection.GetSchema(),
		nil,
		&querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			CollectionID:    meta.GetCollectionId(),
			PartitionIDs:    qvViewPartitionIDs(view.ViewOfQueryNode()),
			DbName:          collection.GetDbName(),
			LoadFields:      append([]int64(nil), meta.GetSettings().GetRequiredFields()...),
			SchemaBarrierTs: collection.GetUpdateTimestamp(),
		},
	); err != nil {
		return nil, err
	}
	var ccollection *segcore.CCollection
	if local := m.collections.Get(meta.GetCollectionId()); local != nil {
		ccollection = local.GetCCollection()
	}
	return &queryViewCollectionRuntimeGuard{
		collections:   m.collections,
		collectionID:  meta.GetCollectionId(),
		databaseName:  collection.GetDbName(),
		schema:        collection.GetSchema(),
		schemaVersion: int64(collection.GetUpdateTimestamp()),
		ccollection:   ccollection,
	}, nil
}

type queryViewCollectionRuntimeGuard struct {
	collections   qvCollectionManager
	collectionID  int64
	databaseName  string
	schema        *schemapb.CollectionSchema
	schemaVersion int64
	ccollection   *segcore.CCollection
}

func (g *queryViewCollectionRuntimeGuard) CollectionID() int64 {
	return g.collectionID
}

func (g *queryViewCollectionRuntimeGuard) DatabaseName() string {
	return g.databaseName
}

func (g *queryViewCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema {
	return g.schema
}

func (g *queryViewCollectionRuntimeGuard) SchemaVersion() int64 {
	return g.schemaVersion
}

func (g *queryViewCollectionRuntimeGuard) CCollection() *segcore.CCollection {
	return g.ccollection
}

func (g *queryViewCollectionRuntimeGuard) UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	if err := g.collections.PutOrRef(
		g.collectionID,
		g.schema,
		segments.ComposeIndexMeta(ctx, indexes, g.schema),
		&querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			CollectionID:    g.collectionID,
			DbName:          g.databaseName,
			SchemaBarrierTs: uint64(g.schemaVersion),
		},
	); err != nil {
		return err
	}
	g.collections.Unref(g.collectionID, 1)
	return nil
}

func (g *queryViewCollectionRuntimeGuard) Release() {
	g.collections.Unref(g.collectionID, 1)
}

func qvViewPartitionIDs(view *viewpb.QueryViewOfQueryNode) []int64 {
	partitions := make([]int64, 0, len(view.GetPartitions()))
	for _, partition := range view.GetPartitions() {
		partitions = append(partitions, partition.GetPartitionId())
	}
	return partitions
}
