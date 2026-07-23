package qvresource

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func (m *queryViewCollectionRuntimeManager) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	if view == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("query view is nil")
	}
	pb := view.IntoProto()
	meta := pb.GetMeta()
	collection, err := m.meta.DescribeCollection(ctx, meta.GetCollectionId())
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if collection == nil || collection.GetSchema() == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("collection metadata is incomplete")
	}
	loadInfo, err := m.loadInfo(ctx, meta)
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if err := m.collections.PutOrRef(
		meta.GetCollectionId(),
		collection.GetSchema(),
		segments.ComposeIndexMeta(ctx, loadInfo.IndexInfos, collection.GetSchema()),
		&querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			CollectionID:    meta.GetCollectionId(),
			PartitionIDs:    loadInfoPartitionIDs(loadInfo, view.ViewOfQueryNode()),
			DbName:          collection.GetDbName(),
			LoadFields:      loadInfoFieldIDs(loadInfo),
			SchemaBarrierTs: collection.GetUpdateTimestamp(),
		},
	); err != nil {
		return nil, false, err
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
	}, false, nil
}

func isRetryableCollectionRuntimeError(err error) bool {
	if err == nil || merr.GetErrorType(err) == merr.InputError {
		return false
	}
	return !errors.Is(err, merr.ErrCollectionNotFound) &&
		!errors.Is(err, merr.ErrDatabaseNotFound) &&
		!errors.Is(err, merr.ErrPartitionNotFound) &&
		!errors.Is(err, merr.ErrSegmentNotFound) &&
		!errors.Is(err, merr.ErrIndexNotFound)
}

func (m *queryViewCollectionRuntimeManager) loadInfo(ctx context.Context, meta *viewpb.QueryViewMeta) (qnview.QueryViewLoadInfo, error) {
	return m.meta.GetQueryViewLoadInfo(ctx, meta.GetCollectionId(), qnview.QueryViewLoadInfoVersionFromProto(meta.GetLoadInfoVersion()))
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

func loadInfoPartitionIDs(info qnview.QueryViewLoadInfo, fallback *viewpb.QueryViewOfQueryNode) []int64 {
	if len(info.PartitionIDs) > 0 {
		return append([]int64(nil), info.PartitionIDs...)
	}
	return qvViewPartitionIDs(fallback)
}

func loadInfoFieldIDs(info qnview.QueryViewLoadInfo) []int64 {
	fields := make([]int64, 0, len(info.LoadFields))
	for _, field := range info.LoadFields {
		fields = append(fields, field.GetFieldId())
	}
	return fields
}

func qvViewPartitionIDs(view *viewpb.QueryViewOfQueryNode) []int64 {
	partitions := make([]int64, 0, len(view.GetPartitions()))
	for _, partition := range view.GetPartitions() {
		partitions = append(partitions, partition.GetPartitionId())
	}
	return partitions
}
