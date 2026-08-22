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
	collections segments.CollectionManager
}

func NewQueryViewCollectionRuntimeManager(
	meta qnview.QueryViewLoadMetadataProvider,
	collections segments.CollectionManager,
) qnview.QueryViewCollectionRuntimeManager {
	return &queryViewCollectionRuntimeManager{meta: meta, collections: collections}
}

func (m *queryViewCollectionRuntimeManager) Acquire(
	ctx context.Context,
	view *qviews.QueryViewAtQueryNode,
) (qnview.CollectionRuntimeGuard, bool, error) {
	if view == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("query view is nil")
	}
	meta := view.IntoProto().GetMeta()
	collection, err := m.meta.DescribeCollection(ctx, meta.GetCollectionId())
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if err := merr.CheckRPCCall(collection, nil); err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if collection.GetSchema() == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("collection metadata is incomplete")
	}

	requestedVersion := qnview.QueryViewLoadInfoVersionFromProto(meta.GetLoadInfoVersion())
	loadInfo, err := m.meta.GetQueryViewLoadInfo(ctx, meta.GetCollectionId(), requestedVersion)
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if loadInfo.CollectionID != meta.GetCollectionId() {
		return nil, true, merr.WrapErrServiceInternalMsg(
			"query view load info collection mismatch, expected=%d, actual=%d",
			meta.GetCollectionId(), loadInfo.CollectionID)
	}
	if loadInfo.Version != requestedVersion {
		return nil, true, merr.WrapErrServiceInternalMsg(
			"query view load info version mismatch, expected=%d, actual=%d",
			requestedVersion, loadInfo.Version)
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
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	local := m.collections.Get(meta.GetCollectionId())
	if local == nil {
		m.collections.Unref(meta.GetCollectionId(), 1)
		return nil, true, merr.WrapErrServiceInternalMsg(
			"query view collection disappeared after pin, collectionID=%d", meta.GetCollectionId())
	}
	return &queryViewCollectionRuntimeGuard{
		collections:  m.collections,
		collection:   local,
		collectionID: meta.GetCollectionId(),
		databaseName: collection.GetDbName(),
		schema:       collection.GetSchema(),
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

type queryViewCollectionRuntimeGuard struct {
	collections  segments.CollectionManager
	collection   *segments.Collection
	collectionID int64
	databaseName string
	schema       *schemapb.CollectionSchema
}

func (g *queryViewCollectionRuntimeGuard) CollectionID() int64                { return g.collectionID }
func (g *queryViewCollectionRuntimeGuard) DatabaseName() string               { return g.databaseName }
func (g *queryViewCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema { return g.schema }
func (g *queryViewCollectionRuntimeGuard) SchemaVersion() int64               { return int64(g.schema.GetVersion()) }
func (g *queryViewCollectionRuntimeGuard) CCollection() *segcore.CCollection {
	return g.collection.GetCCollection()
}

func (g *queryViewCollectionRuntimeGuard) PinnedCollection() *segments.Collection {
	return g.collection
}

func (g *queryViewCollectionRuntimeGuard) UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	return g.collection.UpdateIndexMeta(segments.ComposeIndexMeta(ctx, indexes, g.schema))
}

func (g *queryViewCollectionRuntimeGuard) Release() {
	g.collections.Unref(g.collectionID, 1)
}

func loadInfoPartitionIDs(info qnview.QueryViewLoadInfo, fallback *viewpb.QueryViewOfQueryNode) []int64 {
	if len(info.PartitionIDs) > 0 {
		return append([]int64(nil), info.PartitionIDs...)
	}
	partitions := make([]int64, 0, len(fallback.GetPartitions()))
	for _, partition := range fallback.GetPartitions() {
		partitions = append(partitions, partition.GetPartitionId())
	}
	return partitions
}

func loadInfoFieldIDs(info qnview.QueryViewLoadInfo) []int64 {
	fields := make([]int64, 0, len(info.LoadFields))
	for _, field := range info.LoadFields {
		fields = append(fields, field.GetFieldId())
	}
	return fields
}
