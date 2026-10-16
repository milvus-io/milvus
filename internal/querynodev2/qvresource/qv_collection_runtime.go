package qvresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type collectionRuntimeKey struct {
	collectionID  int64
	schemaVersion int32
}

type queryViewCollectionRuntimeManager struct {
	meta           qnview.QueryViewLoadMetadataProvider
	schemas        streaming.SchemaClient
	mu             sync.Mutex
	collections    map[collectionRuntimeKey]qvCollectionManager
	newCollections func() qvCollectionManager
}

func newQueryViewCollectionRuntimeManager(meta qnview.QueryViewLoadMetadataProvider, schemas streaming.SchemaClient) *queryViewCollectionRuntimeManager {
	return &queryViewCollectionRuntimeManager{
		meta: meta, schemas: schemas,
		collections:    make(map[collectionRuntimeKey]qvCollectionManager),
		newCollections: func() qvCollectionManager { return segments.NewCollectionManager() },
	}
}

func (m *queryViewCollectionRuntimeManager) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	if view == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("query view is nil")
	}
	pb := view.IntoProto()
	meta := pb.GetMeta()
	collection, err := m.schemas.GetCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{
		CollectionId: meta.GetCollectionId(),
		Vchannel:     meta.GetVchannel(),
		Selector:     &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: meta.GetSchemaVersion()},
	})
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if collection == nil || collection.GetSchema() == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("collection metadata is incomplete")
	}
	if collection.GetSchema().GetVersion() != meta.GetSchemaVersion() {
		return nil, false, merr.WrapErrServiceInternalMsg("query view requires schema %d, streaming returned %d", meta.GetSchemaVersion(), collection.GetSchema().GetVersion())
	}
	loadInfo, err := m.loadInfo(ctx, meta)
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	// Native planning resources belong to a SchemaVersion. A target view must never
	// mutate the schema from which a published view builds query plans.
	// Native schema identity is collection-scoped too: per-channel WAL ticks
	// differ for the same ALTER and must not advance this shared runtime.
	key := collectionRuntimeKey{meta.GetCollectionId(), meta.GetSchemaVersion()}
	m.mu.Lock()
	defer m.mu.Unlock()
	collections := m.collections[key]
	shared := collections != nil
	indexMeta := segments.ComposeIndexMeta(ctx, loadInfo.IndexInfos, collection.GetSchema())
	if collections == nil {
		collections = m.newCollections()
		if err := collections.PutOrRef(
			meta.GetCollectionId(), collection.GetSchema(), indexMeta,
			&querypb.LoadMetaInfo{
				LoadType:     querypb.LoadType_LoadCollection,
				CollectionID: meta.GetCollectionId(),
				DbName:       collection.GetSchema().GetDbName(),
			},
		); err != nil {
			return nil, false, err
		}
		m.collections[key] = collections
	} else {
		collections.Ref(meta.GetCollectionId(), 1)
	}
	localCollection := collections.Get(meta.GetCollectionId())
	var ccollection *segcore.CCollection
	if localCollection != nil {
		localCollection = localCollection.WithLoadScope(loadInfoPartitionIDs(loadInfo, view.ViewOfQueryNode()), loadInfoFieldIDs(loadInfo, collection.GetSchema()), loadInfoHintIDs(loadInfo, collection.GetSchema()))
		ccollection = localCollection.GetCCollection()
		if shared {
			if err := localCollection.UpdateIndexMeta(indexMeta); err != nil {
				m.releaseLocked(key)
				return nil, false, err
			}
		}
	}
	return &queryViewCollectionRuntimeGuard{
		manager:       m,
		key:           key,
		collection:    localCollection,
		collectionID:  meta.GetCollectionId(),
		databaseName:  collection.GetSchema().GetDbName(),
		schema:        collection.GetSchema(),
		schemaVersion: int64(meta.GetSchemaVersion()),
		ccollection:   ccollection,
	}, false, nil
}

func isRetryableCollectionRuntimeError(err error) bool {
	if err == nil || merr.GetErrorType(err) == merr.InputError {
		return false
	}
	return !status.AsStreamingError(err).IsUnrecoverable() &&
		!errors.Is(err, merr.ErrCollectionNotFound) &&
		!errors.Is(err, merr.ErrDatabaseNotFound) &&
		!errors.Is(err, merr.ErrPartitionNotFound) &&
		!errors.Is(err, merr.ErrSegmentNotFound) &&
		!errors.Is(err, merr.ErrIndexNotFound)
}

func (m *queryViewCollectionRuntimeManager) loadInfo(ctx context.Context, meta *viewpb.QueryViewMeta) (qnview.QueryViewLoadInfo, error) {
	return m.meta.GetQueryViewLoadInfo(ctx, meta.GetCollectionId(), qnview.QueryViewLoadInfoVersionFromProto(meta.GetLoadInfoVersion()))
}

type queryViewCollectionRuntimeGuard struct {
	manager       *queryViewCollectionRuntimeManager
	key           collectionRuntimeKey
	collection    *segments.Collection
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

func (g *queryViewCollectionRuntimeGuard) Collection() *segments.Collection {
	return g.collection
}

func (g *queryViewCollectionRuntimeGuard) UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	indexMeta := segments.ComposeIndexMeta(ctx, indexes, g.schema)
	return g.collection.UpdateIndexMeta(indexMeta)
}

func (g *queryViewCollectionRuntimeGuard) Release() {
	g.manager.release(g.key)
}

func (m *queryViewCollectionRuntimeManager) release(key collectionRuntimeKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(key)
}

func (m *queryViewCollectionRuntimeManager) releaseLocked(key collectionRuntimeKey) {
	if m.collections[key].Unref(key.collectionID, 1) {
		delete(m.collections, key)
	}
}

func loadInfoPartitionIDs(info qnview.QueryViewLoadInfo, fallback *viewpb.QueryViewOfQueryNode) []int64 {
	if len(info.PartitionIDs) > 0 {
		return append([]int64(nil), info.PartitionIDs...)
	}
	return qvViewPartitionIDs(fallback)
}

func loadInfoFieldIDs(info qnview.QueryViewLoadInfo, schema *schemapb.CollectionSchema) []int64 {
	fields := make([]int64, 0, len(info.RequiredFields))
	for _, field := range qviews.ResolveRequiredFields(schema, info.RequiredFields) {
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

func loadInfoHintIDs(info qnview.QueryViewLoadInfo, schema *schemapb.CollectionSchema) []int64 {
	if len(info.LoadFields) == 0 && !info.SkipLoadDynamicField {
		return nil
	}
	allowed := make(map[int64]bool)
	for _, field := range info.LoadFields {
		allowed[field.GetFieldId()] = true
	}
	fields := make([]int64, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetIsDynamic() && info.SkipLoadDynamicField {
			continue
		}
		if len(info.LoadFields) == 0 || allowed[field.GetFieldID()] {
			fields = append(fields, field.GetFieldID())
		}
	}
	return fields
}
