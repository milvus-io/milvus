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
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type collectionRuntimeKey struct {
	collectionID    int64
	schemaVersion   int32
	loadInfoVersion uint64
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
		newCollections: func() qvCollectionManager { return segments.NewQueryViewCollectionManager() },
	}
}

func (m *queryViewCollectionRuntimeManager) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	if view == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("query view is nil")
	}
	pb := view.IntoProto()
	meta := pb.GetMeta()
	collection, err := m.schemas.GetSchemaVersion(ctx, streaming.GetSchemaVersionRequest{
		VChannel: meta.GetVchannel(), Version: meta.GetSchemaVersion(),
	})
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if collection == nil || collection.Schema == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("collection metadata is incomplete")
	}
	if collection.Schema.GetVersion() != meta.GetSchemaVersion() {
		return nil, false, merr.WrapErrServiceInternalMsg("query view requires schema %d, streaming returned %d", meta.GetSchemaVersion(), collection.Schema.GetVersion())
	}
	loadInfo, err := m.loadInfo(ctx, meta)
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	// The parsed native Schema is shared by collection and SchemaVersion; each
	// LoadInfoVersion gets its own immutable index-metadata binding. Per-channel
	// WAL ticks do not identify either resource.
	key := collectionRuntimeKey{meta.GetCollectionId(), meta.GetSchemaVersion(), meta.GetLoadInfoVersion()}
	m.mu.Lock()
	defer m.mu.Unlock()
	collections := m.collections[key]
	if collections == nil {
		indexMeta := segments.ComposeIndexMeta(ctx, loadInfo.IndexInfos, collection.Schema)
		collections = m.newCollections()
		if err := collections.PutOrRef(
			meta.GetCollectionId(), collection.Schema, indexMeta,
			&querypb.LoadMetaInfo{
				LoadType:     querypb.LoadType_LoadCollection,
				CollectionID: meta.GetCollectionId(),
				DbName:       collection.Schema.GetDbName(),
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
		localCollection = localCollection.WithLoadScope(loadInfo.PartitionIDs, loadInfoFieldIDs(loadInfo), loadInfoHintIDs(loadInfo, collection.Schema))
		ccollection = localCollection.GetCCollection()
	}
	return &queryViewCollectionRuntimeGuard{
		manager:       m,
		key:           key,
		collection:    localCollection,
		collectionID:  meta.GetCollectionId(),
		databaseName:  collection.Schema.GetDbName(),
		schema:        collection.Schema,
		schemaVersion: int64(meta.GetSchemaVersion()),
		ccollection:   ccollection,
		indexes:       loadInfo.IndexInfos,
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
	info, err := m.meta.GetQueryViewLoadInfo(ctx, meta.GetCollectionId(), qnview.QueryViewLoadInfoVersionFromProto(meta.GetLoadInfoVersion()))
	if err != nil {
		return qnview.QueryViewLoadInfo{}, err
	}
	if info.CollectionID != meta.GetCollectionId() || uint64(info.Version) != meta.GetLoadInfoVersion() {
		return qnview.QueryViewLoadInfo{}, merr.WrapErrServiceNotReadyMsg("requested load snapshot (%d, %d), received (%d, %d)", meta.GetCollectionId(), meta.GetLoadInfoVersion(), info.CollectionID, info.Version)
	}
	if len(info.RequiredFields) == 0 {
		return qnview.QueryViewLoadInfo{}, merr.WrapErrServiceInternalMsg("query view load snapshot %d for collection %d has no required fields", info.Version, info.CollectionID)
	}
	return info, nil
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
	indexes       []*indexpb.IndexInfo
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

func loadInfoFieldIDs(info qnview.QueryViewLoadInfo) []int64 {
	fields := make([]int64, 0, len(info.RequiredFields))
	for _, field := range info.RequiredFields {
		fields = append(fields, field.GetFieldId())
	}
	return fields
}

func loadInfoHintIDs(info qnview.QueryViewLoadInfo, schema *schemapb.CollectionSchema) []int64 {
	if len(info.LoadFields) == 0 {
		return nil
	}
	allowed := make(map[int64]bool)
	for _, field := range info.LoadFields {
		allowed[field.GetFieldId()] = true
	}
	fields := make([]int64, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if allowed[field.GetFieldID()] {
			fields = append(fields, field.GetFieldID())
		}
	}
	return fields
}

func (g *queryViewCollectionRuntimeGuard) IndexInfos() []*indexpb.IndexInfo { return g.indexes }
