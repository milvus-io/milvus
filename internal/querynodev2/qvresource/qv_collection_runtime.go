package qvresource

import (
	"context"
	"encoding/base64"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type collectionRuntimeKey struct {
	collectionID  int64
	schemaVersion int64
}
type collectionRuntime struct {
	key          collectionRuntimeKey
	schema       *schemapb.CollectionSchema
	databaseName string
	ccollection  *segcore.CCollection
	refs         int
	mu           sync.Mutex // serializes native index metadata updates
}
type queryViewCollectionRuntimeManager struct {
	meta        qnview.QueryViewLoadMetadataProvider
	mu          sync.Mutex
	collections map[collectionRuntimeKey]*collectionRuntime
}

func newQueryViewCollectionRuntimeManager(meta qnview.QueryViewLoadMetadataProvider) *queryViewCollectionRuntimeManager {
	return &queryViewCollectionRuntimeManager{meta: meta, collections: make(map[collectionRuntimeKey]*collectionRuntime)}
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
	if loadInfo.CollectionID != meta.GetCollectionId() || loadInfo.Version != qnview.QueryViewLoadInfoVersionFromProto(meta.GetLoadInfoVersion()) {
		return nil, false, merr.WrapErrServiceInternalMsg("query view load-info snapshot does not match requested collection/version")
	}
	loadInfo = qnview.CloneQueryViewLoadInfo(loadInfo)
	// Resolve the legacy empty-list convention before merging view demands.
	if len(loadInfo.LoadFields) == 0 {
		for _, field := range collection.GetSchema().GetFields() {
			loadInfo.LoadFields = append(loadInfo.LoadFields, &messagespb.LoadFieldConfig{FieldId: field.GetFieldID()})
		}
		for _, group := range collection.GetSchema().GetStructArrayFields() {
			for _, field := range group.GetFields() {
				loadInfo.LoadFields = append(loadInfo.LoadFields, &messagespb.LoadFieldConfig{FieldId: field.GetFieldID()})
			}
		}
	}
	key := collectionRuntimeKey{meta.GetCollectionId(), int64(collection.GetSchema().GetVersion())}
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime := m.collections[key]
	if runtime == nil {
		schema := proto.Clone(collection.GetSchema()).(*schemapb.CollectionSchema)
		native, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: key.collectionID, Schema: schema, IndexMeta: segments.ComposeIndexMeta(ctx, loadInfo.IndexInfos, schema)})
		if err != nil {
			return nil, false, err
		}
		// Use the same version domain for native query plans and explicit Reopen.
		if err = native.UpdateSchema(schema, uint64(key.schemaVersion)); err != nil {
			native.Release()
			return nil, false, err
		}
		if hookutil.IsClusterEncryptionEnabled() {
			if ez := hookutil.GetEzByCollProperties(schema.GetProperties(), key.collectionID); ez != nil {
				keyBytes := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
				if err = segcore.PutOrRefPluginContext(ez, base64.StdEncoding.EncodeToString(keyBytes)); err != nil {
					native.Release()
					return nil, false, err
				}
			}
		}
		runtime = &collectionRuntime{key: key, schema: schema, databaseName: collection.GetDbName(), ccollection: native}
		m.collections[key] = runtime
	}
	runtime.refs++
	return &queryViewCollectionRuntimeGuard{runtime: runtime, owner: m, loadInfo: loadInfo}, false, nil
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
	runtime  *collectionRuntime
	owner    *queryViewCollectionRuntimeManager
	loadInfo qnview.QueryViewLoadInfo
	once     sync.Once
}

func (g *queryViewCollectionRuntimeGuard) CollectionID() int64  { return g.runtime.key.collectionID }
func (g *queryViewCollectionRuntimeGuard) DatabaseName() string { return g.runtime.databaseName }
func (g *queryViewCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema {
	return g.runtime.schema
}
func (g *queryViewCollectionRuntimeGuard) SchemaVersion() int64 { return g.runtime.key.schemaVersion }
func (g *queryViewCollectionRuntimeGuard) CCollection() *segcore.CCollection {
	return g.runtime.ccollection
}

// Retain pins this concrete runtime independently of the originating view.
func (g *queryViewCollectionRuntimeGuard) Retain() (qnview.CollectionRuntimeGuard, error) {
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if g.runtime.refs == 0 {
		return nil, merr.WrapErrCollectionNotFound(g.CollectionID())
	}
	g.runtime.refs++
	return &queryViewCollectionRuntimeGuard{runtime: g.runtime, owner: g.owner, loadInfo: g.loadInfo}, nil
}

func (g *queryViewCollectionRuntimeGuard) UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	g.runtime.mu.Lock()
	defer g.runtime.mu.Unlock()
	return g.runtime.ccollection.UpdateIndexMeta(segments.ComposeIndexMeta(ctx, indexes, g.Schema()))
}

func (g *queryViewCollectionRuntimeGuard) Release() {
	g.once.Do(func() {
		g.owner.mu.Lock()
		defer g.owner.mu.Unlock()
		g.runtime.refs--
		if g.runtime.refs != 0 {
			return
		}
		delete(g.owner.collections, g.runtime.key)
		g.runtime.ccollection.Release()
		if hookutil.IsClusterEncryptionEnabled() {
			if ez := hookutil.GetEzByCollProperties(g.Schema().GetProperties(), g.CollectionID()); ez != nil {
				if err := segcore.UnRefPluginContext(ez); err != nil {
					mlog.Error(context.TODO(), "release QueryView collection encryption context", mlog.Err(err))
				}
			}
		}
	})
}

func (g *queryViewCollectionRuntimeGuard) LoadInfo() qnview.QueryViewLoadInfo {
	return qnview.CloneQueryViewLoadInfo(g.loadInfo)
}
