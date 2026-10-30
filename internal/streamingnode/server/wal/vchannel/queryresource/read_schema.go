package queryresource

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Read bindings are process-local and isolate index metadata by load version.
// Their CCollections share the parsed immutable Schema through segcore's pool.
type collectionSchemaKey struct {
	collectionID    int64
	schemaVersion   int32
	loadInfoVersion uint64
}
type readSchemaPool struct {
	mu        sync.Mutex
	resources map[collectionSchemaKey]*readSchema
}

var (
	sharedReadSchemas    = readSchemaPool{resources: make(map[collectionSchemaKey]*readSchema)}
	readSchemaGeneration atomic.Uint64
)

type readSchemaBuilder struct{}

func NewReadSchemaModuleBuilder() QueryRuntimeModuleBuilder { return readSchemaBuilder{} }
func (readSchemaBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &readSchemas{pool: &sharedReadSchemas}, nil
}

type readSchemas struct {
	pool    *readSchemaPool
	capture func(*viewpb.QueryViewMeta, func(walview.VChannelWALView)) bool
}

func (r *readSchemas) Prepare(_ context.Context, view walview.VChannelWALView) error {
	r.capture = view.CaptureQueryView
	return nil
}
func (*readSchemas) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}
func (*readSchemas) Advance(qviews.DataVersion)                                    {}
func (*readSchemas) Close()                                                        {} // View guards own the pool references.
func (r *readSchemas) BindView(meta *viewpb.QueryViewMeta) QueryViewResource {
	return &readSchemaBinding{module: r, meta: meta}
}

type readSchemaBinding struct {
	module *readSchemas
	meta   *viewpb.QueryViewMeta
	schema *readSchema
}

func (b *readSchemaBinding) Prepare(ctx context.Context, provider LoadInfoProvider) error {
	if b.schema != nil {
		return nil
	}
	var view walview.VChannelWALView
	if b.module.capture == nil || !b.module.capture(b.meta, func(v walview.VChannelWALView) { view = v }) {
		return merr.WrapErrServiceNotReadyMsg("query view read schema snapshot is not ready")
	}
	if view.Schema == nil || view.Schema.GetVersion() != b.meta.GetSchemaVersion() {
		return merr.WrapErrServiceNotReadyMsg("query view read schema snapshot version mismatch")
	}
	if provider != nil {
		info, err := provider.QueryViewLoadInfo(ctx, view.CollectionID, b.meta.GetLoadInfoVersion())
		if err != nil {
			return err
		}
		view.RequiredFields = qviews.ResolveRequiredFields(view.Schema, info.RequiredFields)
		view.IndexInfos = info.IndexInfos
	}
	schema, err := b.module.pool.acquire(ctx, view, b.meta.GetLoadInfoVersion())
	if err != nil {
		return err
	}
	// Runner initialization may read external resources. The acquired guard keeps
	// its schema alive without holding the shared collection registry lock.
	for _, field := range qviews.ResolveRequiredFields(view.Schema, view.RequiredFields) {
		if _, err := function.GetManager().RunWithRunner(ctx, view.CollectionID, schema.functionKey, field.GetFieldId(), func(function.FunctionRunner) error { return nil }); err != nil {
			b.module.pool.release(schema)
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		b.module.pool.release(schema)
		return err
	}
	b.schema = schema
	return nil
}

func (b *readSchemaBinding) Release() {
	if b.schema != nil {
		b.module.pool.release(b.schema)
	}
}

type readSchema struct {
	mu          sync.Mutex
	collection  *segcore.CCollection
	functionKey string
	refs        int // pool base reference plus native execution handles
	owners      int // View guards, protected by pool.mu
	key         collectionSchemaKey
}

func (p *readSchemaPool) acquire(ctx context.Context, view walview.VChannelWALView, version uint64) (*readSchema, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := collectionSchemaKey{view.CollectionID, view.Schema.GetVersion(), version}
	req := &segcore.CreateCCollectionRequest{CollectionID: view.CollectionID, Schema: view.Schema, ShareSchema: true}
	if len(view.IndexInfos) > 0 {
		index := segcore.ComposeCollectionIndexMeta(ctx, view.IndexInfos, view.Schema)
		if len(index.GetIndexMetas()) > 0 && index.GetMaxIndexRowCount() > 0 {
			req.IndexMeta = index
		}
	}
	if schema := p.resources[key]; schema != nil {
		schema.owners++
		return schema, nil
	}
	collection, err := segcore.CreateCCollection(req)
	if err != nil {
		return nil, err
	}
	schema := &readSchema{collection: collection, refs: 1, owners: 1, key: key, functionKey: fmt.Sprintf("query-read/%d/%d/%d", key.collectionID, key.schemaVersion, readSchemaGeneration.Add(1))}
	if err := function.GetManager().Alloc(view.CollectionID, schema.functionKey, view.Schema); err != nil {
		collection.Release()
		return nil, err
	}
	p.resources[key] = schema
	return schema, nil
}

func (p *readSchemaPool) release(s *readSchema) {
	p.mu.Lock()
	s.owners--
	last := s.owners == 0
	if last {
		delete(p.resources, s.key)
	}
	p.mu.Unlock()
	if last {
		s.release()
	}
}

func (s *readSchema) pin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs == 0 {
		return false
	}
	s.refs++
	return true
}

func (s *readSchema) release() {
	s.mu.Lock()
	s.refs--
	last := s.refs == 0
	s.mu.Unlock()
	if last {
		function.GetManager().Release(s.collection.ID(), s.functionKey)
		s.collection.Release()
	}
}

func (v *viewResources) readRuntime() (*ReadRuntime, bool) {
	if v == nil {
		return nil, false
	}
	var schema *readSchema
	var idf idfOracle
	for _, resource := range v.modules {
		if binding, ok := resource.(*readSchemaBinding); ok {
			schema = binding.schema
		}
		if oracle, ok := resource.(idfOracle); ok {
			idf = oracle
		}
	}
	if schema == nil {
		return nil, false
	}
	return &ReadRuntime{QueryRuntime: v.runtime, schema: schema, resources: v, idf: idf}, true
}

// ReadRuntime binds query planning and handles to one immutable read schema.
// A QueryView lease owns the enclosing runtime; each handle also pins its schema.
type ReadRuntime struct {
	partitionStats map[int64]*storage.PartitionStatsSnapshot
	*QueryRuntime
	schema    *readSchema
	resources *viewResources
	idf       idfOracle
}

func (r *ReadRuntime) AcquireGrowingSegmentHandles(ctx context.Context, version qviews.DataVersion, partitions []int64) ([]snview.GrowingSegmentHandle, error) {
	handles, err := r.QueryRuntime.AcquireGrowingSegmentHandles(ctx, version, partitions)
	if err != nil {
		return nil, err
	}
	for i, handle := range handles {
		if !r.schema.pin() {
			for _, acquired := range handles {
				acquired.Release()
			}
			return nil, merr.WrapErrServiceNotReadyMsg("query read schema is released")
		}
		handles[i] = &readSchemaHandle{GrowingSegmentHandle: handle, schema: r.schema}
	}
	return handles, nil
}

type readSchemaHandle struct {
	snview.GrowingSegmentHandle
	schema *readSchema
	once   sync.Once
}

func (h *readSchemaHandle) Collection() *segcore.CCollection { return h.schema.collection }
func (h *readSchemaHandle) Release() {
	h.once.Do(func() { h.GrowingSegmentHandle.Release(); h.schema.release() })
}

func (r *ReadRuntime) GlobalOptimizer() optimizer.GlobalOptimizer {
	return globalOptimizer{resources: r.resources, schema: r.Schema(), partitionStats: r.partitionStats, idf: r.idf, functionRunnerKey: r.schema.functionKey}
}

func (r *ReadRuntime) Schema() *schemapb.CollectionSchema { return r.schema.collection.Schema() }
