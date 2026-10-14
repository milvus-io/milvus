package queryresource

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadSchemaKey identifies immutable planning resources. LoadInfoVersion is an
// identity, not an ordered revision; two loads of one schema can differ.
type ReadSchemaKey struct {
	SchemaVersion   int32
	LoadInfoVersion uint64
}

func readSchemaKey(meta *viewpb.QueryViewMeta) ReadSchemaKey {
	return ReadSchemaKey{meta.GetSchemaVersion(), meta.GetLoadInfoVersion()}
}

var readSchemaGeneration atomic.Uint64

type readSchemaBuilder struct{}

func NewReadSchemaModuleBuilder() QueryRuntimeModuleBuilder { return readSchemaBuilder{} }

func (readSchemaBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &readSchemas{generation: readSchemaGeneration.Add(1), resources: make(map[ReadSchemaKey]*readSchema)}, nil
}

// readSchemas owns read-only collections and function keys. The physical growing
// runtime and ordered WAL subscription remain shared across all these schemas.
// TODO: reclaim unused schema entries before the enclosing runtime is released.
type readSchemas struct {
	mu         sync.Mutex
	generation uint64
	capture    func(*viewpb.QueryViewMeta, func(walview.VChannelWALView)) bool
	resources  map[ReadSchemaKey]*readSchema
	closed     bool
	ctx        context.Context
}

type readSchema struct {
	mu          sync.Mutex
	collection  *segcore.CCollection
	functionKey string
	refs        int
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

func (r *readSchemas) Prepare(ctx context.Context, view walview.VChannelWALView) error {
	r.mu.Lock()
	r.capture = view.CaptureQueryView
	r.ctx = view.RuntimeContext
	r.mu.Unlock()
	return r.prepare(ctx, view)
}

func (r *readSchemas) prepare(ctx context.Context, view walview.VChannelWALView) error {
	key := ReadSchemaKey{view.Schema.GetVersion(), view.LoadInfoVersion}
	// Preparation can do I/O, but never holds the QueryRuntime application lock.
	// Only this module's preparations and close serialize here.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	if r.ctx != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(r.ctx, cancel)
		defer stop()
	}
	if _, ok := r.resources[key]; ok {
		return nil
	}
	if view.Schema == nil {
		return merr.WrapErrServiceInternalMsg("query view read schema is absent")
	}
	fields := make([]int64, 0, len(view.LoadFields))
	for _, field := range view.LoadFields {
		fields = append(fields, field.GetFieldId())
	}
	req := &segcore.CreateCCollectionRequest{CollectionID: view.CollectionID, Schema: view.Schema}
	if len(fields) > 0 {
		req.LoadFieldList = fields
	}
	if len(view.IndexInfos) > 0 {
		indexMeta := segcore.ComposeCollectionIndexMeta(ctx, view.IndexInfos, view.Schema)
		if len(indexMeta.GetIndexMetas()) > 0 && indexMeta.GetMaxIndexRowCount() > 0 {
			req.IndexMeta = indexMeta
		}
	}
	collection, err := segcore.CreateCCollection(req)
	if err != nil {
		return err
	}
	schema := &readSchema{collection: collection, refs: 1, functionKey: fmt.Sprintf("query-read/%s/%d/%d/%d", view.VChannel, r.generation, key.SchemaVersion, key.LoadInfoVersion)}
	if err := function.GetManager().Alloc(view.CollectionID, schema.functionKey, view.Schema); err != nil {
		collection.Release()
		return err
	}
	for _, fn := range view.Schema.GetFunctions() {
		for _, output := range fn.GetOutputFieldIds() {
			if len(fields) > 0 && !slices.Contains(fields, output) {
				continue
			}
			if _, err := function.GetManager().RunWithRunner(ctx, view.CollectionID, schema.functionKey, output, func(function.FunctionRunner) error { return nil }); err != nil {
				schema.release()
				return err
			}
		}
	}
	r.resources[key] = schema
	return nil
}

func (r *readSchemas) PrepareSchema(ctx context.Context, meta *viewpb.QueryViewMeta, provider LoadInfoProvider) error {
	r.mu.Lock()
	_, ready := r.resources[readSchemaKey(meta)]
	capture, closed := r.capture, r.closed
	r.mu.Unlock()
	if closed {
		return context.Canceled
	}
	if ready {
		return nil
	}
	var view walview.VChannelWALView
	if capture == nil || !capture(meta, func(v walview.VChannelWALView) { view = v }) {
		return merr.WrapErrServiceNotReadyMsg("query view read schema snapshot is not ready")
	}
	if view.Schema.GetVersion() != meta.GetSchemaVersion() {
		return merr.WrapErrServiceNotReadyMsg("query view read schema snapshot version mismatch")
	}
	if provider != nil {
		info, err := provider.QueryViewLoadInfo(ctx, view.CollectionID, meta.GetLoadInfoVersion())
		if err != nil {
			return err
		}
		view.LoadFields, view.IndexInfos = info.LoadFields, info.IndexInfos
	}
	return r.prepare(ctx, view)
}

func (*readSchemas) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}
func (*readSchemas) Advance(qviews.DataVersion)                                    {}

func (r *readSchemas) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	resources := r.resources
	r.resources = nil
	r.mu.Unlock()
	for _, schema := range resources {
		schema.release()
	}
}

func (r *QueryRuntime) ReadRuntime(meta *viewpb.QueryViewMeta) (*ReadRuntime, bool) {
	var schema *readSchema
	r.RangeModules(func(module QueryRuntimeModule) bool {
		if resources, ok := module.(*readSchemas); ok {
			resources.mu.Lock()
			schema = resources.resources[readSchemaKey(meta)]
			resources.mu.Unlock()
			return false
		}
		return true
	})
	if schema == nil {
		return nil, false
	}
	return &ReadRuntime{QueryRuntime: r, schema: schema, key: readSchemaKey(meta)}, true
}

// ReadRuntime binds query planning and handles to one immutable read schema.
// A QueryView lease owns the enclosing runtime; each handle also pins its schema.
type ReadRuntime struct {
	*QueryRuntime
	schema *readSchema
	key    ReadSchemaKey
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
	return globalOptimizer{idf: schemaIDFOracle{runtime: r.QueryRuntime, key: r.key}, functionRunnerKey: r.schema.functionKey}
}

type schemaIDFOracle struct {
	runtime *QueryRuntime
	key     ReadSchemaKey
}

func (s schemaIDFOracle) BuildIDFBatch(ctx context.Context, requests []IDFRequest) ([]IDFResult, error) {
	var results []IDFResult
	err := merr.WrapErrServiceNotReadyMsg("query view BM25 oracle is not ready")
	s.runtime.RangeModules(func(module QueryRuntimeModule) bool {
		if oracle, ok := module.(interface {
			BuildIDFBatchForSchema(context.Context, ReadSchemaKey, []IDFRequest) ([]IDFResult, error)
		}); ok {
			results, err = oracle.BuildIDFBatchForSchema(ctx, s.key, requests)
			return false
		}
		return true
	})
	return results, err
}

func (r *ReadRuntime) Schema() *schemapb.CollectionSchema { return r.schema.collection.Schema() }
