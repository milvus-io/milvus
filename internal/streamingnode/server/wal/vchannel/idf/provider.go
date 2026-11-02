package idf

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var (
	_ queryresource.QueryRuntimeModuleBuilder = (*Provider)(nil)
	_ queryresource.QueryRuntimeModuleBuilder = (*FutureProvider)(nil)

	getGlobalSealedStatsLoadLimiter = sync.OnceValue(func() *syncutil.Semaphore {
		params := paramtable.Get()
		limiter := syncutil.NewSemaphore(1)
		var resizeMu sync.Mutex
		resize := func(_ *config.Event) {
			resizeMu.Lock()
			defer resizeMu.Unlock()
			limiter.SetCapacity(sealedStatsLoadConcurrency(
				hardware.GetCPUNum(),
				params.QueryViewCfg.IDFSealedStatsLoadConcurrencyRatio.GetAsFloat(),
			))
		}
		params.Watch(params.QueryViewCfg.IDFSealedStatsLoadConcurrencyRatio.Key,
			config.NewHandler("sn.bm25.sealed-stats-load", resize))
		resize(nil)
		return limiter
	})
)

func sealedStatsLoadConcurrency(cpu int, ratio float64) int {
	if cpu <= 0 || ratio <= 0 {
		return 1
	}
	return max(1, int(float64(cpu)*ratio))
}

// Provider loads sealed BM25 resources for a DataVersion and aggregates the
// WALView growing BM25 stats into a runtime oracle.
type Provider struct {
	client                 datapb.DataCoordClient
	chunkManager           storage.ChunkManager
	sealedStatsLoadLimiter *syncutil.Semaphore
}

type ProviderOption func(*Provider)

func WithChunkManager(chunkManager storage.ChunkManager) ProviderOption {
	return func(p *Provider) {
		p.chunkManager = chunkManager
	}
}

func NewProvider(client datapb.DataCoordClient, opts ...ProviderOption) *Provider {
	provider := &Provider{
		client:                 client,
		sealedStatsLoadLimiter: getGlobalSealedStatsLoadLimiter(),
	}
	for _, opt := range opts {
		opt(provider)
	}
	return provider
}

type FutureProvider struct {
	client                 *syncutil.Future[types.MixCoordClient]
	chunkManager           storage.ChunkManager
	sealedStatsLoadLimiter *syncutil.Semaphore
}

func NewFutureProvider(client *syncutil.Future[types.MixCoordClient], opts ...ProviderOption) *FutureProvider {
	provider := &Provider{}
	for _, opt := range opts {
		opt(provider)
	}
	return &FutureProvider{
		client:                 client,
		chunkManager:           provider.chunkManager,
		sealedStatsLoadLimiter: getGlobalSealedStatsLoadLimiter(),
	}
}

func (p *FutureProvider) NewRuntime() (queryresource.QueryRuntimeModule, error) {
	return &Runtime{future: p}, nil
}

func (p *Provider) NewRuntime() (queryresource.QueryRuntimeModule, error) {
	return &Runtime{provider: p}, nil
}

type Runtime struct {
	mu             sync.RWMutex
	provider       *Provider
	future         *FutureProvider
	prepareMu      sync.Mutex
	cond           *sync.Cond
	refresh        *schemaRefresh
	fields         map[fieldKey]*fieldResource
	minDataVersion qviews.DataVersion
	view           walview.VChannelWALView
	closed         bool
}

// Bootstrap only installs the WAL capture/ordering context. All schema-dependent
// statistics belong to explicit View bindings, including the initial View.
func (r *Runtime) Prepare(ctx context.Context, view walview.VChannelWALView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	r.cond = sync.NewCond(&r.mu)
	r.view = walview.VChannelWALView{CaptureQueryView: view.CaptureQueryView, RuntimeContext: view.RuntimeContext, ResourceEventBarrier: view.ResourceEventBarrier}
	r.minDataVersion = view.SegmentSnapshot.DataVersion
	r.fields = make(map[fieldKey]*fieldResource)
	return nil
}

func (r *Runtime) resolveProvider(ctx context.Context) (*Provider, error) {
	if r.provider != nil {
		return r.provider, nil
	}
	if r.future == nil {
		return nil, merr.WrapErrServiceInternalMsg("IDF oracle provider is nil")
	}
	if r.future.client == nil {
		return nil, merr.WrapErrServiceInternalMsg("mixcoord client future is nil")
	}
	client, err := r.future.client.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Provider{
		client:                 client,
		chunkManager:           r.future.chunkManager,
		sealedStatsLoadLimiter: r.future.sealedStatsLoadLimiter,
	}, nil
}

func (p *Provider) buildOracle(
	ctx context.Context,
	walView walview.VChannelWALView,
	lazyLoadSealedStats bool,
) (*oracleRuntime, error) {
	if p.client == nil {
		return nil, merr.WrapErrServiceInternalMsg("querycoord client is nil")
	}

	if lazyLoadSealedStats {
		return newOracleRuntime(ctx, p, walView, nil, true)
	}
	resources, err := p.getSealedBM25Resources(ctx, walView.CollectionID, walView.VChannel, walView.SegmentSnapshot.DataVersion, walView.PartitionIDs, walView.LoadInfoVersion)
	if err != nil {
		return nil, err
	}
	return newOracleRuntime(ctx, p, walView, resources, false)
}

func loadFieldIDs(fields []*messagespb.LoadFieldConfig) []int64 {
	ids := make([]int64, 0, len(fields))
	for _, field := range fields {
		ids = append(ids, field.GetFieldId())
	}
	return ids
}

// BeforeRelease advances every still-owned field before old DataView files can
// be collected. Unowned fields have already canceled/drained in binding.Release.
func (r *Runtime) BeforeRelease(ctx context.Context, target qviews.DataVersion) error {
	if !r.prepareMu.TryLock() {
		return merr.WrapErrServiceNotReadyMsg("BM25 field preparation is in progress")
	}
	defer r.prepareMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	if target.GT(r.minDataVersion) {
		r.minDataVersion = target
	}
	target = r.minDataVersion
	fields := make([]*fieldResource, 0, len(r.fields))
	for _, field := range r.fields {
		fields = append(fields, field)
	}
	r.mu.Unlock()
	for _, field := range fields {
		if field.oracle != nil {
			if err := field.oracle.PrepareDataVersion(ctx, target); err != nil {
				return err
			}
			// Advancing a lazy target cancels its old materialization, but the
			// reader can still be unwinding. Its files remain pinned until it exits.
			field.oracle.mu.RLock()
			call := field.oracle.materialization
			field.oracle.mu.RUnlock()
			if call != nil && target.GT(call.target) {
				select {
				case <-call.done:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return nil
}

func (r *Runtime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.refresh != nil && r.refresh.err == nil && len(r.refresh.pending) >= 1024 && !r.closed {
		r.cond.Wait()
	}
	if r.closed {
		return
	}
	if stage := r.refresh; stage != nil && stage.err == nil && stage.includes(event) {
		stage.pending = append(stage.pending, event)
	}
	for _, field := range r.fields {
		if field.oracle != nil && field.coverage.includes(event) {
			field.oracle.ApplyLiveEvent(ctx, event)
		}
	}
}
func (*Runtime) Advance(qviews.DataVersion) {}
func (r *Runtime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	if r.cond != nil {
		r.cond.Broadcast()
	}
	fields := r.fields
	r.fields = nil
	r.mu.Unlock()
	for _, field := range fields {
		if field.oracle != nil {
			field.oracle.Close()
		}
	}
}

func collectGrowingInsertStats(stats bm25Stats, schema *schemapb.CollectionSchema, insert walview.SegmentInsertMessage) error {
	if len(stats) == 0 {
		return nil
	}
	request := insert.Message.MustBody()
	missing := false
	for fieldID := range stats {
		if !function.HasFieldData(request.GetFieldsData(), fieldID) {
			missing = true
			break
		}
	}
	if missing {
		// Only materialize BM25 outputs required by this oracle. A retained read
		// schema may contain dropped columns or unrelated functions; decoding the
		// entire insert against that schema would incorrectly require those columns.
		projected := proto.Clone(schema).(*schemapb.CollectionSchema)
		projected.Functions = nil
		for _, fn := range schema.GetFunctions() {
			if fn.GetType() == schemapb.FunctionType_BM25 && len(fn.GetOutputFieldIds()) == 1 {
				if _, needed := stats[fn.GetOutputFieldIds()[0]]; needed {
					projected.Functions = append(projected.Functions, fn)
				}
			}
		}
		runners, err := function.BuildEmbeddingRunners(projected)
		if err != nil {
			return err
		}
		defer function.CloseRunners(runners)
		request = storage.CopyInsertRequestMetadata(request)
		if _, err := function.FillFunctionFields(runners, request); err != nil {
			return err
		}
	}
	for fieldID, fieldStats := range stats {
		var sparse *schemapb.SparseFloatArray
		for _, data := range request.GetFieldsData() {
			if data.GetFieldId() == fieldID {
				sparse = data.GetVectors().GetSparseFloatVector()
				break
			}
		}
		if sparse == nil || len(sparse.GetContents()) != int(request.GetNumRows()) {
			return merr.WrapErrDataIntegrityMsg("BM25 insert field %d does not contain all %d rows", fieldID, request.GetNumRows())
		}
		fieldStats.AppendBytes(sparse.GetContents()...)
	}
	return nil
}

func validateResourceResponseFor(collectionID int64, vchannel string, dataVersion qviews.DataVersion, resp *datapb.GetStreamingNodeQueryViewResourcesResponse) error {
	if resp.GetCollectionId() != collectionID {
		return merr.WrapErrServiceInternalMsg(
			"bm25 resource response mismatch: request collection %d, response collection %d",
			collectionID,
			resp.GetCollectionId(),
		)
	}
	if resp.GetVchannel() != vchannel {
		return merr.WrapErrServiceInternalMsg(
			"bm25 resource response mismatch: request vchannel %s, response vchannel %s",
			vchannel,
			resp.GetVchannel(),
		)
	}
	if resp.GetDataVersion() == nil {
		return merr.WrapErrServiceInternalMsg("bm25 resource response mismatch: response data version is nil")
	}
	responseVersion := qviews.FromProtoDataVersion(resp.GetDataVersion())
	if !responseVersion.EQ(dataVersion) {
		return merr.WrapErrServiceInternalMsg(
			"bm25 resource response mismatch: request data version %s, response data version %s",
			dataVersion.String(),
			responseVersion.String(),
		)
	}
	return nil
}

func hasLoadedBM25Function(schema *schemapb.CollectionSchema, loadedFields []int64) bool {
	if schema == nil {
		return false
	}
	loadsAllFields := len(loadedFields) == 0
	loaded := make(map[int64]struct{}, len(loadedFields))
	for _, fieldID := range loadedFields {
		loaded[fieldID] = struct{}{}
	}
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		if loadsAllFields {
			return true
		}
		if _, ok := loaded[function.GetOutputFieldIds()[0]]; ok {
			return true
		}
	}
	return false
}
