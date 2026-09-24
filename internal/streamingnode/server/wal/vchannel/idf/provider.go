package idf

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
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
				params.QueryNodeCfg.IDFSealedStatsLoadConcurrencyRatio.GetAsFloat(),
			))
		}
		params.Watch(params.QueryNodeCfg.IDFSealedStatsLoadConcurrencyRatio.Key,
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
	sealedCache            *segmentCache
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
		sealedCache:            newSegmentCache(),
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
	sealedCache            *segmentCache
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
		sealedCache:            newSegmentCache(),
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
	mu       sync.RWMutex
	provider *Provider
	future   *FutureProvider
	oracle   *oracleRuntime
	disabled bool
	closed   bool
}

func (r *Runtime) Prepare(ctx context.Context, walView walview.VChannelWALView) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return context.Canceled
	}
	if r.oracle != nil {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	if !hasLoadedBM25Function(walView.Schema, loadFieldIDs(walView.LoadFields)) {
		r.mu.Lock()
		closed := r.closed
		if !closed {
			r.disabled = true
		}
		r.mu.Unlock()
		if closed {
			return context.Canceled
		}
		return nil
	}
	provider, err := r.resolveProvider(ctx)
	if err != nil {
		return err
	}
	lazyLoadSealedStats := paramtable.Get().QueryNodeCfg.IDFLazyLoadSealedStats.GetAsBool()
	oracle, err := provider.buildOracle(ctx, walView, lazyLoadSealedStats)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if oracle != nil {
			oracle.Close()
		}
		return context.Canceled
	}
	r.oracle = oracle
	return nil
}

func (r *Runtime) resolveProvider(ctx context.Context) (*Provider, error) {
	if r.provider != nil {
		return r.provider, nil
	}
	if r.future == nil {
		return nil, errors.New("IDF oracle provider is nil")
	}
	if r.future.client == nil {
		return nil, errors.New("mixcoord client future is nil")
	}
	client, err := r.future.client.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Provider{
		client:                 client,
		chunkManager:           r.future.chunkManager,
		sealedCache:            r.future.sealedCache,
		sealedStatsLoadLimiter: r.future.sealedStatsLoadLimiter,
	}, nil
}

func (p *Provider) buildOracle(
	ctx context.Context,
	walView walview.VChannelWALView,
	lazyLoadSealedStats bool,
) (*oracleRuntime, error) {
	if p.client == nil {
		return nil, errors.New("querycoord client is nil")
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

func (r *Runtime) BuildIDF(ctx context.Context, dataVersion qviews.DataVersion, fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	oracle := r.currentOracle()
	if oracle == nil {
		return nil, 0, merr.WrapErrServiceNotReadyMsg("BM25 IDF oracle is not initialized")
	}
	return oracle.BuildIDF(ctx, dataVersion, fieldID, tfs)
}

func (r *Runtime) PrepareDataVersion(ctx context.Context, dataVersion qviews.DataVersion) error {
	r.mu.RLock()
	disabled := r.disabled
	r.mu.RUnlock()
	if disabled {
		return nil
	}
	oracle := r.currentOracle()
	if oracle == nil {
		return merr.WrapErrServiceNotReadyMsg("BM25 IDF oracle is not initialized")
	}
	return oracle.PrepareDataVersion(ctx, dataVersion)
}

func (r *Runtime) ReleaseDataVersion(dataVersion qviews.DataVersion) {
	if oracle := r.currentOracle(); oracle != nil {
		oracle.ReleaseDataVersion(dataVersion)
	}
}

func (r *Runtime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	if oracle := r.currentOracle(); oracle != nil {
		oracle.ApplyLiveEvent(ctx, event)
	}
}

// BM25 stats advance during PrepareDataVersion, before QueryView readiness.
func (*Runtime) Advance(qviews.DataVersion) {}

func (r *Runtime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	oracle := r.oracle
	r.oracle = nil
	r.mu.Unlock()
	if oracle != nil {
		oracle.Close()
	}
}

func (r *Runtime) currentOracle() *oracleRuntime {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.oracle
}

func collectGrowingInsertStats(stats bm25Stats, schema *schemapb.CollectionSchema, insert walview.SegmentInsertMessage) error {
	body := insert.Message.MustBody()
	if body == nil {
		return errors.New("bm25 growing insert message has nil request")
	}
	request := proto.Clone(body).(*msgpb.InsertRequest)
	request.PartitionID = insert.Assignment.GetPartitionId()
	request.SegmentID = insert.Assignment.GetSegmentAssignment().GetSegmentId()
	insertData, err := storage.ColumnBasedInsertMsgToInsertData(&msgstream.InsertMsg{InsertRequest: request}, schema)
	if err != nil {
		return err
	}
	for fieldID, fieldStats := range stats {
		fieldData, ok := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData)
		if !ok {
			continue
		}
		fieldStats.AppendFieldData(fieldData)
	}
	return nil
}

func validateResourceResponseFor(collectionID int64, vchannel string, dataVersion qviews.DataVersion, resp *datapb.GetStreamingNodeQueryViewResourcesResponse) error {
	if resp.GetCollectionId() != collectionID {
		return errors.Errorf(
			"bm25 resource response mismatch: request collection %d, response collection %d",
			collectionID,
			resp.GetCollectionId(),
		)
	}
	if resp.GetVchannel() != vchannel {
		return errors.Errorf(
			"bm25 resource response mismatch: request vchannel %s, response vchannel %s",
			vchannel,
			resp.GetVchannel(),
		)
	}
	if resp.GetDataVersion() == nil {
		return errors.New("bm25 resource response mismatch: response data version is nil")
	}
	responseVersion := qviews.FromProtoDataVersion(resp.GetDataVersion())
	if !responseVersion.EQ(dataVersion) {
		return errors.Errorf(
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
