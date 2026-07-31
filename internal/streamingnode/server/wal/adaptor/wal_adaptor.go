package adaptor

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	queryplanprovider "github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)
var _ queryplanprovider.QueryPlanProvider = (*walAdaptorImpl)(nil)

type gracefulCloseFunc func()

// adaptImplsToROWAL creates a new readonly wal from wal impls.
func adaptImplsToROWAL(
	basicWAL walimpls.WALImpls,
	cleanup func(),
) *roWALAdaptorImpl {
	logger := resource.Resource().Logger().With(
		mlog.FieldComponent("wal"),
		mlog.String("channel", basicWAL.Channel().String()),
	)
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is stored in availableCancel and called in Close()
	roWAL := &roWALAdaptorImpl{
		WALRateLimitComponent: rate.NewWALRateLimitComponent(basicWAL.Channel()),

		roWALImpls:      basicWAL,
		lifetime:        typeutil.NewLifetime(),
		availableCtx:    ctx,
		availableCancel: cancel,
		idAllocator:     typeutil.NewIDAllocator(),
		scannerRegistry: scannerRegistry{
			channel:     basicWAL.Channel(),
			idAllocator: typeutil.NewIDAllocator(),
		},
		scanners:    typeutil.NewConcurrentMap[int64, wal.Scanner](),
		cleanup:     cleanup,
		scanMetrics: metricsutil.NewScanMetrics(basicWAL.Channel()),
	}
	roWAL.SetLogger(logger)
	return roWAL
}

// adaptImplsToRWWAL creates a new wal from wal impls.
func adaptImplsToRWWAL(
	roWAL *roWALAdaptorImpl,
	builders []interceptors.InterceptorBuilder,
	interceptorParam *interceptors.InterceptorBuildParam,
) *walAdaptorImpl {
	if roWAL.Channel().AccessMode != types.AccessModeRW {
		panic("wal should be read-write")
	}
	// build append interceptor for a wal.
	interceptorBuildResult := buildInterceptorsAndReleaseInitialSnapshot(builders, interceptorParam)
	wal := &walAdaptorImpl{
		roWALAdaptorImpl: roWAL,
		rwWALImpls:       roWAL.roWALImpls.(walimpls.WALImpls),
		// TODO: remove the pool, use a queue instead.
		appendExecutionPool:    conc.NewPool[struct{}](0),
		param:                  interceptorParam,
		interceptorBuildResult: interceptorBuildResult,
		writeMetrics:           metricsutil.NewWriteMetrics(roWAL.Channel(), roWAL.WALName()),
		isFenced:               atomic.NewBool(false),
		appendRateCounter:      utility.NewAverageRateCounter(10 * time.Second), // 10 second sliding window
	}
	wal.writeMetrics.SetLogger(wal.Logger())
	interceptorParam.WAL.Set(wal)
	wal.RegisterMemoryObserver()
	wal.RegisterAppendRateObserver(wal.appendRateCounter)
	return wal
}

// walAdaptorImpl is a wrapper of WALImpls to extend it into a WAL interface.
type walAdaptorImpl struct {
	*roWALAdaptorImpl

	rwWALImpls             walimpls.WALImpls
	appendExecutionPool    *conc.Pool[struct{}]
	param                  *interceptors.InterceptorBuildParam
	interceptorBuildResult interceptorBuildResult
	writeMetrics           *metricsutil.WriteMetrics
	isFenced               *atomic.Bool
	appendRateCounter      *utility.AverageRateCounter // tracks append rate (bytes/sec)
	queryViewHandler       *snview.SNQueryViewHandler
	viewResourceManager    *vchannel.PChannelRecoveryManager
}

// Metrics returns the metrics of the wal.
func (w *walAdaptorImpl) Metrics() types.WALMetrics {
	currentMVCC := w.param.MVCCManager.GetMVCCOfVChannel(w.Channel().Name)
	recoveryTimeTick := uint64(0)
	if w.param.RecoveryStorage != nil {
		recoveryTimeTick = w.param.RecoveryStorage.Metrics().RecoveryTimeTick
	}
	return types.RWWALMetrics{
		ChannelInfo:      w.Channel(),
		MVCCTimeTick:     currentMVCC.GrowingTimetick,
		RecoveryTimeTick: recoveryTimeTick,
	}
}

// GetLatestMVCCTimestamp returns the growing MVCC frontier of the vchannel.
// TODO: remove it after legacy consumers switch to QueryPlanMVCC.
func (w *walAdaptorImpl) GetLatestMVCCTimestamp(ctx context.Context, vchannel string) (uint64, error) {
	mvcc, err := w.GetLatestQueryPlanMVCC(ctx, vchannel)
	if err != nil {
		return 0, err
	}
	return mvcc.GetGrowingTimetick(), nil
}

func (w *walAdaptorImpl) GetLatestQueryPlanMVCC(ctx context.Context, vchannel string) (*viewpb.QueryPlanMVCC, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()
	currentMVCC := w.param.MVCCManager.GetMVCCOfVChannel(vchannel)
	if currentMVCC.GrowingTimetick == 0 && currentMVCC.TransformingTimetick == 0 {
		return nil, viewerror.NewViewNotFound("query mvcc for vchannel %s is unavailable", vchannel)
	}
	if !currentMVCC.Confirmed {
		// if the mvcc is not confirmed, trigger a sync operation to make it confirmed as soon as possible.
		resource.Resource().TimeTickInspector().TriggerSync(w.rwWALImpls.Channel(), false)
	}
	mlog.Debug(ctx, "query view latest mvcc resolved",
		mlog.FieldVChannel(vchannel),
		mlog.Uint64("growingTimeTick", currentMVCC.GrowingTimetick),
		mlog.Uint64("transformingTimeTick", currentMVCC.TransformingTimetick),
		mlog.Bool("confirmed", currentMVCC.Confirmed),
	)
	return &viewpb.QueryPlanMVCC{
		GrowingTimetick:      currentMVCC.GrowingTimetick,
		TransformingTimetick: currentMVCC.TransformingTimetick,
	}, nil
}

func (w *walAdaptorImpl) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlan, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	if req == nil || req.GetShardId() == nil {
		return nil, viewerror.NewUnknownError("query plan request misses shard id")
	}
	if w.queryViewHandler == nil {
		return nil, viewerror.NewViewNotFound("query view handler is unavailable")
	}

	shardID := qviews.FromProtoShardID(req.GetShardId())
	lease, err := w.queryViewHandler.AcquireLatestUpView(ctx, shardID)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if req.GetCollectionId() != 0 && lease.Meta.GetCollectionId() != req.GetCollectionId() {
		return nil, viewerror.NewViewNotFound("query view collection mismatch, expected %d, got %d", req.GetCollectionId(), lease.Meta.GetCollectionId())
	}

	mvcc, err := w.resolveQueryPlanMVCC(ctx, req, shardID.VChannel)
	if err != nil {
		return nil, err
	}

	var runtime *queryresource.QueryRuntime
	if w.viewResourceManager != nil {
		runtime, _ = w.viewResourceManager.GetQueryRuntime(qviews.QueryViewKey{
			ShardID:          shardID,
			QueryViewVersion: lease.Version,
		})
	}
	optimizer := queryresource.NewGlobalOptimizer(runtime, lease.Version.DataVersion, shard.WALFunctionRunnerKey(shardID.VChannel))
	plan := &viewpb.QueryPlan{
		Version: lease.Version.IntoProto(),
		ShardId: shardID.IntoProto(),
		Mvcc:    mvcc,
	}
	switch request := req.GetRequest().(type) {
	case *viewpb.GetQueryPlanRequest_LegacySearchRequest:
		if request.LegacySearchRequest == nil {
			return nil, viewerror.NewUnknownError("query plan request misses legacy search request")
		}
		searchReq := proto.Clone(request.LegacySearchRequest).(*internalpb.SearchRequest)
		fillSearchRequestPartitionIDs(searchReq, req.GetPartitionIds())
		optimization, err := optimizer.OptimizeSearch(ctx, searchReq)
		if err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: searchReq}
		if !optimization.Skip {
			plan.WorkNodes = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
				ignoreGrowing: searchReq.GetIgnoreGrowing(),
				partitionIDs:  searchReq.GetPartitionIDs(),
				runtime:       runtime,
				mvcc:          mvcc,
			})
		}
	case *viewpb.GetQueryPlanRequest_LegacyRetrieveRequest:
		if request.LegacyRetrieveRequest == nil {
			return nil, viewerror.NewUnknownError("query plan request misses legacy retrieve request")
		}
		retrieveReq := proto.Clone(request.LegacyRetrieveRequest).(*internalpb.RetrieveRequest)
		fillRetrieveRequestPartitionIDs(retrieveReq, req.GetPartitionIds())
		if err := optimizer.OptimizeRetrieve(ctx, retrieveReq); err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: retrieveReq}
		plan.WorkNodes = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
			ignoreGrowing: retrieveReq.GetIgnoreGrowing(),
			partitionIDs:  retrieveReq.GetPartitionIDs(),
			runtime:       runtime,
			mvcc:          mvcc,
		})
	default:
		return nil, viewerror.NewUnknownError("query plan request misses legacy request")
	}
	mlog.Debug(ctx, "query view plan created",
		mlog.FieldCollectionID(lease.Meta.GetCollectionId()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int64("replicaID", shardID.ReplicaID),
		mlog.Uint64("growingTimeTick", mvcc.GetGrowingTimetick()),
		mlog.Uint64("transformingTimeTick", mvcc.GetTransformingTimetick()),
		mlog.Int("workNodeCount", len(plan.WorkNodes)),
	)
	return plan, nil
}

func fillSearchRequestPartitionIDs(req *internalpb.SearchRequest, partitionIDs []int64) {
	if req == nil || len(req.GetPartitionIDs()) > 0 || len(partitionIDs) == 0 {
		return
	}
	req.PartitionIDs = append([]int64(nil), partitionIDs...)
}

func fillRetrieveRequestPartitionIDs(req *internalpb.RetrieveRequest, partitionIDs []int64) {
	if req == nil || len(req.GetPartitionIDs()) > 0 || len(partitionIDs) == 0 {
		return
	}
	req.PartitionIDs = append([]int64(nil), partitionIDs...)
}

func (w *walAdaptorImpl) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	if req == nil || req.GetVchannel() == "" {
		return nil, viewerror.NewUnknownError("mvcc request misses vchannel")
	}
	if w.Channel().AccessMode != types.AccessModeRW {
		return nil, viewerror.NewNotPrimaryError("wal %s is not primary", w.Channel().String())
	}
	mvcc, err := w.GetLatestQueryPlanMVCC(ctx, req.GetVchannel())
	if err != nil {
		return nil, err
	}
	return &viewpb.GetMVCCTimestampResponse{Mvcc: mvcc}, nil
}

func (w *walAdaptorImpl) resolveQueryPlanMVCC(ctx context.Context, req *viewpb.GetQueryPlanRequest, vchannel string) (*viewpb.QueryPlanMVCC, error) {
	switch mvcc := req.GetMvcc().(type) {
	case *viewpb.GetQueryPlanRequest_QueryPlanMvcc:
		return mvcc.QueryPlanMvcc, nil
	case *viewpb.GetQueryPlanRequest_ConsistencyLevel:
		if w.Channel().AccessMode != types.AccessModeRW {
			return nil, viewerror.NewNotPrimaryError("wal %s is not primary", w.Channel().String())
		}
		return w.GetLatestQueryPlanMVCC(ctx, vchannel)
	default:
		return nil, viewerror.NewUnknownError("query plan request misses mvcc source")
	}
}

type queryPlanGrowingRuntime interface {
	MayHaveVisibleGrowingSegments(growingTimetick uint64, transformingTimetick uint64, partitionIDs []int64) bool
}

type queryPlanWorkNodeOptions struct {
	ignoreGrowing bool
	partitionIDs  []int64
	runtime       queryPlanGrowingRuntime
	mvcc          *viewpb.QueryPlanMVCC
}

func buildQueryPlanWorkNodes(view *viewpb.QueryViewOfShard, options queryPlanWorkNodeOptions) []*viewpb.QueryPlanWorkNode {
	nodes := make([]*viewpb.QueryPlanWorkNode, 0, 1+len(view.GetQueryNode()))
	if queryPlanIncludesStreamingNode(view, options) {
		nodes = append(nodes, &viewpb.QueryPlanWorkNode{
			Node: &viewpb.QueryPlanWorkNode_StreamingNode{
				StreamingNode: &viewpb.StreamingWorkNode{
					Pchannel: qviews.NewStreamingNodeFromVChannel(view.GetMeta().GetVchannel()).PChannel,
				},
			},
		})
	}
	for _, qn := range view.GetQueryNode() {
		if !queryNodeHasSelectedSegments(qn, options.partitionIDs) {
			continue
		}
		nodes = append(nodes, &viewpb.QueryPlanWorkNode{
			Node: &viewpb.QueryPlanWorkNode_QueryNode{
				QueryNode: &viewpb.QueryWorkNode{NodeId: qn.GetNodeId()},
			},
		})
	}
	return nodes
}

func queryPlanIncludesStreamingNode(view *viewpb.QueryViewOfShard, options queryPlanWorkNodeOptions) bool {
	if view.GetStreamingNode() == nil || options.ignoreGrowing {
		return false
	}
	if options.runtime == nil || options.mvcc == nil {
		return true
	}
	return options.runtime.MayHaveVisibleGrowingSegments(
		options.mvcc.GetGrowingTimetick(),
		options.mvcc.GetTransformingTimetick(),
		options.partitionIDs,
	)
}

func queryNodeHasSelectedSegments(qn *viewpb.QueryViewOfQueryNode, partitionIDs []int64) bool {
	if len(partitionIDs) == 0 {
		for _, partition := range qn.GetPartitions() {
			if len(partition.GetSegmentIds()) > 0 {
				return true
			}
		}
		return false
	}
	selectedPartitions := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		selectedPartitions[partitionID] = struct{}{}
	}
	for _, partition := range qn.GetPartitions() {
		if _, ok := selectedPartitions[partition.GetPartitionId()]; ok && len(partition.GetSegmentIds()) > 0 {
			return true
		}
	}
	return false
}

func (w *walAdaptorImpl) TransformLog() wal.TransformLogAccesser {
	if w.param == nil || w.param.RecoveryStorage == nil {
		return wal.NewTransformLogErrorAccesser(status.NewOnShutdownError("recovery storage is unavailable"))
	}
	return w.param.RecoveryStorage.TransformLog()
}

// GetReplicateCheckpoint returns the replicate checkpoint of the wal.
func (w *walAdaptorImpl) GetReplicateCheckpoint() (*utility.ReplicateCheckpoint, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.param.ReplicateManager.GetReplicateCheckpoint()
}

// GetSalvageCheckpoint returns all salvage checkpoints captured during force promote.
func (w *walAdaptorImpl) GetSalvageCheckpoint() []*utility.ReplicateCheckpoint {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil
	}
	defer w.lifetime.Done()

	return w.param.ReplicateManager.GetSalvageCheckpoint()
}

// Append writes a record to the log.
func (w *walAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (_ *wal.AppendResult, err error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALAppend)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if w.isFenced.Load() {
		// if the wal is fenced, we should reject all append operations.
		return nil, status.NewChannelFenced(w.Channel().String())
	}

	if msg.MessageType().IsDMLMessageType() && w.IsRejected() {
		// if the wal is rate limit rejected, we reject all the DML operation to protect the wal from being overloaded.
		return nil, status.NewRateLimitRejected("")
	}

	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.availableCtx.Done():
		return nil, status.NewOnShutdownError("wal is on shutdown")
	case <-w.interceptorBuildResult.Interceptor.Ready():
	}

	// Setup the term of wal.
	msg = msg.WithWALTerm(w.Channel().Term)

	// we need to promise the state of wal kept consistent with the memory state of streamingnode.
	// So we don't allow the append operation can be canceled by the append caller to avoid leave a inconsistent state of alive wal,
	// the wal append operation can only be canceled when the wal is shutting down.
	ctx, cancel := contextutil.MergeContext(context.WithoutCancel(ctx), w.availableCtx)
	defer cancel()

	appendMetrics := w.writeMetrics.StartAppend(msg)
	ctx = utility.WithAppendMetricsContext(ctx, appendMetrics)

	// Metrics for append message.
	metricsGuard := appendMetrics.StartAppendGuard()

	// Execute the interceptor and wal append.
	var extraAppendResult utility.ExtraAppendResult
	ctx = utility.WithExtraAppendResult(ctx, &extraAppendResult)
	messageID, err := w.interceptorBuildResult.Interceptor.DoAppend(ctx, msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			// The lock interceptor still holds its lock while this callback runs, so
			// recheck the fence here: an append that passed the check at the entry of
			// Append and then waited for that lock must not be persisted behind an
			// AlterWAL message that was persisted in the meantime.
			if w.isFenced.Load() {
				return nil, walimpls.ErrFenced
			}

			if notPersistHint := utility.GetNotPersisted(ctx); notPersistHint != nil {
				// do not persist the message if the hint is set.
				return notPersistHint.MessageID, nil
			}

			metricsGuard.StartWALImplAppend()
			msgID, err := w.retryAppendWhenRecoverableError(ctx, msg)
			metricsGuard.FinishWALImplAppend()
			if err != nil {
				return msgID, err
			}

			if msg.MessageType() == message.MessageTypeAlterWAL {
				// AlterWAL is exclusive and pchannel-level, so the lock interceptor holds
				// the global exclusive lock here while every other append holds the shared
				// one. Raising the fence inside the callback therefore makes it atomic with
				// respect to persistence: no other append can be persisting right now, and
				// every later one sees the fence in the recheck above.
				w.Logger().Info(ctx, "alter WAL message appended, marking WAL as fenced")
				w.isFenced.Store(true)
			}
			return msgID, nil
		})
	metricsGuard.FinishAppend()
	if err != nil {
		appendMetrics.Done(ctx, nil, err)
		if errors.Is(err, walimpls.ErrFenced) {
			// if the append operation of wal is fenced, we should report the error to the client.
			if w.isFenced.CompareAndSwap(false, true) {
				w.forceCancelAfterGracefulTimeout()
				w.Logger().Warn(context.TODO(), "wal is fenced, mark as unavailable, all append opertions will be rejected", mlog.Err(err))
			}
			return nil, status.NewChannelFenced(w.Channel().String())
		}
		return nil, err
	}
	// The fence itself was already raised inside the append callback.
	if msg.MessageType() == message.MessageTypeAlterWAL {
		w.forceCancelAfterGracefulTimeout()
		w.Logger().Info(ctx, "alter WAL message appended, WAL marked as fenced, all append operations will be rejected")
	}
	w.appendRateCounter.Add(int64(msg.EstimateSize()))

	var extra *anypb.Any
	if extraAppendResult.Extra != nil {
		var err error
		if extra, err = anypb.New(extraAppendResult.Extra); err != nil {
			panic("unreachable: failed to marshal extra append result")
		}
	}

	// unwrap the messageID if needed.
	r := &wal.AppendResult{
		MessageID:              messageID,
		LastConfirmedMessageID: extraAppendResult.LastConfirmedMessageID,
		TimeTick:               extraAppendResult.TimeTick,
		TxnCtx:                 extraAppendResult.TxnCtx,
		Extra:                  extra,
	}
	appendMetrics.Done(ctx, r, nil)
	return r, nil
}

// Read overrides the roWALAdaptorImpl.Read to automatically add the append rate counter.
func (w *walAdaptorImpl) Read(ctx context.Context, opts wal.ReadOption) (wal.Scanner, error) {
	// Automatically add the append rate counter to the read options.
	opts.AppendRateCounter = w.appendRateCounter
	return w.roWALAdaptorImpl.Read(ctx, opts)
}

// retryAppendWhenRecoverableError retries the append operation when recoverable error occurs.
func (w *walAdaptorImpl) retryAppendWhenRecoverableError(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 5 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	// An append operation should be retried until it succeeds or some unrecoverable error occurs.
	for i := 0; ; i++ {
		appendCtx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALAppendImpl)
		message.OverwriteTraceContext(appendCtx, msg)
		msgID, err := w.rwWALImpls.Append(appendCtx, msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if err == nil {
			if msg.MessageType() == message.MessageTypeAlterWAL {
				// if the append operation is a alter WAL message, we should log the message
				w.Logger().Info(context.TODO(), "append alter WAL message to WAL finish", mlog.String("channel", msg.VChannel()), mlog.Uint64("timetick", msg.TimeTick()))
			}
			return msgID, nil
		}
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded, walimpls.ErrFenced) {
			return nil, err
		}
		w.writeMetrics.ObserveRetry()
		nextInterval := backoff.NextBackOff()
		w.Logger().Warn(context.TODO(), "append message into wal impls failed, retrying...", mlog.FieldMessage(msg), mlog.Int("retry", i), mlog.Duration("nextInterval", nextInterval), mlog.Err(err))

		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-w.availableCtx.Done():
			return nil, status.NewOnShutdownError("wal is on shutdown")
		case <-time.After(nextInterval):
		}
	}
}

// AppendAsync writes a record to the log asynchronously.
func (w *walAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		cb(nil, status.NewOnShutdownError("wal is on shutdown"))
		return
	}

	// Submit async append to a background execution pool.
	_ = w.appendExecutionPool.Submit(func() (struct{}, error) {
		defer w.lifetime.Done()

		msgID, err := w.Append(ctx, msg)
		cb(msgID, err)
		return struct{}{}, nil
	})
}

// Close overrides Scanner Close function.
func (w *walAdaptorImpl) Close() {
	w.Logger().Info(context.TODO(), "wal begin to close, start graceful close...")
	// graceful close the interceptors before wal closing.
	w.interceptorBuildResult.GracefulCloseFunc()
	w.Logger().Info(context.TODO(), "wal graceful close done, wait for operation to be finished...")

	// begin to close the wal.
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.forceCancelAfterGracefulTimeout()
	w.lifetime.Wait()

	if w.queryViewHandler != nil {
		w.Logger().Info(context.TODO(), "wal begin to close query view state machine...")
		w.queryViewHandler.CloseForHandoff()
	}
	if w.viewResourceManager != nil {
		w.Logger().Info(context.TODO(), "wal begin to close query view resources...")
		w.viewResourceManager.Close()
	}

	// close the recovery-owned data path.
	w.Logger().Info(context.TODO(), "wal begin to close recovery data path...")
	if w.param.RecoveryStorage != nil {
		w.param.RecoveryStorage.Close()
	}

	w.Logger().Info(context.TODO(), "wal begin to close scanners...")

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		mlog.Info(context.TODO(), "close scanner by wal adaptor", mlog.Int64("id", id), mlog.Any("channel", w.Channel()))
		return true
	})

	w.Logger().Info(context.TODO(), "scanner close done, close inner wal...")
	w.rwWALImpls.Close()

	w.Logger().Info(context.TODO(), "wal close done, close interceptors...")
	w.interceptorBuildResult.Close()

	w.Logger().Info(context.TODO(), "close the write ahead buffer...")
	w.param.WriteAheadBuffer.Close()

	w.Logger().Info(context.TODO(), "close the segment assignment manager...")
	w.param.ShardManager.Close()

	w.Logger().Info(context.TODO(), "call wal cleanup function...")
	w.cleanup()
	w.Logger().Info(context.TODO(), "wal closed")

	// close all metrics.
	w.scanMetrics.Close()
	w.writeMetrics.Close()

	// close the rate limit component.
	w.WALRateLimitComponent.Close()

	if w.appendExecutionPool != nil {
		w.appendExecutionPool.Release()
	}
}

type interceptorBuildResult struct {
	Interceptor       interceptors.InterceptorWithReady
	GracefulCloseFunc gracefulCloseFunc
}

func (r interceptorBuildResult) Close() {
	r.Interceptor.Close()
}

// newWALWithInterceptors creates a new wal with interceptors.
func buildInterceptor(builders []interceptors.InterceptorBuilder, param *interceptors.InterceptorBuildParam) interceptorBuildResult {
	// Build all interceptors.
	builtIterceptors := make([]interceptors.Interceptor, 0, len(builders))
	for _, b := range builders {
		builtIterceptors = append(builtIterceptors, b.Build(param))
	}
	return interceptorBuildResult{
		Interceptor: interceptors.NewChainedInterceptor(builtIterceptors...),
		GracefulCloseFunc: func() {
			for _, i := range builtIterceptors {
				if c, ok := i.(interceptors.InterceptorWithGracefulClose); ok {
					c.GracefulClose()
				}
			}
		},
	}
}

func buildInterceptorsAndReleaseInitialSnapshot(
	builders []interceptors.InterceptorBuilder,
	param *interceptors.InterceptorBuildParam,
) interceptorBuildResult {
	result := buildInterceptor(builders, param)
	param.InitialRecoverSnapshot = nil
	return result
}
