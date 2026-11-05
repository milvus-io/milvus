package grpcquerynode

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	queryViewMetadataBatchSize = 128
	queryViewMetadataBatchWait = time.Millisecond
)

type queryViewMetadataRequest struct {
	ctx       context.Context
	segmentID int64
}

// A batch owns only one RPC. Each resource retains its own cancellation,
// metadata validation and retry policy in the Segment resource manager.
type queryViewMetadataBatch struct {
	ctx          context.Context
	cancel       context.CancelFunc
	collectionID int64
	requests     []queryViewMetadataRequest
	waiters      int
	sent         bool
	timer        *time.Timer
	done         chan struct{}
	results      map[int64]*querypb.QueryViewSegmentLoadInfoResult
	response     *querypb.GetQueryViewSegmentLoadInfoResponse
	err          error
}

func (p *lazyQueryViewLoadMetadataProvider) GetQueryViewSegmentLoadInfo(ctx context.Context, collectionID, segmentID int64) (qnview.SegmentLoadInfoSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return qnview.SegmentLoadInfoSnapshot{}, err
	}
	b := p.enqueueSegmentMetadata(ctx, collectionID, segmentID)
	defer p.releaseSegmentMetadataWaiter(b)
	select {
	case <-ctx.Done():
		return qnview.SegmentLoadInfoSnapshot{}, ctx.Err()
	case <-b.done:
	}
	if err := ctx.Err(); err != nil {
		return qnview.SegmentLoadInfoSnapshot{}, err
	}
	if b.err != nil {
		return qnview.SegmentLoadInfoSnapshot{}, b.err
	}
	result := b.results[segmentID]
	if result == nil || result.GetStatus() == nil {
		return qnview.SegmentLoadInfoSnapshot{}, merr.WrapErrServiceInternalMsg("missing metadata result or status for segment %d", segmentID)
	}
	if err := merr.Error(result.GetStatus()); err != nil {
		return qnview.SegmentLoadInfoSnapshot{}, err
	}
	info := result.GetLoadInfo()
	if info == nil || info.GetSegmentID() != segmentID || info.GetCollectionID() != collectionID {
		return qnview.SegmentLoadInfoSnapshot{}, merr.WrapErrServiceInternalMsg("mismatched metadata snapshot for segment %d", segmentID)
	}
	return qnview.SegmentLoadInfoSnapshot{CollectionID: collectionID, SegmentID: segmentID, LoadInfo: info, IndexInfos: b.response.GetIndexInfoList()}, nil
}

func (p *lazyQueryViewLoadMetadataProvider) enqueueSegmentMetadata(ctx context.Context, collectionID, segmentID int64) *queryViewMetadataBatch {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()
	if p.pending == nil {
		p.pending = make(map[int64]*queryViewMetadataBatch)
	}
	b := p.pending[collectionID]
	if b == nil {
		// The batch owns cancel: flush and the final waiter both release it.
		batchCtx, cancel := context.WithCancel(p.ctx) //nolint:gosec // G118: cancellation is transferred to the batch.
		b = &queryViewMetadataBatch{ctx: batchCtx, cancel: cancel, collectionID: collectionID, done: make(chan struct{})}
		p.pending[collectionID] = b
		b.timer = time.AfterFunc(queryViewMetadataBatchWait, func() { p.flushSegmentMetadata(b) })
	}
	b.requests = append(b.requests, queryViewMetadataRequest{ctx: ctx, segmentID: segmentID})
	b.waiters++
	if len(b.requests) == queryViewMetadataBatchSize {
		delete(p.pending, collectionID)
		b.timer.Stop()
		go p.flushSegmentMetadata(b)
	}
	return b
}

func (p *lazyQueryViewLoadMetadataProvider) releaseSegmentMetadataWaiter(b *queryViewMetadataBatch) {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()
	b.waiters--
	if b.waiters == 0 {
		b.cancel()
		b.timer.Stop()
		if p.pending[b.collectionID] == b {
			delete(p.pending, b.collectionID)
		}
	}
}

func (p *lazyQueryViewLoadMetadataProvider) flushSegmentMetadata(b *queryViewMetadataBatch) {
	p.batchMu.Lock()
	if b.sent {
		p.batchMu.Unlock()
		return
	}
	b.sent = true
	if p.pending[b.collectionID] == b {
		delete(p.pending, b.collectionID)
	}
	p.batchMu.Unlock()
	defer close(b.done)
	defer b.cancel()

	ids := make([]int64, 0, len(b.requests))
	seen := make(map[int64]struct{}, len(b.requests))
	for _, request := range b.requests {
		if request.ctx.Err() != nil {
			continue
		}
		if _, ok := seen[request.segmentID]; !ok {
			seen[request.segmentID] = struct{}{}
			ids = append(ids, request.segmentID)
		}
	}
	if len(ids) == 0 {
		return
	}
	b.err = queryViewMetadataRPC(b.ctx, func(ctx context.Context) error {
		client, err := p.client(ctx)
		if err != nil {
			return err
		}
		b.response, err = client.GetQueryViewSegmentLoadInfo(ctx, &querypb.GetQueryViewSegmentLoadInfoRequest{CollectionID: b.collectionID, SegmentIDs: ids})
		return merr.CheckRPCCall(b.response, err)
	})
	if b.err != nil {
		return
	}
	b.results = make(map[int64]*querypb.QueryViewSegmentLoadInfoResult, len(ids))
	for _, result := range b.response.GetResults() {
		id := result.GetSegmentID()
		if _, requested := seen[id]; !requested {
			b.err = merr.WrapErrServiceInternalMsg("unexpected metadata result for segment %d", id)
			return
		}
		if _, duplicate := b.results[id]; duplicate {
			result = &querypb.QueryViewSegmentLoadInfoResult{SegmentID: id, Status: merr.Status(merr.WrapErrServiceInternalMsg("duplicate metadata result for segment %d", id))}
		}
		b.results[id] = result
	}
}
