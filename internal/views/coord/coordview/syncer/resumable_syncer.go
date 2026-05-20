package syncer

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// resumableSyncer manages a single gRPC bidirectional stream to a work node.
// It owns a pendingSyncQueryViews instance that tracks all views dispatched
// to this node. It runs a single loop that creates a stream, re-pushes all
// pending views, and on stream break reconnects with exponential backoff.
//
// Close stops the loop but does NOT drain pending views.
// Use DrainPendingIfNodeLost after Close for node loss scenarios.
type resumableSyncer struct {
	node    qviews.WorkNode
	client  ViewSyncClient
	pending *pendingSyncQueryViews

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newResumableSyncer(
	ctx context.Context,
	node qviews.WorkNode,
	client ViewSyncClient,
) *resumableSyncer {
	ctx, cancel := context.WithCancel(ctx)
	rs := &resumableSyncer{
		node:    node,
		client:  client,
		pending: newPendingSyncQueryViews(),
		ctx:     ctx,
		cancel:  cancel,
	}
	rs.wg.Add(1)
	go rs.loop()
	return rs
}

// Sync adds views to the pending queue and notifies the send loop.
// All views MUST target the same work node that this resumableSyncer manages.
// Non-blocking: never blocks on backpressure.
func (rs *resumableSyncer) Sync(views []SyncView) {
	for i := range views {
		rs.pending.Upsert(views[i])
	}
}

// Close stops the resumableSyncer and waits for the goroutine to exit.
// Does NOT drain pending views — use DrainPendingIfNodeLost for node loss scenarios.
func (rs *resumableSyncer) Close() {
	rs.cancel()
	rs.wg.Wait()
}

// DrainPendingIfNodeLost drains all remaining pending views.
// QueryNode loss invokes OnQueryNodeLost for each pending entry; StreamingNode
// loss is not a per-view event and only clears pending entries.
// Must only be called after Close, when the node is declared lost.
func (rs *resumableSyncer) DrainPendingIfNodeLost() {
	rs.pending.Drain(rs.node)
}

// loop is the single goroutine that manages the stream lifecycle:
// create stream → re-push pending → send/recv → on break, backoff and retry.
func (rs *resumableSyncer) loop() {
	defer rs.wg.Done()

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 0 // retry forever until closed
	bo.Reset()

	for rs.ctx.Err() == nil {
		// Create stream.
		stream, err := rs.client.OpenSyncStream(rs.ctx, rs.node)
		if err != nil {
			if rs.ctx.Err() != nil {
				return
			}
			log.Warn("ResumableSyncer: failed to open stream",
				zap.String("node", rs.node.String()), zap.Error(err))

			nextBackoff := bo.NextBackOff()
			select {
			case <-time.After(nextBackoff):
			case <-rs.ctx.Done():
				return
			}
			continue
		}

		bo.Reset()

		// Re-push all pending entries for this node.
		if err := rs.rePush(stream); err != nil {
			continue
		}

		// Run send and recv in parallel; recv drives the current goroutine.
		streamCtx, streamCancel := context.WithCancel(rs.ctx)

		var sendWg sync.WaitGroup
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			rs.sendLoop(streamCtx, stream)
		}()

		// Recv blocks until stream breaks.
		rs.recvLoop(stream)

		// Stream broke — cancel send loop and wait.
		streamCancel()
		sendWg.Wait()
	}
}

// sendLoop waits for notifications and sends unsent protos to the stream.
func (rs *resumableSyncer) sendLoop(ctx context.Context, stream viewpb.ViewSyncService_SyncQueryViewClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rs.pending.Ready():
			protos := rs.pending.DrainUnsent()
			if err := rs.sendBatched(stream, protos); err != nil {
				return
			}
		}
	}
}

// recvLoop receives responses and routes them to pending callbacks.
// Returns when the stream breaks.
func (rs *resumableSyncer) recvLoop(stream viewpb.ViewSyncService_SyncQueryViewClient) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Warn("ResumableSyncer: stream recv failed",
				zap.String("node", rs.node.String()), zap.Error(err))
			return
		}

		viewsResp := resp.GetViews()
		if viewsResp == nil {
			continue
		}

		for _, pb := range viewsResp.QueryViews {
			rs.pending.MatchResponse(pb)
		}
	}
}

// rePush sends all pending entries for this node through the stream in batches
// and clears any stale unsent protos to avoid duplicate sends after reconnection.
func (rs *resumableSyncer) rePush(stream viewpb.ViewSyncService_SyncQueryViewClient) error {
	rs.pending.DrainUnsent() // clear stale unsent protos from before reconnection
	return rs.sendBatched(stream, rs.pending.CollectProtos())
}

const sendBatchSize = 16

// sendBatched sends protos in batches of sendBatchSize.
func (rs *resumableSyncer) sendBatched(stream viewpb.ViewSyncService_SyncQueryViewClient, protos []*viewpb.QueryViewOfShard) error {
	for len(protos) > 0 {
		batch := protos
		if len(batch) > sendBatchSize {
			batch = protos[:sendBatchSize]
		}
		protos = protos[len(batch):]

		req := &viewpb.SyncRequest{
			Request: &viewpb.SyncRequest_Views{
				Views: &viewpb.SyncQueryViewsRequest{
					QueryViews: batch,
				},
			},
		}
		if err := stream.Send(req); err != nil {
			log.Warn("ResumableSyncer: stream send failed",
				zap.String("node", rs.node.String()), zap.Error(err))
			return err
		}
	}
	return nil
}
