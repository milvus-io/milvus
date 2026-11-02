package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// beginStopping rejects preparation while existing Ready views continue serving
// until the coordinator migrates them. It serializes with readiness callbacks.
func (h *QNQueryViewHandler) beginStopping() []<-chan struct{} {
	h.mu.Lock()
	h.stopping = true
	shards := h.shardsLocked()
	h.mu.Unlock()
	var dropped []<-chan struct{}
	for _, shard := range shards {
		shard.mu.Lock()
		shard.stopping = true
		for _, entry := range shard.views {
			dropped = append(dropped, entry.dropped)
			if entry.sm.State() == qviews.QueryViewStatePreparing {
				entry.sm.OnUnrecoverable()
				shard.consumeReportAndCleanup(entry.View.QueryViewKey(), entry)
				// Unrecoverable normally retains partial resources for a successor.
				// A stopping node will not prepare successors, so release locally.
				shard.releaseQueryResourceLocked(entry.View.QueryViewKey().QueryViewVersion, entry)
			}
		}
		shard.mu.Unlock()
	}
	return dropped
}

// Closing lets ViewSync streams complete their close handshake during final
// shutdown. Reconnection alone never releases retained views.
func (h *QNQueryViewHandler) Closing() <-chan struct{} { return h.closing }

// Close stops preparation, waits for all views to finish Dropped, and drains all
// local resources. Only a nil result permits the owner to close segcore.
// The first call's context bounds graceful migration. Cancellation withdraws
// remaining views and returns ctx.Err(), but cleanup continues. Later calls can
// wait for that same cleanup with their own context; shutdown is never restarted.
func (h *QNQueryViewHandler) Close(ctx context.Context) error {
	h.closeOnce.Do(func() { go h.close(ctx) })
	select {
	case <-h.closed:
		return nil
	default:
	}
	select {
	case <-h.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *QNQueryViewHandler) close(ctx context.Context) {
	// Stopping prevents new entries, so these completion events cover every
	// admitted View, including Unrecoverable and already-Dropping entries.
	for _, dropped := range h.beginStopping() {
		select {
		case <-ctx.Done():
			h.drain()
			return
		case <-dropped:
		}
	}
	h.drain()
}

func (h *QNQueryViewHandler) drain() {
	h.mu.Lock()
	close(h.closing)
	shards := h.shardsLocked()
	h.mu.Unlock()
	var dropped []<-chan struct{}
	for _, shard := range shards {
		shard.mu.Lock()
		for _, entry := range shard.views {
			dropped = append(dropped, entry.dropped)
			entry.sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
			shard.consumeReportAndCleanup(entry.View.QueryViewKey(), entry)
		}
		shard.mu.Unlock()
	}
	for _, done := range dropped {
		<-done
	}
	// Segment-level Dropped has drained View-owned work. Finish the manager
	// callbacks and its shared buffer workers before declaring node cleanup done.
	h.segMgr.Close()
	close(h.closed)
}

// Never acquire shard.mu while holding h.mu: shard cleanup calls onEmpty.
func (h *QNQueryViewHandler) shardsLocked() []*qnShardView {
	shards := make([]*qnShardView, 0, len(h.shards))
	for _, shard := range h.shards {
		shards = append(shards, shard)
	}
	return shards
}
