package qnview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
)

var _ handler.QueryViewHandler = (*QNQueryViewHandler)(nil)

// QNQueryViewHandler implements QueryViewHandler for QueryNode.
//
// It manages query view state machines across multiple shards using a
// two-level locking scheme:
//   - Outer sync.Mutex: protects the shard map
//   - Per-shard sync.Mutex: serializes SM operations within a shard
//
// QN is stateless: no persistence, no recovery. On restart, Coord
// re-pushes all Preparing views.
//
// Segment loading is delegated to the SegmentManager. When a new Preparing
// view arrives, the handler acquires segments via SegmentManager. The
// SegmentManager drives SM progress by invoking OnReady/OnUnrecoverable
// callbacks asynchronously.
//
// # Response Guarantee
//
// Every view pushed via ApplyViews is guaranteed to eventually produce a
// response (via OnReport callback), provided the SegmentManager fulfills
// its liveness contracts (see SegmentManager doc). The response paths are:
//
// View does not exist in handler:
//
//   - Preparing: creates SM + calls Acquire. No immediate response.
//     Response depends on SegmentManager calling OnReady or OnUnrecoverable.
//   - Dropped: responds immediately with the Dropped view (QN restart case).
//   - Other states: responds immediately with Unrecoverable (state lost after restart).
//
// View already exists in handler:
//
//   - Preparing, SM still Preparing: no immediate response.
//     Response depends on SegmentManager calling OnReady or OnUnrecoverable.
//   - Preparing, SM past Preparing: responds immediately with current state
//     (Ready/Unrecoverable/Dropping/Dropped) for Coord fast-forward.
//   - Dropped, SM in Preparing/Ready/Unrecoverable: transitions to Dropping,
//     calls Release. No immediate response.
//     Response depends on SegmentManager calling OnDropped.
//   - Dropped, SM in Dropping: ignored (Release already in progress).
//     Response depends on prior Release's OnDropped callback.
//   - Dropped, SM in Dropped: responds immediately with Dropped re-report.
//     (In practice unreachable — entry is deleted upon reaching Dropped.)
type QNQueryViewHandler struct {
	mu     sync.Mutex
	shards map[qviews.ShardID]*qnShardView
	segMgr SegmentManager
}

// NewQNQueryViewHandler creates a new QNQueryViewHandler.
func NewQNQueryViewHandler(segMgr SegmentManager) *QNQueryViewHandler {
	return &QNQueryViewHandler{
		shards: make(map[qviews.ShardID]*qnShardView),
		segMgr: segMgr,
	}
}

// ApplyViews applies a batch of coord-pushed views.
// Views are grouped by ShardID and applied atomically per shard.
// All state reports are delivered through the OnReport callback.
func (h *QNQueryViewHandler) ApplyViews(views []handler.ApplyView) {
	// Group views by ShardID.
	grouped := make(map[qviews.ShardID][]handler.ApplyView)
	for i := range views {
		shardID := views[i].View.QueryViewKey().ShardID
		grouped[shardID] = append(grouped[shardID], views[i])
	}

	// Apply each group atomically under the shard lock.
	for shardID, shardViews := range grouped {
		shard := h.getOrCreateShard(shardID)
		shard.ApplyViews(shardViews)
	}
}

func (h *QNQueryViewHandler) getOrCreateShard(shardID qviews.ShardID) *qnShardView {
	h.mu.Lock()
	defer h.mu.Unlock()
	shard, ok := h.shards[shardID]
	if !ok {
		shard = &qnShardView{
			views:  make(map[qviews.QueryViewVersion]*qnViewEntry),
			segMgr: h.segMgr,
			onEmpty: func() {
				h.mu.Lock()
				defer h.mu.Unlock()
				delete(h.shards, shardID)
			},
		}
		h.shards[shardID] = shard
	}
	return shard
}
