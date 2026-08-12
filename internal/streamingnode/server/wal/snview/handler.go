package snview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ handler.QueryViewHandler = (*SNQueryViewHandler)(nil)

// SNQueryViewHandler implements QueryViewHandler for StreamingNode.
//
// It manages query view state machines across multiple shards using a
// two-level locking scheme:
//   - Outer sync.Mutex: protects the shard map
//   - Per-shard sync.Mutex: serializes SM operations within a shard
//
// SN supports crash recovery via persistence. Recovered views start in
// UpRecovering state and transition to Up once WAL catch-up completes.
//
// Resource management is delegated to the StreamingNodeResourceManager.
// When a new Preparing view arrives, the handler acquires resources via
// ResourceManager. The ResourceManager drives SM progress by invoking
// OnReady callbacks asynchronously.
//
// # Response Guarantee
//
// Every view pushed via ApplyViews is guaranteed to eventually produce a
// response (via OnReport callback), provided the StreamingNodeResourceManager
// fulfills its liveness contracts (see StreamingNodeResourceManager doc).
// The response paths are:
//
// View does not exist in handler:
//
//   - Preparing: creates SM + calls Acquire. No immediate response.
//     Response depends on ResourceManager calling OnReady.
//   - Down or Dropped: responds immediately with Dropped (teardown already
//     complete locally after SN restart or handoff).
//   - Other states: responds immediately with Unrecoverable (state lost after restart).
//
// View already exists in handler:
//
//   - Preparing, SM in Preparing/UpRecovering/Dropping: no immediate response.
//     Response depends on ResourceManager callbacks when a resource operation is pending.
//   - Preparing, SM past Preparing/UpRecovering/Dropping: responds immediately with
//     current state (Ready/Up/Down/Unrecoverable/Dropped) for Coord fast-forward.
//   - Dropped, SM in Preparing/Ready/Up/Down/Unrecoverable: transitions to Dropping,
//     calls Release. No immediate response.
//     Response depends on ResourceManager calling OnDropped.
//   - Dropped, SM in Dropping: ignored (Release already in progress).
//     Response depends on prior Release's OnDropped callback.
//   - Dropped, SM in Dropped: responds immediately with Dropped re-report.
//     (In practice unreachable — entry is deleted upon reaching Dropped.)
//   - Other states: SM handles coord push and responds accordingly.
type SNQueryViewHandler struct {
	mu       sync.Mutex
	closed   bool
	ctx      context.Context
	pchannel string
	shards   map[qviews.ShardID]*snShardView
	catalog  metastore.StreamingNodeCataLog
	resMgr   StreamingNodeResourceManager
}

// recoverSNQueryViewHandler reconstructs the handler from persisted views
// during SN startup. Pass nil or empty views for a fresh handler.
func recoverSNQueryViewHandler(
	ctx context.Context,
	pchannel string,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *SNQueryViewHandler {
	h := &SNQueryViewHandler{
		ctx:      ctx,
		pchannel: pchannel,
		shards:   make(map[qviews.ShardID]*snShardView),
		catalog:  catalog,
		resMgr:   resMgr,
	}

	grouped := make(map[qviews.ShardID]map[qviews.QueryViewVersion]*snQueryViewStateMachine)

	for _, view := range views {
		meta := view.Meta
		snView := view.StreamingNode
		shardID := qviews.NewShardIDFromQVMeta(meta)
		version := qviews.FromProtoQueryViewVersion(meta.Version)

		shardViews, ok := grouped[shardID]
		if !ok {
			shardViews = make(map[qviews.QueryViewVersion]*snQueryViewStateMachine)
			grouped[shardID] = shardViews
		}
		shardViews[version] = recoverSNQueryViewStateMachine(meta, snView, view.GetQueryNode())
	}

	for shardID, shardViews := range grouped {
		shard := recoverSnShardView(ctx, pchannel, shardID, shardViews, catalog, resMgr)
		shard.onEmpty = h.makeOnEmpty(shardID)
		h.shards[shardID] = shard
	}
	for _, shard := range h.shards {
		shard.startRecovery()
	}

	return h
}

func RecoverPChannelSNQueryViewHandler(
	ctx context.Context,
	pchannel string,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *SNQueryViewHandler {
	return recoverSNQueryViewHandler(ctx, pchannel, catalog, resMgr, views)
}

func OldestUpDataVersions(views []*viewpb.QueryViewOfShard) map[string]qviews.DataVersion {
	result := make(map[string]qviews.DataVersion)
	for _, view := range views {
		meta := view.GetMeta()
		if qviews.QueryViewState(meta.GetState()) != qviews.QueryViewStateUp || meta.GetVersion() == nil {
			continue
		}
		version := qviews.FromProtoQueryViewVersion(meta.GetVersion())
		current, ok := result[meta.GetVchannel()]
		if !ok || current.GT(version.DataVersion) {
			result[meta.GetVchannel()] = version.DataVersion
		}
	}
	return result
}

// ApplyViews applies a batch of coord-pushed views.
// Views are grouped by ShardID and applied atomically per shard.
// All state reports are delivered through the OnReport callback.
func (h *SNQueryViewHandler) ApplyViews(views []handler.ApplyView) {
	// Group views by ShardID.
	grouped := make(map[qviews.ShardID][]handler.ApplyView)
	for i := range views {
		shardID := views[i].View.QueryViewKey().ShardID
		grouped[shardID] = append(grouped[shardID], views[i])
	}

	// Apply each group atomically under the shard lock.
	for shardID, shardViews := range grouped {
		for {
			shard := h.getOrCreateShard(shardID)
			if shard == nil || shard.ApplyViews(shardViews) {
				break
			}
		}
	}
}

func (h *SNQueryViewHandler) CloseForHandoff() {
	h.mu.Lock()
	h.closed = true
	shards := make([]*snShardView, 0, len(h.shards))
	for _, shard := range h.shards {
		shards = append(shards, shard)
	}
	h.shards = make(map[qviews.ShardID]*snShardView)
	h.mu.Unlock()

	for _, shard := range shards {
		shard.CloseForHandoff()
	}
}

func (h *SNQueryViewHandler) getOrCreateShard(shardID qviews.ShardID) *snShardView {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	shard, ok := h.shards[shardID]
	if !ok {
		shard = &snShardView{
			ctx:      h.ctx,
			pchannel: h.pchannel,
			shardID:  shardID,
			views:    make(map[qviews.QueryViewVersion]*snViewEntry),
			catalog:  h.catalog,
			resMgr:   h.resMgr,
			onEmpty:  h.makeOnEmpty(shardID),
		}
		h.shards[shardID] = shard
	}
	return shard
}

func (h *SNQueryViewHandler) makeOnEmpty(shardID qviews.ShardID) func(*snShardView) {
	return func(emptyShard *snShardView) {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.shards[shardID] == emptyShard {
			delete(h.shards, shardID)
		}
	}
}
