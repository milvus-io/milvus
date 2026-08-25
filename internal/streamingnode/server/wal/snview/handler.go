package snview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
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
// Every view pushed via ApplyViews while the handler is open is guaranteed to
// eventually produce a response (via OnReport callback), provided the
// StreamingNodeResourceManager fulfills its liveness contracts (see
// StreamingNodeResourceManager doc). Views arriving after CloseForHandoff are
// ignored and re-pushed by Coord after it reconnects to the new WAL owner.
// The response paths are:
//
// View does not exist in handler:
//
//   - Preparing: creates SM + calls Acquire. No immediate response.
//     Response depends on ResourceManager calling OnReady.
//   - Dropped: responds immediately with the Dropped view (SN restart case).
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
	mu             sync.Mutex
	closed         bool
	pchannel       string
	shards         map[qviews.ShardID]*snShardView
	catalog        metastore.StreamingNodeCataLog
	resMgr         StreamingNodeResourceManager
	localOptimizer optimizer.LocalOptimizer
}

type QueryViewLease struct {
	Version qviews.QueryViewVersion
	Meta    *viewpb.QueryViewMeta
	View    *viewpb.QueryViewOfShard
	Release func()
}

// recoverSNQueryViewHandler reconstructs the handler from persisted views
// during SN startup. Pass nil or empty views for a fresh handler.
func recoverSNQueryViewHandler(
	pchannel string,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *SNQueryViewHandler {
	h := &SNQueryViewHandler{
		pchannel:       pchannel,
		shards:         make(map[qviews.ShardID]*snShardView),
		catalog:        catalog,
		resMgr:         resMgr,
		localOptimizer: optimizer.NewNoopLocalOptimizer(),
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
		shard := recoverSnShardView(pchannel, shardID, shardViews, catalog, resMgr)
		shard.onEmpty = h.makeOnEmpty(shardID)
		h.shards[shardID] = shard
	}

	return h
}

func RecoverPChannelSNQueryViewHandler(
	pchannel string,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *SNQueryViewHandler {
	return recoverSNQueryViewHandler(pchannel, catalog, resMgr, views)
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
		shard := h.getOrCreateShard(shardID)
		if shard == nil {
			continue
		}
		shard.ApplyViews(shardViews)
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

func (h *SNQueryViewHandler) AcquireLatestUpView(ctx context.Context, shardID qviews.ShardID) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	h.mu.Lock()
	shard := h.shards[shardID]
	h.mu.Unlock()
	if shard == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", shardID.String())
	}
	return shard.acquireLatestUpView(ctx)
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

func (h *SNQueryViewHandler) makeOnEmpty(shardID qviews.ShardID) func() {
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.shards, shardID)
	}
}
