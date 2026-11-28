package adaptor

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func viewKey(v *viewpb.QueryViewOfShard) qviews.QueryViewKey {
	return qviews.QueryViewKey{ShardID: qviews.NewShardIDFromQVMeta(v.Meta), QueryViewVersion: qviews.FromProtoQueryViewVersion(v.Meta.Version)}
}

// Restore pending prepares through the same assignment fence as live commands.
// A reader treats omission from its first snapshot as a retirement.
func (h *writerViewControl) seed(pchannel string, desired []*viewpb.QueryViewOfShard) {
	var prepares []handler.ApplyView
	for _, v := range desired {
		if funcutil.ToPhysicalChannel(v.Meta.Vchannel) != pchannel {
			continue
		}
		key, state := viewKey(v), v.Meta.State
		if state == viewpb.QueryViewState_QueryViewStateUp && !h.up[key] {
			state = viewpb.QueryViewState_QueryViewStatePreparing
		}
		if state == viewpb.QueryViewState_QueryViewStatePreparing && v.Meta.RequiredStreamingVersion > 0 {
			h.publisher.ExpectInitialView(key)
			local := qviews.NewFullQueryViewAtStreamingNode(v.Meta, v.StreamingNode, v.QueryNode).IntoProto()
			local.Meta.State = state
			prepares = append(prepares, handler.ApplyView{View: qviews.NewQueryViewAtWorkNodeFromProto(local)})
		} else {
			h.publisher.PublishView(key, state)
		}
	}
	sort.SliceStable(prepares, func(i, j int) bool { return prepares[j].View.Version().GT(prepares[i].View.Version()) })
	h.ApplyViews(prepares)
}

type viewOperation struct {
	ctx               context.Context
	cancel            context.CancelFunc
	command           handler.ApplyView
	local             chan qviews.QueryViewAtWorkNode
	publication       <-chan struct{}
	cancelPublication context.CancelFunc
}

// snview invokes callbacks serially under the shard lock. Keep the latest
// local state without waiting for the writer's votes, catalog I/O or mutex.
func (op *viewOperation) observe(result qviews.QueryViewAtWorkNode) {
	select {
	case <-op.local:
	default:
	}
	op.local <- result
}

// writerViewControl coordinates physical replicas on behalf of the logical SN.
// Shard locks only cover the local machine. Waiting for votes and catalog I/O
// runs independently so existing leases can keep serving while another RO loads.
type writerViewControl struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wg     sync.WaitGroup

	publisher  *replica.Publisher
	operations map[qviews.QueryViewKey]*viewOperation
	// An entry tracks a published or in-flight Up until its fenced deletion.
	// A failed RPC can leave the catalog outcome unknown.
	up       map[qviews.QueryViewKey]bool
	readSide *roWALAdaptorImpl
}

func newWriterViewControl(ctx context.Context, r *roWALAdaptorImpl, p *replica.Publisher, durable []*viewpb.QueryViewOfShard) *writerViewControl {
	ctx, cancel := context.WithCancel(ctx)
	h := &writerViewControl{ctx: ctx, cancel: cancel, publisher: p, readSide: r, operations: make(map[qviews.QueryViewKey]*viewOperation), up: make(map[qviews.QueryViewKey]bool)}
	for _, v := range durable {
		h.up[viewKey(v)] = true
	}
	return h
}

func (h *writerViewControl) Stop() {
	h.mu.Lock()
	h.cancel()
	h.mu.Unlock()
	h.wg.Wait()
}

func (h *writerViewControl) TakeViewFeedback() qviews.ViewFeedbackWindow {
	return h.readSide.readViews.TakeViewFeedback()
}

func (h *writerViewControl) ViewFeedbackReady() <-chan struct{} {
	return h.readSide.readViews.ViewFeedbackReady()
}

func (h *writerViewControl) ApplyViews(batch []handler.ApplyView) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil {
		return
	}
	for _, cmd := range batch {
		key := cmd.View.QueryViewKey()
		def := cmd.View.IntoProto()
		target := def.Meta.State
		if previous := h.operations[key]; previous != nil {
			previous.cancel()
		}
		ctx, cancel := context.WithCancel(h.ctx)
		op := &viewOperation{ctx: ctx, cancel: cancel, command: cmd, local: make(chan qviews.QueryViewAtWorkNode, 1)}
		h.operations[key] = op
		committed, mayBeUp := h.up[key]
		if target == viewpb.QueryViewState_QueryViewStateUp && !mayBeUp {
			h.up[key] = false
		}
		initial, signal := target, target
		if target == viewpb.QueryViewState_QueryViewStateUp {
			initial = viewpb.QueryViewState_QueryViewStatePreparing
			if !committed {
				signal = initial
			}
		}
		retireUp := target == viewpb.QueryViewState_QueryViewStateDropped && mayBeUp
		if retireUp {
			signal = viewpb.QueryViewState_QueryViewStateDown
		}
		gate := signal == viewpb.QueryViewState_QueryViewStatePreparing && def.Meta.RequiredStreamingVersion > 0
		if !gate {
			h.publisher.PublishView(key, signal)
		}
		// A demoted reader retires keys omitted from its new writer's snapshot.
		f := h.readSide.follower
		f.mu.Lock()
		if target == viewpb.QueryViewState_QueryViewStateDropped {
			delete(f.current, key)
		} else {
			f.current[key] = &followerView{definition: def, target: target}
		}
		f.mu.Unlock()
		local := cmd.View.IntoProto()
		local.Meta.State = initial
		h.readSide.readViews.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(local), OnReport: op.observe}})
		if gate {
			published := make(chan struct{})
			op.publication = published
			publicationCtx, cancelPublication := context.WithCancel(ctx) //nolint:gosec // Local retirement cancels this wait early. Operation completion cancels the parent.
			op.cancelPublication = cancelPublication
			h.wg.Go(func() {
				if h.readSide.readQueries.WaitForAssignments(publicationCtx, def.Meta.Vchannel, def.Meta.RequiredStreamingVersion) != nil {
					return
				}
				h.mu.Lock()
				defer h.mu.Unlock()
				if publicationCtx.Err() == nil {
					h.publisher.PublishView(key, signal)
					close(published)
				}
			})
		}
		h.wg.Go(func() {
			defer cancel()
			h.coordinate(op, committed, retireUp)
		})
	}
}

// One operation owns coordination for this command. Replacing the command
// cancels its waits. Only the current operation may publish, apply or report.
func (h *writerViewControl) coordinate(op *viewOperation, committed, retireUp bool) {
	key := op.command.View.QueryViewKey()
	target := op.command.View.State()
	defer func() {
		h.mu.Lock()
		if h.operations[key] == op {
			delete(h.operations, key)
		}
		h.mu.Unlock()
	}()
	appliedUp := false
	for {
		var result qviews.QueryViewAtWorkNode
		select {
		case <-op.ctx.Done():
			return
		case result = <-op.local:
		}
		state := result.State()
		// A reused runtime may already be Ready. Retirements and failure
		// reports must remain independent of a slow assignment save.
		if op.publication != nil && (state == qviews.QueryViewStateReady || state == qviews.QueryViewStateUp) {
			select {
			case <-op.ctx.Done():
				return
			case <-op.publication:
			}
		}
		switch {
		case target == qviews.QueryViewStateUp && !appliedUp && (state == qviews.QueryViewStateReady || state == qviews.QueryViewStateUp):
			if !committed && !h.confirm(op.ctx, key, viewpb.QueryViewState_QueryViewStateReady, op.command.View.IntoProto()) {
				return
			}
			// Serialize the last publication and local dispatch with the next
			// command. The local callback only enqueues a result.
			h.mu.Lock()
			if op.ctx.Err() == nil {
				h.publisher.PublishView(key, viewpb.QueryViewState_QueryViewStateUp)
				h.readSide.readViews.ApplyViews([]handler.ApplyView{{View: op.command.View, OnReport: op.observe}})
			}
			h.mu.Unlock()
			appliedUp = true
			continue
		case target == qviews.QueryViewStatePreparing && state == qviews.QueryViewStateReady:
			if h.publisher.WaitReports(op.ctx, key, viewpb.QueryViewState_QueryViewStateReady) != nil {
				return
			}
		case state == qviews.QueryViewStateDown || state == qviews.QueryViewStateDropped:
			// A lagging Coord can re-push Preparing/Up after local retirement.
			// Fast-forward its report through the same group Down barrier.
			if target != qviews.QueryViewStateDropped || retireUp {
				if target != qviews.QueryViewStateDown && !retireUp {
					h.mu.Lock()
					if op.ctx.Err() == nil {
						if op.cancelPublication != nil {
							op.cancelPublication()
						}
						h.publisher.PublishView(key, viewpb.QueryViewState_QueryViewStateDown)
					}
					h.mu.Unlock()
				}
				if !h.confirm(op.ctx, key, viewpb.QueryViewState_QueryViewStateDown, result.IntoProto()) {
					return
				}
			}
		}

		h.mu.Lock()
		if op.ctx.Err() == nil {
			if retireUp && state == qviews.QueryViewStateDropped {
				h.publisher.PublishView(key, viewpb.QueryViewState_QueryViewStateDropped)
			}
			if op.command.OnReport != nil {
				op.command.OnReport(result)
			}
		}
		h.mu.Unlock()
		if state != qviews.QueryViewStatePreparing {
			return
		}
	}
}

func (h *writerViewControl) confirm(ctx context.Context, key qviews.QueryViewKey, want viewpb.QueryViewState, v *viewpb.QueryViewOfShard) bool {
	catalog := resource.Resource().StreamingNodeCatalog().(interface {
		CommitReplicaQueryView(context.Context, *streamingpb.WALReplicaTopology, *viewpb.QueryViewOfShard) error
	})
	for ctx.Err() == nil {
		err := h.publisher.WithReports(ctx, key, want, func(t *streamingpb.WALReplicaTopology) error {
			attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			return catalog.CommitReplicaQueryView(attempt, t, v)
		})
		if err == nil {
			h.mu.Lock()
			if ctx.Err() != nil {
				h.mu.Unlock()
				return false
			}
			if v.Meta.State == viewpb.QueryViewState_QueryViewStateUp {
				h.up[key] = true
			} else {
				delete(h.up, key)
			}
			h.mu.Unlock()
			return true
		}
		mlog.RatedWarn(ctx, 5, "retrying WAL replica QueryView commit", mlog.String("view", key.String()), mlog.Err(err))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}
