package adaptor

import (
	"context"
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

// Publish the effective intent the reader can apply. Up can precede this
// writer's durable publication, including on restart with a pending Coord Up.
func seedReplicaViews(p *replica.Publisher, pchannel string, durable, desired []*viewpb.QueryViewOfShard) {
	up := make(map[qviews.QueryViewKey]bool, len(durable))
	for _, v := range durable {
		up[viewKey(v)] = true
	}
	for _, v := range desired {
		if funcutil.ToPhysicalChannel(v.Meta.Vchannel) != pchannel {
			continue
		}
		key := viewKey(v)
		state := v.Meta.State
		if state == viewpb.QueryViewState_QueryViewStateUp && !up[key] {
			state = viewpb.QueryViewState_QueryViewStatePreparing
		}
		p.PublishView(key, state)
	}
}

// writerViewControl coordinates physical replicas on behalf of the logical SN.
// Shard locks only cover the local machine. Waiting for votes and catalog I/O
// runs independently so existing leases can keep serving while another RO loads.
type viewOperation struct{ cancel context.CancelFunc }

type writerViewControl struct {
	handler.QueryViewHandler
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	wg         sync.WaitGroup
	stopped    bool
	publisher  *replica.Publisher
	operations map[qviews.QueryViewKey]*viewOperation
	// An entry tracks a published or in-flight Up until its fenced deletion.
	// A failed RPC can leave the catalog outcome unknown.
	up       map[qviews.QueryViewKey]bool
	readSide *roWALAdaptorImpl
}

func newWriterViewControl(ctx context.Context, r *roWALAdaptorImpl, p *replica.Publisher, durable []*viewpb.QueryViewOfShard) *writerViewControl {
	ctx, cancel := context.WithCancel(ctx)
	h := &writerViewControl{QueryViewHandler: r.readViews, ctx: ctx, cancel: cancel, publisher: p, readSide: r, operations: make(map[qviews.QueryViewKey]*viewOperation), up: make(map[qviews.QueryViewKey]bool)}
	for _, v := range durable {
		h.up[viewKey(v)] = true
	}
	return h
}

func (h *writerViewControl) Stop() {
	h.mu.Lock()
	h.stopped = true
	h.cancel()
	h.mu.Unlock()
	h.wg.Wait()
}

func (h *writerViewControl) ApplyViews(batch []handler.ApplyView) {
	for _, cmd := range batch {
		key := cmd.View.QueryViewKey()
		def := cmd.View.IntoProto()
		target := def.Meta.State
		h.mu.Lock()
		if h.stopped {
			h.mu.Unlock()
			return
		}
		if previous := h.operations[key]; previous != nil {
			previous.cancel()
		}
		ctx, cancel := context.WithCancel(h.ctx)
		op := &viewOperation{cancel: cancel}
		h.operations[key] = op
		committed, mayBeUp := h.up[key]
		if target == viewpb.QueryViewState_QueryViewStateUp && !mayBeUp {
			h.up[key] = false
		}
		h.mu.Unlock()
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
		h.publisher.PublishView(key, signal)
		// Track all local keys so switching this instance to a reader can
		// retire omissions in the new writer's first snapshot.
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
		h.QueryViewHandler.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(local), OnReport: func(result qviews.QueryViewAtWorkNode) {
			if ctx.Err() != nil {
				return
			}
			reported := viewpb.QueryViewState(result.State())
			if target == viewpb.QueryViewState_QueryViewStateDropped && reported == target {
				h.run(ctx, func() {
					if retireUp && !h.confirm(ctx, key, viewpb.QueryViewState_QueryViewStateDown, def) {
						return
					}
					h.mu.Lock()
					if h.operations[key] != op || ctx.Err() != nil {
						h.mu.Unlock()
						return
					}
					if retireUp {
						h.publisher.PublishView(key, target)
					}
					delete(h.operations, key)
					h.mu.Unlock()
					cancel()
					if cmd.OnReport != nil {
						cmd.OnReport(result)
					}
				})
				return
			}
			if target == viewpb.QueryViewState_QueryViewStateUp && (reported == viewpb.QueryViewState_QueryViewStateReady || reported == viewpb.QueryViewState_QueryViewStateUp) {
				h.run(ctx, func() {
					for h.readSide.readQueries.PrepareUp(ctx, key) != nil {
						select {
						case <-ctx.Done():
							return
						case <-time.After(100 * time.Millisecond):
						}
					}
					if !committed && !h.confirm(ctx, key, viewpb.QueryViewState_QueryViewStateReady, def) {
						return
					}
					if ctx.Err() != nil {
						return
					}
					h.publisher.PublishView(key, target)
					h.QueryViewHandler.ApplyViews([]handler.ApplyView{cmd})
				})
				return
			}
			want := viewpb.QueryViewState_QueryViewStateUnknown
			var persist *viewpb.QueryViewOfShard
			if target == viewpb.QueryViewState_QueryViewStatePreparing && reported == viewpb.QueryViewState_QueryViewStateReady {
				want = reported
			}
			if target == viewpb.QueryViewState_QueryViewStateDown && (reported == viewpb.QueryViewState_QueryViewStateDown || reported == viewpb.QueryViewState_QueryViewStateDropped) {
				want = viewpb.QueryViewState_QueryViewStateDown
				persist = def
			}
			if want != viewpb.QueryViewState_QueryViewStateUnknown {
				h.run(ctx, func() {
					if h.confirm(ctx, key, want, persist) && ctx.Err() == nil && cmd.OnReport != nil {
						cmd.OnReport(result)
					}
				})
			} else if cmd.OnReport != nil {
				cmd.OnReport(result)
			}
		}}})
	}
}

func (h *writerViewControl) run(ctx context.Context, work func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped || ctx.Err() != nil {
		return
	}
	h.wg.Add(1)
	go func() { defer h.wg.Done(); work() }()
}

func (h *writerViewControl) confirm(ctx context.Context, key qviews.QueryViewKey, want viewpb.QueryViewState, v *viewpb.QueryViewOfShard) bool {
	if v == nil {
		return h.publisher.WaitReports(ctx, key, want) == nil
	}
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
