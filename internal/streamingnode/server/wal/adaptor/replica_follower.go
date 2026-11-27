package adaptor

import (
	"context"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type followerView struct {
	definition *viewpb.QueryViewOfShard
	target     viewpb.QueryViewState
}
type replicaFollower struct {
	channel string
	handler *snview.SNQueryViewHandler
	mu      sync.Mutex
	current map[qviews.QueryViewKey]*followerView
	pending map[qviews.QueryViewKey]*streamingpb.WALQueryViewReport
	changed chan struct{}
	applyMu sync.Mutex
}

func newFollower(channel string, h *snview.SNQueryViewHandler, recovered []*viewpb.QueryViewOfShard) *replicaFollower {
	f := &replicaFollower{channel: channel, handler: h, current: make(map[qviews.QueryViewKey]*followerView), pending: make(map[qviews.QueryViewKey]*streamingpb.WALQueryViewReport), changed: make(chan struct{}, 1)}
	for _, v := range recovered {
		f.current[viewKey(v)] = &followerView{definition: v, target: v.Meta.State}
	}
	return f
}

func (f *replicaFollower) apply(ctx context.Context, state *streamingpb.WALReplicaState, snapshot bool) error {
	f.applyMu.Lock()
	defer f.applyMu.Unlock()
	if !snapshot && len(state.QueryViews) == 0 {
		return nil
	}
	defs, err := resource.Resource().QueryViewCatalog().ListQueryViews(ctx)
	if err != nil {
		return err
	}
	byKey := make(map[qviews.QueryViewKey]*viewpb.QueryViewOfShard)
	for _, v := range defs {
		if funcutil.ToPhysicalChannel(v.Meta.Vchannel) == f.channel {
			byKey[viewKey(v)] = v
		}
	}
	targets := make(map[qviews.QueryViewKey]viewpb.QueryViewState)
	for _, signal := range state.QueryViews {
		id := signal.View
		key := qviews.QueryViewKey{ShardID: qviews.FromProtoShardID(id.Shard), QueryViewVersion: qviews.FromProtoQueryViewVersion(id.Version)}
		targets[key] = signal.State
	}
	f.mu.Lock()
	if snapshot {
		clear(f.pending)
		for key := range f.current {
			if _, ok := targets[key]; !ok {
				targets[key] = viewpb.QueryViewState_QueryViewStateDropped
			}
		}
	}
	var preparing, retiring []handler.ApplyView
	for key, target := range targets {
		def := byKey[key]
		if def == nil {
			if previous := f.current[key]; previous != nil {
				def = previous.definition
			}
			target = viewpb.QueryViewState_QueryViewStateDropped
		}
		if def == nil {
			f.queueLocked(key, viewpb.QueryViewState_QueryViewStateDropped)
			continue
		}
		// A newer durable retire intent fences a delayed Preparing/Up signal.
		if def.Meta.State == viewpb.QueryViewState_QueryViewStateDown && target != viewpb.QueryViewState_QueryViewStateDropped {
			target = def.Meta.State
		}
		cmd := &followerView{definition: def, target: target}
		f.current[key] = cmd
		first := target
		if target == viewpb.QueryViewState_QueryViewStateUp {
			first = viewpb.QueryViewState_QueryViewStatePreparing
		}
		if target == viewpb.QueryViewState_QueryViewStateDropped {
			first = viewpb.QueryViewState_QueryViewStateDown
		}
		command := f.command(ctx, key, cmd, first)
		if first == viewpb.QueryViewState_QueryViewStatePreparing {
			preparing = append(preparing, command)
		} else {
			retiring = append(retiring, command)
		}
	}
	f.mu.Unlock()
	// Shards on the same vchannel share a resource manager and its DataVersion
	// watermark. Register older bases first, before retiring their holders.
	sort.Slice(preparing, func(i, j int) bool {
		return preparing[j].View.QueryViewKey().QueryViewVersion.GT(preparing[i].View.QueryViewKey().QueryViewVersion)
	})
	for i := range preparing {
		// A multi-shard batch is grouped by ShardID inside the handler.
		f.handler.ApplyViews(preparing[i : i+1])
	}
	f.handler.ApplyViews(retiring)
	return nil
}

func (f *replicaFollower) command(ctx context.Context, key qviews.QueryViewKey, cmd *followerView, state viewpb.QueryViewState) handler.ApplyView {
	def := proto.CloneOf(cmd.definition)
	def.Meta.State = state
	view := qviews.NewQueryViewAtWorkNodeFromProto(def)
	return handler.ApplyView{View: view, OnReport: func(result qviews.QueryViewAtWorkNode) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if ctx.Err() != nil || f.current[key] != cmd {
			return
		}
		local := viewpb.QueryViewState(result.State())
		f.queueLocked(key, local)
		if local == viewpb.QueryViewState_QueryViewStateDropped {
			delete(f.current, key)
			return
		}
		if (cmd.target == viewpb.QueryViewState_QueryViewStateUp && local == viewpb.QueryViewState_QueryViewStateReady) || (cmd.target == viewpb.QueryViewState_QueryViewStateDropped && local == viewpb.QueryViewState_QueryViewStateDown) {
			go func() {
				f.applyMu.Lock()
				defer f.applyMu.Unlock()
				f.mu.Lock()
				current := ctx.Err() == nil && f.current[key] == cmd
				f.mu.Unlock()
				if current {
					f.handler.ApplyViews([]handler.ApplyView{f.command(ctx, key, cmd, cmd.target)})
				}
			}()
		}
	}}
}

func (f *replicaFollower) queueLocked(key qviews.QueryViewKey, state viewpb.QueryViewState) {
	f.pending[key] = &streamingpb.WALQueryViewReport{View: &viewpb.QueryViewKey{Shard: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()}, LocalState: state}
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *replicaFollower) next(ctx context.Context) (*streamingpb.WALQueryViewReport, error) {
	for {
		f.mu.Lock()
		for k, r := range f.pending {
			delete(f.pending, k)
			f.mu.Unlock()
			return r, nil
		}
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.changed:
		}
	}
}
