package balancer

import (
	"context"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TransferChannelsRequest describes a manual WAL move. An empty ChannelName
// selects all PChannels on the source. A VChannel selects its entire PChannel.
// TargetNodeID == 0 selects eligible destinations, excluding the source.
type TransferChannelsRequest struct {
	SourceNodeID int64
	TargetNodeID int64
	ChannelName  string
}

// TransferChannels runs in the same queue as automatic balancing. Success means
// each selected destination has advertised its new writer term in its session.
func (b *balancerImpl) TransferChannels(ctx context.Context, req TransferChannelsRequest) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()
	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	future := syncutil.NewFuture[response]()
	result, err := b.sendRequestAndWaitFinish(ctx, &request{
		ctx: ctx,
		apply: func(impl *balancerImpl) {
			routes, err := impl.requestChannelTransfer(ctx, req)
			future.Set(response{resp: routes, err: err})
		},
		future: future,
	})
	if err != nil {
		return err
	}
	want := result.(map[types.ChannelID]types.PChannelInfoAssigned)
	if len(want) == 0 {
		return nil
	}
	// Wait outside the serialized balancer so reports, retries and a successor
	// assignment can continue to make progress.
	done := errors.New("requested writers are serving")
	err = b.WatchChannelAssignments(ctx, func(state WatchChannelAssignmentsCallbackParam) error {
		ready := 0
		for _, route := range state.Relations {
			if target, ok := want[route.Channel.ChannelID()]; ok && route.Node.ServerID == target.Node.ServerID && route.Channel.Term > target.Channel.Term {
				ready++
			}
		}
		if ready == len(want) {
			return done
		}
		return nil
	})
	if errors.Is(err, done) {
		return nil
	}
	return err
}

func (b *balancerImpl) requestChannelTransfer(ctx context.Context, req TransferChannelsRequest) (map[types.ChannelID]types.PChannelInfoAssigned, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.SourceNodeID <= 0 || req.TargetNodeID < 0 || req.SourceNodeID == req.TargetNodeID {
		return nil, merr.WrapErrParameterInvalidMsg("expected a positive source SN and a different target SN (or zero for automatic selection)")
	}
	allNodes, err := b.GetAllStreamingNodes(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := allNodes[req.SourceNodeID]; !ok {
		return nil, merr.WrapErrNodeNotFound(req.SourceNodeID, "source StreamingNode not found")
	}
	nodes, err := b.fetchStreamingNodeStatus(ctx, paramtable.Get().StreamingCfg.PrimaryResourceGroup.GetValue())
	if err != nil {
		return nil, err
	}
	view := b.channelMetaManager.CurrentPChannelsView()
	assignments, err := planChannelTransfer(view, nodes, req)
	if err != nil || len(assignments) == 0 {
		return nil, err
	}
	b.Logger().Info(ctx, "transfer WAL channels", mlog.Int64("source", req.SourceNodeID),
		mlog.Int64("target", req.TargetNodeID), mlog.String("channel", req.ChannelName), mlog.Int("pchannelCount", len(assignments)))
	_, err = b.balanceWithTargets(ctx, assignments)
	return assignments, err
}

func planChannelTransfer(view *channel.PChannelView, nodes map[int64]*types.StreamingNodeStatus, req TransferChannelsRequest) (map[types.ChannelID]types.PChannelInfoAssigned, error) {
	selected := make([]*channel.PChannelMeta, 0)
	counts := make(map[int64]int)
	for id, meta := range view.Channels {
		counts[meta.CurrentServerID()]++
		_, containsVChannel := view.Stats[id].VChannels[req.ChannelName]
		matches := req.ChannelName == "" || meta.Name() == req.ChannelName || containsVChannel
		if !matches || meta.CurrentServerID() != req.SourceNodeID {
			continue
		}
		if !meta.IsAssigned() {
			return nil, merr.WrapErrServiceUnavailableMsg("WAL %s is not assigned yet", meta.Name())
		}
		selected = append(selected, meta)
	}
	if req.ChannelName != "" && len(selected) == 0 {
		return nil, merr.WrapErrChannelNotFound(req.ChannelName, "channel not owned by source StreamingNode")
	}
	// An empty, registered source is already drained.
	if len(selected) == 0 {
		return nil, nil
	}
	candidates := make([]int64, 0, len(nodes))
	for id, node := range nodes {
		if id != req.SourceNodeID && node.IsHealthy() && (req.TargetNodeID == 0 || req.TargetNodeID == id) {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return nil, merr.WrapErrServiceUnavailableMsg("no eligible destination StreamingNode for WAL transfer (target=%d)", req.TargetNodeID)
	}
	// Stable tie-breaking keeps manual bulk transfers balanced and reproducible.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	sort.Slice(selected, func(i, j int) bool { return selected[i].Name() < selected[j].Name() })
	assignments := make(map[types.ChannelID]types.PChannelInfoAssigned, len(selected))
	for _, meta := range selected {
		target := candidates[0]
		for _, id := range candidates[1:] {
			if counts[id] < counts[target] {
				target = id
			}
		}
		assignments[meta.ChannelID()] = types.PChannelInfoAssigned{
			Channel: meta.ChannelInfo(),
			Node:    nodes[target].StreamingNodeInfo,
		}
		counts[target]++
	}
	return assignments, nil
}
