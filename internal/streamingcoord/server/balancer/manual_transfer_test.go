package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func assignedTransferChannel(name string, nodeID int64) *channel.PChannelMeta {
	m := channel.NewPChannelMeta(name, types.AccessModeRW).CopyForWrite()
	m.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: nodeID})
	m.AssignToServerDone()
	return m.PChannelMeta
}

func TestPlanChannelTransfer(t *testing.T) {
	p1, p2, p3 := assignedTransferChannel("p1", 1), assignedTransferChannel("p2", 1), assignedTransferChannel("p3", 2)
	view := &channel.PChannelView{
		Channels: map[types.ChannelID]*channel.PChannelMeta{p1.ChannelID(): p1, p2.ChannelID(): p2, p3.ChannelID(): p3},
		Stats:    map[types.ChannelID]channel.PChannelStatsView{p1.ChannelID(): {VChannels: map[string]int64{"p1_100v0": 100, "p1_200v0": 200}}},
	}
	nodes := map[int64]*types.StreamingNodeStatus{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2}},
		3: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3}},
		4: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 4}, Err: types.ErrFrozen},
	}
	t.Run("vchannel selects whole pchannel", func(t *testing.T) {
		assignments, err := planChannelTransfer(view, nodes, TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 2, ChannelName: "p1_100v0"})
		require.NoError(t, err)
		require.Len(t, assignments, 1)
		require.Equal(t, int64(2), assignments[p1.ChannelID()].Node.ServerID)
		require.Equal(t, p1.ChannelInfo(), assignments[p1.ChannelID()].Channel)
	})
	t.Run("automatic destination excludes source and frozen nodes", func(t *testing.T) {
		assignments, err := planChannelTransfer(view, nodes, TransferChannelsRequest{SourceNodeID: 1})
		require.NoError(t, err)
		require.Len(t, assignments, 2)
		require.Equal(t, int64(3), assignments[p1.ChannelID()].Node.ServerID)
		require.Equal(t, int64(2), assignments[p2.ChannelID()].Node.ServerID)
	})
	for _, name := range []string{"missing", "p3", "p1_999v0"} {
		t.Run(name, func(t *testing.T) {
			_, err := planChannelTransfer(view, nodes, TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 2, ChannelName: name})
			require.ErrorIs(t, err, merr.ErrChannelNotFound)
		})
	}
	for _, target := range []int64{4, 99} {
		_, err := planChannelTransfer(view, nodes, TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: target})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	}
	t.Run("empty source", func(t *testing.T) {
		assignments, err := planChannelTransfer(view, nodes, TransferChannelsRequest{SourceNodeID: 3})
		require.NoError(t, err)
		require.Empty(t, assignments)
	})
	t.Run("pending assignment", func(t *testing.T) {
		pending := p1.CopyForWrite()
		pending.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 1})
		// Force a new assignment to a different node, still in ASSIGNING.
		pending.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 3})
		v := &channel.PChannelView{Channels: map[types.ChannelID]*channel.PChannelMeta{p1.ChannelID(): pending.PChannelMeta}}
		_, err := planChannelTransfer(v, nodes, TransferChannelsRequest{SourceNodeID: 3})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})
}

func TestTransferChannelsWaitsForWALConfirmation(t *testing.T) {
	paramtable.Init()
	for _, failure := range []string{"", "persist-intent", "remove", "assign", "persist-done", "cancel"} {
		t.Run("failure="+failure, func(t *testing.T) {
			mockey.PatchConvey("manual assignment uses the existing WAL protocol", t, func() {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				cm := &channel.ChannelManager{}
				b := &balancerImpl{ctx: ctx, lifetime: typeutil.NewLifetime(), channelMetaManager: cm, reqCh: make(chan *request, 1)}
				b.SetLogger(mlog.With())
				manager := &mock_manager.MockManagerClient{}
				resource.InitForTest(resource.OptStreamingManagerClient(manager))
				p := assignedTransferChannel("p1", 1)
				view := &channel.PChannelView{Channels: map[types.ChannelID]*channel.PChannelMeta{p.ChannelID(): p}}
				mockey.Mock((*channel.ChannelManager).CurrentPChannelsView).Return(view).Build()
				mockey.Mock((*balancerImpl).GetAllStreamingNodes).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}}, nil).Build()
				mockey.Mock((*balancerImpl).fetchStreamingNodeStatus).Return(map[int64]*types.StreamingNodeStatus{2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2}}}, nil).Build()
				stages := make(chan string, 8)
				confirmAssign, confirmPersist := make(chan struct{}), make(chan struct{})
				failAt := func(stage string) error {
					stages <- stage
					if stage == failure {
						return merr.ErrServiceUnavailable
					}
					return nil
				}
				mockey.Mock((*channel.ChannelManager).AssignPChannels).To(func(_ *channel.ChannelManager, _ context.Context, assignments map[types.ChannelID]types.PChannelInfoAssigned) (map[types.ChannelID]*channel.PChannelMeta, error) {
					if err := failAt("persist-intent"); err != nil {
						return nil, err
					}
					m := p.CopyForWrite()
					a := assignments[p.ChannelID()]
					m.TryAssignToServerID(a.Channel.AccessMode, a.Node)
					return map[types.ChannelID]*channel.PChannelMeta{p.ChannelID(): m.PChannelMeta}, nil
				}).Build()
				mockey.Mock((*mock_manager.MockManagerClient).Remove).To(func(_ *mock_manager.MockManagerClient, _ context.Context, a types.PChannelInfoAssigned) error {
					if a.Node.ServerID != 1 {
						return merr.ErrServiceInternal
					}
					return failAt("remove")
				}).Build()
				mockey.Mock((*mock_manager.MockManagerClient).Assign).To(func(_ *mock_manager.MockManagerClient, ctx context.Context, a types.PChannelInfoAssigned) error {
					if a.Node.ServerID != 2 || a.Channel.Term != p.CurrentTerm()+1 {
						return merr.ErrServiceInternal
					}
					if err := failAt("assign"); err != nil {
						return err
					}
					select {
					case <-confirmAssign:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}).Build()
				mockey.Mock((*channel.ChannelManager).AssignPChannelsDone).To(func(_ *channel.ChannelManager, ctx context.Context, ids []types.ChannelID) error {
					if len(ids) != 1 || ids[0] != p.ChannelID() {
						return merr.ErrServiceInternal
					}
					if err := failAt("persist-done"); err != nil {
						return err
					}
					select {
					case <-confirmPersist:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}).Build()
				workerDone := make(chan struct{})
				go func() {
					defer close(workerDone)
					select {
					case req := <-b.reqCh:
						req.apply(b)
					case <-ctx.Done():
					}
				}()
				defer func() { cancel(); <-workerDone }()
				result := make(chan error, 1)
				go func() { result <- b.TransferChannels(ctx, TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 2}) }()
				nextStage := func(expected string) {
					select {
					case got := <-stages:
						require.Equal(t, expected, got)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				switch failure {
				case "":
					nextStage("persist-intent")
					nextStage("remove")
					nextStage("assign")
					select {
					case err := <-result:
						t.Fatalf("returned before Assign confirmation: %v", err)
					default:
					}
					close(confirmAssign)
					nextStage("persist-done")
					select {
					case err := <-result:
						t.Fatalf("returned before ASSIGNED persistence: %v", err)
					default:
					}
					close(confirmPersist)
				case "cancel":
					nextStage("persist-intent")
					nextStage("remove")
					nextStage("assign")
					cancel()
				default:
					close(confirmAssign)
					close(confirmPersist)
				}
				select {
				case err := <-result:
					switch failure {
					case "":
						require.NoError(t, err)
					case "cancel":
						require.ErrorIs(t, err, context.Canceled)
					default:
						require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("transfer did not return")
				}
			})
		})
	}
}

func TestTransferChannelsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	b := &balancerImpl{ctx: t.Context(), lifetime: typeutil.NewLifetime(), reqCh: make(chan *request)}
	result := make(chan error, 1)
	go func() { result <- b.TransferChannels(ctx, TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 2}) }()
	// Cancel after enqueue but before the serialized worker can apply the request.
	req := <-b.reqCh
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued transfer ignored cancellation")
	}
	// Applying the canceled request must not dereference the uninitialized manager.
	req.apply(b)
	require.ErrorIs(t, req.future.Get().err, context.Canceled)
}

func TestTransferChannelsValidation(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name      string
		req       TransferChannelsRequest
		nodes     map[int64]*types.StreamingNodeInfoWithResourceGroup
		listErr   error
		statusErr error
		expected  error
	}{
		{name: "invalid source", req: TransferChannelsRequest{SourceNodeID: -1}, expected: merr.ErrParameterInvalid},
		{name: "same destination", req: TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 1}, expected: merr.ErrParameterInvalid},
		{name: "invalid destination", req: TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: -1}, expected: merr.ErrParameterInvalid},
		{name: "missing source", req: TransferChannelsRequest{SourceNodeID: 1}, expected: merr.ErrNodeNotFound},
		{name: "discovery failed", req: TransferChannelsRequest{SourceNodeID: 1}, listErr: merr.ErrServiceUnavailable, expected: merr.ErrServiceUnavailable},
		{name: "status failed", req: TransferChannelsRequest{SourceNodeID: 1}, nodes: map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}}, statusErr: merr.ErrServiceUnavailable, expected: merr.ErrServiceUnavailable},
		{name: "channel absent", req: TransferChannelsRequest{SourceNodeID: 1, ChannelName: "missing"}, nodes: map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}}, expected: merr.ErrChannelNotFound},
		{name: "registered empty source", req: TransferChannelsRequest{SourceNodeID: 1}, nodes: map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey("reject before any assignment write", t, func() {
				b := &balancerImpl{channelMetaManager: &channel.ChannelManager{}}
				mockey.Mock((*balancerImpl).GetAllStreamingNodes).Return(tc.nodes, tc.listErr).Build()
				mockey.Mock((*balancerImpl).fetchStreamingNodeStatus).Return(nil, tc.statusErr).Build()
				mockey.Mock((*channel.ChannelManager).CurrentPChannelsView).Return(&channel.PChannelView{}).Build()
				err := b.transferChannels(t.Context(), tc.req)
				if tc.expected == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.expected)
				}
			})
		})
	}
	b := &balancerImpl{lifetime: typeutil.NewLifetime()}
	b.lifetime.SetState(typeutil.LifetimeStateStopped)
	require.ErrorContains(t, b.TransferChannels(t.Context(), TransferChannelsRequest{}), "balancer is closing")
}
