package balancer

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	versionChecker260 = "<2.6.0-dev"
)

// RecoverBalancer recover the balancer working.
func RecoverBalancer(
	ctx context.Context,
	incomingNewChannel ...string, // Concurrent incoming new channel directly from the configuration.
	// we should add a rpc interface for creating new incoming new channel.
) (Balancer, error) {
	sort.Strings(incomingNewChannel)

	policyBuilder := mustGetPolicy(paramtable.Get().StreamingCfg.WALBalancerPolicyName.GetValue())
	policy := policyBuilder.Build()
	logger := resource.Resource().Logger().With(log.FieldComponent("balancer"), zap.String("policy", policyBuilder.Name()))
	policy.SetLogger(logger)

	// Recover the channel view from catalog.
	manager, err := channel.RecoverChannelManager(ctx, incomingNewChannel...)
	if err != nil {
		return nil, errors.Wrap(err, "fail to recover channel manager")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	b := &balancerImpl{
		ctx:                    ctx,
		cancel:                 cancel,
		lifetime:               typeutil.NewLifetime(),
		channelMetaManager:     manager,
		policy:                 policy,
		reqCh:                  make(chan *request, 5),
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		freezeNodes:            typeutil.NewSet[int64](),
	}
	b.SetLogger(logger)
	ready260Future, err := b.checkIfAllNodeGreaterThan260AndWatch(ctx)
	if err != nil {
		return nil, err
	}
	go b.execute(ready260Future)
	return b, nil
}

// balancerImpl is a implementation of Balancer.
type balancerImpl struct {
	log.Binder

	ctx                    context.Context
	cancel                 context.CancelCauseFunc
	lifetime               *typeutil.Lifetime
	channelMetaManager     *channel.ChannelManager
	policy                 Policy                                // policy is the balance policy, TODO: should be dynamic in future.
	reqCh                  chan *request                         // reqCh is the request channel, send the operation to background task.
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}] // backgroundTaskNotifier is used to conmunicate with the background task.
	freezeNodes            typeutil.Set[int64]                   // freezeNodes is the nodes that will be frozen, no more wal will be assigned to these nodes and wal will be removed from these nodes.
}

// RegisterStreamingEnabledNotifier registers a notifier into the balancer.
func (b *balancerImpl) RegisterStreamingEnabledNotifier(notifier *syncutil.AsyncTaskNotifier[struct{}]) {
	b.channelMetaManager.RegisterStreamingEnabledNotifier(notifier)
}

func (b *balancerImpl) GetLatestChannelAssignment() (*WatchChannelAssignmentsCallbackParam, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	return b.channelMetaManager.GetLatestChannelAssignment()
}

// ReplicateRole returns the replicate role of the balancer.
func (b *balancerImpl) ReplicateRole() replicateutil.Role {
	return b.channelMetaManager.ReplicateRole()
}

// GetAllStreamingNodes fetches all streaming node info.
func (b *balancerImpl) GetAllStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfo, error) {
	return resource.Resource().StreamingNodeManagerClient().GetAllStreamingNodes(ctx)
}

// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
func (b *balancerImpl) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	return b.channelMetaManager.GetLatestWALLocated(ctx, pchannel)
}

// WatchChannelAssignments watches the balance result.
func (b *balancerImpl) WatchChannelAssignments(ctx context.Context, cb WatchChannelAssignmentsCallback) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	return b.channelMetaManager.WatchAssignmentResult(ctx, cb)
}

// UpdateReplicateConfiguration updates the replicate configuration.
func (b *balancerImpl) UpdateReplicateConfiguration(ctx context.Context, msgs ...message.ImmutableAlterReplicateConfigMessageV2) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()

	if err := b.channelMetaManager.UpdateReplicateConfiguration(ctx, msgs...); err != nil {
		return err
	}
	return nil
}

// UpdateBalancePolicy update the balance policy.
func (b *balancerImpl) UpdateBalancePolicy(ctx context.Context, req *types.UpdateWALBalancePolicyRequest) (*types.UpdateWALBalancePolicyResponse, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	resp, err := b.sendRequestAndWaitFinish(ctx, newOpUpdateBalancePolicy(ctx, req))
	if err != nil {
		return nil, err
	}
	return resp.(*types.UpdateWALBalancePolicyResponse), nil
}

// MarkAsUnavailable mark the pchannels as unavailable.
func (b *balancerImpl) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	_, err := b.sendRequestAndWaitFinish(ctx, newOpMarkAsUnavailable(ctx, pChannels))
	return err
}

// Trigger trigger a re-balance.
func (b *balancerImpl) Trigger(ctx context.Context) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	_, err := b.sendRequestAndWaitFinish(ctx, newOpTrigger(ctx))
	return err
}

// sendRequestAndWaitFinish send a request to the background task and wait for it to finish.
func (b *balancerImpl) sendRequestAndWaitFinish(ctx context.Context, newReq *request) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.reqCh <- newReq:
	}
	resp := newReq.future.Get()
	return resp.resp, resp.err
}

// Close close the balancer.
func (b *balancerImpl) Close() {
	b.lifetime.SetState(typeutil.LifetimeStateStopped)
	// cancel all watch opeartion by context.
	b.cancel(ErrBalancerClosed)
	b.lifetime.Wait()

	b.backgroundTaskNotifier.Cancel()
	b.backgroundTaskNotifier.BlockUntilFinish()
}

// execute the balancer.
func (b *balancerImpl) execute(ready260Future *syncutil.Future[error]) {
	b.Logger().Info("balancer start to execute")
	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.Logger().Info("balancer execute finished")
	}()

	balanceTimer := typeutil.NewBackoffTimer(&backoffConfigFetcher{})
	nodeChanged, err := resource.Resource().StreamingNodeManagerClient().WatchNodeChanged(b.backgroundTaskNotifier.Context())
	if err != nil {
		b.Logger().Warn("fail to watch node changed", zap.Error(err))
		return
	}
	statsManager, err := channel.StaticPChannelStatsManager.GetWithContext(b.backgroundTaskNotifier.Context())
	if err != nil {
		b.Logger().Warn("fail to get pchannel stats manager", zap.Error(err))
		return
	}
	channelChanged := statsManager.WatchAtChannelCountChanged()

	for {
		// Wait for next balance trigger.
		// Maybe trigger by timer or by request.
		nextTimer, nextBalanceInterval := balanceTimer.NextTimer()
		var ready260 <-chan struct{}
		if ready260Future != nil {
			ready260 = ready260Future.Done()
		}

		b.Logger().Info("balance wait", zap.Duration("nextBalanceInterval", nextBalanceInterval))
		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case newReq := <-b.reqCh:
			newReq.apply(b)
			b.applyAllRequest()
		case <-ready260:
			if err := ready260Future.Get(); err != nil {
				b.Logger().Warn("fail to block until all node is greater than 2.6.0", zap.Error(err))
				return
			}
			b.Logger().Info("all nodes is greater than 2.6.0, start to open read-write wal")
			ready260Future = nil
		case <-nextTimer:
			// balance triggered by timer.
		case _, ok := <-nodeChanged:
			if !ok {
				return // nodeChanged is only closed if context cancel.
				// in other word, balancer is closed.
			}
			// trigger the watch update.
			b.channelMetaManager.TriggerWatchUpdate()
			// balance triggered by new streaming node changed.
		case <-channelChanged.WaitChan():
			// balance triggered by channel changed.
			channelChanged.Sync()
		}
		if err := b.balanceUntilNoChanged(b.backgroundTaskNotifier.Context()); err != nil {
			if b.backgroundTaskNotifier.Context().Err() != nil {
				// balancer is closed.
				return
			}
			b.Logger().Warn("fail to apply balance, start a backoff...", zap.Error(err))
			balanceTimer.EnableBackoff()
			continue
		}
		b.Logger().Info("apply balance success")
		balanceTimer.DisableBackoff()
	}
}

// checkIfAllNodeGreaterThan260AndWatch check if all node is greater than 2.6.0.
// It will return a future if there's any node with version < 2.6.0,
// and the future will be set when the all node version is greater than 2.6.0.
func (b *balancerImpl) checkIfAllNodeGreaterThan260AndWatch(ctx context.Context) (*syncutil.Future[error], error) {
	f := syncutil.NewFuture[error]()
	if b.channelMetaManager.IsStreamingEnabledOnce() {
		// Once the streaming is enabled, we can not check the node version anymore.
		// because the first channel-assignment is generated after the old node is down.
		return nil, nil
	}

	if greaterThan260, err := b.checkIfAllNodeGreaterThan260(ctx); err != nil || greaterThan260 {
		return nil, err
	}
	go func() {
		err := b.blockUntilAllNodeIsGreaterThan260AtBackground(ctx)
		f.Set(err)
	}()
	return f, nil
}

// checkIfAllNodeGreaterThan260 check if all node is greater than 2.6.0.
func (b *balancerImpl) checkIfAllNodeGreaterThan260(ctx context.Context) (bool, error) {
	expectedRoles := []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.QueryNodeRole}
	for _, role := range expectedRoles {
		if greaterThan260, err := b.checkIfRoleGreaterThan260(ctx, role); err != nil || !greaterThan260 {
			return greaterThan260, err
		}
	}
	b.Logger().Info("all nodes is greater than 2.6.0 when checking")
	return true, b.channelMetaManager.MarkStreamingHasEnabled(ctx)
}

// checkIfRoleGreaterThan260 check if the role is greater than 2.6.0.
func (b *balancerImpl) checkIfRoleGreaterThan260(ctx context.Context, role string) (bool, error) {
	logger := b.Logger().With(zap.String("role", role))
	rb := resolver.NewSessionBuilder(resource.Resource().ETCD(), sessionutil.GetSessionPrefixByRole(role), versionChecker260)
	defer rb.Close()

	r := rb.Resolver()
	state, err := r.GetLatestState(ctx)
	if err != nil {
		logger.Warn("fail to get latest state", zap.Error(err))
		return false, err
	}
	if len(state.Sessions()) > 0 {
		logger.Info("node is not greater than 2.6.0 when checking", zap.Int("sessionCount", len(state.Sessions())))
		return false, nil
	}
	return true, nil
}

// blockUntilAllNodeIsGreaterThan260AtBackground block until all node is greater than 2.6.0 at background.
func (b *balancerImpl) blockUntilAllNodeIsGreaterThan260AtBackground(ctx context.Context) error {
	expectedRoles := []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.QueryNodeRole}
	for _, role := range expectedRoles {
		if err := b.blockUntilRoleGreaterThan260AtBackground(ctx, role); err != nil {
			return err
		}
	}
	return b.channelMetaManager.MarkStreamingHasEnabled(ctx)
}

// blockUntilRoleGreaterThan260AtBackground block until the role is greater than 2.6.0 at background.
func (b *balancerImpl) blockUntilRoleGreaterThan260AtBackground(ctx context.Context, role string) error {
	doneErr := errors.New("done")
	logger := b.Logger().With(zap.String("role", role))
	logger.Info("start to wait that the nodes is greater than 2.6.0")
	// Check if there's any proxy or data node with version < 2.6.0.
	rb := resolver.NewSessionBuilder(resource.Resource().ETCD(), sessionutil.GetSessionPrefixByRole(role), versionChecker260)
	defer rb.Close()

	r := rb.Resolver()
	err := r.Watch(ctx, func(vs resolver.VersionedState) error {
		if len(vs.Sessions()) == 0 {
			return doneErr
		}
		logger.Info("session changes", zap.Int("sessionCount", len(vs.Sessions())))
		return nil
	})
	if err != nil && !errors.Is(err, doneErr) {
		logger.Info("fail to wait that the nodes is greater than 2.6.0", zap.Error(err))
		return err
	}
	logger.Info("all nodes is greater than 2.6.0 when watching")
	return nil
}

// applyAllRequest apply all request in the request channel.
func (b *balancerImpl) applyAllRequest() {
	for {
		select {
		case newReq := <-b.reqCh:
			newReq.apply(b)
		default:
			return
		}
	}
}

// balanceUntilNoChanged try to balance until there's changed.
func (b *balancerImpl) balanceUntilNoChanged(ctx context.Context) error {
	for {
		layoutChanged, err := b.balance(ctx)
		if err != nil {
			return err
		}
		if !layoutChanged {
			return nil
		}
	}
}

// Trigger a balance of layout.
// Return a nil chan to avoid
// Return a channel to notify the balance trigger again.
func (b *balancerImpl) balance(ctx context.Context) (bool, error) {
	b.Logger().Info("start to balance")
	pchannelView := b.channelMetaManager.CurrentPChannelsView()

	b.Logger().Info("collect all status...")
	nodeStatus, err := b.fetchStreamingNodeStatus(ctx)
	if err != nil {
		return false, err
	}

	// call the balance strategy to generate the expected layout.
	accessMode := types.AccessModeRO
	if b.channelMetaManager.IsStreamingEnabledOnce() {
		accessMode = types.AccessModeRW
	}
	currentLayout := generateCurrentLayout(pchannelView, nodeStatus, accessMode)
	expectedLayout, err := b.policy.Balance(currentLayout)
	if err != nil {
		return false, errors.Wrap(err, "fail to balance")
	}

	b.Logger().Info("balance policy generate result success, try to assign...", zap.Stringer("expectedLayout", expectedLayout))
	// bookkeeping the meta assignment started.
	modifiedChannels, err := b.channelMetaManager.AssignPChannels(ctx, expectedLayout.ChannelAssignment)
	if err != nil {
		return false, errors.Wrap(err, "fail to assign pchannels")
	}

	if len(modifiedChannels) == 0 {
		b.Logger().Info("no change of balance result need to be applied")
		return false, nil
	}
	return true, b.applyBalanceResultToStreamingNode(ctx, modifiedChannels)
}

// fetchStreamingNodeStatus fetch the streaming node status.
func (b *balancerImpl) fetchStreamingNodeStatus(ctx context.Context) (map[int64]*types.StreamingNodeStatus, error) {
	nodeStatus, err := resource.Resource().StreamingNodeManagerClient().CollectAllStatus(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fail to collect all status")
	}

	// mark the frozen node as frozen in the node status.
	for _, node := range nodeStatus {
		if b.freezeNodes.Contain(node.ServerID) && node.IsHealthy() {
			node.Err = types.ErrFrozen
		}
	}
	// clean up the freeze node that has been removed from session.
	for serverID := range b.freezeNodes {
		if _, ok := nodeStatus[serverID]; !ok {
			b.Logger().Info("freeze node has been removed from session", zap.Int64("serverID", serverID))
			b.freezeNodes.Remove(serverID)
		}
	}
	return nodeStatus, nil
}

// applyBalanceResultToStreamingNode apply the balance result to streaming node.
func (b *balancerImpl) applyBalanceResultToStreamingNode(ctx context.Context, modifiedChannels map[types.ChannelID]*channel.PChannelMeta) error {
	b.Logger().Info("balance result need to be applied...", zap.Int("modifiedChannelCount", len(modifiedChannels)))

	// different channel can be execute concurrently.
	g, _ := errgroup.WithContext(ctx)
	opTimeout := paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse()
	// generate balance operations and applied them.
	for _, channel := range modifiedChannels {
		channel := channel
		g.Go(func() error {
			// all history channels should be remove from related nodes.
			for _, assignment := range channel.AssignHistories() {
				opCtx, cancel := context.WithTimeout(ctx, opTimeout)
				defer cancel()
				if err := resource.Resource().StreamingNodeManagerClient().Remove(opCtx, assignment); err != nil {
					b.Logger().Warn("fail to remove channel", zap.String("assignment", assignment.String()), zap.Error(err))
					return err
				}
				b.Logger().Info("remove channel success", zap.String("assignment", assignment.String()))
			}

			// assign the channel to the target node.
			opCtx, cancel := context.WithTimeout(ctx, opTimeout)
			defer cancel()
			if err := resource.Resource().StreamingNodeManagerClient().Assign(opCtx, channel.CurrentAssignment()); err != nil {
				b.Logger().Warn("fail to assign channel", zap.String("assignment", channel.CurrentAssignment().String()), zap.Error(err))
				return err
			}
			b.Logger().Info("assign channel success", zap.String("assignment", channel.CurrentAssignment().String()))

			// bookkeeping the meta assignment done.
			if err := b.channelMetaManager.AssignPChannelsDone(ctx, []types.ChannelID{channel.ChannelID()}); err != nil {
				b.Logger().Warn("fail to bookkeep pchannel assignment done", zap.String("assignment", channel.CurrentAssignment().String()))
				return err
			}
			return nil
		})
	}
	// TODO: Current implementation recovery will wait for all node reply,
	// huge unavaiable time may be caused by this,
	// should be fixed in future.
	return g.Wait()
}

// generateCurrentLayout generate layout from all nodes info and meta.
func generateCurrentLayout(view *channel.PChannelView, allNodesStatus map[int64]*types.StreamingNodeStatus, accessMode types.AccessMode) (layout CurrentLayout) {
	channelsToNodes := make(map[types.ChannelID]int64, len(view.Channels))
	channels := make(map[channel.ChannelID]types.PChannelInfo, len(view.Channels))
	expectedAccessMode := make(map[types.ChannelID]types.AccessMode, len(view.Channels))

	for id, meta := range view.Channels {
		expectedAccessMode[id] = accessMode
		channels[id] = meta.ChannelInfo()
		if !meta.IsAssigned() {
			// dead or expired relationship.
			log.Warn("channel is not assigned to any server",
				zap.Stringer("channel", id),
				zap.Int64("term", meta.CurrentTerm()),
				zap.Int64("serverID", meta.CurrentServerID()),
				zap.String("state", meta.State().String()),
			)
			continue
		}
		if nodeStatus, ok := allNodesStatus[meta.CurrentServerID()]; ok && nodeStatus.IsHealthy() {
			channelsToNodes[id] = meta.CurrentServerID()
		} else {
			// dead or expired relationship.
			log.Warn("channel of current server id is not healthy or not alive",
				zap.Stringer("channel", id),
				zap.Int64("term", meta.CurrentTerm()),
				zap.Int64("serverID", meta.CurrentServerID()),
				zap.Error(nodeStatus.ErrorOfNode()),
			)
		}
	}
	allNodesInfo := make(map[int64]types.StreamingNodeStatus, len(allNodesStatus))
	for serverID, nodeStatus := range allNodesStatus {
		// filter out the unhealthy nodes.
		if nodeStatus.IsHealthy() {
			allNodesInfo[serverID] = *nodeStatus
		}
	}
	return CurrentLayout{
		Config:             newCommonBalancePolicyConfig(),
		Channels:           channels,
		Stats:              view.Stats,
		AllNodesInfo:       allNodesInfo,
		ChannelsToNodes:    channelsToNodes,
		ExpectedAccessMode: expectedAccessMode,
	}
}

type backoffConfigFetcher struct{}

func (f *backoffConfigFetcher) BackoffConfig() typeutil.BackoffConfig {
	return typeutil.BackoffConfig{
		InitialInterval: paramtable.Get().StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse(),
		Multiplier:      paramtable.Get().StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat(),
		MaxInterval:     paramtable.Get().StreamingCfg.WALBalancerBackoffMaxInterval.GetAsDurationByParse(),
	}
}

func (f *backoffConfigFetcher) DefaultInterval() time.Duration {
	return paramtable.Get().StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse()
}
