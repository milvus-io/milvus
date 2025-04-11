package balancer

import (
	"context"
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
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// RecoverBalancer recover the balancer working.
func RecoverBalancer(
	ctx context.Context,
	incomingNewChannel ...string, // Concurrent incoming new channel directly from the configuration.
	// we should add a rpc interface for creating new incoming new channel.
) (Balancer, error) {
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
	}
	b.SetLogger(logger)
	go b.execute()
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
}

// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
func (b *balancerImpl) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	return b.channelMetaManager.GetLatestWALLocated(ctx, pchannel)
}

// WatchChannelAssignments watches the balance result.
func (b *balancerImpl) WatchChannelAssignments(ctx context.Context, cb func(version typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	return b.channelMetaManager.WatchAssignmentResult(ctx, cb)
}

func (b *balancerImpl) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	return b.sendRequestAndWaitFinish(ctx, newOpMarkAsUnavailable(ctx, pChannels))
}

// Trigger trigger a re-balance.
func (b *balancerImpl) Trigger(ctx context.Context) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	return b.sendRequestAndWaitFinish(ctx, newOpTrigger(ctx))
}

// sendRequestAndWaitFinish send a request to the background task and wait for it to finish.
func (b *balancerImpl) sendRequestAndWaitFinish(ctx context.Context, newReq *request) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.reqCh <- newReq:
	}
	return newReq.future.Get()
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
func (b *balancerImpl) execute() {
	b.Logger().Info("balancer start to execute")
	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.Logger().Info("balancer execute finished")
	}()

	if err := b.blockUntilAllNodeIsGreaterThan260(b.ctx); err != nil {
		b.Logger().Warn("fail to block until all node is greater than 2.6.0", zap.Error(err))
		return
	}

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
		b.Logger().Info("balance wait", zap.Duration("nextBalanceInterval", nextBalanceInterval))
		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case newReq := <-b.reqCh:
			newReq.apply(b)
			b.applyAllRequest()
		case <-nextTimer:
			// balance triggered by timer.
		case _, ok := <-nodeChanged:
			if !ok {
				return // nodeChanged is only closed if context cancel.
				// in other word, balancer is closed.
			}
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

// blockUntilAllNodeIsGreaterThan260 block until all node is greater than 2.6.0.
// It's just a protection, but didn't promised that there will never be a node with version < 2.6.0 join the cluster.
// These promise can only be achieved by the cluster dev-ops.
func (b *balancerImpl) blockUntilAllNodeIsGreaterThan260(ctx context.Context) error {
	doneErr := errors.New("done")
	expectedRoles := []string{typeutil.ProxyRole, typeutil.DataNodeRole}
	for _, role := range expectedRoles {
		logger := b.Logger().With(zap.String("role", role))
		logger.Info("start to wait that the nodes is greater than 2.6.0")
		// Check if there's any proxy or data node with version < 2.6.0.
		proxyResolver := resolver.NewSessionBuilder(resource.Resource().ETCD(), sessionutil.GetSessionPrefixByRole(role), "<2.6.0-dev")
		r := proxyResolver.Resolver()
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
		logger.Info("all nodes is greater than 2.6.0")
		proxyResolver.Close()
	}
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
	nodeStatus, err := resource.Resource().StreamingNodeManagerClient().CollectAllStatus(ctx)
	if err != nil {
		return false, errors.Wrap(err, "fail to collect all status")
	}

	// call the balance strategy to generate the expected layout.
	currentLayout := generateCurrentLayout(pchannelView, nodeStatus)
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

// applyBalanceResultToStreamingNode apply the balance result to streaming node.
func (b *balancerImpl) applyBalanceResultToStreamingNode(ctx context.Context, modifiedChannels map[types.ChannelID]*channel.PChannelMeta) error {
	b.Logger().Info("balance result need to be applied...", zap.Int("modifiedChannelCount", len(modifiedChannels)))

	// different channel can be execute concurrently.
	g, _ := errgroup.WithContext(ctx)
	// generate balance operations and applied them.
	for _, channel := range modifiedChannels {
		channel := channel
		g.Go(func() error {
			// all history channels should be remove from related nodes.
			for _, assignment := range channel.AssignHistories() {
				if err := resource.Resource().StreamingNodeManagerClient().Remove(ctx, assignment); err != nil {
					b.Logger().Warn("fail to remove channel", zap.Any("assignment", assignment), zap.Error(err))
					return err
				}
				b.Logger().Info("remove channel success", zap.Any("assignment", assignment))
			}

			// assign the channel to the target node.
			if err := resource.Resource().StreamingNodeManagerClient().Assign(ctx, channel.CurrentAssignment()); err != nil {
				b.Logger().Warn("fail to assign channel", zap.Any("assignment", channel.CurrentAssignment()), zap.Error(err))
				return err
			}
			b.Logger().Info("assign channel success", zap.Any("assignment", channel.CurrentAssignment()))

			// bookkeeping the meta assignment done.
			if err := b.channelMetaManager.AssignPChannelsDone(ctx, []types.ChannelID{channel.ChannelID()}); err != nil {
				b.Logger().Warn("fail to bookkeep pchannel assignment done", zap.Any("assignment", channel.CurrentAssignment()))
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
func generateCurrentLayout(view *channel.PChannelView, allNodesStatus map[int64]*types.StreamingNodeStatus) (layout CurrentLayout) {
	channelsToNodes := make(map[types.ChannelID]int64, len(view.Channels))

	for id, meta := range view.Channels {
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
	allNodesInfo := make(map[int64]types.StreamingNodeInfo, len(allNodesStatus))
	for serverID, nodeStatus := range allNodesStatus {
		// filter out the unhealthy nodes.
		if nodeStatus.IsHealthy() {
			allNodesInfo[serverID] = nodeStatus.StreamingNodeInfo
		}
	}
	return CurrentLayout{
		Channels:        view.Stats,
		AllNodesInfo:    allNodesInfo,
		ChannelsToNodes: channelsToNodes,
	}
}

type backoffConfigFetcher struct{}

func (f *backoffConfigFetcher) BackoffConfig() typeutil.BackoffConfig {
	return typeutil.BackoffConfig{
		InitialInterval: paramtable.Get().StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse(),
		Multiplier:      paramtable.Get().StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat(),
		MaxInterval:     paramtable.Get().StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse(),
	}
}

func (f *backoffConfigFetcher) DefaultInterval() time.Duration {
	return paramtable.Get().StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse()
}
