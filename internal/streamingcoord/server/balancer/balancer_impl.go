package balancer

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// RecoverBalancer recover the balancer working.
func RecoverBalancer(
	ctx context.Context,
	policy string,
	incomingNewChannel ...string, // Concurrent incoming new channel directly from the configuration.
	// we should add a rpc interface for creating new incoming new channel.
) (Balancer, error) {
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
		logger:                 resource.Resource().Logger().With(log.FieldComponent("balancer"), zap.String("policy", policy)),
		channelMetaManager:     manager,
		policy:                 mustGetPolicy(policy),
		reqCh:                  make(chan *request, 5),
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
	}
	go b.execute()
	return b, nil
}

// balancerImpl is a implementation of Balancer.
type balancerImpl struct {
	ctx                    context.Context
	cancel                 context.CancelCauseFunc
	lifetime               *typeutil.Lifetime
	logger                 *log.MLogger
	channelMetaManager     *channel.ChannelManager
	policy                 Policy                                // policy is the balance policy, TODO: should be dynamic in future.
	reqCh                  chan *request                         // reqCh is the request channel, send the operation to background task.
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}] // backgroundTaskNotifier is used to conmunicate with the background task.
}

// WatchChannelAssignments watches the balance result.
func (b *balancerImpl) WatchChannelAssignments(ctx context.Context, cb func(version typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, _ = contextutil.MergeContext(ctx, b.ctx)
	return b.channelMetaManager.WatchAssignmentResult(ctx, cb)
}

func (b *balancerImpl) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	return b.sendRequestAndWaitFinish(ctx, newOpMarkAsUnavailable(ctx, pChannels))
}

// Trigger trigger a re-balance.
func (b *balancerImpl) Trigger(ctx context.Context) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

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
	b.logger.Info("balancer start to execute")
	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.logger.Info("balancer execute finished")
	}()

	balanceTimer := typeutil.NewBackoffTimer(&backoffConfigFetcher{})
	nodeChanged, err := resource.Resource().StreamingNodeManagerClient().WatchNodeChanged(b.backgroundTaskNotifier.Context())
	if err != nil {
		b.logger.Error("fail to watch node changed", zap.Error(err))
		return
	}
	for {
		// Wait for next balance trigger.
		// Maybe trigger by timer or by request.
		nextTimer, nextBalanceInterval := balanceTimer.NextTimer()
		b.logger.Info("balance wait", zap.Duration("nextBalanceInterval", nextBalanceInterval))
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
		}

		if err := b.balance(b.backgroundTaskNotifier.Context()); err != nil {
			if b.backgroundTaskNotifier.Context().Err() != nil {
				// balancer is closed.
				return
			}
			b.logger.Warn("fail to apply balance, start a backoff...", zap.Error(err))
			balanceTimer.EnableBackoff()
			continue
		}

		b.logger.Info("apply balance success")
		balanceTimer.DisableBackoff()
	}
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

// Trigger a balance of layout.
// Return a nil chan to avoid
// Return a channel to notify the balance trigger again.
func (b *balancerImpl) balance(ctx context.Context) error {
	b.logger.Info("start to balance")
	pchannelView := b.channelMetaManager.CurrentPChannelsView()

	b.logger.Info("collect all status...")
	nodeStatus, err := resource.Resource().StreamingNodeManagerClient().CollectAllStatus(ctx)
	if err != nil {
		return errors.Wrap(err, "fail to collect all status")
	}

	// call the balance strategy to generate the expected layout.
	currentLayout := generateCurrentLayout(pchannelView, nodeStatus)
	expectedLayout, err := b.policy.Balance(currentLayout)
	if err != nil {
		return errors.Wrap(err, "fail to balance")
	}

	b.logger.Info("balance policy generate result success, try to assign...", zap.Any("expectedLayout", expectedLayout))
	// bookkeeping the meta assignment started.
	modifiedChannels, err := b.channelMetaManager.AssignPChannels(ctx, expectedLayout.ChannelAssignment)
	if err != nil {
		return errors.Wrap(err, "fail to assign pchannels")
	}

	if len(modifiedChannels) == 0 {
		b.logger.Info("no change of balance result need to be applied")
		return nil
	}
	return b.applyBalanceResultToStreamingNode(ctx, modifiedChannels)
}

// applyBalanceResultToStreamingNode apply the balance result to streaming node.
func (b *balancerImpl) applyBalanceResultToStreamingNode(ctx context.Context, modifiedChannels map[string]*channel.PChannelMeta) error {
	b.logger.Info("balance result need to be applied...", zap.Int("modifiedChannelCount", len(modifiedChannels)))

	// different channel can be execute concurrently.
	g, _ := errgroup.WithContext(ctx)
	// generate balance operations and applied them.
	for _, channel := range modifiedChannels {
		channel := channel
		g.Go(func() error {
			// all history channels should be remove from related nodes.
			for _, assignment := range channel.AssignHistories() {
				if err := resource.Resource().StreamingNodeManagerClient().Remove(ctx, assignment); err != nil {
					b.logger.Warn("fail to remove channel", zap.Any("assignment", assignment), zap.Error(err))
					return err
				}
				b.logger.Info("remove channel success", zap.Any("assignment", assignment))
			}

			// assign the channel to the target node.
			if err := resource.Resource().StreamingNodeManagerClient().Assign(ctx, channel.CurrentAssignment()); err != nil {
				b.logger.Warn("fail to assign channel", zap.Any("assignment", channel.CurrentAssignment()))
				return err
			}
			b.logger.Info("assign channel success", zap.Any("assignment", channel.CurrentAssignment()))

			// bookkeeping the meta assignment done.
			if err := b.channelMetaManager.AssignPChannelsDone(ctx, []string{channel.Name()}); err != nil {
				b.logger.Warn("fail to bookkeep pchannel assignment done", zap.Any("assignment", channel.CurrentAssignment()))
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

// generateCurrentLayout generate layout from all nodes info and meta.
func generateCurrentLayout(channelsInMeta map[string]*channel.PChannelMeta, allNodesStatus map[int64]*types.StreamingNodeStatus) (layout CurrentLayout) {
	activeRelations := make(map[int64][]types.PChannelInfo, len(allNodesStatus))
	incomingChannels := make([]string, 0)
	channelsToNodes := make(map[string]int64, len(channelsInMeta))
	assigned := make(map[int64][]types.PChannelInfo, len(allNodesStatus))
	for _, meta := range channelsInMeta {
		if !meta.IsAssigned() {
			incomingChannels = append(incomingChannels, meta.Name())
			// dead or expired relationship.
			log.Warn("channel is not assigned to any server",
				zap.String("channel", meta.Name()),
				zap.Int64("term", meta.CurrentTerm()),
				zap.Int64("serverID", meta.CurrentServerID()),
				zap.String("state", meta.State().String()),
			)
			continue
		}
		if nodeStatus, ok := allNodesStatus[meta.CurrentServerID()]; ok && nodeStatus.IsHealthy() {
			// active relationship.
			activeRelations[meta.CurrentServerID()] = append(activeRelations[meta.CurrentServerID()], types.PChannelInfo{
				Name: meta.Name(),
				Term: meta.CurrentTerm(),
			})
			channelsToNodes[meta.Name()] = meta.CurrentServerID()
			assigned[meta.CurrentServerID()] = append(assigned[meta.CurrentServerID()], meta.ChannelInfo())
		} else {
			incomingChannels = append(incomingChannels, meta.Name())
			// dead or expired relationship.
			log.Warn("channel of current server id is not healthy or not alive",
				zap.String("channel", meta.Name()),
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
		IncomingChannels: incomingChannels,
		ChannelsToNodes:  channelsToNodes,
		AssignedChannels: assigned,
		AllNodesInfo:     allNodesInfo,
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
