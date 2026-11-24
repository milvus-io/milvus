package manager

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ ManagerClient = (*managerClientImpl)(nil)

const defaultCollectStatusProbeTimeout = 2 * time.Second

// managerClientImpl implements ManagerClient.
type managerClientImpl struct {
	lifetime *typeutil.Lifetime
	stopped  chan struct{}

	rb           resolver.Builder
	service      lazygrpc.Service[streamingpb.StreamingNodeManagerServiceClient]
	probeTimeout time.Duration
}

func (c *managerClientImpl) getProbeTimeout() time.Duration {
	if c.probeTimeout > 0 {
		return c.probeTimeout
	}
	return defaultCollectStatusProbeTimeout
}

func (c *managerClientImpl) WatchNodeChanged(ctx context.Context) (<-chan struct{}, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	resultCh := make(chan struct{}, 1)
	go func() {
		defer close(resultCh)
		c.rb.Resolver().Watch(ctx, func(state resolver.VersionedState) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.stopped:
				return status.NewOnShutdownError("manager client is closing")
			case resultCh <- struct{}{}:
			}
			return nil
		})
	}()
	return resultCh, nil
}

// GetAllStreamingNodes fetches all streaming node info with resource group.
func (c *managerClientImpl) GetAllStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// Get all discovered streamingnode.
	state, err := c.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[int64]*types.StreamingNodeInfoWithResourceGroup, len(state.State.Addresses))
	for serverID, session := range state.Sessions() {
		rg := session.GetResourceGroupName()
		if rg == "" {
			rg = common.DefaultResourceGroupName
		}
		result[serverID] = &types.StreamingNodeInfoWithResourceGroup{
			StreamingNodeInfo: types.StreamingNodeInfo{
				ServerID: serverID,
				Address:  session.Address,
			},
			ResourceGroup: rg,
		}
	}
	return result, nil
}

// CollectAllStatus collects status in underlying streamingnodes.
// If resourceGroupHint has discovered nodes, it probes that resource group first.
// If the selected resource group yields no healthy nodes (e.g., unreachable or stale registrations),
// it falls back across remaining discovered resource groups.
func (c *managerClientImpl) CollectAllStatus(ctx context.Context, resourceGroupHint string) (map[int64]*types.StreamingNodeStatus, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// Get all discovered streamingnode.
	state, err := c.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, err
	}
	if len(state.State.Addresses) == 0 {
		return make(map[int64]*types.StreamingNodeStatus), nil
	}

	effectiveHint := effectiveResourceGroupHintForCollectAllStatus(state, resourceGroupHint)
	if effectiveHint == "" {
		result, _, err := c.probeAndAnnotateResourceGroup(ctx, state, "")
		return result, err
	}

	remainingStatsByRG := groupStreamingNodeSessionStatsByResourceGroup(state)
	var fallbackResult map[int64]*types.StreamingNodeStatus
	reprobedNewPrimary := false
	for len(remainingStatsByRG) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		targetRG, ok := selectResourceGroupForProbe(remainingStatsByRG, effectiveHint)
		if !ok {
			break
		}
		delete(remainingStatsByRG, targetRG)

		result, latestState, err := c.probeAndAnnotateResourceGroup(ctx, state, targetRG)
		if err != nil {
			return nil, err
		}
		if len(fallbackResult) == 0 {
			fallbackResult = result
		}
		if hasHealthyStreamingNode(result) {
			if targetRG != effectiveHint && !reprobedNewPrimary && hasNewHealthySessionInResourceGroup(state, latestState, effectiveHint) {
				reprobedNewPrimary = true
				primaryResult, primaryLatestState, err := c.probeAndAnnotateResourceGroup(ctx, latestState, effectiveHint)
				if err != nil {
					return nil, err
				}
				if hasHealthyStreamingNode(primaryResult) {
					return primaryResult, nil
				}
				result = applyLatestSessionState(latestState, primaryLatestState, result)
				if hasHealthyStreamingNode(result) {
					return result, nil
				}
				fallbackResult = result
				state = primaryLatestState
				continue
			}
			return result, nil
		}
		mlog.Warn(ctx, "no healthy streaming node in resource group, trying fallback",
			mlog.String("resourceGroup", targetRG),
			mlog.Int("probedCount", len(result)))
	}
	if fallbackResult != nil {
		return fallbackResult, nil
	}
	return make(map[int64]*types.StreamingNodeStatus), nil
}

func (c *managerClientImpl) probeAndAnnotateResourceGroup(
	ctx context.Context,
	state discoverer.VersionedState,
	resourceGroup string,
) (map[int64]*types.StreamingNodeStatus, discoverer.VersionedState, error) {
	result, err := c.getAllStreamingNodeStatus(ctx, state, resourceGroup)
	if err != nil {
		return nil, discoverer.VersionedState{}, err
	}
	if len(result) == 0 {
		return result, discoverer.VersionedState{}, nil
	}
	return c.annotateLatestSessionState(ctx, state, result)
}

func (c *managerClientImpl) annotateLatestSessionState(
	ctx context.Context,
	state discoverer.VersionedState,
	result map[int64]*types.StreamingNodeStatus,
) (map[int64]*types.StreamingNodeStatus, discoverer.VersionedState, error) {
	// Collect status may cost some time, so we need to check the lifetime again.
	newState, err := c.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, discoverer.VersionedState{}, err
	}
	return applyLatestSessionState(state, newState, result), newState, nil
}

func applyLatestSessionState(
	prevState discoverer.VersionedState,
	newState discoverer.VersionedState,
	result map[int64]*types.StreamingNodeStatus,
) map[int64]*types.StreamingNodeStatus {
	if len(result) == 0 || !newState.Version.GT(prevState.Version) {
		return result
	}
	newSession := newState.Sessions()
	for serverID := range result {
		if session, ok := newSession[serverID]; !ok {
			result[serverID].Err = types.ErrNotAlive
		} else if session.Stopping {
			result[serverID].Err = types.ErrStopping
		}
	}
	return result
}

func hasNewHealthySessionInResourceGroup(
	prevState discoverer.VersionedState,
	latestState discoverer.VersionedState,
	resourceGroup string,
) bool {
	if !latestState.Version.GT(prevState.Version) {
		return false
	}
	prevSessions := prevState.Sessions()
	for serverID, session := range latestState.Sessions() {
		if normalizedResourceGroupName(session) != resourceGroup || session.Stopping {
			continue
		}
		if prevSession, ok := prevSessions[serverID]; !ok || prevSession.Address != session.Address {
			return true
		}
	}
	return false
}

func normalizedResourceGroupName(session *sessionutil.SessionRaw) string {
	rg := session.GetResourceGroupName()
	if rg == "" {
		return common.DefaultResourceGroupName
	}
	return rg
}

func hasHealthyStreamingNode(nodes map[int64]*types.StreamingNodeStatus) bool {
	for _, node := range nodes {
		if node != nil && node.IsHealthy() {
			return true
		}
	}
	return false
}

func selectResourceGroupForProbe(statsByRG map[string]resourceGroupSessionStats, preferredRG string) (string, bool) {
	if preferredRG != "" {
		if _, ok := statsByRG[preferredRG]; ok {
			return preferredRG, true
		}
	}
	return chooseFallbackResourceGroup(statsByRG)
}

func effectiveResourceGroupHintForCollectAllStatus(state discoverer.VersionedState, resourceGroupHint string) string {
	if resourceGroupHint != "" {
		return resourceGroupHint
	}
	for _, session := range state.Sessions() {
		if normalizedResourceGroupName(session) == common.DefaultResourceGroupName {
			return common.DefaultResourceGroupName
		}
	}
	return ""
}

type resourceGroupSessionStats struct {
	count       int
	maxServerID int64
}

func groupStreamingNodeSessionStatsByResourceGroup(state discoverer.VersionedState) map[string]resourceGroupSessionStats {
	statsByRG := make(map[string]resourceGroupSessionStats)
	for serverID, session := range state.Sessions() {
		rg := normalizedResourceGroupName(session)
		stats := statsByRG[rg]
		stats.count++
		if serverID > stats.maxServerID {
			stats.maxServerID = serverID
		}
		statsByRG[rg] = stats
	}
	return statsByRG
}

func chooseFallbackResourceGroup(statsByRG map[string]resourceGroupSessionStats) (string, bool) {
	var selectedRG string
	var selectedCount int
	var selectedMaxServerID int64
	for rg, stats := range statsByRG {
		if stats.count == 0 {
			continue
		}
		if selectedRG == "" || stats.count > selectedCount || (stats.count == selectedCount && stats.maxServerID > selectedMaxServerID) {
			selectedRG = rg
			selectedCount = stats.count
			selectedMaxServerID = stats.maxServerID
		}
	}
	return selectedRG, selectedRG != ""
}

func (c *managerClientImpl) getAllStreamingNodeStatus(ctx context.Context, state discoverer.VersionedState, resourceGroup string) (map[int64]*types.StreamingNodeStatus, error) {
	// wait for manager service ready.
	manager, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(16)
	var mu sync.Mutex
	result := make(map[int64]*types.StreamingNodeStatus, len(state.Sessions()))
	for serverID, session := range state.Sessions() {
		serverID := serverID
		address := session.Address
		rg := normalizedResourceGroupName(session)
		if resourceGroup != "" && rg != resourceGroup {
			continue
		}
		g.Go(func() error {
			probeCtx, cancel := context.WithTimeout(ctx, c.getProbeTimeout())
			defer cancel()
			probeCtx = contextutil.WithPickServerID(probeCtx, serverID)
			resp, err := manager.CollectStatus(probeCtx, &streamingpb.StreamingNodeManagerCollectStatusRequest{})
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				mlog.Warn(probeCtx, "collect status failed, skip", mlog.Int64("serverID", serverID), mlog.Err(err))
				return err
			}
			result[serverID] = &types.StreamingNodeStatus{
				StreamingNodeInfo: types.StreamingNodeInfo{
					ServerID: serverID,
					Address:  address,
				},
				Metrics: types.NewStreamingNodeBalanceAttrsFromProto(resp.Metrics),
				Err:     err,
			}
			mlog.Debug(probeCtx, "collect status success", mlog.Int64("serverID", serverID), mlog.Any("status", resp))
			return nil
		})
	}
	g.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Assign a wal instance for the channel on log node of given server id.
func (c *managerClientImpl) Assign(ctx context.Context, pchannel types.PChannelInfoAssigned) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// wait for manager service ready.
	manager, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}

	// Select a log node to assign the wal instance.
	ctx = contextutil.WithPickServerID(ctx, pchannel.Node.ServerID)
	_, err = manager.Assign(ctx, &streamingpb.StreamingNodeManagerAssignRequest{
		Pchannel: types.NewProtoFromPChannelInfo(pchannel.Channel),
	})
	return err
}

// Remove the wal instance for the channel on log node of given server id.
func (c *managerClientImpl) Remove(ctx context.Context, pchannel types.PChannelInfoAssigned) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// wait for manager service ready.
	manager, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}

	// Select a streaming node to remove the wal instance.
	ctx = contextutil.WithPickServerID(ctx, pchannel.Node.ServerID)
	_, err = manager.Remove(ctx, &streamingpb.StreamingNodeManagerRemoveRequest{
		Pchannel: types.NewProtoFromPChannelInfo(pchannel.Channel),
	})
	// A real remove operation at streaming node has been happened.
	if err == nil {
		return nil
	}
	// The streaming node is not in the session view, the wal on it is not confirmed closed.
	if picker.IsErrSubConnNoExist(err) {
		return types.ErrNotAlive
	}
	// The streaming node is not the owner of the wal (SkippedOperation), so the wal on it is already removed.
	statusErr := status.AsStreamingError(err)
	if statusErr == nil || statusErr.IsSkippedOperation() {
		return nil
	}
	return err
}

// Close closes the manager client.
func (c *managerClientImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	close(c.stopped)
	c.lifetime.Wait()

	c.service.Close()
	c.rb.Close()
}
