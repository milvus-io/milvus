package balancer

import (
	"context"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// EnsureReadOnlyWALReplica materializes a serviceable read-only WAL replica for
// the PChannel in the requested resource group.
func (b *balancerImpl) EnsureReadOnlyWALReplica(ctx context.Context, pchannel string, resourceGroup string) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()

	resourceGroup = effectiveWALReplicaResourceGroup(resourceGroup)
	nodes, err := b.GetAvailableStreamingNodes(ctx)
	if err != nil {
		return err
	}
	latest, err := b.channelMetaManager.GetLatestChannelAssignment()
	if err != nil {
		return err
	}

	view := b.channelMetaManager.CurrentPChannelsView()
	pchannelMeta, ok := view.Channels[types.ChannelID{Name: pchannel}]
	if !ok {
		return status.NewInner("pchannel %s does not exist when ensuring read-only WAL replica", pchannel)
	}
	if replicaID, cleanupAssignments, ok := serviceableReadOnlyWALReplicaPendingCleanup(pchannelMeta, resourceGroup, latest.WALReplicaRelations, nodes); ok {
		return b.cleanupWALReplicaHistories(ctx, replicaID, cleanupAssignments)
	}
	if hasServiceableReadOnlyWALReplica(latest.WALReplicaRelations, pchannel, resourceGroup, nodes) {
		return nil
	}
	replicaID, target, err := b.selectReadOnlyWALReplicaTarget(pchannelMeta, resourceGroup, latest.WALReplicaRelations, nodes)
	if err != nil {
		return err
	}
	if replicaID.IsZero() {
		replicaID, err = b.channelMetaManager.CreateReadOnlyWALReplica(ctx, pchannel, resourceGroup)
		if err != nil {
			return err
		}
	}
	return b.assignReadOnlyWALReplica(ctx, replicaID, target)
}

// ReleaseReadOnlyWALReplica releases a non-primary read-only WAL replica after
// the caller has established that no external dependency remains.
func (b *balancerImpl) ReleaseReadOnlyWALReplica(ctx context.Context, pchannel string, walReplicaID int64) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()

	replicaID := types.ChannelID{Name: pchannel, WALReplicaID: walReplicaID}
	view := b.channelMetaManager.CurrentPChannelsView()
	pchannelMeta, ok := view.Channels[types.ChannelID{Name: pchannel}]
	if !ok {
		return channel.ErrChannelNotExist
	}
	replica, ok := pchannelMeta.WALReplica(walReplicaID)
	if !ok {
		return channel.ErrWALReplicaNotExist
	}
	cleanupAssignments := walReplicaCleanupAssignments(pchannelMeta, walReplicaID)

	if replica.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING {
		if err := b.channelMetaManager.MarkWALReplicasAsDropping(ctx, []types.ChannelID{replicaID}); err != nil {
			return err
		}
	}
	for _, cleanupAssignment := range cleanupAssignments {
		opCtx, cancel := context.WithTimeout(ctx, paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse())
		err := resource.Resource().StreamingNodeManagerClient().Remove(opCtx, cleanupAssignment)
		cancel()
		if err != nil {
			return err
		}
	}
	return b.channelMetaManager.RemoveWALReplicas(ctx, []types.ChannelID{replicaID})
}

// SwitchWALPrimaryReplica promotes an existing serviceable read-only WAL replica
// to the PChannel read-write primary.
func (b *balancerImpl) SwitchWALPrimaryReplica(ctx context.Context, pchannel string, targetReplicaID int64) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()

	view := b.channelMetaManager.CurrentPChannelsView()
	before, ok := view.Channels[types.ChannelID{Name: pchannel}]
	if !ok {
		return status.NewInner("pchannel %s does not exist before WAL primary switch", pchannel)
	}
	oldPrimaryID := before.PrimaryReplicaID()

	if err := b.channelMetaManager.SwitchWALPrimaryReplica(ctx, pchannel, targetReplicaID); err != nil {
		return err
	}
	view = b.channelMetaManager.CurrentPChannelsView()
	pchannelMeta, ok := view.Channels[types.ChannelID{Name: pchannel}]
	if !ok {
		return status.NewInner("pchannel %s does not exist after WAL primary switch", pchannel)
	}
	var demoteErr error
	var oldPrimaryAssignment types.PChannelInfoAssigned
	var hasOldPrimaryAssignment bool
	if oldPrimaryID != targetReplicaID {
		var ok bool
		oldPrimaryAssignment, ok = walReplicaRuntimeAssignment(pchannelMeta, oldPrimaryID)
		if ok {
			hasOldPrimaryAssignment = true
			opCtx, cancel := context.WithTimeout(ctx, paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse())
			demoteErr = resource.Resource().StreamingNodeManagerClient().Assign(opCtx, oldPrimaryAssignment)
			cancel()
		}
	}
	if demoteErr != nil && hasOldPrimaryAssignment {
		markErr := b.channelMetaManager.MarkWALReplicasAsUnavailable(ctx, []types.ChannelID{{
			Name:         pchannel,
			WALReplicaID: oldPrimaryID,
		}}, oldPrimaryAssignment.AssignmentEpoch)
		demoteErr = errors.CombineErrors(demoteErr, markErr)
	}
	assignment := pchannelMeta.CurrentAssignment()
	opCtx, cancel := context.WithTimeout(ctx, paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse())
	promoteErr := resource.Resource().StreamingNodeManagerClient().Assign(opCtx, assignment)
	cancel()
	if promoteErr != nil {
		markErr := b.channelMetaManager.MarkWALPrimaryReplicaAsUnavailable(ctx, types.ChannelID{
			Name:         pchannel,
			WALReplicaID: targetReplicaID,
		}, assignment.AssignmentEpoch)
		return errors.CombineErrors(errors.CombineErrors(promoteErr, markErr), demoteErr)
	}
	if err := b.channelMetaManager.AssignWALReplicasDone(ctx, map[types.ChannelID]int64{
		{
			Name:         pchannel,
			WALReplicaID: targetReplicaID,
		}: assignment.AssignmentEpoch,
	}); err != nil {
		return errors.CombineErrors(err, demoteErr)
	}
	return demoteErr
}

func hasServiceableReadOnlyWALReplica(
	assignments []types.WALReplicaInfoAssigned,
	pchannel string,
	resourceGroup string,
	nodes map[int64]*types.StreamingNodeInfoWithResourceGroup,
) bool {
	for _, assignment := range assignments {
		replica := assignment.Replica
		if replica.ChannelID.Name != pchannel {
			continue
		}
		if replica.AccessMode != types.AccessModeRO {
			continue
		}
		if effectiveWALReplicaResourceGroup(replica.ResourceGroup) != resourceGroup {
			continue
		}
		node, ok := nodes[assignment.Node.ServerID]
		if !ok {
			continue
		}
		if effectiveWALReplicaResourceGroup(node.ResourceGroup) != resourceGroup {
			continue
		}
		return true
	}
	return false
}

func serviceableReadOnlyWALReplicaPendingCleanup(
	pchannelMeta *channel.PChannelMeta,
	resourceGroup string,
	assignments []types.WALReplicaInfoAssigned,
	nodes map[int64]*types.StreamingNodeInfoWithResourceGroup,
) (types.ChannelID, []types.PChannelInfoAssigned, bool) {
	resourceGroup = effectiveWALReplicaResourceGroup(resourceGroup)
	for _, assignment := range assignments {
		replicaInfo := assignment.Replica
		if replicaInfo.ChannelID.Name != pchannelMeta.Name() {
			continue
		}
		if replicaInfo.AccessMode != types.AccessModeRO {
			continue
		}
		if effectiveWALReplicaResourceGroup(replicaInfo.ResourceGroup) != resourceGroup {
			continue
		}
		node, ok := nodes[assignment.Node.ServerID]
		if !ok || effectiveWALReplicaResourceGroup(node.ResourceGroup) != resourceGroup {
			continue
		}
		replica, ok := pchannelMeta.WALReplica(replicaInfo.ChannelID.WALReplicaID)
		if !ok || len(replica.GetHistories()) == 0 {
			continue
		}
		cleanupAssignments := walReplicaHistoryAssignments(replicaInfo.ChannelID, replica)
		if len(cleanupAssignments) == 0 {
			continue
		}
		return replicaInfo.ChannelID, cleanupAssignments, true
	}
	return types.ChannelID{}, nil, false
}

func (b *balancerImpl) selectReadOnlyWALReplicaTarget(
	pchannelMeta *channel.PChannelMeta,
	resourceGroup string,
	assignments []types.WALReplicaInfoAssigned,
	nodes map[int64]*types.StreamingNodeInfoWithResourceGroup,
) (types.ChannelID, types.StreamingNodeInfo, error) {
	targetNode, ok := selectWALReplicaTargetNode(nodes, resourceGroup, assignments)
	if !ok {
		return types.ChannelID{}, types.StreamingNodeInfo{},
			status.NewInner("no available StreamingNode in resource group %s for read-only WAL replica of pchannel %s", resourceGroup, pchannelMeta.Name())
	}

	for _, replica := range pchannelMeta.Replicas() {
		if replica.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY {
			continue
		}
		if effectiveWALReplicaResourceGroup(replica.GetResourceGroup()) != resourceGroup {
			continue
		}
		if replica.GetState() == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING {
			continue
		}
		if target := replica.GetTargetNode(); target != nil {
			if node, ok := nodes[target.GetServerId()]; ok && effectiveWALReplicaResourceGroup(node.ResourceGroup) == resourceGroup {
				targetNode = node.StreamingNodeInfo
			}
		}
		return types.ChannelID{Name: pchannelMeta.Name(), WALReplicaID: replica.GetReplicaId()}, targetNode, nil
	}
	return types.ChannelID{}, targetNode, nil
}

func selectWALReplicaTargetNode(
	nodes map[int64]*types.StreamingNodeInfoWithResourceGroup,
	resourceGroup string,
	assignments []types.WALReplicaInfoAssigned,
) (types.StreamingNodeInfo, bool) {
	loads := make(map[int64]int)
	for _, assignment := range assignments {
		loads[assignment.Node.ServerID]++
	}
	candidates := make([]*types.StreamingNodeInfoWithResourceGroup, 0, len(nodes))
	for _, node := range nodes {
		if effectiveWALReplicaResourceGroup(node.ResourceGroup) != resourceGroup {
			continue
		}
		candidates = append(candidates, node)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if loads[left.ServerID] != loads[right.ServerID] {
			return loads[left.ServerID] < loads[right.ServerID]
		}
		return left.ServerID < right.ServerID
	})
	if len(candidates) == 0 {
		return types.StreamingNodeInfo{}, false
	}
	return candidates[0].StreamingNodeInfo, true
}

func (b *balancerImpl) assignReadOnlyWALReplica(ctx context.Context, replicaID types.ChannelID, target types.StreamingNodeInfo) error {
	if _, err := b.channelMetaManager.AssignWALReplicas(ctx, map[types.ChannelID]types.StreamingNodeInfo{
		replicaID: target,
	}); err != nil {
		return err
	}

	view := b.channelMetaManager.CurrentPChannelsView()
	pchannelMeta, ok := view.Channels[types.ChannelID{Name: replicaID.Name}]
	if !ok {
		return status.NewInner("pchannel %s does not exist after read-only WAL replica assignment", replicaID.Name)
	}
	replica, ok := pchannelMeta.WALReplica(replicaID.WALReplicaID)
	if !ok {
		return status.NewInner("wal replica %s does not exist after read-only WAL replica assignment", replicaID.String())
	}
	cleanupAssignments := walReplicaHistoryAssignments(replicaID, replica)
	assignment := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       replicaID.Name,
			Term:       pchannelMeta.CurrentTerm(),
			AccessMode: types.AccessModeRO,
		},
		WALReplicaID:    replicaID.WALReplicaID,
		AssignmentEpoch: replica.GetAssignmentEpoch(),
		Node:            target,
	}
	opCtx, cancel := context.WithTimeout(ctx, paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse())
	defer cancel()
	if err := resource.Resource().StreamingNodeManagerClient().Assign(opCtx, assignment); err != nil {
		return err
	}
	if err := b.channelMetaManager.AssignWALReplicasDone(ctx, map[types.ChannelID]int64{
		replicaID: assignment.AssignmentEpoch,
	}); err != nil {
		return err
	}
	return b.cleanupWALReplicaHistories(ctx, replicaID, cleanupAssignments)
}

func (b *balancerImpl) cleanupWALReplicaHistories(
	ctx context.Context,
	replicaID types.ChannelID,
	cleanupAssignments []types.PChannelInfoAssigned,
) error {
	if len(cleanupAssignments) == 0 {
		return nil
	}
	for _, cleanup := range cleanupAssignments {
		opCtx, cancel := context.WithTimeout(ctx, paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse())
		err := resource.Resource().StreamingNodeManagerClient().Remove(opCtx, cleanup)
		cancel()
		if err != nil {
			return err
		}
	}
	return b.channelMetaManager.ClearWALReplicaHistories(ctx, []types.ChannelID{replicaID})
}

func walReplicaHistoryAssignments(replicaID types.ChannelID, replica *streamingpb.WALReplicaAssignment) []types.PChannelInfoAssigned {
	assignments := make([]types.PChannelInfoAssigned, 0, len(replica.GetHistories()))
	for _, history := range replica.GetHistories() {
		assignments = append(assignments, types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{
				Name:       replicaID.Name,
				Term:       history.GetTerm(),
				AccessMode: types.AccessMode(history.GetAccessMode()),
			},
			WALReplicaID:    replicaID.WALReplicaID,
			AssignmentEpoch: history.GetAssignmentEpoch(),
			Node:            types.NewStreamingNodeInfoFromProto(history.GetNode()),
		})
	}
	return assignments
}

func walReplicaRuntimeAssignment(pchannelMeta *channel.PChannelMeta, replicaID int64) (types.PChannelInfoAssigned, bool) {
	replica, ok := pchannelMeta.WALReplica(replicaID)
	if !ok || replica.GetActiveNode() == nil {
		return types.PChannelInfoAssigned{}, false
	}
	return types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       pchannelMeta.Name(),
			Term:       pchannelMeta.CurrentTerm(),
			AccessMode: types.AccessMode(replica.GetAccessMode()),
		},
		WALReplicaID:    replicaID,
		AssignmentEpoch: replica.GetAssignmentEpoch(),
		Node:            types.NewStreamingNodeInfoFromProto(replica.GetActiveNode()),
	}, true
}

func walReplicaCleanupAssignments(pchannelMeta *channel.PChannelMeta, replicaID int64) []types.PChannelInfoAssigned {
	replica, ok := pchannelMeta.WALReplica(replicaID)
	if !ok {
		return nil
	}
	assignments := make([]types.PChannelInfoAssigned, 0, 2)
	for _, assignment := range walReplicaHistoryAssignments(types.ChannelID{Name: pchannelMeta.Name(), WALReplicaID: replicaID}, replica) {
		assignments = appendUniqueWALReplicaCleanupAssignment(assignments, assignment)
	}
	if active := replica.GetActiveNode(); active != nil {
		assignments = appendUniqueWALReplicaCleanupAssignment(assignments, walReplicaNodeAssignment(
			pchannelMeta,
			replica,
			replicaID,
			active,
			walReplicaCleanupEpochForNode(replica, active),
		))
	}
	if target := replica.GetTargetNode(); target != nil {
		assignments = appendUniqueWALReplicaCleanupAssignment(assignments, walReplicaNodeAssignment(
			pchannelMeta,
			replica,
			replicaID,
			target,
			walReplicaCleanupEpochForNode(replica, target),
		))
	}
	return assignments
}

func appendUniqueWALReplicaCleanupAssignment(
	assignments []types.PChannelInfoAssigned,
	assignment types.PChannelInfoAssigned,
) []types.PChannelInfoAssigned {
	for _, existing := range assignments {
		if existing.Channel.Name == assignment.Channel.Name &&
			existing.Channel.AccessMode == assignment.Channel.AccessMode &&
			existing.WALReplicaID == assignment.WALReplicaID &&
			existing.AssignmentEpoch == assignment.AssignmentEpoch &&
			existing.Node.ServerID == assignment.Node.ServerID {
			return assignments
		}
	}
	return append(assignments, assignment)
}

func walReplicaCleanupEpochForNode(replica *streamingpb.WALReplicaAssignment, node *streamingpb.StreamingNodeInfo) int64 {
	for _, history := range replica.GetHistories() {
		if history.GetNode().GetServerId() == node.GetServerId() &&
			history.GetAccessMode() == replica.GetAccessMode() {
			return history.GetAssignmentEpoch()
		}
	}
	return replica.GetAssignmentEpoch()
}

func walReplicaNodeAssignment(
	pchannelMeta *channel.PChannelMeta,
	replica *streamingpb.WALReplicaAssignment,
	replicaID int64,
	node *streamingpb.StreamingNodeInfo,
	assignmentEpoch int64,
) types.PChannelInfoAssigned {
	return types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       pchannelMeta.Name(),
			Term:       pchannelMeta.CurrentTerm(),
			AccessMode: types.AccessMode(replica.GetAccessMode()),
		},
		WALReplicaID:    replicaID,
		AssignmentEpoch: assignmentEpoch,
		Node:            types.NewStreamingNodeInfoFromProto(node),
	}
}

func effectiveWALReplicaResourceGroup(resourceGroup string) string {
	if resourceGroup == "" {
		return common.DefaultResourceGroupName
	}
	return resourceGroup
}
