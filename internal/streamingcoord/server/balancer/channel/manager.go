package channel

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	StreamingVersion260 = 1 // streaming version that since 2.6.0, the streaming based WAL is available.
	StreamingVersion265 = 2 // streaming version that since 2.6.5, the WAL based DDL is available.
)

var ErrChannelNotExist = errors.New("channel not exist")

type (
	AllocVChannelParam struct {
		CollectionID int64
		Num          int
	}

	WatchChannelAssignmentsCallbackParam struct {
		Version                typeutil.VersionInt64Pair
		CChannelAssignment     *streamingpb.CChannelAssignment
		PChannelView           *PChannelView
		Relations              []types.PChannelInfoAssigned
		ReplicateConfiguration *commonpb.ReplicateConfiguration
	}
	WatchChannelAssignmentsCallback func(param WatchChannelAssignmentsCallbackParam) error
)

// RecoverChannelManager creates a new channel manager.
func RecoverChannelManager(ctx context.Context, incomingChannel ...string) (*ChannelManager, error) {
	// streamingVersion is used to identify current streaming service version.
	// Used to check if there's some upgrade happens.
	streamingVersion, err := resource.Resource().StreamingCatalog().GetVersion(ctx)
	if err != nil {
		return nil, err
	}
	cchannelMeta, err := recoverCChannelMeta(ctx, incomingChannel...)
	if err != nil {
		return nil, err
	}
	replicateConfig, err := recoverReplicateConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	channels, metrics, err := recoverFromConfigurationAndMeta(ctx, streamingVersion, incomingChannel...)
	if err != nil {
		return nil, err
	}

	globalVersion := paramtable.GetNodeID()
	return &ChannelManager{
		cond:     syncutil.NewContextCond(&sync.Mutex{}),
		channels: channels,
		version: typeutil.VersionInt64Pair{
			Global: globalVersion, // global version should be keep increasing globally, it's ok to use node id.
			Local:  0,
		},
		metrics:          metrics,
		cchannelMeta:     cchannelMeta,
		streamingVersion: streamingVersion,
		replicateConfig:  replicateConfig,
	}, nil
}

// recoverCChannelMeta recovers the control channel meta.
func recoverCChannelMeta(ctx context.Context, incomingChannel ...string) (*streamingpb.CChannelMeta, error) {
	cchannelMeta, err := resource.Resource().StreamingCatalog().GetCChannel(ctx)
	if err != nil {
		return nil, err
	}
	if cchannelMeta == nil {
		if len(incomingChannel) == 0 {
			return nil, errors.New("no incoming channel while no control channel meta found")
		}
		cchannelMeta = &streamingpb.CChannelMeta{
			Pchannel: incomingChannel[0],
		}
		if err := resource.Resource().StreamingCatalog().SaveCChannel(ctx, cchannelMeta); err != nil {
			return nil, err
		}
		return cchannelMeta, nil
	}
	return cchannelMeta, nil
}

// recoverFromConfigurationAndMeta recovers the channel manager from configuration and meta.
func recoverFromConfigurationAndMeta(ctx context.Context, streamingVersion *streamingpb.StreamingVersion, incomingChannel ...string) (map[ChannelID]*PChannelMeta, *channelMetrics, error) {
	// Recover metrics.
	metrics := newPChannelMetrics()

	// Get all channels from meta.
	channelMetas, err := resource.Resource().StreamingCatalog().ListPChannel(ctx)
	if err != nil {
		return nil, metrics, err
	}

	// TODO: only support rw channel here now, add ro channel in future.
	channels := make(map[ChannelID]*PChannelMeta, len(channelMetas))
	for _, channel := range channelMetas {
		c := newPChannelMetaFromProto(channel)
		metrics.AssignPChannelStatus(c)
		channels[c.ChannelID()] = c
	}

	// Get new incoming meta from configuration.
	for _, newChannel := range incomingChannel {
		var c *PChannelMeta
		if streamingVersion == nil {
			// if streaming service has never been enabled, we treat all channels as read-only.
			c = NewPChannelMeta(newChannel, types.AccessModeRO)
		} else {
			// once the streaming service is enabled, we treat all channels as read-write.
			c = NewPChannelMeta(newChannel, types.AccessModeRW)
		}
		if _, ok := channels[c.ChannelID()]; !ok {
			channels[c.ChannelID()] = c
		}
	}
	return channels, metrics, nil
}

func recoverReplicateConfiguration(ctx context.Context) (*replicateutil.ConfigHelper, error) {
	config, err := resource.Resource().StreamingCatalog().GetReplicateConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	return replicateutil.MustNewConfigHelper(
		paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		config.GetReplicateConfiguration(),
	), nil
}

// ChannelManager manages the channels.
// ChannelManager is the `wal` of channel assignment and unassignment.
// Every operation applied to the streaming node should be recorded in ChannelManager first.
type ChannelManager struct {
	log.Binder

	cond             *syncutil.ContextCond
	channels         map[ChannelID]*PChannelMeta
	version          typeutil.VersionInt64Pair
	metrics          *channelMetrics
	cchannelMeta     *streamingpb.CChannelMeta
	streamingVersion *streamingpb.StreamingVersion // used to identify the current streaming service version.
	// null if no streaming service has been run.
	// 1 if streaming service has been run once.
	streamingEnableNotifiers []*syncutil.AsyncTaskNotifier[struct{}]
	replicateConfig          *replicateutil.ConfigHelper
}

// RegisterStreamingEnabledNotifier registers a notifier into the balancer.
func (cm *ChannelManager) RegisterStreamingEnabledNotifier(notifier *syncutil.AsyncTaskNotifier[struct{}]) {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.streamingVersion != nil {
		// If the streaming service is already enabled once, notify the notifier and ignore it.
		notifier.Cancel()
		return
	}
	cm.streamingEnableNotifiers = append(cm.streamingEnableNotifiers, notifier)
}

// IsStreamingEnabledOnce returns true if streaming is enabled once.
func (cm *ChannelManager) IsStreamingEnabledOnce() bool {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	return cm.streamingVersion != nil
}

// WaitUntilStreamingEnabled waits until the streaming service is enabled.
func (cm *ChannelManager) WaitUntilStreamingEnabled(ctx context.Context) error {
	cm.cond.L.Lock()
	for cm.streamingVersion == nil {
		if err := cm.cond.Wait(ctx); err != nil {
			return err
		}
	}
	cm.cond.L.Unlock()
	return nil
}

// IsWALBasedDDLEnabled returns true if the WAL based DDL is enabled.
func (cm *ChannelManager) IsWALBasedDDLEnabled() bool {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	return cm.streamingVersion != nil && cm.streamingVersion.Version >= StreamingVersion265
}

// ReplicateRole returns the replicate role of the channel manager.
func (cm *ChannelManager) ReplicateRole() replicateutil.Role {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.replicateConfig == nil {
		return replicateutil.RolePrimary
	}
	return cm.replicateConfig.GetCurrentCluster().Role()
}

// TriggerWatchUpdate triggers the watch update.
// Because current watch must see new incoming streaming node right away,
// so a watch updating trigger will be called if there's new incoming streaming node.
func (cm *ChannelManager) TriggerWatchUpdate() {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	cm.version.Local++
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
}

// MarkStreamingHasEnabled marks the streaming service has been enabled.
func (cm *ChannelManager) MarkStreamingHasEnabled(ctx context.Context) error {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.streamingVersion != nil {
		return nil
	}

	cm.streamingVersion = &streamingpb.StreamingVersion{
		Version: StreamingVersion260,
	}

	if err := resource.Resource().StreamingCatalog().SaveVersion(ctx, cm.streamingVersion); err != nil {
		cm.Logger().Error("failed to save streaming version", zap.Error(err))
		return err
	}

	// notify all notifiers that the streaming service has been enabled.
	for _, notifier := range cm.streamingEnableNotifiers {
		notifier.Cancel()
	}
	// and block until the listener of notifiers are finished.
	for _, notifier := range cm.streamingEnableNotifiers {
		notifier.BlockUntilFinish()
	}
	cm.streamingEnableNotifiers = nil
	return nil
}

func (cm *ChannelManager) MarkWALBasedDDLEnabled(ctx context.Context) error {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.streamingVersion == nil {
		return errors.New("streaming service is not enabled, cannot mark WAL based DDL enabled")
	}
	if cm.streamingVersion.Version >= StreamingVersion265 {
		return nil
	}
	cm.streamingVersion.Version = StreamingVersion265
	if err := resource.Resource().StreamingCatalog().SaveVersion(ctx, cm.streamingVersion); err != nil {
		cm.Logger().Error("failed to save streaming version", zap.Error(err))
		return err
	}
	return nil
}

// CurrentPChannelsView returns the current view of pchannels.
func (cm *ChannelManager) CurrentPChannelsView() *PChannelView {
	cm.cond.L.Lock()
	view := newPChannelView(cm.channels)
	cm.cond.L.Unlock()

	for _, channel := range view.Channels {
		cm.metrics.UpdateVChannelTotal(channel)
	}
	return view
}

// AllocVirtualChannels allocates virtual channels for a collection.
func (cm *ChannelManager) AllocVirtualChannels(ctx context.Context, param AllocVChannelParam) ([]string, error) {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()
	if len(cm.channels) < param.Num {
		return nil, errors.Errorf("not enough pchannels to allocate, expected: %d, got: %d", param.Num, len(cm.channels))
	}

	vchannels := make([]string, 0, param.Num)
	for _, channel := range cm.sortChannelsByVChannelCount() {
		if len(vchannels) >= param.Num {
			break
		}
		vchannels = append(vchannels, funcutil.GetVirtualChannel(channel.id.Name, param.CollectionID, len(vchannels)))
	}
	return vchannels, nil
}

// withVChannelCount is a helper struct to sort the channels by the vchannel count.
type withVChannelCount struct {
	id            ChannelID
	vchannelCount int
}

// sortChannelsByVChannelCount sorts the channels by the vchannel count.
func (cm *ChannelManager) sortChannelsByVChannelCount() []withVChannelCount {
	vchannelCounts := make([]withVChannelCount, 0, len(cm.channels))
	for id := range cm.channels {
		vchannelCounts = append(vchannelCounts, withVChannelCount{
			id:            id,
			vchannelCount: StaticPChannelStatsManager.Get().GetPChannelStats(id).VChannelCount(),
		})
	}
	sort.Slice(vchannelCounts, func(i, j int) bool {
		if vchannelCounts[i].vchannelCount == vchannelCounts[j].vchannelCount {
			// make a stable sort result, so get the order of sort result with same vchannel count by name.
			return vchannelCounts[i].id.Name < vchannelCounts[j].id.Name
		}
		return vchannelCounts[i].vchannelCount < vchannelCounts[j].vchannelCount
	})
	return vchannelCounts
}

// AssignPChannels update the pchannels to servers and return the modified pchannels.
// When the balancer want to assign a pchannel into a new server.
// It should always call this function to update the pchannel assignment first.
// Otherwise, the pchannel assignment tracing is lost at meta.
func (cm *ChannelManager) AssignPChannels(ctx context.Context, pChannelToStreamingNode map[ChannelID]types.PChannelInfoAssigned) (map[ChannelID]*PChannelMeta, error) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannelToStreamingNode))
	for id, assign := range pChannelToStreamingNode {
		pchannel, ok := cm.channels[id]
		if !ok {
			return nil, ErrChannelNotExist
		}
		mutablePchannel := pchannel.CopyForWrite()
		if mutablePchannel.TryAssignToServerID(assign.Channel.AccessMode, assign.Node) {
			pChannelMetas = append(pChannelMetas, mutablePchannel.IntoRawMeta())
		}
	}

	err := cm.updatePChannelMeta(ctx, pChannelMetas)
	if err != nil {
		return nil, err
	}
	updates := make(map[ChannelID]*PChannelMeta, len(pChannelMetas))
	for _, pchannel := range pChannelMetas {
		meta := newPChannelMetaFromProto(pchannel)
		updates[meta.ChannelID()] = meta
		cm.metrics.AssignPChannelStatus(meta)
	}
	return updates, nil
}

// AssignPChannelsDone clear up the history data of the pchannels and transfer the state into assigned.
// When the balancer want to cleanup the history data of a pchannel.
// It should always remove the pchannel on the server first.
// Otherwise, the pchannel assignment tracing is lost at meta.
func (cm *ChannelManager) AssignPChannelsDone(ctx context.Context, pChannels []ChannelID) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannels))
	for _, channelID := range pChannels {
		pchannel, ok := cm.channels[channelID]
		if !ok {
			return ErrChannelNotExist
		}
		mutablePChannel := pchannel.CopyForWrite()
		mutablePChannel.AssignToServerDone()
		pChannelMetas = append(pChannelMetas, mutablePChannel.IntoRawMeta())
	}

	if err := cm.updatePChannelMeta(ctx, pChannelMetas); err != nil {
		return err
	}

	// Update metrics.
	for _, pchannel := range pChannelMetas {
		cm.metrics.AssignPChannelStatus(newPChannelMetaFromProto(pchannel))
	}
	return nil
}

// MarkAsUnavailable mark the pchannels as unavailable.
func (cm *ChannelManager) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannels))
	for _, channel := range pChannels {
		pchannel, ok := cm.channels[channel.ChannelID()]
		if !ok {
			return ErrChannelNotExist
		}
		mutablePChannel := pchannel.CopyForWrite()
		mutablePChannel.MarkAsUnavailable(channel.Term)
		pChannelMetas = append(pChannelMetas, mutablePChannel.IntoRawMeta())
	}

	if err := cm.updatePChannelMeta(ctx, pChannelMetas); err != nil {
		return err
	}
	for _, pchannel := range pChannelMetas {
		cm.metrics.AssignPChannelStatus(newPChannelMetaFromProto(pchannel))
	}
	return nil
}

// updatePChannelMeta updates the pchannel metas.
func (cm *ChannelManager) updatePChannelMeta(ctx context.Context, pChannelMetas []*streamingpb.PChannelMeta) error {
	if len(pChannelMetas) == 0 {
		return nil
	}

	if err := resource.Resource().StreamingCatalog().SavePChannels(ctx, pChannelMetas); err != nil {
		cm.Logger().Error("failed to save pchannels", zap.Error(err))
		return err
	}

	// update in-memory copy and increase the version.
	for _, pchannel := range pChannelMetas {
		c := newPChannelMetaFromProto(pchannel)
		cm.channels[c.ChannelID()] = c
	}
	cm.version.Local++
	// update metrics.
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
	return nil
}

// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
func (cm *ChannelManager) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	pChannelMeta, ok := cm.channels[types.ChannelID{Name: pchannel}]
	if !ok {
		return 0, false
	}
	if pChannelMeta.IsAssigned() {
		return pChannelMeta.CurrentServerID(), true
	}
	return 0, false
}

// GetLatestChannelAssignment returns the latest channel assignment.
func (cm *ChannelManager) GetLatestChannelAssignment() (*WatchChannelAssignmentsCallbackParam, error) {
	var result WatchChannelAssignmentsCallbackParam
	if _, err := cm.applyAssignments(func(param WatchChannelAssignmentsCallbackParam) error {
		result = param
		return nil
	}); err != nil {
		return nil, err
	}
	return &result, nil
}

func (cm *ChannelManager) WatchAssignmentResult(ctx context.Context, cb WatchChannelAssignmentsCallback) error {
	// push the first balance result to watcher callback function if balance result is ready.
	version, err := cm.applyAssignments(cb)
	if err != nil {
		return err
	}
	for {
		// wait for version change, and apply the latest assignment to callback.
		if err := cm.waitChanges(ctx, version); err != nil {
			return err
		}
		if version, err = cm.applyAssignments(cb); err != nil {
			return err
		}
	}
}

// UpdateReplicateConfiguration updates the in-memory replicate configuration.
func (cm *ChannelManager) UpdateReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
	msg := result.Message
	config := replicateutil.MustNewConfigHelper(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), msg.Header().ReplicateConfiguration)
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.replicateConfig != nil && proto.Equal(config.GetReplicateConfiguration(), cm.replicateConfig.GetReplicateConfiguration()) {
		// check if the replicate configuration is changed.
		// if not changed, return it directly.
		return nil
	}

	appendResults := lo.MapKeys(result.Results, func(_ *message.AppendResult, key string) string {
		return funcutil.ToPhysicalChannel(key)
	})
	newIncomingCDCTasks := cm.getNewIncomingTask(config, appendResults)
	if err := resource.Resource().StreamingCatalog().SaveReplicateConfiguration(ctx,
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: config.GetReplicateConfiguration()},
		newIncomingCDCTasks); err != nil {
		cm.Logger().Error("failed to save replicate configuration", zap.Error(err))
		return err
	}

	cm.replicateConfig = config
	cm.cond.UnsafeBroadcast()
	cm.version.Local++
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
	return nil
}

// getNewIncomingTask gets the new incoming task from replicatingTasks.
func (cm *ChannelManager) getNewIncomingTask(newConfig *replicateutil.ConfigHelper, appendResults map[string]*message.AppendResult) []*streamingpb.ReplicatePChannelMeta {
	incoming := newConfig.GetCurrentCluster()
	var current *replicateutil.MilvusCluster
	if cm.replicateConfig != nil {
		current = cm.replicateConfig.GetCurrentCluster()
	}
	incomingReplicatingTasks := make([]*streamingpb.ReplicatePChannelMeta, 0, len(incoming.TargetClusters()))
	for _, targetCluster := range incoming.TargetClusters() {
		if current != nil && current.TargetCluster(targetCluster.GetClusterId()) != nil {
			// target already exists, skip it.
			continue
		}
		// TODO: support add new pchannels into existing clusters.
		for _, pchannel := range targetCluster.GetPchannels() {
			sourceClusterID := targetCluster.SourceCluster().ClusterId
			sourcePChannel := targetCluster.MustGetSourceChannel(pchannel)
			incomingReplicatingTasks = append(incomingReplicatingTasks, &streamingpb.ReplicatePChannelMeta{
				SourceChannelName: sourcePChannel,
				TargetChannelName: pchannel,
				TargetCluster:     targetCluster.MilvusCluster,
				// The checkpoint is set as the initialized checkpoint for one cdc-task,
				// when the startup of one cdc-task, the checkpoint returned from the target cluster is nil,
				// so we set the initialized checkpoint here to start operation from here.
				// the InitializedCheckpoint is always keep same semantic with the checkpoint at target cluster.
				// so the cluster id is the source cluster id (aka. current cluster id)
				InitializedCheckpoint: &commonpb.ReplicateCheckpoint{
					ClusterId: sourceClusterID,
					Pchannel:  sourcePChannel,
					MessageId: appendResults[sourcePChannel].LastConfirmedMessageID.IntoProto(),
					TimeTick:  appendResults[sourcePChannel].TimeTick,
				},
			})
		}
	}
	return incomingReplicatingTasks
}

// applyAssignments applies the assignments.
func (cm *ChannelManager) applyAssignments(cb WatchChannelAssignmentsCallback) (typeutil.VersionInt64Pair, error) {
	cm.cond.L.Lock()
	assignments := make([]types.PChannelInfoAssigned, 0, len(cm.channels))
	for _, c := range cm.channels {
		if c.IsAssigned() {
			assignments = append(assignments, c.CurrentAssignment())
		}
	}
	version := cm.version
	cchannelAssignment := proto.Clone(cm.cchannelMeta).(*streamingpb.CChannelMeta)
	pchannelViews := newPChannelView(cm.channels)
	cm.cond.L.Unlock()

	var replicateConfig *commonpb.ReplicateConfiguration
	if cm.replicateConfig != nil {
		replicateConfig = cm.replicateConfig.GetReplicateConfiguration()
	}
	return version, cb(WatchChannelAssignmentsCallbackParam{
		Version: version,
		CChannelAssignment: &streamingpb.CChannelAssignment{
			Meta: cchannelAssignment,
		},
		PChannelView:           pchannelViews,
		Relations:              assignments,
		ReplicateConfiguration: replicateConfig,
	})
}

// waitChanges waits for the layout to be updated.
func (cm *ChannelManager) waitChanges(ctx context.Context, version typeutil.Version) error {
	cm.cond.L.Lock()
	for version.EQ(cm.version) {
		if err := cm.cond.Wait(ctx); err != nil {
			return err
		}
	}
	cm.cond.L.Unlock()
	return nil
}
