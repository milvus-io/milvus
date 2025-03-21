package channel

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrChannelNotExist = errors.New("channel not exist")

// RecoverChannelManager creates a new channel manager.
func RecoverChannelManager(ctx context.Context, incomingChannel ...string) (*ChannelManager, error) {
	view, metrics, err := recoverFromConfigurationAndMeta(ctx, incomingChannel...)
	if err != nil {
		return nil, err
	}
	globalVersion := paramtable.GetNodeID()
	return &ChannelManager{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		view: view,
		version: typeutil.VersionInt64Pair{
			Global: globalVersion, // global version should be keep increasing globally, it's ok to use node id.
			Local:  0,
		},
		metrics: metrics,
	}, nil
}

// recoverFromConfigurationAndMeta recovers the channel manager from configuration and meta.
func recoverFromConfigurationAndMeta(ctx context.Context, incomingChannel ...string) (*PChannelView, *channelMetrics, error) {
	// Recover metrics.
	metrics := newPChannelMetrics()

	// Get all channels from meta.
	channelMetas, err := resource.Resource().StreamingCatalog().ListPChannel(ctx)
	if err != nil {
		return nil, metrics, err
	}

	// TODO: only support rw channel here now, add ro channel in future.
	channels := make(map[string]*PChannelMeta, len(channelMetas))
	for _, channel := range channelMetas {
		metrics.AssignPChannelStatus(channel)
		channels[channel.GetChannel().GetName()] = newPChannelMetaFromProto(channel)
	}

	// Get new incoming meta from configuration.
	for _, newChannel := range incomingChannel {
		if _, ok := channels[newChannel]; !ok {
			channels[newChannel] = newPChannelMeta(newChannel)
		}
	}
	chs := lo.Values(channels)
	return newPChannelView(chs), metrics, nil
}

// ChannelManager manages the channels.
// ChannelManager is the `wal` of channel assignment and unassignment.
// Every operation applied to the streaming node should be recorded in ChannelManager first.
type ChannelManager struct {
	cond    *syncutil.ContextCond
	view    *PChannelView
	version typeutil.VersionInt64Pair
	metrics *channelMetrics
}

// CurrentPChannelsView returns the current view of pchannels.
func (cm *ChannelManager) CurrentPChannelsView() *PChannelView {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	return cm.view.Clone()
}

// AssignPChannels update the pchannels to servers and return the modified pchannels.
// When the balancer want to assign a pchannel into a new server.
// It should always call this function to update the pchannel assignment first.
// Otherwise, the pchannel assignment tracing is lost at meta.
func (cm *ChannelManager) AssignPChannels(ctx context.Context, pChannelToStreamingNode map[ChannelID]types.StreamingNodeInfo) (map[ChannelID]*PChannelMeta, error) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannelToStreamingNode))
	for id, streamingNode := range pChannelToStreamingNode {
		pchannel, ok := cm.view.GetChannel(id)
		if !ok {
			return nil, ErrChannelNotExist
		}
		mutablePchannel := pchannel.CopyForWrite()
		if mutablePchannel.TryAssignToServerID(streamingNode) {
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
		cm.metrics.AssignPChannelStatus(pchannel)
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
		pchannel, ok := cm.view.GetChannel(channelID)
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
		cm.metrics.AssignPChannelStatus(pchannel)
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
		pchannel, ok := cm.view.GetChannel(channel.ChannelID())
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
		cm.metrics.AssignPChannelStatus(pchannel)
	}
	return nil
}

// updatePChannelMeta updates the pchannel metas.
func (cm *ChannelManager) updatePChannelMeta(ctx context.Context, pChannelMetas []*streamingpb.PChannelMeta) error {
	if len(pChannelMetas) == 0 {
		return nil
	}
	if err := resource.Resource().StreamingCatalog().SavePChannels(ctx, pChannelMetas); err != nil {
		return errors.Wrap(err, "update meta at catalog")
	}

	// update in-memory copy and increase the version.
	for _, pchannel := range pChannelMetas {
		cm.view.Update(newPChannelMetaFromProto(pchannel))
	}
	cm.version.Local++
	// update metrics.
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
	return nil
}

func (cm *ChannelManager) WatchAssignmentResult(ctx context.Context, cb func(version typeutil.VersionInt64Pair, assignments []types.PChannelInfoAssigned) error) error {
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

// applyAssignments applies the assignments.
func (cm *ChannelManager) applyAssignments(cb func(version typeutil.VersionInt64Pair, assignments []types.PChannelInfoAssigned) error) (typeutil.VersionInt64Pair, error) {
	cm.cond.L.Lock()
	assignments := cm.view.GetAssignment()
	version := cm.version
	cm.cond.L.Unlock()
	return version, cb(version, assignments)
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
