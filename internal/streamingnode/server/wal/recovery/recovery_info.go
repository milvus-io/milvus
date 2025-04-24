package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type recoverInfo struct {
	VChannels  []*streamingpb.VChannelMeta
	Segments   []*streamingpb.SegmentAssignmentMeta
	Checkpoint *WALCheckpoint
}

// recoverRecoveryInfoFromMeta retrieves the recovery info for the given channel.
func recoverRecoveryInfoFromMeta(
	ctx context.Context,
	walName string,
	channelInfo types.PChannelInfo,
	lastTimeTickMessage message.ImmutableMessage,
) (*recoverInfo, error) {
	catalog := resource.Resource().StreamingNodeCatalog()
	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, channelInfo.Name)
	if err != nil {
		return nil, err
	}
	if checkpoint == nil {
		// There's no checkpoint for current pchannel, so we need to initialize the recover info.
		if checkpoint, err = initializeRecoverInfo(ctx, channelInfo, lastTimeTickMessage); err != nil {
			return nil, err
		}
	}
	vchannels, err := catalog.ListVChannel(ctx, channelInfo.Name)
	if err != nil {
		return nil, err
	}
	segmentAssign, err := catalog.ListSegmentAssignment(ctx, channelInfo.Name)
	if err != nil {
		return nil, err
	}
	return &recoverInfo{
		VChannels:  vchannels,
		Segments:   segmentAssign,
		Checkpoint: newWALCheckpointFromProto(walName, checkpoint),
	}, nil
}

// initializeRecoverInfo initializes the recover info for the given channel.
// before first streaming service is enabled, there's no recovery info for channel.
// we should initialize the recover info for the channel.
// !!! This function will only call once for each channel when the streaming service is enabled.
func initializeRecoverInfo(ctx context.Context, channelInfo types.PChannelInfo, untilMessage message.ImmutableMessage) (*streamingpb.WALCheckpoint, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "when wait for rootcoord client ready")
	}
	resp, err := coord.GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
		Pchannel: channelInfo.Name,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	// save the vchannel recovery info into the catalog
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(resp.GetCollections()))
	for _, collection := range resp.GetCollections() {
		partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(collection.Partitions))
		for _, partition := range collection.Partitions {
			partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{PartitionId: partition.PartitionId})
		}
		vchannels[collection.Vchannel] = &streamingpb.VChannelMeta{
			Vchannel: collection.Vchannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: collection.CollectionId,
				Partitions:   partitions,
			},
		}
	}

	// SaveVChannels saves the vchannels into the catalog.
	if err := resource.Resource().StreamingNodeCatalog().SaveVChannels(ctx, channelInfo.Name, vchannels); err != nil {
		return nil, err
	}

	// Use the first timesync message as the initial checkpoint.
	checkpoint := &streamingpb.WALCheckpoint{
		WriteAheadCheckpoint: &messagespb.MessageID{
			Id: untilMessage.LastConfirmedMessageID().Marshal(),
		},
		WriteAheadCheckpointTimeTick: untilMessage.TimeTick(),
		RecoveryMagic:                recoveryMagicStreamingInitialized,
	}
	if err := resource.Resource().StreamingNodeCatalog().SaveConsumeCheckpoint(ctx, channelInfo.Name, checkpoint); err != nil {
		return nil, err
	}
	return checkpoint, nil
}
