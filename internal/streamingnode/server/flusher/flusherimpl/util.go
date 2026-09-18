package flusherimpl

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

var defaultCollectionNotFoundTolerance = 10

// getRecoveryInfos gets the recovery info of the vchannels from datacoord.
// metas are the vchannels' metas in the recovery snapshot (see getRecoveryInfo).
func (impl *WALFlusherImpl) getRecoveryInfos(ctx context.Context, vchannel []string, metas map[string]*streamingpb.VChannelMeta) (map[string]*datapb.GetChannelRecoveryInfoResponse, message.MessageID, error) {
	futures := make([]*conc.Future[interface{}], 0, len(vchannel))
	for _, v := range vchannel {
		v := v
		future := GetExecPool().Submit(func() (interface{}, error) {
			return impl.getRecoveryInfo(ctx, v, metas[v])
		})
		futures = append(futures, future)
	}
	walName := impl.wal.Get().WALName()
	recoveryInfos := make(map[string]*datapb.GetChannelRecoveryInfoResponse, len(futures))
	for i, future := range futures {
		resp, err := future.Await()
		if err == nil {
			// Clamped before the scan start below is taken as the minimum, so
			// the pchannel scan starts from the corrected position too.
			recoveryInfos[vchannel[i]] = impl.recoverNoEarlierThanSplitGenesis(ctx, vchannel[i], metas[vchannel[i]], walName,
				resp.(*datapb.GetChannelRecoveryInfoResponse))
			continue
		}
		if errors.Is(err, errChannelLifetimeUnrecoverable) {
			impl.logger.Warn(ctx, "channel has been dropped, skip to recover flusher for vchannel", mlog.FieldVChannel(vchannel[i]))
			continue
		}
		return nil, nil, errors.Wrapf(err, "when get recovery info of vchannel %s", vchannel[i])
	}

	var checkpoint message.MessageID
	for _, info := range recoveryInfos {
		messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(walName, info.GetInfo().GetSeekPosition().GetMsgID())
		if checkpoint == nil || messageID.LT(checkpoint) {
			checkpoint = messageID
		}
	}
	return recoveryInfos, checkpoint, nil
}

// getRecoveryInfo gets the recovery info of the vchannel.
//
// meta is the vchannel's meta in the recovery snapshot. A vchannel datacoord
// has no position for (ErrChannelNotAvailable) is skipped, unless meta records
// the genesis of a shard split target: that target is recovered from its
// genesis instead (see splitTargetGenesisRecoveryInfo).
func (impl *WALFlusherImpl) getRecoveryInfo(ctx context.Context, vchannel string, meta *streamingpb.VChannelMeta) (*datapb.GetChannelRecoveryInfoResponse, error) {
	var resp *datapb.GetChannelRecoveryInfoResponse
	retryCnt := -1
	logger := impl.logger.With(mlog.FieldVChannel(vchannel))
	err := retry.Do(ctx, func() error {
		retryCnt++
		logger := logger.With(mlog.Int("retryCnt", retryCnt))
		dc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
		if err != nil {
			return err
		}
		resp, err = dc.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
		err = merr.CheckRPCCall(resp, err)
		if errors.Is(err, merr.ErrChannelNotAvailable) {
			if genesis := splitTargetGenesisRecoveryInfo(vchannel, meta); genesis != nil {
				logger.Warn(ctx, "datacoord has no position for the shard split target yet, recover it from its genesis position",
					mlog.Uint64("genesisTimeTick", genesis.GetInfo().GetSeekPosition().GetTimestamp()),
					mlog.Err(err))
				resp = genesis
				return nil
			}
			logger.Warn(ctx, "channel not available because of collection dropped", mlog.Err(err))
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		if errors.Is(err, merr.ErrCollectionNotFound) {
			if retryCnt >= defaultCollectionNotFoundTolerance {
				// TODO: It's not strong guarantee to make no resource lost or leak. Should be removed after wal-driven-ddl framework is ready.
				logger.Warn(ctx, "too many collection not found, the create collection may undone by coord", mlog.Err(err))
				return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
			}
			logger.Warn(ctx, "collection not found, maybe the create collection is not done or create collection undone by coord", mlog.Err(err))
			return err
		}
		if err != nil {
			logger.Warn(ctx, "get channel recovery info failed", mlog.Err(err))
			return err
		}
		// The channel has been dropped, skip to recover it.
		if isDroppedChannel(resp) {
			logger.Info(ctx, "channel has been dropped, the vchannel can not be recovered")
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		return nil
	}, retry.AttemptAlways(), retry.RetryErr(func(error) bool { return true }))
	return resp, err
}

// splitTargetGenesisRecoveryInfo rebuilds, from the genesis position the
// recovery storage recorded (VChannelMeta.split_genesis_checkpoint), the
// recovery info a shard split target's data sync service is spawned with when
// its genesis replica is consumed (flusherComponents.spawnGenesisDataSyncService).
// It returns nil for a vchannel that is not a live split target.
//
// DataCoord learns a target's position only when the SplitShard ack callback
// seeds its genesis checkpoint (CommitShardSplit), or once the target's own
// data sync service reports a checkpoint or a synced segment. A StreamingNode
// restarting before either gets ErrChannelNotAvailable for a target that is live
// in its own recovery meta. Skipping it, as a dropped vchannel is skipped, would
// leave the target without a data sync service until a restart after the
// callback: its writes never flushed and, with no flusher checkpoint for it,
// truncation of the whole pchannel stopped.
//
// Recovering from the genesis is what the no-restart path already does: that
// service allocates segments and reports checkpoints to DataCoord before the
// callback runs, and the callback's seeding leaves a target that already has a
// checkpoint alone. Nothing the target holds precedes its genesis, and anything
// it synced would have given DataCoord a position, so the genesis is never too
// late a start. Waiting for DataCoord instead would hold the recovery of every
// vchannel on the pchannel behind one collection's callback, which may itself
// be retrying.
func splitTargetGenesisRecoveryInfo(vchannel string, meta *streamingpb.VChannelMeta) *datapb.GetChannelRecoveryInfoResponse {
	position := splitTargetGenesisPosition(vchannel, meta)
	if position == nil {
		return nil
	}
	return &datapb.GetChannelRecoveryInfoResponse{
		Status: merr.Success(),
		Info: &datapb.VchannelInfo{
			CollectionID: meta.GetCollectionInfo().GetCollectionId(),
			ChannelName:  vchannel,
			SeekPosition: position,
		},
	}
}

// splitTargetGenesisPosition is the genesis seek position of a live shard split
// target recorded in meta, nil for any other vchannel.
func splitTargetGenesisPosition(vchannel string, meta *streamingpb.VChannelMeta) *msgpb.MsgPosition {
	genesis := meta.GetSplitGenesisCheckpoint()
	if genesis == nil || meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return nil
	}
	return genesisSeekPosition(vchannel, message.MustUnmarshalMessageID(genesis.GetMessageId()), genesis.GetTimeTick())
}

// recoverNoEarlierThanSplitGenesis moves the seek position datacoord answered
// for a live shard split target up to the target's genesis when datacoord's is
// earlier; every other field of the answer, and the answer for any vchannel
// without a recorded genesis, is returned unchanged.
//
// Before the SplitShard ack callback seeds the target's checkpoint, datacoord
// has no position of its own for it and falls back to the collection's
// creation start position whenever the target's pchannel is in the
// collection's start-position table -- the fenced source's own pchannel is the
// common case. Seeking there replays the pchannel from the collection's
// creation, a position the WAL may already have truncated. Nothing of a split
// target precedes its genesis, so the later of the two positions loses nothing;
// and every position datacoord records for the target itself (the seeded
// genesis checkpoint, a checkpoint the target reported, a synced segment's
// position) is at or after the genesis, so it is kept.
func (impl *WALFlusherImpl) recoverNoEarlierThanSplitGenesis(
	ctx context.Context,
	vchannel string,
	meta *streamingpb.VChannelMeta,
	walName message.WALName,
	resp *datapb.GetChannelRecoveryInfoResponse,
) *datapb.GetChannelRecoveryInfoResponse {
	genesisPosition := splitTargetGenesisPosition(vchannel, meta)
	if genesisPosition == nil {
		return resp
	}
	seek := resp.GetInfo().GetSeekPosition()
	if !positionBeforeSplitGenesis(walName, seek, meta.GetSplitGenesisCheckpoint()) {
		return resp
	}
	impl.logger.Warn(ctx, "datacoord's seek position of the shard split target precedes its genesis, recover it from the genesis position",
		mlog.FieldVChannel(vchannel),
		mlog.Uint64("datacoordTimeTick", seek.GetTimestamp()),
		mlog.Uint64("genesisTimeTick", genesisPosition.GetTimestamp()))
	clamped := proto.Clone(resp).(*datapb.GetChannelRecoveryInfoResponse)
	if clamped.Info == nil {
		clamped.Info = &datapb.VchannelInfo{
			CollectionID: meta.GetCollectionInfo().GetCollectionId(),
			ChannelName:  vchannel,
		}
	}
	clamped.Info.SeekPosition = genesisPosition
	return clamped
}

// positionBeforeSplitGenesis reports whether seek is earlier than the genesis:
// by time tick, and by message id when seek carries one of the flusher's WAL
// and the genesis was recorded on that same WAL (message ids of different WALs
// are not comparable).
func positionBeforeSplitGenesis(walName message.WALName, seek *msgpb.MsgPosition, genesis *streamingpb.WALCheckpoint) bool {
	if seek.GetTimestamp() < genesis.GetTimeTick() {
		return true
	}
	if len(seek.GetMsgID()) == 0 {
		return false
	}
	genesisID := message.MustUnmarshalMessageID(genesis.GetMessageId())
	if genesisID.WALName() != walName {
		return false
	}
	return adaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(walName, seek.GetMsgID()).LT(genesisID)
}

func isDroppedChannel(resp *datapb.GetChannelRecoveryInfoResponse) bool {
	return len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64
}
