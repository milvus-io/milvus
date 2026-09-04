package flusherimpl

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

var defaultCollectionNotFoundTolerance = 10

// getRecoveryInfos gets the recovery info of the vchannels from datacoord
func (impl *WALFlusherImpl) getRecoveryInfos(ctx context.Context, vchannel []string) (map[string]*datapb.GetChannelRecoveryInfoResponse, message.MessageID, error) {
	// The vchannel travels WITH its future rather than being recovered by index
	// from the input slice: the two are the same length by construction, but
	// nothing in the loop below says so, and an index into a slice the loop does
	// not range over is both a static-analysis finding and one refactor away
	// from being wrong.
	type pending struct {
		vchannel string
		future   *conc.Future[interface{}]
	}
	futures := make([]pending, 0, len(vchannel))
	for _, v := range vchannel {
		v := v
		futures = append(futures, pending{
			vchannel: v,
			future: GetExecPool().Submit(func() (interface{}, error) {
				return impl.getRecoveryInfo(ctx, v)
			}),
		})
	}
	recoveryInfos := make(map[string]*datapb.GetChannelRecoveryInfoResponse, len(futures))
	for _, p := range futures {
		resp, err := p.future.Await()
		if err == nil {
			recoveryInfos[p.vchannel] = resp.(*datapb.GetChannelRecoveryInfoResponse)
			continue
		}
		if errors.Is(err, errChannelLifetimeUnrecoverable) {
			// Two different situations share this answer, and only one of them
			// is benign. A vchannel datacoord has genuinely dropped is expected.
			// A vchannel datacoord does not know YET is not: a split target
			// whose genesis landed and whose recovery snapshot persisted, but
			// whose start position the coordinator has not recorded, gets the
			// same ErrChannelNotAvailable -- and skipping it leaves a vchannel
			// the shard manager accepts writes for and the flusher silently
			// drops, until the next restart after the coordinator catches up.
			// Nothing here can tell the two apart, so it is logged where it can
			// be noticed rather than at the level of an expected event.
			impl.logger.Error(ctx, "datacoord has no recovery info for a vchannel in the recovery snapshot; "+
				"if this vchannel is not dropped its writes will not be flushed until the next restart",
				mlog.FieldVChannel(p.vchannel))
			continue
		}
		return nil, nil, errors.Wrapf(err, "when get recovery info of vchannel %s", p.vchannel)
	}

	var checkpoint message.MessageID
	walName := impl.wal.Get().WALName()
	for v, info := range recoveryInfos {
		messageID := recoveryMessageID(walName, info.GetInfo().GetSeekPosition())
		if checkpoint == nil || messageID.LT(checkpoint) {
			if len(info.GetInfo().GetSeekPosition().GetMsgID()) == 0 {
				// The whole pchannel replays from the earliest position the WAL
				// retains on this vchannel's account: correct, but the cost is
				// the retained history, so it is worth a Warn rather than a
				// line among the ordinary candidates.
				impl.logger.Warn(ctx, "flusher recovery checkpoint candidate has no message id, replaying from the earliest position",
					mlog.FieldVChannel(v), mlog.Stringer("messageID", messageID))
			} else {
				impl.logger.Info(ctx, "flusher recovery checkpoint candidate",
					mlog.FieldVChannel(v), mlog.Stringer("messageID", messageID))
			}
			checkpoint = messageID
		}
	}
	return recoveryInfos, checkpoint, nil
}

// getRecoveryInfo gets the recovery info of the vchannel.
func (impl *WALFlusherImpl) getRecoveryInfo(ctx context.Context, vchannel string) (*datapb.GetChannelRecoveryInfoResponse, error) {
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

// recoveryMessageID turns the seek position datacoord answered with into the
// message id the scanner starts from.
//
// An EMPTY message id is a position datacoord built without one, not a
// corrupt one. Every compaction output and every import segment records only
// a channel name and a timestamp, and when a vchannel has no channel checkpoint
// yet, GetChannelSeekPosition falls back to its earliest segment's DML position
// -- which is such a record whenever that segment came out of a compaction or an
// import. The dropped-channel marker is a different shape (timestamp MaxUint64)
// and is filtered before this point.
//
// The only safe reading of "no known position" is the EARLIEST the WAL retains:
// seeking earlier than necessary costs replay time, which recovery is built to
// absorb, while seeking later than necessary loses data. WoodPecker already did
// this by accident of its serialization -- its deserializer reads empty bytes
// as {0, 0} -- and every other backend PANICKED in the deserializer instead,
// which took the whole pchannel's flusher down with it. Make the reading
// explicit and the same on every backend.
//
// The WAL name is the FLUSHER's, not the position's: datacoord does not fill
// the position's WALName in, and a guard keyed on it rejected WoodPecker for a
// field that was never going to be set.
func recoveryMessageID(walName message.WALName, position *msgpb.MsgPosition) message.MessageID {
	if walName == message.WALNameUnknown {
		walName = message.MustGetDefaultWALName()
	}
	if len(position.GetMsgID()) == 0 {
		earliest, _ := adaptor.MustGetEarliestMessageIDFromMQType(commonpb.WALName(walName))
		return adaptor.MustGetMessageIDFromMQWrapperID(earliest)
	}
	return adaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(walName, position.GetMsgID())
}

func isDroppedChannel(resp *datapb.GetChannelRecoveryInfoResponse) bool {
	return len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64
}
