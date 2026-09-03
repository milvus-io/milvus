package flusherimpl

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
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
		// MustGet... panics on an empty msg id, and an empty one is legitimate
		// for exactly one backend, so test the condition the dispatcher tests
		// rather than trusting datacoord to have filled it in.
		if !msgdispatcher.SeekablePosition(info.GetInfo().GetSeekPosition()) {
			// System, not transient: datacoord answered with a position whose
			// msg id is empty on a backend where that is not legal. Retrying
			// asks the same question and gets the same answer.
			return nil, nil, merr.WrapErrServiceInternalMsg(
				"vchannel %s has an unseekable recovery position", v)
		}
		messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(walName, info.GetInfo().GetSeekPosition().GetMsgID())
		if checkpoint == nil || messageID.LT(checkpoint) {
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

func isDroppedChannel(resp *datapb.GetChannelRecoveryInfoResponse) bool {
	return len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64
}
