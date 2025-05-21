package flusherimpl

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

var defaultCollectionNotFoundTolerance = 10

// getRecoveryInfos gets the recovery info of the vchannels from datacoord
func (impl *WALFlusherImpl) getRecoveryInfos(ctx context.Context, vchannel []string) (map[string]*datapb.GetChannelRecoveryInfoResponse, message.MessageID, error) {
	futures := make([]*conc.Future[interface{}], 0, len(vchannel))
	for _, v := range vchannel {
		v := v
		future := GetExecPool().Submit(func() (interface{}, error) {
			return impl.getRecoveryInfo(ctx, v)
		})
		futures = append(futures, future)
	}
	recoveryInfos := make(map[string]*datapb.GetChannelRecoveryInfoResponse, len(futures))
	for i, future := range futures {
		resp, err := future.Await()
		if err == nil {
			recoveryInfos[vchannel[i]] = resp.(*datapb.GetChannelRecoveryInfoResponse)
			continue
		}
		if errors.Is(err, errChannelLifetimeUnrecoverable) {
			impl.logger.Warn("channel has been dropped, skip to recover flusher for vchannel", zap.String("vchannel", vchannel[i]))
			continue
		}
		return nil, nil, errors.Wrapf(err, "when get recovery info of vchannel %s", vchannel[i])
	}

	var checkpoint message.MessageID
	for _, info := range recoveryInfos {
		messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytes(impl.wal.Get().WALName(), info.GetInfo().GetSeekPosition().GetMsgID())
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
	logger := impl.logger.With(zap.String("vchannel", vchannel))
	err := retry.Do(ctx, func() error {
		retryCnt++
		logger := logger.With(zap.Int("retryCnt", retryCnt))
		dc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
		if err != nil {
			return err
		}
		resp, err = dc.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
		err = merr.CheckRPCCall(resp, err)
		if errors.Is(err, merr.ErrChannelNotAvailable) {
			logger.Warn("channel not available because of collection dropped", zap.Error(err))
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		if errors.Is(err, merr.ErrCollectionNotFound) {
			if retryCnt >= defaultCollectionNotFoundTolerance {
				// TODO: It's not strong guarantee to make no resource lost or leak. Should be removed after wal-driven-ddl framework is ready.
				logger.Warn("too many collection not found, the create collection may undone by coord", zap.Error(err))
				return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
			}
			logger.Warn("collection not found, maybe the create collection is not done or create collection undone by coord", zap.Error(err))
			return err
		}
		if err != nil {
			logger.Warn("get channel recovery info failed", zap.Error(err))
			return err
		}
		// The channel has been dropped, skip to recover it.
		if isDroppedChannel(resp) {
			logger.Info("channel has been dropped, the vchannel can not be recovered")
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		return nil
	}, retry.AttemptAlways())
	return resp, err
}

func isDroppedChannel(resp *datapb.GetChannelRecoveryInfoResponse) bool {
	return len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64
}
