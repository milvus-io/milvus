package flusherimpl

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

// getVchannels gets the vchannels of current pchannel.
func (impl *WALFlusherImpl) getVchannels(ctx context.Context, pchannel string) ([]string, error) {
	var vchannels []string
	rc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "when wait for rootcoord client ready")
	}
	retryCnt := -1
	if err := retry.Do(ctx, func() error {
		retryCnt++

		resp, err := rc.GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
			Pchannel: pchannel,
		})
		if err = merr.CheckRPCCall(resp, err); err != nil {
			log.Warn("get pchannel info failed", zap.Error(err), zap.Int("retryCnt", retryCnt))
			return err
		}
		for _, collection := range resp.GetCollections() {
			vchannels = append(vchannels, collection.Vchannel)
		}
		return nil
	}, retry.AttemptAlways()); err != nil {
		return nil, errors.Wrapf(err, "when get existed vchannels of pchannel")
	}
	return vchannels, nil
}

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
		msgID := adaptor.MustGetMessageIDFromMQWrapperIDBytes(impl.wal.Get().WALName(), info.GetInfo().GetSeekPosition().GetMsgID())
		if checkpoint == nil || msgID.LT(checkpoint) {
			checkpoint = msgID
		}
	}
	return recoveryInfos, checkpoint, nil
}

// getRecoveryInfo gets the recovery info of the vchannel.
func (impl *WALFlusherImpl) getRecoveryInfo(ctx context.Context, vchannel string) (*datapb.GetChannelRecoveryInfoResponse, error) {
	var resp *datapb.GetChannelRecoveryInfoResponse
	retryCnt := -1
	err := retry.Do(ctx, func() error {
		retryCnt++
		dc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
		if err != nil {
			return err
		}
		resp, err = dc.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
		err = merr.CheckRPCCall(resp, err)
		if errors.Is(err, merr.ErrChannelNotAvailable) {
			impl.logger.Warn("channel not available because of collection dropped", zap.String("vchannel", vchannel), zap.Int("retryCnt", retryCnt))
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		if err != nil {
			impl.logger.Warn("get channel recovery info failed", zap.Error(err), zap.String("vchannel", vchannel), zap.Int("retryCnt", retryCnt))
			return err
		}
		// The channel has been dropped, skip to recover it.
		if isDroppedChannel(resp) {
			impl.logger.Info("channel has been dropped, the vchannel can not be recovered", zap.String("vchannel", vchannel))
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		return nil
	}, retry.AttemptAlways())
	return resp, err
}

func isDroppedChannel(resp *datapb.GetChannelRecoveryInfoResponse) bool {
	return len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64
}
