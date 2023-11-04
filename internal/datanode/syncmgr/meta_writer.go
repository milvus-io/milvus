package syncmgr

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// MetaWriter is the interface for SyncManager to write segment sync meta.
type MetaWriter interface {
	UpdateSync(*SyncTask) error
}

type brokerMetaWriter struct {
	broker broker.Broker
	opts   []retry.Option
}

func BrokerMetaWriter(broker broker.Broker, opts ...retry.Option) MetaWriter {
	return &brokerMetaWriter{
		broker: broker,
		opts:   opts,
	}
}

func (b *brokerMetaWriter) UpdateSync(pack *SyncTask) error {
	var (
		fieldInsert = []*datapb.FieldBinlog{}
		fieldStats  = []*datapb.FieldBinlog{}
		deltaInfos  = make([]*datapb.FieldBinlog, 1)
		checkPoints = []*datapb.CheckPoint{}
	)

	for k, v := range pack.insertBinlogs {
		fieldInsert = append(fieldInsert, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
	}
	for k, v := range pack.statsBinlogs {
		fieldStats = append(fieldStats, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
	}
	deltaInfos[0] = &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{pack.deltaBinlog}}

	// only current segment checkpoint info,
	segments := pack.metacache.GetSegmentsBy(metacache.WithSegmentIDs(pack.segmentID))
	if len(segments) == 0 {
		return merr.WrapErrSegmentNotFound(pack.segmentID)
	}
	segment := segments[0]
	checkPoints = append(checkPoints, &datapb.CheckPoint{
		SegmentID: pack.segmentID,
		NumOfRows: segment.NumOfRows(), //+ pack.option.Row,
		Position:  pack.checkpoint,
	})

	startPos := lo.Map(pack.metacache.GetSegmentsBy(metacache.WithStartPosNotRecorded()), func(info *metacache.SegmentInfo, _ int) *datapb.SegmentStartPosition {
		return &datapb.SegmentStartPosition{
			SegmentID:     info.SegmentID(),
			StartPosition: info.StartPosition(),
		}
	})

	log.Info("SaveBinlogPath",
		zap.Int64("SegmentID", pack.segmentID),
		zap.Int64("CollectionID", pack.collectionID),
		zap.Any("startPos", startPos),
		zap.Any("checkPoints", checkPoints),
		zap.Int("Length of Field2BinlogPaths", len(fieldInsert)),
		zap.Int("Length of Field2Stats", len(fieldStats)),
		zap.Int("Length of Field2Deltalogs", len(deltaInfos[0].GetBinlogs())),
		zap.String("vChannelName", pack.channelName),
	)

	req := &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SegmentID:           pack.segmentID,
		CollectionID:        pack.collectionID,
		Field2BinlogPaths:   fieldInsert,
		Field2StatslogPaths: fieldStats,
		Deltalogs:           deltaInfos,

		CheckPoints: checkPoints,

		StartPositions: startPos,
		Flushed:        pack.isFlush,
		Dropped:        pack.isDrop,
		Channel:        pack.channelName,
	}
	err := retry.Do(context.Background(), func() error {
		err := b.broker.SaveBinlogPaths(context.Background(), req)
		// Segment not found during stale segment flush. Segment might get compacted already.
		// Stop retry and still proceed to the end, ignoring this error.
		if !pack.isFlush && errors.Is(err, merr.ErrSegmentNotFound) {
			log.Warn("stale segment not found, could be compacted",
				zap.Int64("segmentID", pack.segmentID))
			log.Warn("failed to SaveBinlogPaths",
				zap.Int64("segmentID", pack.segmentID),
				zap.Error(err))
			return nil
		}
		// meta error, datanode handles a virtual channel does not belong here
		if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
			log.Warn("meta error found, skip sync and start to drop virtual channel", zap.String("channel", pack.channelName))
			return nil
		}

		if err != nil {
			return err
		}

		return nil
	}, b.opts...)
	if err != nil {
		log.Warn("failed to SaveBinlogPaths",
			zap.Int64("segmentID", pack.segmentID),
			zap.Error(err))
	}
	return err
}
