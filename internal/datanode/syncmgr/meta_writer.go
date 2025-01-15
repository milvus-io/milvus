package syncmgr

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// MetaWriter is the interface for SyncManager to write segment sync meta.
type MetaWriter interface {
	UpdateSync(*SyncTask) error
	UpdateSyncV2(*SyncTaskV2) error
	DropChannel(string) error
}

type brokerMetaWriter struct {
	broker   broker.Broker
	opts     []retry.Option
	serverID int64
}

func BrokerMetaWriter(broker broker.Broker, serverID int64, opts ...retry.Option) MetaWriter {
	return &brokerMetaWriter{
		broker:   broker,
		serverID: serverID,
		opts:     opts,
	}
}

func (b *brokerMetaWriter) UpdateSync(pack *SyncTask) error {
	var (
		checkPoints       = []*datapb.CheckPoint{}
		deltaFieldBinlogs = []*datapb.FieldBinlog{}
	)

	insertFieldBinlogs := lo.MapToSlice(pack.insertBinlogs, func(_ int64, fieldBinlog *datapb.FieldBinlog) *datapb.FieldBinlog { return fieldBinlog })
	statsFieldBinlogs := lo.MapToSlice(pack.statsBinlogs, func(_ int64, fieldBinlog *datapb.FieldBinlog) *datapb.FieldBinlog { return fieldBinlog })
	if len(pack.deltaBinlog.Binlogs) > 0 {
		deltaFieldBinlogs = append(deltaFieldBinlogs, pack.deltaBinlog)
	}

	// only current segment checkpoint info
	segment, ok := pack.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(pack.segmentID)
	}
	checkPoints = append(checkPoints, &datapb.CheckPoint{
		SegmentID: pack.segmentID,
		NumOfRows: segment.FlushedRows() + pack.batchSize,
		Position:  pack.checkpoint,
	})

	startPos := lo.Map(pack.metacache.GetSegmentsBy(metacache.WithStartPosNotRecorded()), func(info *metacache.SegmentInfo, _ int) *datapb.SegmentStartPosition {
		return &datapb.SegmentStartPosition{
			SegmentID:     info.SegmentID(),
			StartPosition: info.StartPosition(),
		}
	})
	getBinlogNum := func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) }
	log.Info("SaveBinlogPath",
		zap.Int64("SegmentID", pack.segmentID),
		zap.Int64("CollectionID", pack.collectionID),
		zap.Int64("ParitionID", pack.partitionID),
		zap.Any("startPos", startPos),
		zap.Any("checkPoints", checkPoints),
		zap.Int("binlogNum", lo.SumBy(insertFieldBinlogs, getBinlogNum)),
		zap.Int("statslogNum", lo.SumBy(statsFieldBinlogs, getBinlogNum)),
		zap.Int("deltalogNum", lo.SumBy(deltaFieldBinlogs, getBinlogNum)),
		zap.String("vChannelName", pack.channelName),
	)

	req := &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(b.serverID),
		),
		SegmentID:           pack.segmentID,
		CollectionID:        pack.collectionID,
		PartitionID:         pack.partitionID,
		Field2BinlogPaths:   insertFieldBinlogs,
		Field2StatslogPaths: statsFieldBinlogs,
		Deltalogs:           deltaFieldBinlogs,

		CheckPoints: checkPoints,

		StartPositions: startPos,
		Flushed:        pack.isFlush,
		Dropped:        pack.isDrop,
		Channel:        pack.channelName,
		SegLevel:       pack.level,
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
		return err
	}

	pack.metacache.UpdateSegments(metacache.SetStartPosRecorded(true), metacache.WithSegmentIDs(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) int64 { return pos.GetSegmentID() })...))

	return nil
}

func (b *brokerMetaWriter) UpdateSyncV2(pack *SyncTaskV2) error {
	checkPoints := []*datapb.CheckPoint{}

	// only current segment checkpoint info,
	segment, ok := pack.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(pack.segmentID)
	}
	checkPoints = append(checkPoints, &datapb.CheckPoint{
		SegmentID: pack.segmentID,
		NumOfRows: segment.FlushedRows() + pack.batchSize,
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
		zap.String("vChannelName", pack.channelName),
	)

	req := &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(b.serverID),
		),
		SegmentID:    pack.segmentID,
		CollectionID: pack.collectionID,

		CheckPoints:    checkPoints,
		StorageVersion: pack.storageVersion,

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

func (b *brokerMetaWriter) DropChannel(channelName string) error {
	err := retry.Do(context.Background(), func() error {
		status, err := b.broker.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(b.serverID),
			),
			ChannelName: channelName,
		})
		return merr.CheckRPCCall(status, err)
	}, b.opts...)
	if err != nil {
		log.Warn("failed to DropChannel",
			zap.String("channel", channelName),
			zap.Error(err))
	}
	return err
}
