package syncmgr

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// MetaWriter is the interface for SyncManager to write segment sync meta.
type MetaWriter interface {
	UpdateSync(context.Context, *SyncTask) error
	UpdateGrowingSourceSync(context.Context, *GrowingSourceSyncTask) error
	DropChannel(context.Context, string) error
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

func (b *brokerMetaWriter) UpdateSync(ctx context.Context, pack *SyncTask) error {
	checkPoints := []*datapb.CheckPoint{}
	// only current segment checkpoint info
	segment, ok := pack.metacache.GetSegmentByID(pack.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(pack.segmentID)
	}

	insertFieldBinlogs := append(segment.Binlogs(), storage.SortFieldBinlogs(pack.insertBinlogs)...)
	statsFieldBinlogs := append(segment.Statslogs(), lo.MapToSlice(pack.statsBinlogs, func(_ int64, fieldBinlog *datapb.FieldBinlog) *datapb.FieldBinlog { return fieldBinlog })...)

	deltaFieldBinlogs := segment.Deltalogs()
	if pack.deltaBinlog != nil && len(pack.deltaBinlog.Binlogs) > 0 {
		deltaFieldBinlogs = append(deltaFieldBinlogs, pack.deltaBinlog)
	}

	deltaBm25StatsBinlogs := segment.Bm25logs()
	if len(pack.bm25Binlogs) > 0 {
		deltaBm25StatsBinlogs = append(segment.Bm25logs(), lo.MapToSlice(pack.bm25Binlogs, func(_ int64, fieldBinlog *datapb.FieldBinlog) *datapb.FieldBinlog { return fieldBinlog })...)
	}

	checkPoints = append(checkPoints, &datapb.CheckPoint{
		SegmentID: pack.segmentID,
		NumOfRows: segment.FlushedRows() + pack.batchRows,
		Position:  pack.checkpoint,
	})

	// Get not reported L1's start positions
	startPos := lo.Map(pack.metacache.GetSegmentsBy(
		metacache.WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing),
		metacache.WithLevel(datapb.SegmentLevel_L1), metacache.WithStartPosNotRecorded()),
		func(info *metacache.SegmentInfo, _ int) *datapb.SegmentStartPosition {
			return &datapb.SegmentStartPosition{
				SegmentID:     info.SegmentID(),
				StartPosition: info.StartPosition(),
			}
		})

	// L0 brings its own start position
	if segment.Level() == datapb.SegmentLevel_L0 {
		startPos = append(startPos, &datapb.SegmentStartPosition{SegmentID: pack.segmentID, StartPosition: pack.StartPosition()})
	}

	getBinlogNum := func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) }
	mlog.Info(ctx, "SaveBinlogPath",
		mlog.Int64("SegmentID", pack.segmentID),
		mlog.Int64("CollectionID", pack.collectionID),
		mlog.Int64("ParitionID", pack.partitionID),
		mlog.Any("startPos", startPos),
		mlog.Any("checkPoints", checkPoints),
		mlog.Int("binlogNum", lo.SumBy(insertFieldBinlogs, getBinlogNum)),
		mlog.Int("statslogNum", lo.SumBy(statsFieldBinlogs, getBinlogNum)),
		mlog.Int("deltalogNum", lo.SumBy(deltaFieldBinlogs, getBinlogNum)),
		mlog.Int("bm25logNum", lo.SumBy(deltaBm25StatsBinlogs, getBinlogNum)),
		mlog.String("manifestPath", pack.manifestPath),
		mlog.String("vChannelName", pack.channelName),
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
		Field2Bm25LogPaths:  deltaBm25StatsBinlogs,
		Deltalogs:           deltaFieldBinlogs,

		CheckPoints: checkPoints,

		StartPositions:  startPos,
		Flushed:         pack.pack.isFlush,
		Dropped:         pack.pack.isDrop,
		Channel:         pack.channelName,
		SegLevel:        pack.level,
		StorageVersion:  segment.GetStorageVersion(),
		WithFullBinlogs: true,
		ManifestPath:    pack.manifestPath,
	}
	err := retry.Handle(ctx, func() (bool, error) {
		err := b.broker.SaveBinlogPaths(ctx, req)
		// Segment not found during stale segment flush. Segment might get compacted already.
		// Stop retry and still proceed to the end, ignoring this error.
		if !pack.pack.isFlush && errors.Is(err, merr.ErrSegmentNotFound) {
			mlog.Warn(ctx, "stale segment not found, could be compacted",
				mlog.FieldSegmentID(pack.segmentID))
			mlog.Warn(ctx, "failed to SaveBinlogPaths",
				mlog.FieldSegmentID(pack.segmentID),
				mlog.Err(err))
			return false, nil
		}
		// meta error, datanode handles a virtual channel does not belong here
		if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
			mlog.Warn(ctx, "meta error found, skip sync and start to drop virtual channel", mlog.String("channel", pack.channelName))
			return false, nil
		}

		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}

		return false, nil
	}, b.opts...)
	if err != nil {
		mlog.Warn(ctx, "failed to SaveBinlogPaths",
			mlog.FieldSegmentID(pack.segmentID),
			mlog.Err(err))
		return err
	}

	pack.metacache.UpdateSegments(metacache.SetStartPosRecorded(true), metacache.WithSegmentIDs(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) int64 { return pos.GetSegmentID() })...))
	pack.metacache.UpdateSegments(metacache.MergeSegmentAction(
		metacache.UpdateBinlogs(insertFieldBinlogs),
		metacache.UpdateStatslogs(statsFieldBinlogs),
		metacache.UpdateDeltalogs(deltaFieldBinlogs),
		metacache.UpdateBm25logs(deltaBm25StatsBinlogs),
	), metacache.WithSegmentIDs(pack.segmentID))
	return nil
}

func (b *brokerMetaWriter) UpdateGrowingSourceSync(ctx context.Context, task *GrowingSourceSyncTask) error {
	segment, ok := task.metacache.GetSegmentByID(task.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(task.segmentID)
	}

	insertFieldBinlogs := segment.Binlogs()
	if len(task.insertBinlogs) > 0 {
		insertFieldBinlogs = append(segment.Binlogs(), storage.SortFieldBinlogs(task.insertBinlogs)...)
	}
	statsFieldBinlogs := segment.Statslogs()
	deltaFieldBinlogs := segment.Deltalogs()
	bm25FieldBinlogs := segment.Bm25logs()
	startPos := task.startPositions()
	checkPoints := []*datapb.CheckPoint{{
		SegmentID: task.segmentID,
		NumOfRows: segment.FlushedRows() + task.batchRows,
		Position:  task.checkpoint,
	}}

	mlog.Info(ctx, "SaveBinlogPath for growing source sync",
		mlog.Int64("SegmentID", task.segmentID),
		mlog.Int64("CollectionID", task.collectionID),
		mlog.Int64("ParitionID", task.partitionID),
		mlog.Any("startPos", startPos),
		mlog.Any("checkPoints", checkPoints),
		mlog.Int("binlogNum", lo.SumBy(insertFieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.Int("statslogNum", lo.SumBy(statsFieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.Int("deltalogNum", lo.SumBy(deltaFieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.Int("bm25logNum", lo.SumBy(bm25FieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.String("manifestPath", task.manifestPath),
		mlog.String("vChannelName", task.channelName),
	)

	req := &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(b.serverID),
		),
		SegmentID:           task.segmentID,
		CollectionID:        task.collectionID,
		PartitionID:         task.partitionID,
		Field2BinlogPaths:   insertFieldBinlogs,
		Field2StatslogPaths: statsFieldBinlogs,
		Field2Bm25LogPaths:  bm25FieldBinlogs,
		Deltalogs:           deltaFieldBinlogs,
		CheckPoints:         checkPoints,
		StartPositions:      startPos,
		Flushed:             task.IsFlush(),
		Dropped:             task.IsDrop(),
		Channel:             task.channelName,
		SegLevel:            task.level,
		StorageVersion:      storage.StorageV3,
		WithFullBinlogs:     true,
		ManifestPath:        task.manifestPath,
	}

	err := retry.Handle(ctx, func() (bool, error) {
		err := b.broker.SaveBinlogPaths(ctx, req)
		if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
			mlog.Warn(ctx, "meta error found, fail growing source sync",
				mlog.String("channel", task.channelName),
				mlog.Int64("segmentID", task.segmentID),
				mlog.Err(err))
			return false, err
		}
		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}
		return false, nil
	}, b.opts...)
	if err != nil {
		mlog.Warn(ctx, "failed to SaveBinlogPaths for growing source sync",
			mlog.Int64("segmentID", task.segmentID),
			mlog.Err(err))
		return err
	}

	task.metacache.UpdateSegments(metacache.SetStartPosRecorded(true), metacache.WithSegmentIDs(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) int64 {
		return pos.GetSegmentID()
	})...))
	task.metacache.UpdateSegments(metacache.MergeSegmentAction(
		metacache.UpdateBinlogs(insertFieldBinlogs),
		metacache.UpdateStatslogs(statsFieldBinlogs),
		metacache.UpdateDeltalogs(deltaFieldBinlogs),
		metacache.UpdateBm25logs(bm25FieldBinlogs),
	), metacache.WithSegmentIDs(task.segmentID))
	return nil
}

func (b *brokerMetaWriter) DropChannel(ctx context.Context, channelName string) error {
	err := retry.Handle(ctx, func() (bool, error) {
		status, err := b.broker.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(b.serverID),
			),
			ChannelName: channelName,
		})
		err = merr.CheckRPCCall(status, err)
		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}
		return false, nil
	}, b.opts...)
	if err != nil {
		mlog.Warn(ctx, "failed to DropChannel",
			mlog.String("channel", channelName),
			mlog.Err(err))
	}
	return err
}
