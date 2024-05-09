// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	iter "github.com/milvus-io/milvus/internal/datanode/iterators"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type levelZeroCompactionTask struct {
	compactor
	io.BinlogIO

	allocator allocator.Allocator
	metacache metacache.MetaCache
	syncmgr   syncmgr.SyncManager
	cm        storage.ChunkManager

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	tr   *timerecord.TimeRecorder
}

func newLevelZeroCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	alloc allocator.Allocator,
	metaCache metacache.MetaCache,
	syncmgr syncmgr.SyncManager,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
) *levelZeroCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &levelZeroCompactionTask{
		ctx:    ctx,
		cancel: cancel,

		BinlogIO:  binlogIO,
		allocator: alloc,
		metacache: metaCache,
		syncmgr:   syncmgr,
		cm:        cm,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("levelzero compaction"),
		done:      make(chan struct{}, 1),
	}
}

func (t *levelZeroCompactionTask) complete() {
	t.done <- struct{}{}
}

func (t *levelZeroCompactionTask) stop() {
	t.cancel()
	<-t.done
}

func (t *levelZeroCompactionTask) getPlanID() UniqueID {
	return t.plan.GetPlanID()
}

func (t *levelZeroCompactionTask) getChannelName() string {
	return t.plan.GetChannel()
}

func (t *levelZeroCompactionTask) getCollection() int64 {
	return t.metacache.Collection()
}

// Do nothing for levelzero compaction
func (t *levelZeroCompactionTask) injectDone() {}

func (t *levelZeroCompactionTask) compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, "L0Compact")
	defer span.End()
	log := log.Ctx(t.ctx).With(zap.Int64("planID", t.plan.GetPlanID()), zap.String("type", t.plan.GetType().String()))
	log.Info("L0 compaction", zap.Duration("wait in queue elapse", t.tr.RecordSpan()))

	if !funcutil.CheckCtxValid(ctx) {
		log.Warn("compact wrong, task context done or timeout")
		return nil, errContext
	}

	ctxTimeout, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	l0Segments := lo.Filter(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	l1Segments := lo.Filter(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L1
	})
	if len(l1Segments) == 0 {
		log.Warn("compact wrong, not target sealed segments")
		return nil, errIllegalCompactionPlan
	}
	err := binlog.DecompressCompactionBinlogs(l0Segments)
	if err != nil {
		log.Warn("DecompressCompactionBinlogs failed", zap.Error(err))
		return nil, err
	}

	var (
		totalSize      int64
		totalDeltalogs = make(map[UniqueID][]string)
	)
	for _, s := range l0Segments {
		paths := []string{}
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				paths = append(paths, l.GetLogPath())
				totalSize += l.GetLogSize()
			}
		}
		if len(paths) > 0 {
			totalDeltalogs[s.GetSegmentID()] = paths
		}
	}

	var resultSegments []*datapb.CompactionSegment

	if float64(hardware.GetFreeMemoryCount())*paramtable.Get().DataNodeCfg.L0BatchMemoryRatio.GetAsFloat() < float64(totalSize) {
		resultSegments, err = t.linearProcess(ctxTimeout, l1Segments, totalDeltalogs)
	} else {
		resultSegments, err = t.batchProcess(ctxTimeout, l1Segments, lo.Values(totalDeltalogs)...)
	}
	if err != nil {
		return nil, err
	}

	result := &datapb.CompactionPlanResult{
		PlanID:   t.plan.GetPlanID(),
		State:    commonpb.CompactionState_Completed,
		Segments: resultSegments,
		Channel:  t.plan.GetChannel(),
		Type:     t.plan.GetType(),
	}

	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).
		Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	log.Info("L0 compaction finished", zap.Duration("elapse", t.tr.ElapseSpan()))

	return result, nil
}

func (t *levelZeroCompactionTask) linearProcess(ctx context.Context, l1Segments []*datapb.CompactionSegmentBinlogs, totalDeltalogs map[int64][]string) ([]*datapb.CompactionSegment, error) {
	log := log.Ctx(t.ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.String("type", t.plan.GetType().String()),
		zap.Int("target segment counts", len(l1Segments)),
	)

	// just for logging
	l1SegmentIDs := lo.Map(l1Segments, func(segment *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return segment.GetSegmentID()
	})

	var (
		resultSegments  = make(map[int64]*datapb.CompactionSegment)
		alteredSegments = make(map[int64]*storage.DeleteData)
	)

	segmentBFs, err := t.loadBF(l1Segments)
	if err != nil {
		return nil, err
	}
	for segID, deltaLogs := range totalDeltalogs {
		log := log.With(zap.Int64("levelzero segment", segID))

		log.Info("Linear L0 compaction start processing segment")
		allIters, err := t.loadDelta(ctx, deltaLogs)
		if err != nil {
			log.Warn("Linear L0 compaction loadDelta fail", zap.Int64s("target segments", l1SegmentIDs), zap.Error(err))
			return nil, err
		}

		t.splitDelta(ctx, allIters, alteredSegments, segmentBFs)

		err = t.uploadByCheck(ctx, true, alteredSegments, resultSegments)
		if err != nil {
			log.Warn("Linear L0 compaction upload buffer fail", zap.Int64s("target segments", l1SegmentIDs), zap.Error(err))
			return nil, err
		}
	}

	err = t.uploadByCheck(ctx, false, alteredSegments, resultSegments)
	if err != nil {
		log.Warn("Linear L0 compaction upload all buffer fail", zap.Int64s("target segment", l1SegmentIDs), zap.Error(err))
		return nil, err
	}
	log.Info("Linear L0 compaction finished", zap.Duration("elapse", t.tr.RecordSpan()))
	return lo.Values(resultSegments), nil
}

func (t *levelZeroCompactionTask) batchProcess(ctx context.Context, l1Segments []*datapb.CompactionSegmentBinlogs, deltaLogs ...[]string) ([]*datapb.CompactionSegment, error) {
	log := log.Ctx(t.ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.String("type", t.plan.GetType().String()),
		zap.Int("target segment counts", len(l1Segments)),
	)

	// just for logging
	l1SegmentIDs := lo.Map(l1Segments, func(segment *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return segment.GetSegmentID()
	})

	log.Info("Batch L0 compaction start processing")
	resultSegments := make(map[int64]*datapb.CompactionSegment)

	iters, err := t.loadDelta(ctx, lo.Flatten(deltaLogs))
	if err != nil {
		log.Warn("Batch L0 compaction loadDelta fail", zap.Int64s("target segments", l1SegmentIDs), zap.Error(err))
		return nil, err
	}

	segmentBFs, err := t.loadBF(l1Segments)
	if err != nil {
		return nil, err
	}

	alteredSegments := make(map[int64]*storage.DeleteData)
	t.splitDelta(ctx, iters, alteredSegments, segmentBFs)

	err = t.uploadByCheck(ctx, false, alteredSegments, resultSegments)
	if err != nil {
		log.Warn("Batch L0 compaction upload fail", zap.Int64s("target segments", l1SegmentIDs), zap.Error(err))
		return nil, err
	}
	log.Info("Batch L0 compaction finished", zap.Duration("elapse", t.tr.RecordSpan()))
	return lo.Values(resultSegments), nil
}

func (t *levelZeroCompactionTask) loadDelta(ctx context.Context, deltaLogs ...[]string) ([]*iter.DeltalogIterator, error) {
	allIters := make([]*iter.DeltalogIterator, 0)

	for _, paths := range deltaLogs {
		blobs, err := t.Download(ctx, paths)
		if err != nil {
			return nil, err
		}

		allIters = append(allIters, iter.NewDeltalogIterator(blobs, nil))
	}
	return allIters, nil
}

func (t *levelZeroCompactionTask) splitDelta(
	ctx context.Context,
	allIters []*iter.DeltalogIterator,
	targetSegBuffer map[int64]*storage.DeleteData,
	segmentBfs map[int64]*metacache.BloomFilterSet,
) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact splitDelta")
	defer span.End()

	split := func(pk storage.PrimaryKey) []int64 {
		predicts := make([]int64, 0, len(segmentBfs))
		for segmentID, bf := range segmentBfs {
			if bf.PkExists(pk) {
				predicts = append(predicts, segmentID)
			}
		}
		return predicts
	}

	// spilt all delete data to segments
	for _, deltaIter := range allIters {
		for deltaIter.HasNext() {
			// checked by HasNext, no error here
			labeled, _ := deltaIter.Next()

			predicted := split(labeled.GetPk())

			for _, gotSeg := range predicted {
				delBuffer, ok := targetSegBuffer[gotSeg]
				if !ok {
					delBuffer = &storage.DeleteData{}
					targetSegBuffer[gotSeg] = delBuffer
				}

				delBuffer.Append(labeled.GetPk(), labeled.GetTimestamp())
			}
		}
	}
}

func (t *levelZeroCompactionTask) composeDeltalog(segmentID int64, dData *storage.DeleteData) (map[string][]byte, *datapb.Binlog, error) {
	var (
		collID   = t.metacache.Collection()
		uploadKv = make(map[string][]byte)
	)

	seg, ok := t.metacache.GetSegmentByID(segmentID)
	if !ok {
		return nil, nil, merr.WrapErrSegmentLack(segmentID)
	}
	blob, err := storage.NewDeleteCodec().Serialize(collID, seg.PartitionID(), segmentID, dData)
	if err != nil {
		return nil, nil, err
	}

	logID, err := t.allocator.AllocOne()
	if err != nil {
		return nil, nil, err
	}

	blobKey := metautil.JoinIDPath(collID, seg.PartitionID(), segmentID, logID)
	blobPath := t.BinlogIO.JoinFullPath(common.SegmentDeltaLogPath, blobKey)

	uploadKv[blobPath] = blob.GetValue()

	// TODO Timestamp?
	deltalog := &datapb.Binlog{
		LogSize: int64(len(blob.GetValue())),
		LogPath: blobPath,
		LogID:   logID,
	}

	return uploadKv, deltalog, nil
}

func (t *levelZeroCompactionTask) uploadByCheck(ctx context.Context, requireCheck bool, alteredSegments map[int64]*storage.DeleteData, resultSegments map[int64]*datapb.CompactionSegment) error {
	for segID, dData := range alteredSegments {
		if !requireCheck || (dData.Size() >= paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64()) {
			blobs, binlog, err := t.composeDeltalog(segID, dData)
			if err != nil {
				log.Warn("L0 compaction composeDelta fail", zap.Int64("segmentID", segID), zap.Error(err))
				return err
			}
			err = t.Upload(ctx, blobs)
			if err != nil {
				log.Warn("L0 compaction upload blobs fail", zap.Int64("segmentID", segID), zap.Any("binlog", binlog), zap.Error(err))
				return err
			}

			if _, ok := resultSegments[segID]; !ok {
				resultSegments[segID] = &datapb.CompactionSegment{
					SegmentID: segID,
					Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{binlog}}},
					Channel:   t.plan.GetChannel(),
				}
			} else {
				resultSegments[segID].Deltalogs[0].Binlogs = append(resultSegments[segID].Deltalogs[0].Binlogs, binlog)
			}

			delete(alteredSegments, segID)
		}
	}
	return nil
}

func (t *levelZeroCompactionTask) loadBF(l1Segments []*datapb.CompactionSegmentBinlogs) (map[int64]*metacache.BloomFilterSet, error) {
	log := log.Ctx(t.ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.String("type", t.plan.GetType().String()),
	)

	var (
		futures = make([]*conc.Future[any], 0, len(l1Segments))
		pool    = getOrCreateStatsPool()

		mu  = &sync.Mutex{}
		bfs = make(map[int64]*metacache.BloomFilterSet)
	)

	for _, segment := range l1Segments {
		segment := segment
		future := pool.Submit(func() (any, error) {
			err := binlog.DecompressBinLog(storage.StatsBinlog, segment.GetCollectionID(),
				segment.GetPartitionID(), segment.GetSegmentID(), segment.GetField2StatslogPaths())
			if err != nil {
				log.Warn("failed to DecompressBinLog", zap.Error(err))
				return err, err
			}
			pks, err := loadStats(t.ctx, t.cm,
				t.metacache.Schema(), segment.GetSegmentID(), segment.GetField2StatslogPaths())
			if err != nil {
				log.Warn("failed to load segment stats log", zap.Error(err))
				return err, err
			}
			bf := metacache.NewBloomFilterSet(pks...)
			mu.Lock()
			defer mu.Unlock()
			bfs[segment.GetSegmentID()] = bf
			return nil, nil
		})
		futures = append(futures, future)
	}

	err := conc.AwaitAll(futures...)
	return bfs, err
}
