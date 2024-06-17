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
	"math"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
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

	l0Segments := lo.Filter(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegIDs := lo.FilterMap(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) (int64, bool) {
		if s.Level == datapb.SegmentLevel_L1 {
			return s.GetSegmentID(), true
		}
		return 0, false
	})
	if len(targetSegIDs) == 0 {
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
				totalSize += l.GetMemorySize()
			}
		}
		if len(paths) > 0 {
			totalDeltalogs[s.GetSegmentID()] = paths
		}
	}

	batchSize := getMaxBatchSize(totalSize)
	resultSegments, err := t.process(ctx, batchSize, targetSegIDs, lo.Values(totalDeltalogs)...)
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

// batch size means segment count
func getMaxBatchSize(totalSize int64) int {
	max := 1
	memLimit := float64(hardware.GetFreeMemoryCount()) * paramtable.Get().DataNodeCfg.L0BatchMemoryRatio.GetAsFloat()
	if memLimit > float64(totalSize) {
		max = int(memLimit / float64(totalSize))
	}

	return max
}

func (t *levelZeroCompactionTask) serializeUpload(ctx context.Context, segmentWriters map[int64]*SegmentDeltaWriter) ([]*datapb.CompactionSegment, error) {
	traceCtx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact serializeUpload")
	defer span.End()
	allBlobs := make(map[string][]byte)
	results := make([]*datapb.CompactionSegment, 0)
	for segID, writer := range segmentWriters {
		blob, tr, err := writer.Finish()
		if err != nil {
			log.Warn("L0 compaction serializeUpload serialize failed", zap.Error(err))
			return nil, err
		}

		logID, err := t.allocator.AllocOne()
		if err != nil {
			log.Warn("L0 compaction serializeUpload alloc failed", zap.Error(err))
			return nil, err
		}

		blobKey, _ := binlog.BuildLogPath(storage.DeleteBinlog, writer.collectionID, writer.partitionID, writer.segmentID, -1, logID)

		allBlobs[blobKey] = blob.GetValue()
		deltalog := &datapb.Binlog{
			EntriesNum:    writer.GetRowNum(),
			LogSize:       int64(len(blob.GetValue())),
			MemorySize:    blob.GetMemorySize(),
			LogPath:       blobKey,
			LogID:         logID,
			TimestampFrom: tr.GetMinTimestamp(),
			TimestampTo:   tr.GetMaxTimestamp(),
		}

		results = append(results, &datapb.CompactionSegment{
			SegmentID: segID,
			Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{deltalog}}},
			Channel:   t.plan.GetChannel(),
		})
	}

	if len(allBlobs) == 0 {
		return nil, nil
	}

	if err := t.Upload(traceCtx, allBlobs); err != nil {
		log.Warn("L0 compaction serializeUpload upload failed", zap.Error(err))
		return nil, err
	}

	return results, nil
}

func (t *levelZeroCompactionTask) splitDelta(
	ctx context.Context,
	allDelta []*storage.DeleteData,
	targetSegIDs []int64,
) map[int64]*SegmentDeltaWriter {
	traceCtx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact splitDelta")
	defer span.End()

	allSeg := lo.Associate(t.plan.GetSegmentBinlogs(), func(segment *datapb.CompactionSegmentBinlogs) (int64, *datapb.CompactionSegmentBinlogs) {
		return segment.GetSegmentID(), segment
	})

	// segments shall be safe to read outside
	segments := t.metacache.GetSegmentsBy(metacache.WithSegmentIDs(targetSegIDs...))
	// spilt all delete data to segments
	retMap := t.applyBFInParallel(traceCtx, allDelta, io.GetBFApplyPool(), segments)

	targetSegBuffer := make(map[int64]*SegmentDeltaWriter)
	retMap.Range(func(key int, value *BatchApplyRet) bool {
		startIdx := value.StartIdx
		pk2SegmentIDs := value.Segment2Hits

		pks := allDelta[value.DeleteDataIdx].Pks
		tss := allDelta[value.DeleteDataIdx].Tss

		for segmentID, hits := range pk2SegmentIDs {
			for i, hit := range hits {
				if hit {
					writer, ok := targetSegBuffer[segmentID]
					if !ok {
						segment := allSeg[segmentID]
						writer = NewSegmentDeltaWriter(segmentID, segment.GetPartitionID(), t.getCollection())
						targetSegBuffer[segmentID] = writer
					}
					writer.Write(pks[startIdx+i], tss[startIdx+i])
				}
			}
		}
		return true
	})

	return targetSegBuffer
}

type BatchApplyRet = struct {
	DeleteDataIdx int
	StartIdx      int
	Segment2Hits  map[int64][]bool
}

func (t *levelZeroCompactionTask) applyBFInParallel(ctx context.Context, deleteDatas []*storage.DeleteData, pool *conc.Pool[any], segmentBfs []*metacache.SegmentInfo) *typeutil.ConcurrentMap[int, *BatchApplyRet] {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact applyBFInParallel")
	defer span.End()
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()

	batchPredict := func(pks []storage.PrimaryKey) map[int64][]bool {
		segment2Hits := make(map[int64][]bool, 0)
		lc := storage.NewBatchLocationsCache(pks)
		for _, s := range segmentBfs {
			segmentID := s.SegmentID()
			hits := s.GetBloomFilterSet().BatchPkExist(lc)
			segment2Hits[segmentID] = hits
		}
		return segment2Hits
	}

	retIdx := 0
	retMap := typeutil.NewConcurrentMap[int, *BatchApplyRet]()
	var futures []*conc.Future[any]
	for didx, data := range deleteDatas {
		pks := data.Pks
		for idx := 0; idx < len(pks); idx += batchSize {
			startIdx := idx
			endIdx := startIdx + batchSize
			if endIdx > len(pks) {
				endIdx = len(pks)
			}

			retIdx += 1
			tmpRetIndex := retIdx
			deleteDataId := didx
			future := pool.Submit(func() (any, error) {
				ret := batchPredict(pks[startIdx:endIdx])
				retMap.Insert(tmpRetIndex, &BatchApplyRet{
					DeleteDataIdx: deleteDataId,
					StartIdx:      startIdx,
					Segment2Hits:  ret,
				})
				return nil, nil
			})
			futures = append(futures, future)
		}
	}
	conc.AwaitAll(futures...)

	return retMap
}

func (t *levelZeroCompactionTask) process(ctx context.Context, batchSize int, targetSegments []int64, deltaLogs ...[]string) ([]*datapb.CompactionSegment, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact process")
	defer span.End()

	results := make([]*datapb.CompactionSegment, 0)
	batch := int(math.Ceil(float64(len(targetSegments)) / float64(batchSize)))
	log := log.Ctx(t.ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int("max conc segment counts", batchSize),
		zap.Int("total segment counts", len(targetSegments)),
		zap.Int("total batch", batch),
	)

	log.Info("L0 compaction process start")
	allDelta, err := t.loadDelta(ctx, lo.Flatten(deltaLogs))
	if err != nil {
		log.Warn("L0 compaction loadDelta fail", zap.Error(err))
		return nil, err
	}

	for i := 0; i < batch; i++ {
		left, right := i*batchSize, (i+1)*batchSize
		if right > len(targetSegments) {
			right = len(targetSegments)
		}

		batchSegments := targetSegments[left:right]
		batchSegWriter := t.splitDelta(ctx, allDelta, batchSegments)
		batchResults, err := t.serializeUpload(ctx, batchSegWriter)
		if err != nil {
			log.Warn("L0 compaction serialize upload fail", zap.Error(err))
			return nil, err
		}

		log.Info("L0 compaction finished one batch", zap.Int("batch no.", i), zap.Int("batch segment count", len(batchResults)))
		results = append(results, batchResults...)
	}

	log.Info("L0 compaction process done")
	return results, nil
}

func (t *levelZeroCompactionTask) loadDelta(ctx context.Context, deltaLogs ...[]string) ([]*storage.DeleteData, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact loadDelta")
	defer span.End()
	allData := make([]*storage.DeleteData, 0, len(deltaLogs))
	for _, paths := range deltaLogs {
		blobBytes, err := t.Download(ctx, paths)
		if err != nil {
			return nil, err
		}
		blobs := make([]*storage.Blob, 0, len(blobBytes))
		for _, blob := range blobBytes {
			blobs = append(blobs, &storage.Blob{Value: blob})
		}
		_, _, dData, err := storage.NewDeleteCodec().Deserialize(blobs)
		if err != nil {
			return nil, err
		}

		allData = append(allData, dData)
	}
	return allData, nil
}
