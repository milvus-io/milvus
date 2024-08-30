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

package compaction

import (
	"context"
	"fmt"
	sio "io"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
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

type LevelZeroCompactionTask struct {
	io.BinlogIO
	allocator allocator.Interface
	cm        storage.ChunkManager

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	tr   *timerecord.TimeRecorder
}

// make sure compactionTask implements compactor interface
var _ Compactor = (*LevelZeroCompactionTask)(nil)

func NewLevelZeroCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
) *LevelZeroCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	alloc := allocator.NewLocalAllocator(plan.GetBeginLogID(), math.MaxInt64)
	return &LevelZeroCompactionTask{
		ctx:    ctx,
		cancel: cancel,

		BinlogIO:  binlogIO,
		allocator: alloc,
		cm:        cm,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("levelzero compaction"),
		done:      make(chan struct{}, 1),
	}
}

func (t *LevelZeroCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *LevelZeroCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *LevelZeroCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *LevelZeroCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *LevelZeroCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}

func (t *LevelZeroCompactionTask) GetCollection() int64 {
	// The length of SegmentBinlogs is checked before task enqueueing.
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *LevelZeroCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, "L0Compact")
	defer span.End()
	log := log.Ctx(t.ctx).With(zap.Int64("planID", t.plan.GetPlanID()), zap.String("type", t.plan.GetType().String()))
	log.Info("L0 compaction", zap.Duration("wait in queue elapse", t.tr.RecordSpan()))

	if !funcutil.CheckCtxValid(ctx) {
		log.Warn("compact wrong, task context done or timeout")
		return nil, ctx.Err()
	}

	l0Segments := lo.Filter(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level == datapb.SegmentLevel_L0
	})

	targetSegments := lo.Filter(t.plan.GetSegmentBinlogs(), func(s *datapb.CompactionSegmentBinlogs, _ int) bool {
		return s.Level != datapb.SegmentLevel_L0
	})
	if len(targetSegments) == 0 {
		log.Warn("compact wrong, not target sealed segments")
		return nil, errors.New("illegal compaction plan with empty target segments")
	}
	err := binlog.DecompressCompactionBinlogs(l0Segments)
	if err != nil {
		log.Warn("DecompressCompactionBinlogs failed", zap.Error(err))
		return nil, err
	}

	var (
		memorySize     int64
		totalDeltalogs = []string{}
	)
	for _, s := range l0Segments {
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				totalDeltalogs = append(totalDeltalogs, l.GetLogPath())
				memorySize += l.GetMemorySize()
			}
		}
	}

	resultSegments, err := t.process(ctx, memorySize, targetSegments, totalDeltalogs)
	if err != nil {
		return nil, err
	}

	result := &datapb.CompactionPlanResult{
		PlanID:   t.plan.GetPlanID(),
		State:    datapb.CompactionTaskState_completed,
		Segments: resultSegments,
		Channel:  t.plan.GetChannel(),
		Type:     t.plan.GetType(),
	}

	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).
		Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	log.Info("L0 compaction finished", zap.Duration("elapse", t.tr.ElapseSpan()))

	return result, nil
}

// BatchSize refers to the L1/L2 segments count that in one batch, batchSize controls the expansion ratio
// of deltadata in memory.
func getMaxBatchSize(baseMemSize, memLimit float64) int {
	batchSize := 1
	if memLimit > baseMemSize {
		batchSize = int(memLimit / baseMemSize)
	}

	maxSizeLimit := paramtable.Get().DataNodeCfg.L0CompactionMaxBatchSize.GetAsInt()
	// Set batch size to maxSizeLimit if it is larger than maxSizeLimit.
	// When maxSizeLimit <= 0, it means no limit.
	if maxSizeLimit > 0 && batchSize > maxSizeLimit {
		return maxSizeLimit
	}

	return batchSize
}

func (t *LevelZeroCompactionTask) serializeUpload(ctx context.Context, segmentWriters map[int64]*SegmentDeltaWriter) ([]*datapb.CompactionSegment, error) {
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

		blobKey, _ := binlog.BuildLogPath(storage.DeleteBinlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), -1, logID)

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

func (t *LevelZeroCompactionTask) splitDelta(
	ctx context.Context,
	allDelta *storage.DeleteData,
	segmentBfs map[int64]*pkoracle.BloomFilterSet,
) map[int64]*SegmentDeltaWriter {
	traceCtx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact splitDelta")
	defer span.End()

	allSeg := lo.Associate(t.plan.GetSegmentBinlogs(), func(segment *datapb.CompactionSegmentBinlogs) (int64, *datapb.CompactionSegmentBinlogs) {
		return segment.GetSegmentID(), segment
	})

	// spilt all delete data to segments

	retMap := t.applyBFInParallel(traceCtx, allDelta, io.GetBFApplyPool(), segmentBfs)

	targetSegBuffer := make(map[int64]*SegmentDeltaWriter)
	retMap.Range(func(key int, value *BatchApplyRet) bool {
		startIdx := value.StartIdx
		pk2SegmentIDs := value.Segment2Hits

		for segmentID, hits := range pk2SegmentIDs {
			for i, hit := range hits {
				if hit {
					writer, ok := targetSegBuffer[segmentID]
					if !ok {
						segment := allSeg[segmentID]
						writer = NewSegmentDeltaWriter(segmentID, segment.GetPartitionID(), segment.GetCollectionID())
						targetSegBuffer[segmentID] = writer
					}
					writer.Write(allDelta.Pks[startIdx+i], allDelta.Tss[startIdx+i])
				}
			}
		}
		return true
	})
	return targetSegBuffer
}

type BatchApplyRet = struct {
	StartIdx     int
	Segment2Hits map[int64][]bool
}

func (t *LevelZeroCompactionTask) applyBFInParallel(ctx context.Context, deltaData *storage.DeleteData, pool *conc.Pool[any], segmentBfs map[int64]*pkoracle.BloomFilterSet) *typeutil.ConcurrentMap[int, *BatchApplyRet] {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact applyBFInParallel")
	defer span.End()
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()

	batchPredict := func(pks []storage.PrimaryKey) map[int64][]bool {
		segment2Hits := make(map[int64][]bool, 0)
		lc := storage.NewBatchLocationsCache(pks)
		for segmentID, bf := range segmentBfs {
			hits := bf.BatchPkExist(lc)
			segment2Hits[segmentID] = hits
		}
		return segment2Hits
	}

	retIdx := 0
	retMap := typeutil.NewConcurrentMap[int, *BatchApplyRet]()
	var futures []*conc.Future[any]
	pks := deltaData.Pks
	for idx := 0; idx < len(pks); idx += batchSize {
		startIdx := idx
		endIdx := startIdx + batchSize
		if endIdx > len(pks) {
			endIdx = len(pks)
		}

		retIdx += 1
		tmpRetIndex := retIdx
		future := pool.Submit(func() (any, error) {
			ret := batchPredict(pks[startIdx:endIdx])
			retMap.Insert(tmpRetIndex, &BatchApplyRet{
				StartIdx:     startIdx,
				Segment2Hits: ret,
			})
			return nil, nil
		})
		futures = append(futures, future)
	}
	conc.AwaitAll(futures...)
	return retMap
}

func (t *LevelZeroCompactionTask) process(ctx context.Context, l0MemSize int64, targetSegments []*datapb.CompactionSegmentBinlogs, deltaLogs ...[]string) ([]*datapb.CompactionSegment, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact process")
	defer span.End()

	ratio := paramtable.Get().DataNodeCfg.L0BatchMemoryRatio.GetAsFloat()
	memLimit := float64(hardware.GetFreeMemoryCount()) * ratio
	if float64(l0MemSize) > memLimit {
		return nil, errors.Newf("L0 compaction failed, not enough memory, request memory size: %v, memory limit: %v", l0MemSize, memLimit)
	}

	log.Info("L0 compaction process start")
	allDelta, err := t.loadDelta(ctx, lo.Flatten(deltaLogs))
	if err != nil {
		log.Warn("L0 compaction loadDelta fail", zap.Error(err))
		return nil, err
	}

	batchSize := getMaxBatchSize(float64(allDelta.Size()), memLimit)
	batch := int(math.Ceil(float64(len(targetSegments)) / float64(batchSize)))
	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int("max conc segment counts", batchSize),
		zap.Int("total segment counts", len(targetSegments)),
		zap.Int("total batch", batch),
	)

	results := make([]*datapb.CompactionSegment, 0)
	for i := 0; i < batch; i++ {
		left, right := i*batchSize, (i+1)*batchSize
		if right >= len(targetSegments) {
			right = len(targetSegments)
		}
		batchSegments := targetSegments[left:right]
		segmentBFs, err := t.loadBF(ctx, batchSegments)
		if err != nil {
			log.Warn("L0 compaction loadBF fail", zap.Error(err))
			return nil, err
		}

		batchSegWriter := t.splitDelta(ctx, allDelta, segmentBFs)
		batchResults, err := t.serializeUpload(ctx, batchSegWriter)
		if err != nil {
			log.Warn("L0 compaction serialize upload fail", zap.Error(err))
			return nil, err
		}

		log.Info("L0 compaction finished one batch",
			zap.Int("batch no.", i),
			zap.Int("total deltaRowCount", int(allDelta.RowCount)),
			zap.Int("batch segment count", len(batchResults)))
		results = append(results, batchResults...)
	}

	log.Info("L0 compaction process done")
	return results, nil
}

func (t *LevelZeroCompactionTask) loadDelta(ctx context.Context, deltaLogs []string) (*storage.DeleteData, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact loadDelta")
	defer span.End()

	blobBytes, err := t.Download(ctx, deltaLogs)
	if err != nil {
		return nil, err
	}
	blobs := make([]*storage.Blob, 0, len(blobBytes))
	for _, blob := range blobBytes {
		blobs = append(blobs, &storage.Blob{Value: blob})
	}

	reader, err := storage.CreateDeltalogReader(blobs)
	if err != nil {
		log.Error("malformed delta file", zap.Error(err))
		return nil, err
	}
	defer reader.Close()

	dData := &storage.DeleteData{}
	for {
		err := reader.Next()
		if err != nil {
			if err == sio.EOF {
				break
			}
			log.Error("compact wrong, fail to read deltalogs", zap.Error(err))
			return nil, err
		}

		dl := reader.Value()
		dData.Append(dl.Pk, dl.Ts)
	}

	return dData, nil
}

func (t *LevelZeroCompactionTask) loadBF(ctx context.Context, targetSegments []*datapb.CompactionSegmentBinlogs) (map[int64]*pkoracle.BloomFilterSet, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact loadBF")
	defer span.End()

	var (
		futures = make([]*conc.Future[any], 0, len(targetSegments))
		pool    = io.GetOrCreateStatsPool()

		mu  = &sync.Mutex{}
		bfs = make(map[int64]*pkoracle.BloomFilterSet)
	)

	for _, segment := range targetSegments {
		segment := segment
		innerCtx := ctx
		future := pool.Submit(func() (any, error) {
			_ = binlog.DecompressBinLog(storage.StatsBinlog, segment.GetCollectionID(),
				segment.GetPartitionID(), segment.GetSegmentID(), segment.GetField2StatslogPaths())
			pks, err := LoadStats(innerCtx, t.cm,
				t.plan.GetSchema(), segment.GetSegmentID(), segment.GetField2StatslogPaths())
			if err != nil {
				log.Warn("failed to load segment stats log",
					zap.Int64("planID", t.plan.GetPlanID()),
					zap.String("type", t.plan.GetType().String()),
					zap.Error(err))
				return err, err
			}
			bf := pkoracle.NewBloomFilterSet(pks...)
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

func (t *LevelZeroCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}
