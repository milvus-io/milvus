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
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/util"
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
	allocator allocator.Allocator
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
	alloc allocator.Allocator,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
) *LevelZeroCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
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
		totalSize      int64
		totalDeltalogs = make(map[int64][]string)
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
	resultSegments, err := t.process(ctx, batchSize, targetSegments, lo.Values(totalDeltalogs)...)
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

// batch size means segment count
func getMaxBatchSize(totalSize int64) int {
	max := 1
	memLimit := float64(hardware.GetFreeMemoryCount()) * paramtable.Get().DataNodeCfg.L0BatchMemoryRatio.GetAsFloat()
	if memLimit > float64(totalSize) {
		max = int(memLimit / float64(totalSize))
	}

	return max
}

func (t *LevelZeroCompactionTask) serializeUpload(ctx context.Context, segmentWriters map[int64]*SegmentDeltaWriter) ([]*datapb.CompactionSegment, error) {
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

	if err := t.Upload(ctx, allBlobs); err != nil {
		log.Warn("L0 compaction serializeUpload upload failed", zap.Error(err))
		return nil, err
	}

	return results, nil
}

func (t *LevelZeroCompactionTask) splitDelta(
	ctx context.Context,
	allDelta []*storage.DeleteData,
	segmentBfs map[int64]*metacache.BloomFilterSet,
) map[int64]*SegmentDeltaWriter {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact splitDelta")
	defer span.End()

	allSeg := lo.Associate(t.plan.GetSegmentBinlogs(), func(segment *datapb.CompactionSegmentBinlogs) (int64, *datapb.CompactionSegmentBinlogs) {
		return segment.GetSegmentID(), segment
	})

	// spilt all delete data to segments

	retMap := t.applyBFInParallel(allDelta, io.GetBFApplyPool(), segmentBfs)

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
						writer = NewSegmentDeltaWriter(segmentID, segment.GetPartitionID(), segment.GetCollectionID())
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

func (t *LevelZeroCompactionTask) applyBFInParallel(deleteDatas []*storage.DeleteData, pool *conc.Pool[any], segmentBfs map[int64]*metacache.BloomFilterSet) *typeutil.ConcurrentMap[int, *BatchApplyRet] {
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

func (t *LevelZeroCompactionTask) process(ctx context.Context, batchSize int, targetSegments []*datapb.CompactionSegmentBinlogs, deltaLogs ...[]string) ([]*datapb.CompactionSegment, error) {
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

		log.Info("L0 compaction finished one batch", zap.Int("batch no.", i), zap.Int("batch segment count", len(batchResults)))
		results = append(results, batchResults...)
	}

	log.Info("L0 compaction process done")
	return results, nil
}

func (t *LevelZeroCompactionTask) loadDelta(ctx context.Context, deltaLogs ...[]string) ([]*storage.DeleteData, error) {
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

func (t *LevelZeroCompactionTask) loadBF(ctx context.Context, targetSegments []*datapb.CompactionSegmentBinlogs) (map[int64]*metacache.BloomFilterSet, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact loadBF")
	defer span.End()

	var (
		futures = make([]*conc.Future[any], 0, len(targetSegments))
		pool    = io.GetOrCreateStatsPool()

		mu  = &sync.Mutex{}
		bfs = make(map[int64]*metacache.BloomFilterSet)
	)

	for _, segment := range targetSegments {
		segment := segment
		innerCtx := ctx
		future := pool.Submit(func() (any, error) {
			_ = binlog.DecompressBinLog(storage.StatsBinlog, segment.GetCollectionID(),
				segment.GetPartitionID(), segment.GetSegmentID(), segment.GetField2StatslogPaths())
			pks, err := util.LoadStats(innerCtx, t.cm,
				t.plan.GetSchema(), segment.GetSegmentID(), segment.GetField2StatslogPaths())
			if err != nil {
				log.Warn("failed to load segment stats log",
					zap.Int64("planID", t.plan.GetPlanID()),
					zap.String("type", t.plan.GetType().String()),
					zap.Error(err))
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
