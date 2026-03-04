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

package compactor

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	compactionParams compaction.Params
}

// make sure compactionTask implements compactor interface
var _ Compactor = (*LevelZeroCompactionTask)(nil)

func NewLevelZeroCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
) *LevelZeroCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	alloc := allocator.NewLocalAllocator(plan.GetPreAllocatedLogIDs().GetBegin(), plan.GetPreAllocatedLogIDs().GetEnd())
	return &LevelZeroCompactionTask{
		ctx:    ctx,
		cancel: cancel,

		BinlogIO:         binlogIO,
		allocator:        alloc,
		cm:               cm,
		plan:             plan,
		tr:               timerecord.NewTimeRecorder("levelzero compaction"),
		done:             make(chan struct{}, 1),
		compactionParams: compactionParams,
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

	var err error
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
	err = binlog.DecompressCompactionBinlogsWithRootPath(t.compactionParams.StorageConfig.GetRootPath(), l0Segments)
	if err != nil {
		log.Warn("DecompressCompactionBinlogs failed", zap.Error(err))
		return nil, err
	}

	var memorySize int64
	for _, s := range l0Segments {
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				memorySize += l.GetMemorySize()
			}
		}
	}

	resultSegments, err := t.process(ctx, memorySize, targetSegments, l0Segments)
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

	metrics.DataNodeCompactionLatency.WithLabelValues(paramtable.GetStringNodeID(), t.plan.GetType().String()).
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

func (t *LevelZeroCompactionTask) splitAndWrite(
	ctx context.Context,
	allDelta *storage.DeleteData,
	segmentBfs map[int64]*pkoracle.BloomFilterSet,
) ([]*datapb.CompactionSegment, error) {
	traceCtx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact splitAndWrite")
	defer span.End()

	allSeg := lo.Associate(t.plan.GetSegmentBinlogs(),
		func(segment *datapb.CompactionSegmentBinlogs) (int64, *datapb.CompactionSegmentBinlogs) {
			return segment.GetSegmentID(), segment
		})

	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		return nil, err
	}

	// spilt all delete data to segments
	retMap := t.applyBFInParallel(traceCtx, allDelta, io.GetBFApplyPool(), segmentBfs)

	// Collect deletes for each segment
	type segmentDeletes struct {
		pks []storage.PrimaryKey
		tss []typeutil.Timestamp
	}
	segmentData := make(map[int64]*segmentDeletes)

	retMap.Range(func(key int, value *BatchApplyRet) bool {
		startIdx := value.StartIdx
		pk2SegmentIDs := value.Segment2Hits

		for segmentID, hits := range pk2SegmentIDs {
			for i, hit := range hits {
				if hit {
					if _, ok := segmentData[segmentID]; !ok {
						segmentData[segmentID] = &segmentDeletes{
							pks: make([]storage.PrimaryKey, 0),
							tss: make([]typeutil.Timestamp, 0),
						}
					}
					pk := allDelta.Pks[startIdx+i]
					ts := allDelta.Tss[startIdx+i]

					segmentData[segmentID].pks = append(segmentData[segmentID].pks, pk)
					segmentData[segmentID].tss = append(segmentData[segmentID].tss, ts)
				}
			}
		}
		return true
	})

	// Write collected deletes for each segment
	results := make([]*datapb.CompactionSegment, 0, len(segmentData))
	for segmentID, deletes := range segmentData {
		if len(deletes.pks) == 0 {
			continue
		}

		result, err := func() (*datapb.CompactionSegment, error) {
			segment := allSeg[segmentID]
			logID, err := t.allocator.AllocOne()
			if err != nil {
				log.Warn("L0 compaction allocate log ID fail", zap.Int64("segmentID", segmentID), zap.Error(err))
				return nil, err
			}

			path := metautil.BuildDeltaLogPath(
				t.compactionParams.StorageConfig.GetRootPath(), segment.GetCollectionID(), segment.GetPartitionID(), segment.GetSegmentID(), logID)

			// Use V2 storage for segments with manifest, V1 otherwise
			storageVersion := storage.StorageV1
			if segment.GetManifest() != "" {
				storageVersion = storage.StorageV2
			}

			writer, err := storage.NewDeltalogWriter(ctx,
				segment.GetCollectionID(), segment.GetPartitionID(), segment.GetSegmentID(),
				logID, pkField.GetDataType(), path,
				storage.WithUploader(t.Upload),
				storage.WithStorageConfig(t.compactionParams.StorageConfig),
				storage.WithVersion(storageVersion),
			)
			if err != nil {
				log.Warn("L0 compaction create deltalog writer fail", zap.Int64("segmentID", segmentID), zap.Error(err))
				return nil, err
			}

			// Create Arrow record from collected deletes
			record, tsFrom, tsTo, err := storage.BuildDeleteRecord(deletes.pks, deletes.tss)
			if err != nil {
				log.Warn("L0 compaction build delete record fail", zap.Int64("segmentID", segmentID), zap.Error(err))
				return nil, err
			}
			defer record.Release()

			// Write the entire record at once
			if err := writer.Write(record); err != nil {
				log.Warn("L0 compaction write record fail", zap.Int64("segmentID", segmentID), zap.Error(err))
				return nil, err
			}

			if err := writer.Close(); err != nil {
				log.Warn("L0 compaction close writer fail", zap.Int64("segmentID", segmentID), zap.Error(err))
				return nil, err
			}

			log.Info("L0 compaction write record success", zap.String("path", path), zap.Int64("entries", int64(len(deletes.pks))))

			// Check if this is a V2 segment (has manifest)
			if segment.GetManifest() != "" {
				// V2: Update manifest with new deltalog
				newManifest, err := packed.AddDeltaLogsToManifest(
					segment.GetManifest(),
					t.compactionParams.StorageConfig,
					[]packed.DeltaLogEntry{{Path: path, NumEntries: int64(len(deletes.pks))}},
				)
				if err != nil {
					log.Warn("L0 compaction update manifest fail", zap.Int64("segmentID", segmentID), zap.Error(err))
					return nil, err
				}
				return &datapb.CompactionSegment{
					SegmentID: segmentID,
					Channel:   t.plan.GetChannel(),
					Manifest:  newManifest,
					NumOfRows: int64(len(deletes.pks)),
				}, nil
			}
			// V1: Return deltalog in FieldBinlog format
			return &datapb.CompactionSegment{
				SegmentID: segmentID,
				Channel:   t.plan.GetChannel(),
				Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{
								LogPath:       path,
								LogID:         logID,
								LogSize:       int64(writer.GetWrittenUncompressed()),
								MemorySize:    int64(writer.GetWrittenUncompressed()),
								EntriesNum:    int64(len(deletes.pks)),
								TimestampFrom: tsFrom,
								TimestampTo:   tsTo,
							},
						},
					},
				},
				NumOfRows: int64(len(deletes.pks)),
			}, nil
		}()
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

type BatchApplyRet = struct {
	StartIdx     int
	Segment2Hits map[int64][]bool
}

func (t *LevelZeroCompactionTask) applyBFInParallel(
	ctx context.Context,
	deltaData *storage.DeleteData,
	pool *conc.Pool[any],
	segmentBfs map[int64]*pkoracle.BloomFilterSet,
) *typeutil.ConcurrentMap[int, *BatchApplyRet] {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact applyBFInParallel")
	defer span.End()
	batchSize := t.compactionParams.BloomFilterApplyBatchSize

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

func (t *LevelZeroCompactionTask) process(ctx context.Context, l0MemSize int64, targetSegments []*datapb.CompactionSegmentBinlogs,
	l0Segments []*datapb.CompactionSegmentBinlogs,
) ([]*datapb.CompactionSegment, error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "L0Compact process")
	defer span.End()

	ratio := paramtable.Get().DataNodeCfg.L0BatchMemoryRatio.GetAsFloat()
	memLimit := float64(hardware.GetFreeMemoryCount()) * ratio
	if float64(l0MemSize) > memLimit {
		return nil, errors.Newf("L0 compaction failed, not enough memory, request memory size: %v, memory limit: %v", l0MemSize, memLimit)
	}

	log.Info("L0 compaction process start")
	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		return nil, err
	}

	allDelta, err := compaction.ComposeDeleteDataFromSegments(ctx, pkField.DataType, l0Segments,
		storage.WithDownloader(t.BinlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		log.Warn("L0 compaction compose delete data fail", zap.Error(err))
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

		batchResults, err := t.splitAndWrite(ctx, allDelta, segmentBFs)
		if err != nil {
			log.Warn("L0 compaction splitAndWrite fail", zap.Error(err))
			return nil, err
		}

		log.Info("L0 compaction finished one batch",
			zap.Int("batch no.", i),
			zap.Int64("total deltaRowCount", allDelta.RowCount),
			zap.Int("batch segment count", len(batchResults)))
		results = append(results, batchResults...)
	}

	log.Info("L0 compaction process done")
	return results, nil
}

func (t *LevelZeroCompactionTask) loadBF(ctx context.Context, targetSegments []*datapb.CompactionSegmentBinlogs,
) (map[int64]*pkoracle.BloomFilterSet, error) {
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
			err := binlog.DecompressBinLogWithRootPath(
				t.compactionParams.StorageConfig.GetRootPath(),
				storage.StatsBinlog,
				segment.GetCollectionID(),
				segment.GetPartitionID(),
				segment.GetSegmentID(),
				segment.GetField2StatslogPaths())
			if err != nil {
				log.Warn("failed to decompress segment stats log",
					zap.Int64("planID", t.plan.GetPlanID()),
					zap.String("type", t.plan.GetType().String()),
					zap.Error(err))
				return err, err
			}
			pks, err := compaction.LoadStats(innerCtx, t.cm,
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

func (t *LevelZeroCompactionTask) GetStorageConfig() *indexpb.StorageConfig {
	return t.compactionParams.StorageConfig
}
