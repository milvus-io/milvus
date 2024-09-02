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
	"path"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/clusteringpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ Compactor = (*clusteringCompactionTask)(nil)

type clusteringCompactionTask struct {
	binlogIO   io.BinlogIO
	logIDAlloc allocator.Interface
	segIDAlloc allocator.Interface

	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	tr          *timerecord.TimeRecorder
	mappingPool *conc.Pool[any]
	flushPool   *conc.Pool[any]

	plan *datapb.CompactionPlan

	// flush
	flushMutex sync.Mutex
	flushCount *atomic.Int64
	flushChan  chan FlushSignal
	doneChan   chan struct{}

	// metrics, don't use
	writtenRowNum *atomic.Int64
	hasSignal     *atomic.Bool

	// inner field
	collectionID          int64
	partitionID           int64
	currentTs             typeutil.Timestamp // for TTL
	isVectorClusteringKey bool
	clusteringKeyField    *schemapb.FieldSchema
	primaryKeyField       *schemapb.FieldSchema

	memoryBufferSize   int64
	clusterBuffers     []*ClusterBuffer
	clusterBufferLocks *lock.KeyLock[int]
	// scalar
	keyToBufferFunc func(interface{}) *ClusterBuffer
	// vector
	segmentIDOffsetMapping map[int64]string
	offsetToBufferFunc     func(int64, []uint32) *ClusterBuffer
}

type ClusterBuffer struct {
	id int

	writer    *SegmentWriter
	flushLock lock.RWMutex

	bufferMemorySize atomic.Int64

	flushedRowNum        map[typeutil.UniqueID]atomic.Int64
	currentSegmentRowNum atomic.Int64
	// segID -> fieldID -> binlogs
	flushedBinlogs map[typeutil.UniqueID]map[typeutil.UniqueID]*datapb.FieldBinlog

	uploadedSegments     []*datapb.CompactionSegment
	uploadedSegmentStats map[typeutil.UniqueID]storage.SegmentStats

	clusteringKeyFieldStats *storage.FieldStats
}

type FlushSignal struct {
	writer *SegmentWriter
	pack   bool
	id     int
	done   bool
}

func NewClusteringCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
) *clusteringCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &clusteringCompactionTask{
		ctx:                ctx,
		cancel:             cancel,
		binlogIO:           binlogIO,
		plan:               plan,
		tr:                 timerecord.NewTimeRecorder("clustering_compaction"),
		done:               make(chan struct{}, 1),
		flushChan:          make(chan FlushSignal, 100),
		doneChan:           make(chan struct{}),
		clusterBuffers:     make([]*ClusterBuffer, 0),
		clusterBufferLocks: lock.NewKeyLock[int](),
		flushCount:         atomic.NewInt64(0),
		writtenRowNum:      atomic.NewInt64(0),
		hasSignal:          atomic.NewBool(false),
	}
}

func (t *clusteringCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *clusteringCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *clusteringCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *clusteringCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *clusteringCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}

func (t *clusteringCompactionTask) GetCollection() int64 {
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *clusteringCompactionTask) init() error {
	if t.plan.GetType() != datapb.CompactionType_ClusteringCompaction {
		return merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
	if len(t.plan.GetSegmentBinlogs()) == 0 {
		return merr.WrapErrIllegalCompactionPlan("empty segment binlogs")
	}
	t.collectionID = t.GetCollection()
	t.partitionID = t.plan.GetSegmentBinlogs()[0].GetPartitionID()

	logIDAlloc := allocator.NewLocalAllocator(t.plan.GetBeginLogID(), math.MaxInt64)
	segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
	t.logIDAlloc = logIDAlloc
	t.segIDAlloc = segIDAlloc

	var pkField *schemapb.FieldSchema
	if t.plan.Schema == nil {
		return merr.WrapErrIllegalCompactionPlan("empty schema in compactionPlan")
	}
	for _, field := range t.plan.Schema.Fields {
		if field.GetIsPrimaryKey() && field.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(field.GetDataType()) {
			pkField = field
		}
		if field.GetFieldID() == t.plan.GetClusteringKeyField() {
			t.clusteringKeyField = field
		}
	}
	t.primaryKeyField = pkField
	t.isVectorClusteringKey = typeutil.IsVectorType(t.clusteringKeyField.DataType)
	t.currentTs = tsoutil.GetCurrentTime()
	t.memoryBufferSize = t.getMemoryBufferSize()
	workerPoolSize := t.getWorkerPoolSize()
	t.mappingPool = conc.NewPool[any](workerPoolSize)
	t.flushPool = conc.NewPool[any](workerPoolSize)
	log.Info("clustering compaction task initialed", zap.Int64("memory_buffer_size", t.memoryBufferSize), zap.Int("worker_pool_size", workerPoolSize))
	return nil
}

func (t *clusteringCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("clusteringCompaction-%d", t.GetPlanID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.plan.GetPlanID()), zap.String("type", t.plan.GetType().String()))
	// 0, verify and init
	err := t.init()
	if err != nil {
		log.Error("compaction task init failed", zap.Error(err))
		return nil, err
	}

	if !funcutil.CheckCtxValid(ctx) {
		log.Warn("compact wrong, task context done or timeout")
		return nil, ctx.Err()
	}
	ctxTimeout, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()
	defer t.cleanUp(ctx)

	// 1, download delta logs to build deltaMap
	deltaBlobs, _, err := composePaths(t.plan.GetSegmentBinlogs())
	if err != nil {
		return nil, err
	}
	deltaPk2Ts, err := mergeDeltalogs(ctxTimeout, t.binlogIO, deltaBlobs)
	if err != nil {
		return nil, err
	}

	// 2, get analyze result
	if t.isVectorClusteringKey {
		if err := t.getVectorAnalyzeResult(ctx); err != nil {
			return nil, err
		}
	} else {
		if err := t.getScalarAnalyzeResult(ctx); err != nil {
			return nil, err
		}
	}

	// 3, mapping
	log.Info("Clustering compaction start mapping", zap.Int("bufferNum", len(t.clusterBuffers)))
	uploadSegments, partitionStats, err := t.mapping(ctx, deltaPk2Ts)
	if err != nil {
		return nil, err
	}

	// 4, collect partition stats
	err = t.uploadPartitionStats(ctx, t.collectionID, t.partitionID, partitionStats)
	if err != nil {
		return nil, err
	}

	// 5, assemble CompactionPlanResult
	planResult := &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Segments: uploadSegments,
		Type:     t.plan.GetType(),
		Channel:  t.plan.GetChannel(),
	}

	metrics.DataNodeCompactionLatency.
		WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).
		Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	log.Info("Clustering compaction finished", zap.Duration("elapse", t.tr.ElapseSpan()), zap.Int64("flushTimes", t.flushCount.Load()))

	return planResult, nil
}

func (t *clusteringCompactionTask) getScalarAnalyzeResult(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("getScalarAnalyzeResult-%d", t.GetPlanID()))
	defer span.End()
	analyzeDict, err := t.scalarAnalyze(ctx)
	if err != nil {
		return err
	}
	plan := t.scalarPlan(analyzeDict)
	scalarToClusterBufferMap := make(map[interface{}]*ClusterBuffer, 0)
	for id, bucket := range plan {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}
		for _, key := range bucket {
			fieldStats.UpdateMinMax(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, key))
		}
		buffer := &ClusterBuffer{
			id:                      id,
			flushedRowNum:           map[typeutil.UniqueID]atomic.Int64{},
			flushedBinlogs:          make(map[typeutil.UniqueID]map[typeutil.UniqueID]*datapb.FieldBinlog, 0),
			uploadedSegments:        make([]*datapb.CompactionSegment, 0),
			uploadedSegmentStats:    make(map[typeutil.UniqueID]storage.SegmentStats, 0),
			clusteringKeyFieldStats: fieldStats,
		}
		if _, err = t.refreshBufferWriterWithPack(buffer); err != nil {
			return err
		}
		t.clusterBuffers = append(t.clusterBuffers, buffer)
		for _, key := range bucket {
			scalarToClusterBufferMap[key] = buffer
		}
	}
	t.keyToBufferFunc = func(key interface{}) *ClusterBuffer {
		// todo: if keys are too many, the map will be quite large, we should mark the range of each buffer and select buffer by range
		return scalarToClusterBufferMap[key]
	}
	return nil
}

func (t *clusteringCompactionTask) getVectorAnalyzeResult(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("getVectorAnalyzeResult-%d", t.GetPlanID()))
	defer span.End()
	analyzeResultPath := t.plan.AnalyzeResultPath
	centroidFilePath := path.Join(analyzeResultPath, metautil.JoinIDPath(t.collectionID, t.partitionID, t.clusteringKeyField.FieldID), common.Centroids)
	offsetMappingFiles := make(map[int64]string, 0)
	for _, segmentID := range t.plan.AnalyzeSegmentIds {
		path := path.Join(analyzeResultPath, metautil.JoinIDPath(t.collectionID, t.partitionID, t.clusteringKeyField.FieldID, segmentID), common.OffsetMapping)
		offsetMappingFiles[segmentID] = path
		log.Debug("read segment offset mapping file", zap.Int64("segmentID", segmentID), zap.String("path", path))
	}
	t.segmentIDOffsetMapping = offsetMappingFiles
	centroidBytes, err := t.binlogIO.Download(ctx, []string{centroidFilePath})
	if err != nil {
		return err
	}
	centroids := &clusteringpb.ClusteringCentroidsStats{}
	err = proto.Unmarshal(centroidBytes[0], centroids)
	if err != nil {
		return err
	}
	log.Debug("read clustering centroids stats", zap.String("path", centroidFilePath),
		zap.Int("centroidNum", len(centroids.GetCentroids())),
		zap.Any("offsetMappingFiles", t.segmentIDOffsetMapping))

	for id, centroid := range centroids.GetCentroids() {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}
		fieldStats.SetVectorCentroids(storage.NewVectorFieldValue(t.clusteringKeyField.DataType, centroid))
		clusterBuffer := &ClusterBuffer{
			id:                      id,
			flushedRowNum:           map[typeutil.UniqueID]atomic.Int64{},
			flushedBinlogs:          make(map[typeutil.UniqueID]map[typeutil.UniqueID]*datapb.FieldBinlog, 0),
			uploadedSegments:        make([]*datapb.CompactionSegment, 0),
			uploadedSegmentStats:    make(map[typeutil.UniqueID]storage.SegmentStats, 0),
			clusteringKeyFieldStats: fieldStats,
		}
		if _, err = t.refreshBufferWriterWithPack(clusterBuffer); err != nil {
			return err
		}
		t.clusterBuffers = append(t.clusterBuffers, clusterBuffer)
	}
	t.offsetToBufferFunc = func(offset int64, idMapping []uint32) *ClusterBuffer {
		return t.clusterBuffers[idMapping[offset]]
	}
	return nil
}

// mapping read and split input segments into buffers
func (t *clusteringCompactionTask) mapping(ctx context.Context,
	deltaPk2Ts map[interface{}]typeutil.Timestamp,
) ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("mapping-%d", t.GetPlanID()))
	defer span.End()
	inputSegments := t.plan.GetSegmentBinlogs()
	mapStart := time.Now()

	// start flush goroutine
	go t.backgroundFlush(ctx)

	futures := make([]*conc.Future[any], 0, len(inputSegments))
	for _, segment := range inputSegments {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID: segment.SegmentID,
			// only FieldBinlogs needed
			FieldBinlogs: segment.FieldBinlogs,
		}
		future := t.mappingPool.Submit(func() (any, error) {
			err := t.mappingSegment(ctx, segmentClone, deltaPk2Ts)
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, nil, err
	}

	t.flushChan <- FlushSignal{
		done: true,
	}

	// block util all writer flushed.
	<-t.doneChan

	// force flush all buffers
	err := t.flushAll(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err := t.checkBuffersAfterCompaction(); err != nil {
		return nil, nil, err
	}

	resultSegments := make([]*datapb.CompactionSegment, 0)
	resultPartitionStats := &storage.PartitionStatsSnapshot{
		SegmentStats: make(map[typeutil.UniqueID]storage.SegmentStats),
	}
	for _, buffer := range t.clusterBuffers {
		for _, seg := range buffer.uploadedSegments {
			se := &datapb.CompactionSegment{
				PlanID:              seg.GetPlanID(),
				SegmentID:           seg.GetSegmentID(),
				NumOfRows:           seg.GetNumOfRows(),
				InsertLogs:          seg.GetInsertLogs(),
				Field2StatslogPaths: seg.GetField2StatslogPaths(),
				Deltalogs:           seg.GetDeltalogs(),
				Channel:             seg.GetChannel(),
			}
			log.Debug("put segment into final compaction result", zap.String("segment", se.String()))
			resultSegments = append(resultSegments, se)
		}
		for segID, segmentStat := range buffer.uploadedSegmentStats {
			log.Debug("put segment into final partition stats", zap.Int64("segmentID", segID), zap.Any("stats", segmentStat))
			resultPartitionStats.SegmentStats[segID] = segmentStat
		}
	}

	log.Info("mapping end",
		zap.Int64("collectionID", t.GetCollection()),
		zap.Int64("partitionID", t.partitionID),
		zap.Int("segmentFrom", len(inputSegments)),
		zap.Int("segmentTo", len(resultSegments)),
		zap.Duration("elapse", time.Since(mapStart)))

	return resultSegments, resultPartitionStats, nil
}

func (t *clusteringCompactionTask) getBufferTotalUsedMemorySize() int64 {
	var totalBufferSize int64 = 0
	for _, buffer := range t.clusterBuffers {
		totalBufferSize = totalBufferSize + int64(buffer.writer.WrittenMemorySize()) + buffer.bufferMemorySize.Load()
	}
	return totalBufferSize
}

// read insert log of one segment, mappingSegment into buckets according to clusteringKey. flush data to file when necessary
func (t *clusteringCompactionTask) mappingSegment(
	ctx context.Context,
	segment *datapb.CompactionSegmentBinlogs,
	delta map[interface{}]typeutil.Timestamp,
) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("mappingSegment-%d-%d", t.GetPlanID(), segment.GetSegmentID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.GetCollection()),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", segment.GetSegmentID()))
	log.Info("mapping segment start")
	processStart := time.Now()
	fieldBinlogPaths := make([][]string, 0)
	var (
		expired  int64 = 0
		deleted  int64 = 0
		remained int64 = 0
	)

	isDeletedValue := func(v *storage.Value) bool {
		ts, ok := delta[v.PK.GetValue()]
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if ok && uint64(v.Timestamp) < ts {
			return true
		}
		return false
	}

	mappingStats := &clusteringpb.ClusteringCentroidIdMappingStats{}
	if t.isVectorClusteringKey {
		offSetPath := t.segmentIDOffsetMapping[segment.SegmentID]
		offsetBytes, err := t.binlogIO.Download(ctx, []string{offSetPath})
		if err != nil {
			return err
		}
		err = proto.Unmarshal(offsetBytes[0], mappingStats)
		if err != nil {
			return err
		}
	}

	// Get the number of field binlog files from non-empty segment
	var binlogNum int
	for _, b := range segment.GetFieldBinlogs() {
		if b != nil {
			binlogNum = len(b.GetBinlogs())
			break
		}
	}
	// Unable to deal with all empty segments cases, so return error
	if binlogNum == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return merr.WrapErrIllegalCompactionPlan()
	}
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, f := range segment.GetFieldBinlogs() {
			ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	for _, paths := range fieldBinlogPaths {
		allValues, err := t.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
			return err
		}
		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})
		pkIter, err := storage.NewBinlogDeserializeReader(blobs, t.primaryKeyField.GetFieldID())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("paths", paths), zap.Error(err))
			return err
		}

		var offset int64 = -1
		for {
			err := pkIter.Next()
			if err != nil {
				if err == sio.EOF {
					pkIter.Close()
					break
				} else {
					log.Warn("compact wrong, failed to iter through data", zap.Error(err))
					return err
				}
			}
			v := pkIter.Value()
			offset++

			// Filtering deleted entity
			if isDeletedValue(v) {
				deleted++
				continue
			}
			// Filtering expired entity
			ts := typeutil.Timestamp(v.Timestamp)
			if isExpiredEntity(t.plan.GetCollectionTtl(), t.currentTs, ts) {
				expired++
				continue
			}

			row, ok := v.Value.(map[typeutil.UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("paths", paths))
				return errors.New("unexpected error")
			}

			clusteringKey := row[t.clusteringKeyField.FieldID]
			var clusterBuffer *ClusterBuffer
			if t.isVectorClusteringKey {
				clusterBuffer = t.offsetToBufferFunc(offset, mappingStats.GetCentroidIdMapping())
			} else {
				clusterBuffer = t.keyToBufferFunc(clusteringKey)
			}
			err = t.writeToBuffer(ctx, clusterBuffer, v)
			if err != nil {
				return err
			}
			remained++

			if (remained+1)%100 == 0 {
				currentBufferTotalMemorySize := t.getBufferTotalUsedMemorySize()
				if clusterBuffer.currentSegmentRowNum.Load() > t.plan.GetMaxSegmentRows() || clusterBuffer.writer.IsFull() {
					// reach segment/binlog max size
					flushWriterFunc := func() {
						t.clusterBufferLocks.Lock(clusterBuffer.id)
						currentSegmentNumRows := clusterBuffer.currentSegmentRowNum.Load()
						// double-check the condition is still met
						if currentSegmentNumRows > t.plan.GetMaxSegmentRows() || clusterBuffer.writer.IsFull() {
							writer := clusterBuffer.writer
							pack, _ := t.refreshBufferWriterWithPack(clusterBuffer)
							log.Debug("buffer need to flush", zap.Int("bufferID", clusterBuffer.id),
								zap.Bool("pack", pack),
								zap.Int64("current segment", writer.GetSegmentID()),
								zap.Int64("current segment num rows", currentSegmentNumRows),
								zap.Int64("writer num", writer.GetRowNum()))

							t.clusterBufferLocks.Unlock(clusterBuffer.id)
							// release the lock before sending the signal, avoid long wait caused by a full channel.
							t.flushChan <- FlushSignal{
								writer: writer,
								pack:   pack,
								id:     clusterBuffer.id,
							}
							return
						}
						// release the lock even if the conditions are no longer met.
						t.clusterBufferLocks.Unlock(clusterBuffer.id)
					}
					flushWriterFunc()
				} else if currentBufferTotalMemorySize > t.getMemoryBufferHighWatermark() && !t.hasSignal.Load() {
					// reach flushBinlog trigger threshold
					log.Debug("largest buffer need to flush",
						zap.Int64("currentBufferTotalMemorySize", currentBufferTotalMemorySize))
					t.flushChan <- FlushSignal{}
					t.hasSignal.Store(true)
				}

				// if the total buffer size is too large, block here, wait for memory release by flushBinlog
				if t.getBufferTotalUsedMemorySize() > t.getMemoryBufferBlockFlushThreshold() {
					log.Debug("memory is already above the block watermark, pause writing",
						zap.Int64("currentBufferTotalMemorySize", currentBufferTotalMemorySize))
				loop:
					for {
						select {
						case <-ctx.Done():
							log.Warn("stop waiting for memory buffer release as context done")
							return nil
						case <-t.done:
							log.Warn("stop waiting for memory buffer release as task chan done")
							return nil
						default:
							// currentSize := t.getCurrentBufferWrittenMemorySize()
							currentSize := t.getBufferTotalUsedMemorySize()
							if currentSize < t.getMemoryBufferHighWatermark() {
								log.Debug("memory is already below the high watermark, continue writing",
									zap.Int64("currentSize", currentSize))
								break loop
							}
							time.Sleep(time.Millisecond * 200)
						}
					}
				}
			}
		}
	}

	log.Info("mapping segment end",
		zap.Int64("remained_entities", remained),
		zap.Int64("deleted_entities", deleted),
		zap.Int64("expired_entities", expired),
		zap.Int64("written_row_num", t.writtenRowNum.Load()),
		zap.Duration("elapse", time.Since(processStart)))
	return nil
}

func (t *clusteringCompactionTask) writeToBuffer(ctx context.Context, clusterBuffer *ClusterBuffer, value *storage.Value) error {
	t.clusterBufferLocks.Lock(clusterBuffer.id)
	defer t.clusterBufferLocks.Unlock(clusterBuffer.id)
	// prepare
	if clusterBuffer.writer == nil {
		log.Warn("unexpected behavior, please check", zap.Int("buffer id", clusterBuffer.id))
		return fmt.Errorf("unexpected behavior, please check buffer id: %d", clusterBuffer.id)
	}
	err := clusterBuffer.writer.Write(value)
	if err != nil {
		return err
	}
	t.writtenRowNum.Inc()
	clusterBuffer.currentSegmentRowNum.Inc()
	return nil
}

func (t *clusteringCompactionTask) getWorkerPoolSize() int {
	return int(math.Max(float64(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.GetAsInt()), 1.0))
}

// getMemoryBufferSize return memoryBufferSize
func (t *clusteringCompactionTask) getMemoryBufferSize() int64 {
	return int64(float64(hardware.GetMemoryCount()) * paramtable.Get().DataNodeCfg.ClusteringCompactionMemoryBufferRatio.GetAsFloat())
}

func (t *clusteringCompactionTask) getMemoryBufferLowWatermark() int64 {
	return int64(float64(t.memoryBufferSize) * 0.3)
}

func (t *clusteringCompactionTask) getMemoryBufferHighWatermark() int64 {
	return int64(float64(t.memoryBufferSize) * 0.7)
}

func (t *clusteringCompactionTask) getMemoryBufferBlockFlushThreshold() int64 {
	return t.memoryBufferSize
}

func (t *clusteringCompactionTask) backgroundFlush(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("clustering compaction task context exit")
			return
		case <-t.done:
			log.Info("clustering compaction task done")
			return
		case signal := <-t.flushChan:
			var err error
			if signal.done {
				t.doneChan <- struct{}{}
			} else if signal.writer == nil {
				t.hasSignal.Store(false)
				err = t.flushLargestBuffers(ctx)
			} else {
				future := t.flushPool.Submit(func() (any, error) {
					err := t.flushBinlog(ctx, t.clusterBuffers[signal.id], signal.writer, signal.pack)
					if err != nil {
						return nil, err
					}
					return struct{}{}, nil
				})
				err = conc.AwaitAll(future)
			}
			if err != nil {
				log.Warn("fail to flushBinlog data", zap.Error(err))
				// todo handle error
			}
		}
	}
}

func (t *clusteringCompactionTask) flushLargestBuffers(ctx context.Context) error {
	// only one flushLargestBuffers or flushAll should do at the same time
	getLock := t.flushMutex.TryLock()
	if !getLock {
		return nil
	}
	defer t.flushMutex.Unlock()
	currentMemorySize := t.getBufferTotalUsedMemorySize()
	if currentMemorySize <= t.getMemoryBufferLowWatermark() {
		log.Info("memory low water mark", zap.Int64("memoryBufferSize", t.getBufferTotalUsedMemorySize()))
		return nil
	}
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "flushLargestBuffers")
	defer span.End()
	bufferIDs := make([]int, 0)
	bufferRowNums := make([]int64, 0)
	for _, buffer := range t.clusterBuffers {
		bufferIDs = append(bufferIDs, buffer.id)
		t.clusterBufferLocks.RLock(buffer.id)
		bufferRowNums = append(bufferRowNums, buffer.writer.GetRowNum())
		t.clusterBufferLocks.RUnlock(buffer.id)
	}
	sort.Slice(bufferIDs, func(i, j int) bool {
		return bufferRowNums[i] > bufferRowNums[j]
	})
	log.Info("start flushLargestBuffers", zap.Ints("bufferIDs", bufferIDs), zap.Int64("currentMemorySize", currentMemorySize))

	futures := make([]*conc.Future[any], 0)
	for _, bufferId := range bufferIDs {
		t.clusterBufferLocks.Lock(bufferId)
		buffer := t.clusterBuffers[bufferId]
		writer := buffer.writer
		currentMemorySize -= int64(writer.WrittenMemorySize())
		if err := t.refreshBufferWriter(buffer); err != nil {
			t.clusterBufferLocks.Unlock(bufferId)
			return err
		}
		t.clusterBufferLocks.Unlock(bufferId)

		log.Info("currentMemorySize after flush buffer binlog",
			zap.Int64("currentMemorySize", currentMemorySize),
			zap.Int("bufferID", bufferId),
			zap.Uint64("WrittenMemorySize()", writer.WrittenMemorySize()),
			zap.Int64("RowNum", writer.GetRowNum()))
		future := t.flushPool.Submit(func() (any, error) {
			err := t.flushBinlog(ctx, buffer, writer, false)
			if err != nil {
				return nil, err
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)

		if currentMemorySize <= t.getMemoryBufferLowWatermark() {
			log.Info("reach memory low water mark", zap.Int64("memoryBufferSize", t.getBufferTotalUsedMemorySize()))
			break
		}
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}

	log.Info("flushLargestBuffers end", zap.Int64("currentMemorySize", currentMemorySize))
	return nil
}

func (t *clusteringCompactionTask) flushAll(ctx context.Context) error {
	// only one flushLargestBuffers or flushAll should do at the same time
	t.flushMutex.Lock()
	defer t.flushMutex.Unlock()
	futures := make([]*conc.Future[any], 0)
	for _, buffer := range t.clusterBuffers {
		buffer := buffer
		future := t.flushPool.Submit(func() (any, error) {
			err := t.flushBinlog(ctx, buffer, buffer.writer, true)
			if err != nil {
				return nil, err
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}

	return nil
}

func (t *clusteringCompactionTask) packBufferToSegment(ctx context.Context, buffer *ClusterBuffer, segmentID int64) error {
	if binlogs, ok := buffer.flushedBinlogs[segmentID]; !ok || len(binlogs) == 0 {
		return nil
	}

	binlogNum := 0
	numRows := buffer.flushedRowNum[segmentID]
	insertLogs := make([]*datapb.FieldBinlog, 0)
	for _, fieldBinlog := range buffer.flushedBinlogs[segmentID] {
		insertLogs = append(insertLogs, fieldBinlog)
		binlogNum = len(fieldBinlog.GetBinlogs())
	}

	fieldBinlogPaths := make([][]string, 0)
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, fieldID := range []int64{t.primaryKeyField.GetFieldID(), common.RowIDField, common.TimeStampField} {
			ps = append(ps, buffer.flushedBinlogs[segmentID][fieldID].GetBinlogs()[idx].GetLogPath())
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	statsLogs, err := t.generatePkStats(ctx, segmentID, numRows.Load(), fieldBinlogPaths)
	if err != nil {
		return err
	}

	// pack current flushBinlog data into a segment
	seg := &datapb.CompactionSegment{
		PlanID:              t.plan.GetPlanID(),
		SegmentID:           segmentID,
		NumOfRows:           numRows.Load(),
		InsertLogs:          insertLogs,
		Field2StatslogPaths: []*datapb.FieldBinlog{statsLogs},
		Channel:             t.plan.GetChannel(),
	}
	buffer.uploadedSegments = append(buffer.uploadedSegments, seg)
	segmentStats := storage.SegmentStats{
		FieldStats: []storage.FieldStats{buffer.clusteringKeyFieldStats.Clone()},
		NumRows:    int(numRows.Load()),
	}
	buffer.uploadedSegmentStats[segmentID] = segmentStats

	for _, binlog := range seg.InsertLogs {
		log.Debug("pack binlog in segment", zap.Int64("partitionID", t.partitionID),
			zap.Int64("segID", segmentID), zap.String("binlog", binlog.String()))
	}
	for _, statsLog := range seg.Field2StatslogPaths {
		log.Debug("pack binlog in segment", zap.Int64("partitionID", t.partitionID),
			zap.Int64("segID", segmentID), zap.String("binlog", statsLog.String()))
	}

	log.Debug("finish pack segment", zap.Int64("partitionID", t.partitionID),
		zap.Int64("segID", seg.GetSegmentID()),
		zap.Int64("row num", seg.GetNumOfRows()))

	// clear segment binlogs cache
	delete(buffer.flushedBinlogs, segmentID)
	return nil
}

func (t *clusteringCompactionTask) flushBinlog(ctx context.Context, buffer *ClusterBuffer, writer *SegmentWriter, pack bool) error {
	segmentID := writer.GetSegmentID()
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("flushBinlog-%d", segmentID))
	defer span.End()
	if writer == nil {
		log.Warn("buffer writer is nil, please check", zap.Int("buffer id", buffer.id))
		return fmt.Errorf("buffer: %d writer is nil, please check", buffer.id)
	}
	defer func() {
		// set old writer nil
		writer = nil
	}()
	buffer.flushLock.Lock()
	defer buffer.flushLock.Unlock()
	writtenMemorySize := int64(writer.WrittenMemorySize())
	writtenRowNum := writer.GetRowNum()
	log := log.With(zap.Int("bufferID", buffer.id),
		zap.Int64("segmentID", segmentID),
		zap.Bool("pack", pack),
		zap.Int64("writerRowNum", writtenRowNum),
		zap.Int64("writtenMemorySize", writtenMemorySize),
		zap.Int64("bufferMemorySize", buffer.bufferMemorySize.Load()),
	)

	log.Info("start flush binlog")
	if writtenRowNum <= 0 {
		log.Debug("writerRowNum is zero, skip flush")
		if pack {
			return t.packBufferToSegment(ctx, buffer, segmentID)
		}
		return nil
	}

	start := time.Now()
	kvs, partialBinlogs, err := serializeWrite(ctx, t.logIDAlloc, writer)
	if err != nil {
		log.Warn("compact wrong, failed to serialize writer", zap.Error(err))
		return err
	}

	if err := t.binlogIO.Upload(ctx, kvs); err != nil {
		log.Warn("compact wrong, failed to upload kvs", zap.Error(err))
		return err
	}

	if info, ok := buffer.flushedBinlogs[segmentID]; !ok || info == nil {
		buffer.flushedBinlogs[segmentID] = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
	}

	for fID, path := range partialBinlogs {
		tmpBinlog, ok := buffer.flushedBinlogs[segmentID][fID]
		if !ok {
			tmpBinlog = path
		} else {
			tmpBinlog.Binlogs = append(tmpBinlog.Binlogs, path.GetBinlogs()...)
		}
		buffer.flushedBinlogs[segmentID][fID] = tmpBinlog
	}

	curSegFlushedRowNum := buffer.flushedRowNum[segmentID]
	curSegFlushedRowNum.Add(writtenRowNum)
	buffer.flushedRowNum[segmentID] = curSegFlushedRowNum

	// clean buffer with writer
	buffer.bufferMemorySize.Sub(writtenMemorySize)

	t.flushCount.Inc()
	if pack {
		if err := t.packBufferToSegment(ctx, buffer, segmentID); err != nil {
			return err
		}
	}

	writer = nil
	runtime.GC()
	debug.FreeOSMemory()
	log.Info("finish flush binlogs", zap.Int64("flushCount", t.flushCount.Load()),
		zap.Int64("cost", time.Since(start).Milliseconds()))
	return nil
}

func (t *clusteringCompactionTask) uploadPartitionStats(ctx context.Context, collectionID, partitionID typeutil.UniqueID, partitionStats *storage.PartitionStatsSnapshot) error {
	// use planID as partitionStats version
	version := t.plan.PlanID
	partitionStats.Version = version
	partitionStatsBytes, err := storage.SerializePartitionStatsSnapshot(partitionStats)
	if err != nil {
		return err
	}
	rootPath := strings.Split(t.plan.AnalyzeResultPath, common.AnalyzeStatsPath)[0]
	newStatsPath := path.Join(rootPath, common.PartitionStatsPath, metautil.JoinIDPath(collectionID, partitionID), t.plan.GetChannel(), strconv.FormatInt(version, 10))
	kv := map[string][]byte{
		newStatsPath: partitionStatsBytes,
	}
	err = t.binlogIO.Upload(ctx, kv)
	if err != nil {
		return err
	}
	log.Info("Finish upload PartitionStats file", zap.String("key", newStatsPath), zap.Int("length", len(partitionStatsBytes)))
	return nil
}

// cleanUp try best to clean all temp datas
func (t *clusteringCompactionTask) cleanUp(ctx context.Context) {
}

func (t *clusteringCompactionTask) scalarAnalyze(ctx context.Context) (map[interface{}]int64, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("scalarAnalyze-%d", t.GetPlanID()))
	defer span.End()
	inputSegments := t.plan.GetSegmentBinlogs()
	futures := make([]*conc.Future[any], 0, len(inputSegments))
	analyzeStart := time.Now()
	var mutex sync.Mutex
	analyzeDict := make(map[interface{}]int64, 0)
	for _, segment := range inputSegments {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID:           segment.SegmentID,
			FieldBinlogs:        segment.FieldBinlogs,
			Field2StatslogPaths: segment.Field2StatslogPaths,
			Deltalogs:           segment.Deltalogs,
			InsertChannel:       segment.InsertChannel,
			Level:               segment.Level,
			CollectionID:        segment.CollectionID,
			PartitionID:         segment.PartitionID,
			IsSorted:            segment.IsSorted,
		}
		future := t.mappingPool.Submit(func() (any, error) {
			analyzeResult, err := t.scalarAnalyzeSegment(ctx, segmentClone)
			mutex.Lock()
			defer mutex.Unlock()
			for key, v := range analyzeResult {
				if _, exist := analyzeDict[key]; exist {
					analyzeDict[key] = analyzeDict[key] + v
				} else {
					analyzeDict[key] = v
				}
			}
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}
	log.Info("analyze end",
		zap.Int64("collectionID", t.GetCollection()),
		zap.Int64("partitionID", t.partitionID),
		zap.Int("segments", len(inputSegments)),
		zap.Duration("elapse", time.Since(analyzeStart)))
	return analyzeDict, nil
}

func (t *clusteringCompactionTask) scalarAnalyzeSegment(
	ctx context.Context,
	segment *datapb.CompactionSegmentBinlogs,
) (map[interface{}]int64, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("scalarAnalyzeSegment-%d-%d", t.GetPlanID(), segment.GetSegmentID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.Int64("segmentID", segment.GetSegmentID()))

	// vars
	processStart := time.Now()
	fieldBinlogPaths := make([][]string, 0)
	// initial timestampFrom, timestampTo = -1, -1 is an illegal value, only to mark initial state
	var (
		timestampTo   int64                 = -1
		timestampFrom int64                 = -1
		expired       int64                 = 0
		deleted       int64                 = 0
		remained      int64                 = 0
		analyzeResult map[interface{}]int64 = make(map[interface{}]int64, 0)
	)

	// Get the number of field binlog files from non-empty segment
	var binlogNum int
	for _, b := range segment.GetFieldBinlogs() {
		if b != nil {
			binlogNum = len(b.GetBinlogs())
			break
		}
	}
	// Unable to deal with all empty segments cases, so return error
	if binlogNum == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, merr.WrapErrIllegalCompactionPlan("all segments' binlogs are empty")
	}
	log.Debug("binlogNum", zap.Int("binlogNum", binlogNum))
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, f := range segment.GetFieldBinlogs() {
			// todo add a new reader only read one column
			if f.FieldID == t.primaryKeyField.GetFieldID() || f.FieldID == t.clusteringKeyField.GetFieldID() || f.FieldID == common.RowIDField || f.FieldID == common.TimeStampField {
				ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
			}
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	for _, path := range fieldBinlogPaths {
		bytesArr, err := t.binlogIO.Download(ctx, path)
		blobs := make([]*storage.Blob, len(bytesArr))
		for i := range bytesArr {
			blobs[i] = &storage.Blob{Value: bytesArr[i]}
		}
		if err != nil {
			log.Warn("download insertlogs wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		pkIter, err := storage.NewInsertBinlogIterator(blobs, t.primaryKeyField.GetFieldID(), t.primaryKeyField.GetDataType())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		// log.Info("pkIter.RowNum()", zap.Int("pkIter.RowNum()", pkIter.RowNum()), zap.Bool("hasNext", pkIter.HasNext()))
		for pkIter.HasNext() {
			vIter, _ := pkIter.Next()
			v, ok := vIter.(*storage.Value)
			if !ok {
				log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}

			// Filtering expired entity
			ts := typeutil.Timestamp(v.Timestamp)
			if isExpiredEntity(t.plan.GetCollectionTtl(), t.currentTs, ts) {
				expired++
				continue
			}

			// Update timestampFrom, timestampTo
			if v.Timestamp < timestampFrom || timestampFrom == -1 {
				timestampFrom = v.Timestamp
			}
			if v.Timestamp > timestampTo || timestampFrom == -1 {
				timestampTo = v.Timestamp
			}
			// rowValue := vIter.GetData().(*iterators.InsertRow).GetValue()
			row, ok := v.Value.(map[typeutil.UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}
			key := row[t.clusteringKeyField.GetFieldID()]
			if _, exist := analyzeResult[key]; exist {
				analyzeResult[key] = analyzeResult[key] + 1
			} else {
				analyzeResult[key] = 1
			}
			remained++
		}
	}

	log.Info("analyze segment end",
		zap.Int64("remained entities", remained),
		zap.Int64("deleted entities", deleted),
		zap.Int64("expired entities", expired),
		zap.Duration("map elapse", time.Since(processStart)))
	return analyzeResult, nil
}

func (t *clusteringCompactionTask) scalarPlan(dict map[interface{}]int64) [][]interface{} {
	keys := lo.MapToSlice(dict, func(k interface{}, _ int64) interface{} {
		return k
	})
	sort.Slice(keys, func(i, j int) bool {
		return storage.NewScalarFieldValue(t.clusteringKeyField.DataType, keys[i]).LE(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, keys[j]))
	})

	buckets := make([][]interface{}, 0)
	currentBucket := make([]interface{}, 0)
	var currentBucketSize int64 = 0
	maxRows := t.plan.MaxSegmentRows
	preferRows := t.plan.PreferSegmentRows
	for _, key := range keys {
		// todo can optimize
		if dict[key] > preferRows {
			if len(currentBucket) != 0 {
				buckets = append(buckets, currentBucket)
				currentBucket = make([]interface{}, 0)
				currentBucketSize = 0
			}
			buckets = append(buckets, []interface{}{key})
		} else if currentBucketSize+dict[key] > maxRows {
			buckets = append(buckets, currentBucket)
			currentBucket = []interface{}{key}
			currentBucketSize = dict[key]
		} else if currentBucketSize+dict[key] > preferRows {
			currentBucket = append(currentBucket, key)
			buckets = append(buckets, currentBucket)
			currentBucket = make([]interface{}, 0)
			currentBucketSize = 0
		} else {
			currentBucket = append(currentBucket, key)
			currentBucketSize += dict[key]
		}
	}
	buckets = append(buckets, currentBucket)
	return buckets
}

func (t *clusteringCompactionTask) refreshBufferWriterWithPack(buffer *ClusterBuffer) (bool, error) {
	var segmentID int64
	var err error
	var pack bool
	if buffer.writer != nil {
		segmentID = buffer.writer.GetSegmentID()
		buffer.bufferMemorySize.Add(int64(buffer.writer.WrittenMemorySize()))
	}
	if buffer.writer == nil || buffer.currentSegmentRowNum.Load() > t.plan.GetMaxSegmentRows() {
		pack = true
		segmentID, err = t.segIDAlloc.AllocOne()
		if err != nil {
			return pack, err
		}
		buffer.currentSegmentRowNum.Store(0)
	}

	writer, err := NewSegmentWriter(t.plan.GetSchema(), t.plan.MaxSegmentRows, segmentID, t.partitionID, t.collectionID)
	if err != nil {
		return pack, err
	}

	buffer.writer = writer
	return pack, nil
}

func (t *clusteringCompactionTask) refreshBufferWriter(buffer *ClusterBuffer) error {
	var segmentID int64
	var err error
	segmentID = buffer.writer.GetSegmentID()
	buffer.bufferMemorySize.Add(int64(buffer.writer.WrittenMemorySize()))

	writer, err := NewSegmentWriter(t.plan.GetSchema(), t.plan.MaxSegmentRows, segmentID, t.partitionID, t.collectionID)
	if err != nil {
		return err
	}

	buffer.writer = writer
	return nil
}

func (t *clusteringCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}

func (t *clusteringCompactionTask) checkBuffersAfterCompaction() error {
	for _, buffer := range t.clusterBuffers {
		if len(buffer.flushedBinlogs) != 0 {
			log.Warn("there are some binlogs have leaked, please check", zap.Int("buffer id", buffer.id),
				zap.Int64s("leak segments", lo.Keys(buffer.flushedBinlogs)))
			log.Debug("leak binlogs", zap.Any("buffer flushedBinlogs", buffer.flushedBinlogs))
			return fmt.Errorf("there are some binlogs have leaked")
		}
	}
	return nil
}

func (t *clusteringCompactionTask) generatePkStats(ctx context.Context, segmentID int64,
	numRows int64, binlogPaths [][]string,
) (*datapb.FieldBinlog, error) {
	stats, err := storage.NewPrimaryKeyStats(t.primaryKeyField.GetFieldID(), int64(t.primaryKeyField.GetDataType()), numRows)
	if err != nil {
		return nil, err
	}

	for _, path := range binlogPaths {
		bytesArr, err := t.binlogIO.Download(ctx, path)
		if err != nil {
			log.Warn("download insertlogs wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}
		blobs := make([]*storage.Blob, len(bytesArr))
		for i := range bytesArr {
			blobs[i] = &storage.Blob{Value: bytesArr[i]}
		}

		pkIter, err := storage.NewInsertBinlogIterator(blobs, t.primaryKeyField.GetFieldID(), t.primaryKeyField.GetDataType())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", path), zap.Error(err))
			return nil, err
		}

		for pkIter.HasNext() {
			vIter, _ := pkIter.Next()
			v, ok := vIter.(*storage.Value)
			if !ok {
				log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
				return nil, errors.New("unexpected error")
			}
			stats.Update(v.PK)
		}
	}

	codec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: t.collectionID, Schema: t.plan.GetSchema()})
	sblob, err := codec.SerializePkStats(stats, numRows)
	if err != nil {
		return nil, err
	}

	return uploadStatsBlobs(ctx, t.collectionID, t.partitionID, segmentID, t.primaryKeyField.GetFieldID(), numRows, t.binlogIO, t.logIDAlloc, sblob)
}
