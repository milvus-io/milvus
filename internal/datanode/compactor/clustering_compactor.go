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
	"fmt"
	sio "io"
	"math"
	"path"
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
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	expectedBinlogSize = 16 * 1024 * 1024
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
	flushCount *atomic.Int64

	// metrics, don't use
	writtenRowNum *atomic.Int64

	// inner field
	collectionID          int64
	partitionID           int64
	currentTime           time.Time // for TTL
	isVectorClusteringKey bool
	clusteringKeyField    *schemapb.FieldSchema
	primaryKeyField       *schemapb.FieldSchema

	memoryBufferSize int64
	clusterBuffers   []*ClusterBuffer
	// scalar
	keyToBufferFunc func(interface{}) *ClusterBuffer
	// vector
	segmentIDOffsetMapping map[int64]string
	offsetToBufferFunc     func(int64, []uint32) *ClusterBuffer
	// bm25
	bm25FieldIds []int64
}

type ClusterBuffer struct {
	id                      int
	writer                  *MultiSegmentWriter
	clusteringKeyFieldStats *storage.FieldStats

	lock sync.RWMutex
}

func (b *ClusterBuffer) Write(v *storage.Value) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.writer.WriteValue(v)
}

func (b *ClusterBuffer) Flush() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.writer.FlushChunk()
}

func (b *ClusterBuffer) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.writer.Close()
}

func (b *ClusterBuffer) GetCompactionSegments() []*datapb.CompactionSegment {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.writer.GetCompactionSegments()
}

func (b *ClusterBuffer) GetBufferSize() uint64 {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.writer.GetBufferUncompressed()
}

func newClusterBuffer(id int, writer *MultiSegmentWriter, clusteringKeyFieldStats *storage.FieldStats) *ClusterBuffer {
	return &ClusterBuffer{
		id:                      id,
		writer:                  writer,
		clusteringKeyFieldStats: clusteringKeyFieldStats,
		lock:                    sync.RWMutex{},
	}
}

func NewClusteringCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
) *clusteringCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &clusteringCompactionTask{
		ctx:            ctx,
		cancel:         cancel,
		binlogIO:       binlogIO,
		plan:           plan,
		tr:             timerecord.NewTimeRecorder("clustering_compaction"),
		done:           make(chan struct{}, 1),
		clusterBuffers: make([]*ClusterBuffer, 0),
		flushCount:     atomic.NewInt64(0),
		writtenRowNum:  atomic.NewInt64(0),
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
	log.Info("segment ID range", zap.Int64("begin", t.plan.GetPreAllocatedSegmentIDs().GetBegin()), zap.Int64("end", t.plan.GetPreAllocatedSegmentIDs().GetEnd()))
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

	for _, function := range t.plan.Schema.Functions {
		if function.GetType() == schemapb.FunctionType_BM25 {
			t.bm25FieldIds = append(t.bm25FieldIds, function.GetOutputFieldIds()[0])
		}
	}

	t.primaryKeyField = pkField
	t.isVectorClusteringKey = typeutil.IsVectorType(t.clusteringKeyField.DataType)
	t.currentTime = time.Now()
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
	defer t.cleanUp(ctx)

	// 1, decompose binlogs as preparation for later mapping
	if err := binlog.DecompressCompactionBinlogs(t.plan.SegmentBinlogs); err != nil {
		log.Warn("compact wrong, fail to decompress compaction binlogs", zap.Error(err))
		return nil, err
	}

	// 2, get analyze result
	if t.isVectorClusteringKey {
		if err := t.getVectorAnalyzeResult(ctx); err != nil {
			log.Error("failed in analyze vector", zap.Error(err))
			return nil, err
		}
	} else {
		if err := t.getScalarAnalyzeResult(ctx); err != nil {
			log.Error("failed in analyze scalar", zap.Error(err))
			return nil, err
		}
	}

	// 3, mapping
	log.Info("Clustering compaction start mapping", zap.Int("bufferNum", len(t.clusterBuffers)))
	uploadSegments, partitionStats, err := t.mapping(ctx)
	if err != nil {
		log.Error("failed in mapping", zap.Error(err))
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
	// clear the buffer cache
	t.keyToBufferFunc = nil

	return planResult, nil
}

func (t *clusteringCompactionTask) getScalarAnalyzeResult(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("getScalarAnalyzeResult-%d", t.GetPlanID()))
	defer span.End()
	analyzeDict, err := t.scalarAnalyze(ctx)
	if err != nil {
		return err
	}
	buckets, containsNull := t.splitClusterByScalarValue(analyzeDict)
	scalarToClusterBufferMap := make(map[interface{}]*ClusterBuffer, 0)
	for id, bucket := range buckets {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}
		for _, key := range bucket {
			fieldStats.UpdateMinMax(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, key))
		}

		alloc := NewCompactionAllocator(t.segIDAlloc, t.logIDAlloc)
		writer := NewMultiSegmentWriter(ctx, t.binlogIO, alloc, t.plan.GetMaxSize(), t.plan.GetSchema(), t.plan.MaxSegmentRows, t.partitionID, t.collectionID, t.plan.Channel, 100)

		buffer := newClusterBuffer(id, writer, fieldStats)
		t.clusterBuffers = append(t.clusterBuffers, buffer)
		for _, key := range bucket {
			scalarToClusterBufferMap[key] = buffer
		}
	}
	var nullBuffer *ClusterBuffer
	if containsNull {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}

		alloc := NewCompactionAllocator(t.segIDAlloc, t.logIDAlloc)
		writer := NewMultiSegmentWriter(ctx, t.binlogIO, alloc, t.plan.GetMaxSize(), t.plan.GetSchema(), t.plan.MaxSegmentRows, t.partitionID, t.collectionID, t.plan.Channel, 100)

		nullBuffer = newClusterBuffer(len(buckets), writer, fieldStats)
		t.clusterBuffers = append(t.clusterBuffers, nullBuffer)
	}
	t.keyToBufferFunc = func(key interface{}) *ClusterBuffer {
		if key == nil {
			return nullBuffer
		}
		// todo: if keys are too many, the map will be quite large, we should mark the range of each buffer and select buffer by range
		return scalarToClusterBufferMap[key]
	}
	return nil
}

func splitCentroids(centroids []int, num int) ([][]int, map[int]int) {
	if num <= 0 {
		return nil, nil
	}

	result := make([][]int, num)
	resultIndex := make(map[int]int, len(centroids))
	listLen := len(centroids)

	for i := 0; i < listLen; i++ {
		group := i % num
		result[group] = append(result[group], centroids[i])
		resultIndex[i] = group
	}

	return result, resultIndex
}

func (t *clusteringCompactionTask) generatedVectorPlan(ctx context.Context, bufferNum int, centroids []*schemapb.VectorField) error {
	centroidsOffset := make([]int, len(centroids))
	for i := 0; i < len(centroids); i++ {
		centroidsOffset[i] = i
	}
	centroidGroups, groupIndex := splitCentroids(centroidsOffset, bufferNum)
	for id, group := range centroidGroups {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}

		centroidValues := make([]storage.VectorFieldValue, len(group))
		for i, offset := range group {
			centroidValues[i] = storage.NewVectorFieldValue(t.clusteringKeyField.DataType, centroids[offset])
		}

		fieldStats.SetVectorCentroids(centroidValues...)

		alloc := NewCompactionAllocator(t.segIDAlloc, t.logIDAlloc)
		writer := NewMultiSegmentWriter(ctx, t.binlogIO, alloc, t.plan.GetMaxSize(), t.plan.GetSchema(), t.plan.MaxSegmentRows, t.partitionID, t.collectionID, t.plan.Channel, 100)

		buffer := newClusterBuffer(id, writer, fieldStats)
		t.clusterBuffers = append(t.clusterBuffers, buffer)
	}
	t.offsetToBufferFunc = func(offset int64, idMapping []uint32) *ClusterBuffer {
		centroidGroupOffset := groupIndex[int(idMapping[offset])]
		return t.clusterBuffers[centroidGroupOffset]
	}
	return nil
}

func (t *clusteringCompactionTask) switchPolicyForVectorPlan(ctx context.Context, centroids *clusteringpb.ClusteringCentroidsStats) error {
	bufferNum := len(centroids.GetCentroids())
	bufferNumByMemory := int(t.memoryBufferSize / expectedBinlogSize)
	if bufferNumByMemory < bufferNum {
		bufferNum = bufferNumByMemory
	}
	return t.generatedVectorPlan(ctx, bufferNum, centroids.GetCentroids())
}

func (t *clusteringCompactionTask) getVectorAnalyzeResult(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("getVectorAnalyzeResult-%d", t.GetPlanID()))
	defer span.End()
	log := log.Ctx(ctx)
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

	return t.switchPolicyForVectorPlan(ctx, centroids)
}

// mapping read and split input segments into buffers
func (t *clusteringCompactionTask) mapping(ctx context.Context,
) ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("mapping-%d", t.GetPlanID()))
	defer span.End()
	inputSegments := t.plan.GetSegmentBinlogs()
	mapStart := time.Now()
	log := log.Ctx(ctx)

	futures := make([]*conc.Future[any], 0, len(inputSegments))
	for _, segment := range inputSegments {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID: segment.SegmentID,
			// only FieldBinlogs and deltalogs needed
			Deltalogs:    segment.Deltalogs,
			FieldBinlogs: segment.FieldBinlogs,
		}
		future := t.mappingPool.Submit(func() (any, error) {
			err := t.mappingSegment(ctx, segmentClone)
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return nil, nil, err
	}

	// force flush all buffers
	err := t.flushAll()
	if err != nil {
		return nil, nil, err
	}

	resultSegments := make([]*datapb.CompactionSegment, 0)
	resultPartitionStats := &storage.PartitionStatsSnapshot{
		SegmentStats: make(map[typeutil.UniqueID]storage.SegmentStats),
	}
	for _, buffer := range t.clusterBuffers {
		segments := buffer.GetCompactionSegments()
		log.Debug("compaction segments", zap.Any("segments", segments))
		resultSegments = append(resultSegments, segments...)

		for _, segment := range segments {
			segmentStats := storage.SegmentStats{
				FieldStats: []storage.FieldStats{buffer.clusteringKeyFieldStats.Clone()},
				NumRows:    int(segment.NumOfRows),
			}
			resultPartitionStats.SegmentStats[segment.SegmentID] = segmentStats
			log.Debug("compaction segment partitioning stats", zap.Int64("segmentID", segment.SegmentID), zap.Any("stats", segmentStats))
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
		totalBufferSize = totalBufferSize + int64(buffer.GetBufferSize())
	}
	return totalBufferSize
}

// read insert log of one segment, mappingSegment into buckets according to clusteringKey. flush data to file when necessary
func (t *clusteringCompactionTask) mappingSegment(
	ctx context.Context,
	segment *datapb.CompactionSegmentBinlogs,
) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("mappingSegment-%d-%d", t.GetPlanID(), segment.GetSegmentID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.GetCollection()),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", segment.GetSegmentID()))
	log.Info("mapping segment start")
	processStart := time.Now()
	var remained int64 = 0

	deltaPaths := make([]string, 0)
	for _, d := range segment.GetDeltalogs() {
		for _, l := range d.GetBinlogs() {
			deltaPaths = append(deltaPaths, l.GetLogPath())
		}
	}
	delta, err := compaction.ComposeDeleteFromDeltalogs(ctx, t.binlogIO, deltaPaths)
	if err != nil {
		return err
	}
	entityFilter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime)

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

	rr, err := storage.NewBinlogRecordReader(ctx, segment.GetFieldBinlogs(), t.plan.Schema, storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
		return t.binlogIO.Download(ctx, paths)
	}))
	if err != nil {
		log.Warn("new binlog record reader wrong", zap.Error(err))
		return err
	}

	reader := storage.NewDeserializeReader(rr, func(r storage.Record, v []*storage.Value) error {
		return storage.ValueDeserializer(r, v, t.plan.Schema.Fields)
	})
	defer reader.Close()

	offset := int64(-1)
	for {
		v, err := reader.NextValue()
		if err != nil {
			if err == sio.EOF {
				reader.Close()
				break
			} else {
				log.Warn("compact wrong, failed to iter through data", zap.Error(err))
				return err
			}
		}
		offset++

		if entityFilter.Filtered((*v).PK.GetValue(), uint64((*v).Timestamp)) {
			continue
		}

		row, ok := (*v).Value.(map[typeutil.UniqueID]interface{})
		if !ok {
			log.Warn("convert interface to map wrong")
			return errors.New("unexpected error")
		}

		clusteringKey := row[t.clusteringKeyField.FieldID]
		var clusterBuffer *ClusterBuffer
		if t.isVectorClusteringKey {
			clusterBuffer = t.offsetToBufferFunc(offset, mappingStats.GetCentroidIdMapping())
		} else {
			clusterBuffer = t.keyToBufferFunc(clusteringKey)
		}
		if err := clusterBuffer.Write(*v); err != nil {
			return err
		}
		t.writtenRowNum.Inc()
		remained++

		if (remained+1)%100 == 0 {
			currentBufferTotalMemorySize := t.getBufferTotalUsedMemorySize()
			if currentBufferTotalMemorySize > t.getMemoryBufferHighWatermark() {
				// reach flushBinlog trigger threshold
				log.Debug("largest buffer need to flush",
					zap.Int64("currentBufferTotalMemorySize", currentBufferTotalMemorySize))
				if err := t.flushLargestBuffers(ctx); err != nil {
					return err
				}
			}
		}
	}

	missing := entityFilter.GetMissingDeleteCount()

	log.Info("mapping segment end",
		zap.Int64("remained_entities", remained),
		zap.Int("deleted_entities", entityFilter.GetDeletedCount()),
		zap.Int("expired_entities", entityFilter.GetExpiredCount()),
		zap.Int("deltalog deletes", entityFilter.GetDeltalogDeleteCount()),
		zap.Int("missing deletes", missing),
		zap.Int64("written_row_num", t.writtenRowNum.Load()),
		zap.Duration("elapse", time.Since(processStart)))

	metrics.DataNodeCompactionDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(entityFilter.GetDeltalogDeleteCount()))
	metrics.DataNodeCompactionMissingDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(missing))
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

func (t *clusteringCompactionTask) flushLargestBuffers(ctx context.Context) error {
	currentMemorySize := t.getBufferTotalUsedMemorySize()
	if currentMemorySize <= t.getMemoryBufferLowWatermark() {
		log.Info("memory low water mark", zap.Int64("memoryBufferSize", currentMemorySize))
		return nil
	}
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "flushLargestBuffers")
	defer span.End()
	bufferIDs := make([]int, 0)
	bufferSizes := make([]int64, 0)
	for _, buffer := range t.clusterBuffers {
		bufferIDs = append(bufferIDs, buffer.id)
		bufferSizes = append(bufferSizes, int64(buffer.GetBufferSize()))
	}
	sort.Slice(bufferIDs, func(i, j int) bool {
		return bufferSizes[bufferIDs[i]] > bufferSizes[bufferIDs[j]]
	})
	log.Info("start flushLargestBuffers", zap.Ints("bufferIDs", bufferIDs), zap.Int64("currentMemorySize", currentMemorySize))

	futures := make([]*conc.Future[any], 0)
	for _, bufferId := range bufferIDs {
		buffer := t.clusterBuffers[bufferId]
		size := buffer.GetBufferSize()
		currentMemorySize -= int64(size)

		log.Info("currentMemorySize after flush buffer binlog",
			zap.Int64("currentMemorySize", currentMemorySize),
			zap.Int("bufferID", bufferId),
			zap.Uint64("WrittenUncompressed", size))

		future := t.flushPool.Submit(func() (any, error) {
			err := buffer.Flush()
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

func (t *clusteringCompactionTask) flushAll() error {
	futures := make([]*conc.Future[any], 0)
	for _, buffer := range t.clusterBuffers {
		future := t.flushPool.Submit(func() (any, error) {
			err := buffer.Close()
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
		segmentClone := proto.Clone(segment).(*datapb.CompactionSegmentBinlogs)
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
		zap.Int("clustering num", len(analyzeDict)),
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
			ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	expiredFilter := compaction.NewEntityFilter(nil, t.plan.GetCollectionTtl(), t.currentTime)
	for _, paths := range fieldBinlogPaths {
		allValues, err := t.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
			return nil, err
		}
		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})

		pkIter, err := storage.NewBinlogDeserializeReader(t.plan.Schema, storage.MakeBlobsReader(blobs))
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", paths), zap.Error(err))
			return nil, err
		}

		for {
			v, err := pkIter.NextValue()
			if err != nil {
				if err == sio.EOF {
					pkIter.Close()
					break
				} else {
					log.Warn("compact wrong, failed to iter through data", zap.Error(err))
					return nil, err
				}
			}

			// Filtering expired entity
			if expiredFilter.Filtered((*v).PK.GetValue(), uint64((*v).Timestamp)) {
				continue
			}

			// Update timestampFrom, timestampTo
			if (*v).Timestamp < timestampFrom || timestampFrom == -1 {
				timestampFrom = (*v).Timestamp
			}
			if (*v).Timestamp > timestampTo || timestampFrom == -1 {
				timestampTo = (*v).Timestamp
			}
			// rowValue := vIter.GetData().(*iterators.InsertRow).GetValue()
			row, ok := (*v).Value.(map[typeutil.UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("path", paths))
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
		zap.Int("expired entities", expiredFilter.GetExpiredCount()),
		zap.Duration("map elapse", time.Since(processStart)))
	return analyzeResult, nil
}

func (t *clusteringCompactionTask) generatedScalarPlan(maxRows, preferRows int64, keys []interface{}, dict map[interface{}]int64) [][]interface{} {
	buckets := make([][]interface{}, 0)
	currentBucket := make([]interface{}, 0)
	var currentBucketSize int64 = 0
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

func (t *clusteringCompactionTask) switchPolicyForScalarPlan(totalRows int64, keys []interface{}, dict map[interface{}]int64) [][]interface{} {
	bufferNumBySegmentMaxRows := totalRows / t.plan.MaxSegmentRows
	bufferNumByMemory := t.memoryBufferSize / expectedBinlogSize
	log.Info("switchPolicyForScalarPlan", zap.Int64("totalRows", totalRows),
		zap.Int64("bufferNumBySegmentMaxRows", bufferNumBySegmentMaxRows),
		zap.Int64("bufferNumByMemory", bufferNumByMemory))
	if bufferNumByMemory > bufferNumBySegmentMaxRows {
		return t.generatedScalarPlan(t.plan.GetMaxSegmentRows(), t.plan.GetPreferSegmentRows(), keys, dict)
	}

	maxRows := totalRows / bufferNumByMemory
	return t.generatedScalarPlan(maxRows, int64(float64(maxRows)*paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat()), keys, dict)
}

func (t *clusteringCompactionTask) splitClusterByScalarValue(dict map[interface{}]int64) ([][]interface{}, bool) {
	totalRows := int64(0)
	keys := lo.MapToSlice(dict, func(k interface{}, v int64) interface{} {
		totalRows += v
		return k
	})

	notNullKeys := lo.Filter(keys, func(i interface{}, j int) bool {
		return i != nil
	})
	sort.Slice(notNullKeys, func(i, j int) bool {
		return storage.NewScalarFieldValue(t.clusteringKeyField.DataType, notNullKeys[i]).LE(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, notNullKeys[j]))
	})

	return t.switchPolicyForScalarPlan(totalRows, notNullKeys, dict), len(keys) > len(notNullKeys)
}

func (t *clusteringCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}
