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
	"encoding/json"
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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
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

	plan *datapb.CompactionPlan

	// inner field
	collectionID          int64
	partitionID           int64
	currentTs             typeutil.Timestamp // for TTL
	isVectorClusteringKey bool
	clusteringKeyField    *schemapb.FieldSchema
	primaryKeyField       *schemapb.FieldSchema

	memoryBufferSize int64

	// bm25
	bm25FieldIds []int64

	writer *SplitClusterWriter
}

func NewClusteringCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
) *clusteringCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &clusteringCompactionTask{
		ctx:      ctx,
		cancel:   cancel,
		binlogIO: binlogIO,
		plan:     plan,
		tr:       timerecord.NewTimeRecorder("clustering_compaction"),
		done:     make(chan struct{}, 1),
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

	t.bm25FieldIds = GetBM25FieldIDs(t.plan.Schema)
	t.primaryKeyField = pkField
	t.isVectorClusteringKey = typeutil.IsVectorType(t.clusteringKeyField.DataType)
	t.currentTs = tsoutil.GetCurrentTime()
	t.memoryBufferSize = t.getMemoryBufferSize()
	workerPoolSize := t.getWorkerPoolSize()
	t.mappingPool = conc.NewPool[any](workerPoolSize)
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
	log.Info("Clustering compaction start mapping")
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
	log.Info("Clustering compaction finished", zap.Duration("elapse", t.tr.ElapseSpan()))

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
	// assemble split keys and mapping function
	splitKeys := make([]string, 0)
	splitFieldStats := make([]*storage.FieldStats, 0)
	for _, bucket := range buckets {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}
		for _, key := range bucket {
			fieldStats.UpdateMinMax(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, key))
		}
		splitFieldStats = append(splitFieldStats, fieldStats)
		bytes, err := json.Marshal(fieldStats)
		if err != nil {
			return err
		}
		splitKeys = append(splitKeys, string(bytes))
	}

	if containsNull {
		splitKeys = append(splitKeys, "null")
	}

	mappingFunc := func(value *storage.Value) (string, error) {
		data, _ := value.Value.(map[storage.FieldID]interface{})[t.clusteringKeyField.FieldID]
		if data == nil {
			return "null", nil
		}
		fieldValue := storage.NewScalarFieldValue(t.clusteringKeyField.DataType, data)
		for _, fieldStats := range splitFieldStats {
			if fieldValue.GE(fieldStats.Min) && fieldValue.LE(fieldStats.Max) {
				bytes, err := json.Marshal(fieldStats)
				if err != nil {
					return "", err
				}
				return string(bytes), nil
			}
		}
		return "", errors.New("no matching cluster")
	}

	splitWriter, err := NewSplitClusterWriterBuilder().
		SetCollectionID(t.GetCollection()).
		SetPartitionID(t.partitionID).
		SetSchema(t.plan.GetSchema()).
		SetSegmentMaxSize(t.plan.GetMaxSize()).
		SetSegmentMaxRowCount(t.plan.GetMaxSegmentRows()).
		SetClusterKeys(splitKeys).
		SetAllocator(&compactionAlloactor{
			segmentAlloc: t.segIDAlloc,
			logIDAlloc:   t.logIDAlloc,
		}).
		SetBinlogIO(t.binlogIO).
		SetMappingFunc(mappingFunc).
		SetWorkerPoolSize(t.getWorkerPoolSize()).
		Build()
	log.Info("create split cluster writer",
		zap.Int("clusterKeys", len(splitKeys)),
		zap.Int64("segmentMaxSize", t.plan.GetMaxSize()),
		zap.Int64("segmentMaxRowCount", t.plan.GetMaxSegmentRows()))
	if err != nil {
		log.Warn("fail to build splits buffer", zap.Error(err))
		return err
	}
	t.writer = splitWriter

	return nil
}

func (t *clusteringCompactionTask) getVectorAnalyzeResult(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("getVectorAnalyzeResult-%d", t.GetPlanID()))
	defer span.End()
	analyzeResultPath := t.plan.AnalyzeResultPath
	centroidFilePath := path.Join(analyzeResultPath, metautil.JoinIDPath(t.collectionID, t.partitionID, t.clusteringKeyField.FieldID), common.Centroids)
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
		zap.Int("centroidNum", len(centroids.GetCentroids())))

	splitKeys := make([]string, 0)
	splitFieldStats := make([]*storage.FieldStats, 0)
	for id, centroid := range centroids.GetCentroids() {
		fieldStats, err := storage.NewFieldStats(t.clusteringKeyField.FieldID, t.clusteringKeyField.DataType, 0)
		if err != nil {
			return err
		}
		fieldStats.SetVectorCentroids(storage.NewVectorFieldValue(t.clusteringKeyField.DataType, centroid))
		splitFieldStats = append(splitFieldStats, fieldStats)
		splitKeys = append(splitKeys, fmt.Sprint(id))
	}

	splitWriter, err := NewSplitClusterWriterBuilder().
		SetCollectionID(t.GetCollection()).
		SetPartitionID(t.partitionID).
		SetSchema(t.plan.GetSchema()).
		SetSegmentMaxSize(t.plan.GetMaxSize()).
		SetSegmentMaxRowCount(t.plan.GetMaxSegmentRows()).
		SetClusterKeys(splitKeys).
		SetAllocator(&compactionAlloactor{
			segmentAlloc: t.segIDAlloc,
			logIDAlloc:   t.logIDAlloc,
		}).
		SetBinlogIO(t.binlogIO).
		SetWorkerPoolSize(t.getWorkerPoolSize()).
		Build()
	t.writer = splitWriter
	log.Info("create split cluster writer",
		zap.Int("clusterKeys", len(splitKeys)),
		zap.Int64("segmentMaxSize", t.plan.GetMaxSize()),
		zap.Int64("segmentMaxRowCount", t.plan.GetTotalRows()))

	return nil
}

// mapping read and split input segments into buffers
func (t *clusteringCompactionTask) mapping(ctx context.Context,
) ([]*datapb.CompactionSegment, *storage.PartitionStatsSnapshot, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("mapping-%d", t.GetPlanID()))
	defer span.End()
	inputSegments := t.plan.GetSegmentBinlogs()
	mapStart := time.Now()

	futures := make([]*conc.Future[any], 0, len(inputSegments))
	for _, segment := range inputSegments {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID: segment.SegmentID,
			// only FieldBinlogs needed
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
	compactionResults, err := t.writer.Finish()
	if err != nil {
		return nil, nil, err
	}

	resultSegments := make([]*datapb.CompactionSegment, 0)
	resultPartitionStats := &storage.PartitionStatsSnapshot{
		SegmentStats: make(map[typeutil.UniqueID]storage.SegmentStats),
	}
	for clusterKey, segments := range compactionResults {
		fieldStat := &storage.FieldStats{}
		err := json.Unmarshal([]byte(clusterKey), fieldStat)
		if err != nil {
			return nil, nil, err
		}
		for _, seg := range segments {
			resultSegments = append(resultSegments, seg)
			segmentStats := storage.SegmentStats{
				FieldStats: []storage.FieldStats{
					fieldStat.Clone(),
				},
				NumRows: int(seg.GetNumOfRows()),
			}
			resultPartitionStats.SegmentStats[seg.GetSegmentID()] = segmentStats
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
	fieldBinlogPaths := make([][]string, 0)
	var (
		expired  int64 = 0
		deleted  int64 = 0
		remained int64 = 0
	)

	deltaPaths := make([]string, 0)
	for _, d := range segment.GetDeltalogs() {
		for _, l := range d.GetBinlogs() {
			deltaPaths = append(deltaPaths, l.GetLogPath())
		}
	}
	delta, err := mergeDeltalogs(ctx, t.binlogIO, deltaPaths)
	if err != nil {
		return err
	}

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
		offSetPath := path.Join(t.plan.AnalyzeResultPath, metautil.JoinIDPath(t.collectionID, t.partitionID, t.clusteringKeyField.FieldID, segment.SegmentID), common.OffsetMapping)
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

			if t.isVectorClusteringKey {
				err = t.writer.WriteToCluster(v, fmt.Sprint(mappingStats.GetCentroidIdMapping()[offset]))
			} else {
				err = t.writer.Write(v)
			}
			if err != nil {
				log.Error("write fail", zap.Error(err))
				return err
			}
			remained++

			if (remained+1)%100 == 0 {
				currentBufferTotalMemorySize := t.writer.getTotalUsedMemorySize()
				if currentBufferTotalMemorySize > t.getMemoryBufferHighWatermark() {
					log.Info("flush writer", zap.Int64("currentBufferTotalMemorySize", currentBufferTotalMemorySize))
					go t.writer.FlushTo(t.getMemoryBufferLowWatermark())
				}

				// if the total buffer size is too large, block here, wait for memory release by flushBinlog
				if t.writer.getTotalUsedMemorySize() > t.getMemoryBufferBlockFlushThreshold() {
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
							currentSize := t.writer.getTotalUsedMemorySize()
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
		zap.Int64("written_row_num", t.writer.GetRowNum()),
		zap.Duration("elapse", time.Since(processStart)))
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
	log := log.With(zap.Int64("collectionID", t.GetCollection()), zap.Int64("partitionID", t.partitionID))
	log.Info("analyze start", zap.Int("segments", len(inputSegments)))
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
	log.Info("analyze end", zap.Int("segments", len(inputSegments)), zap.Duration("elapse", time.Since(analyzeStart)))
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
			ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	for _, paths := range fieldBinlogPaths {
		allValues, err := t.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
			return nil, err
		}
		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})

		pkIter, err := storage.NewBinlogDeserializeReader(blobs, t.primaryKeyField.GetFieldID())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", paths), zap.Error(err))
			return nil, err
		}

		for {
			err := pkIter.Next()
			if err != nil {
				if err == sio.EOF {
					pkIter.Close()
					break
				} else {
					log.Warn("compact wrong, failed to iter through data", zap.Error(err))
					return nil, err
				}
			}
			v := pkIter.Value()

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
		zap.Int64("deleted entities", deleted),
		zap.Int64("expired entities", expired),
		zap.Duration("map elapse", time.Since(processStart)))
	return analyzeResult, nil
}

func (t *clusteringCompactionTask) splitClusterByScalarValue(dict map[interface{}]int64) ([][]interface{}, bool) {
	keys := lo.MapToSlice(dict, func(k interface{}, _ int64) interface{} {
		return k
	})

	notNullKeys := lo.Filter(keys, func(i interface{}, j int) bool {
		return i != nil
	})
	sort.Slice(notNullKeys, func(i, j int) bool {
		return storage.NewScalarFieldValue(t.clusteringKeyField.DataType, notNullKeys[i]).LE(storage.NewScalarFieldValue(t.clusteringKeyField.DataType, notNullKeys[j]))
	})

	buckets := make([][]interface{}, 0)
	currentBucket := make([]interface{}, 0)
	var currentBucketSize int64 = 0
	maxRows := t.plan.MaxSegmentRows
	preferRows := t.plan.PreferSegmentRows
	containsNull := len(keys) > len(notNullKeys)
	for _, key := range notNullKeys {
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
	return buckets, containsNull
}

func (t *clusteringCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}

func (t *clusteringCompactionTask) generateBM25Stats(ctx context.Context, segmentID int64, statsMap map[int64]*storage.BM25Stats) ([]*datapb.FieldBinlog, error) {
	binlogs := []*datapb.FieldBinlog{}
	kvs := map[string][]byte{}
	logID, _, err := t.logIDAlloc.Alloc(uint32(len(statsMap)))
	if err != nil {
		return nil, err
	}

	for fieldID, stats := range statsMap {
		key, _ := binlog.BuildLogPath(storage.BM25Binlog, t.collectionID, t.partitionID, segmentID, fieldID, logID)
		bytes, err := stats.Serialize()
		if err != nil {
			log.Warn("failed to seralize bm25 stats", zap.Int64("collection", t.collectionID),
				zap.Int64("partition", t.partitionID), zap.Int64("segment", segmentID), zap.Error(err))
			return nil, err
		}

		kvs[key] = bytes

		binlogs = append(binlogs, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{
				LogSize:    int64(len(bytes)),
				MemorySize: int64(len(bytes)),
				LogPath:    key,
				EntriesNum: stats.NumRow(),
			}},
		})
		logID++
	}

	if err := t.binlogIO.Upload(ctx, kvs); err != nil {
		log.Warn("failed to upload bm25 stats log",
			zap.Int64("collection", t.collectionID),
			zap.Int64("partition", t.partitionID),
			zap.Int64("segment", segmentID),
			zap.Error(err))
		return nil, err
	}
	return binlogs, nil
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
