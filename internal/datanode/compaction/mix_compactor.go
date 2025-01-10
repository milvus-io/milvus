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
	"time"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type mixCompactionTask struct {
	binlogIO    io.BinlogIO
	currentTime time.Time

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	collectionID int64
	partitionID  int64
	targetSize   int64
	maxRows      int64
	pkID         int64

	bm25FieldIDs []int64

	done chan struct{}
	tr   *timerecord.TimeRecorder
}

var _ Compactor = (*mixCompactionTask)(nil)

func NewMixCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
) *mixCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &mixCompactionTask{
		ctx:         ctx1,
		cancel:      cancel,
		binlogIO:    binlogIO,
		plan:        plan,
		tr:          timerecord.NewTimeRecorder("mergeSplit compaction"),
		currentTime: time.Now(),
		done:        make(chan struct{}, 1),
	}
}

// preCompact exams whether its a valid compaction plan, and init the collectionID and partitionID
func (t *mixCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
	}

	if len(t.plan.GetSegmentBinlogs()) < 1 {
		return errors.Newf("compaction plan is illegal, there's no segments in compaction plan, planID = %d", t.GetPlanID())
	}

	if t.plan.GetMaxSize() == 0 {
		return errors.Newf("compaction plan is illegal, empty maxSize, planID = %d", t.GetPlanID())
	}

	t.collectionID = t.plan.GetSegmentBinlogs()[0].GetCollectionID()
	t.partitionID = t.plan.GetSegmentBinlogs()[0].GetPartitionID()
	t.targetSize = t.plan.GetMaxSize()
	t.bm25FieldIDs = GetBM25FieldIDs(t.plan.GetSchema())

	currSize := int64(0)
	for _, segmentBinlog := range t.plan.GetSegmentBinlogs() {
		for i, fieldBinlog := range segmentBinlog.GetFieldBinlogs() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				// numRows just need to add entries num of ONE field.
				if i == 0 {
					t.maxRows += binlog.GetEntriesNum()
				}

				// MemorySize might be incorrectly
				currSize += binlog.GetMemorySize()
			}
		}
	}

	outputSegmentCount := int64(math.Ceil(float64(currSize) / float64(t.targetSize)))
	log.Info("preCompaction analyze",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("inputSize", currSize),
		zap.Int64("targetSize", t.targetSize),
		zap.Int("inputSegmentCount", len(t.plan.GetSegmentBinlogs())),
		zap.Int64("estimatedOutputSegmentCount", outputSegmentCount),
	)

	return nil
}

func (t *mixCompactionTask) mergeSplit(
	ctx context.Context,
	insertPaths map[int64][]string,
	deltaPaths map[int64][]string,
) ([]*datapb.CompactionSegment, error) {
	_ = t.tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "MergeSplit")
	defer span.End()

	log := log.With(zap.Int64("planID", t.GetPlanID()))

	segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(t.plan.GetBeginLogID(), math.MaxInt64)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)
	mWriter := NewMultiSegmentWriter(t.binlogIO, compAlloc, t.plan, t.maxRows, t.partitionID, t.collectionID, t.bm25FieldIDs)

	deletedRowCount := int64(0)
	expiredRowCount := int64(0)

	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
	}
	for segId, binlogPaths := range insertPaths {
		deltaPaths := deltaPaths[segId]
		del, exp, err := t.writeSegment(ctx, binlogPaths, deltaPaths, mWriter, pkField)
		if err != nil {
			return nil, err
		}
		deletedRowCount += del
		expiredRowCount += exp
	}
	res, err := mWriter.Finish()
	if err != nil {
		log.Warn("compact wrong, failed to finish writer", zap.Error(err))
		return nil, err
	}

	totalElapse := t.tr.RecordSpan()
	log.Info("compact mergeSplit end",
		zap.Int64s("mergeSplit to segments", lo.Keys(mWriter.cachedMeta)),
		zap.Int64("deleted row count", deletedRowCount),
		zap.Int64("expired entities", expiredRowCount),
		zap.Duration("total elapse", totalElapse))

	return res, nil
}

func (t *mixCompactionTask) writeSegment(ctx context.Context,
	binlogPaths []string,
	deltaPaths []string,
	mWriter *MultiSegmentWriter, pkField *schemapb.FieldSchema,
) (deletedRowCount, expiredRowCount int64, err error) {
	log := log.With(zap.Strings("paths", binlogPaths))
	allValues, err := t.binlogIO.Download(ctx, binlogPaths)
	if err != nil {
		log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
		return
	}

	blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
		return &storage.Blob{Key: binlogPaths[i], Value: v}
	})

	delta, err := mergeDeltalogs(ctx, t.binlogIO, deltaPaths)
	if err != nil {
		log.Warn("compact wrong, fail to merge deltalogs", zap.Error(err))
		return
	}
	entityFilter := newEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime)

	reader, err := storage.NewCompositeBinlogRecordReader(blobs)
	if err != nil {
		log.Warn("compact wrong, failed to new insert binlogs reader", zap.Error(err))
		return
	}
	defer reader.Close()

	writeSlice := func(r storage.Record, start, end int) error {
		sliced := r.Slice(start, end)
		defer sliced.Release()
		err = mWriter.WriteRecord(sliced)
		if err != nil {
			log.Warn("compact wrong, failed to writer row", zap.Error(err))
			return err
		}
		return nil
	}
	for {
		err = reader.Next()
		if err != nil {
			if err == sio.EOF {
				err = nil
				break
			} else {
				log.Warn("compact wrong, failed to iter through data", zap.Error(err))
				return
			}
		}
		r := reader.Record()
		pkArray := r.Column(pkField.FieldID)
		tsArray := r.Column(common.TimeStampField).(*array.Int64)

		sliceStart := -1
		rows := r.Len()
		for i := 0; i < rows; i++ {
			// Filtering deleted entities
			var pk any
			switch pkField.DataType {
			case schemapb.DataType_Int64:
				pk = pkArray.(*array.Int64).Value(i)
			case schemapb.DataType_VarChar:
				pk = pkArray.(*array.String).Value(i)
			default:
				panic("invalid data type")
			}
			ts := typeutil.Timestamp(tsArray.Value(i))
			if entityFilter.Filtered(pk, ts) {
				if sliceStart != -1 {
					err = writeSlice(r, sliceStart, i)
					if err != nil {
						return
					}
					sliceStart = -1
				}
				continue
			}

			if sliceStart == -1 {
				sliceStart = i
			}
		}

		if sliceStart != -1 {
			err = writeSlice(r, sliceStart, r.Len())
			if err != nil {
				return
			}
		}
	}

	deltalogDeleteEntriesCount := len(delta)
	deletedRowCount = int64(entityFilter.GetDeletedCount())
	expiredRowCount = int64(entityFilter.GetExpiredCount())

	metrics.DataNodeCompactionDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(deltalogDeleteEntriesCount))
	metrics.DataNodeCompactionMissingDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(entityFilter.GetMissingDeleteCount()))

	return
}

func (t *mixCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	durInQueue := t.tr.RecordSpan()
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("MixCompact-%d", t.GetPlanID()))
	defer span.End()
	compactStart := time.Now()

	if err := t.preCompact(); err != nil {
		log.Warn("compact wrong, failed to preCompact", zap.Error(err))
		return nil, err
	}

	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))

	ctxTimeout, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	log.Info("compact start")
	deltaPaths, insertPaths, err := composePaths(t.plan.GetSegmentBinlogs())
	if err != nil {
		log.Warn("compact wrong, failed to composePaths", zap.Error(err))
		return nil, err
	}
	// Unable to deal with all empty segments cases, so return error
	isEmpty := true
	for _, paths := range insertPaths {
		if len(paths) > 0 {
			isEmpty = false
			break
		}
	}
	if isEmpty {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errors.New("illegal compaction plan")
	}

	sortMergeAppicable := paramtable.Get().DataNodeCfg.UseMergeSort.GetAsBool()
	if sortMergeAppicable {
		for _, segment := range t.plan.GetSegmentBinlogs() {
			if !segment.GetIsSorted() {
				sortMergeAppicable = false
				break
			}
		}
		if len(insertPaths) <= 1 || len(insertPaths) > paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetAsInt() {
			// sort merge is not applicable if there is only one segment or too many segments
			sortMergeAppicable = false
		}
	}

	var res []*datapb.CompactionSegment
	if sortMergeAppicable {
		log.Info("compact by merge sort")
		res, err = mergeSortMultipleSegments(ctxTimeout, t.plan, t.collectionID, t.partitionID, t.maxRows, t.binlogIO,
			t.plan.GetSegmentBinlogs(), t.tr, t.currentTime, t.plan.GetCollectionTtl(), t.bm25FieldIDs)
		if err != nil {
			log.Warn("compact wrong, fail to merge sort segments", zap.Error(err))
			return nil, err
		}
	} else {
		res, err = t.mergeSplit(ctxTimeout, insertPaths, deltaPaths)
		if err != nil {
			log.Warn("compact wrong, failed to mergeSplit", zap.Error(err))
			return nil, err
		}
	}

	log.Info("compact done", zap.Duration("compact elapse", time.Since(compactStart)))

	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	metrics.DataNodeCompactionLatencyInQueue.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(durInQueue.Milliseconds()))

	planResult := &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Channel:  t.GetChannelName(),
		Segments: res,
		Type:     t.plan.GetType(),
	}
	return planResult, nil
}

func (t *mixCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *mixCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *mixCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *mixCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *mixCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}

func (t *mixCompactionTask) GetCollection() typeutil.UniqueID {
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *mixCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}

func GetBM25FieldIDs(coll *schemapb.CollectionSchema) []int64 {
	return lo.FilterMap(coll.GetFunctions(), func(function *schemapb.FunctionSchema, _ int) (int64, bool) {
		if function.GetType() == schemapb.FunctionType_BM25 {
			return function.GetOutputFieldIds()[0], true
		}
		return 0, false
	})
}
