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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type mixCompactionTask struct {
	binlogIO  io.BinlogIO
	currentTs typeutil.Timestamp

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	collectionID int64
	partitionID  int64
	targetSize   int64
	maxRows      int64
	pkID         int64

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
		ctx:       ctx1,
		cancel:    cancel,
		binlogIO:  binlogIO,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("mergeSplit compaction"),
		currentTs: tsoutil.GetCurrentTime(),
		done:      make(chan struct{}, 1),
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
		zap.Int64("currSize", currSize),
		zap.Int64("targetSize", t.targetSize),
		zap.Int64("estimatedSegmentCount", outputSegmentCount),
	)

	return nil
}

func (t *mixCompactionTask) mergeSplit(
	ctx context.Context,
	binlogPaths [][]string,
	delta map[interface{}]typeutil.Timestamp,
) ([]*datapb.CompactionSegment, error) {
	_ = t.tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "MergeSplit")
	defer span.End()

	log := log.With(zap.Int64("planID", t.GetPlanID()))

	segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(t.plan.GetBeginLogID(), math.MaxInt64)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)
	mWriter := NewMultiSegmentWriter(t.binlogIO, compAlloc, t.plan, t.maxRows, t.partitionID, t.collectionID)

	isValueDeleted := func(v *storage.Value) bool {
		ts, ok := delta[v.PK.GetValue()]
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if ok && uint64(v.Timestamp) < ts {
			return true
		}
		return false
	}

	deletedRowCount := int64(0)
	expiredRowCount := int64(0)

	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
	}
	for _, paths := range binlogPaths {
		log := log.With(zap.Strings("paths", paths))
		allValues, err := t.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
			return nil, err
		}

		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})

		iter, err := storage.NewBinlogDeserializeReader(blobs, pkField.GetFieldID())
		if err != nil {
			log.Warn("compact wrong, failed to new insert binlogs reader", zap.Error(err))
			return nil, err
		}

		for {
			err := iter.Next()
			if err != nil {
				if err == sio.EOF {
					break
				} else {
					log.Warn("compact wrong, failed to iter through data", zap.Error(err))
					return nil, err
				}
			}
			v := iter.Value()
			if isValueDeleted(v) {
				deletedRowCount++
				continue
			}

			// Filtering expired entity
			if isExpiredEntity(t.plan.GetCollectionTtl(), t.currentTs, typeutil.Timestamp(v.Timestamp)) {
				expiredRowCount++
				continue
			}

			err = mWriter.Write(v)
			if err != nil {
				log.Warn("compact wrong, failed to writer row", zap.Error(err))
				return nil, err
			}
		}
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
	deltaPaths, allBatchPaths, err := composePaths(t.plan.GetSegmentBinlogs())
	if err != nil {
		log.Warn("compact wrong, failed to composePaths", zap.Error(err))
		return nil, err
	}
	// Unable to deal with all empty segments cases, so return error
	if len(allBatchPaths) == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errors.New("illegal compaction plan")
	}

	deltaPk2Ts, err := mergeDeltalogs(ctxTimeout, t.binlogIO, deltaPaths)
	if err != nil {
		log.Warn("compact wrong, fail to merge deltalogs", zap.Error(err))
		return nil, err
	}

	allSorted := true
	for _, segment := range t.plan.GetSegmentBinlogs() {
		if !segment.GetIsSorted() {
			allSorted = false
			break
		}
	}

	var res []*datapb.CompactionSegment
	if allSorted && len(t.plan.GetSegmentBinlogs()) > 1 {
		log.Info("all segments are sorted, use merge sort")
		res, err = mergeSortMultipleSegments(ctxTimeout, t.plan, t.collectionID, t.partitionID, t.maxRows, t.binlogIO,
			t.plan.GetSegmentBinlogs(), deltaPk2Ts, t.tr, t.currentTs, t.plan.GetCollectionTtl())
		if err != nil {
			log.Warn("compact wrong, fail to merge sort segments", zap.Error(err))
			return nil, err
		}
	} else {
		res, err = t.mergeSplit(ctxTimeout, allBatchPaths, deltaPk2Ts)
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
