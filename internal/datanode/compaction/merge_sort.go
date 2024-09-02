package compaction

import (
	"container/heap"
	"context"
	sio "io"
	"math"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func mergeSortMultipleSegments(ctx context.Context,
	plan *datapb.CompactionPlan,
	collectionID, partitionID, maxRows int64,
	binlogIO io.BinlogIO,
	binlogs []*datapb.CompactionSegmentBinlogs,
	delta map[interface{}]typeutil.Timestamp,
	tr *timerecord.TimeRecorder,
	currentTs typeutil.Timestamp,
	collectionTtl int64,
) ([]*datapb.CompactionSegment, error) {
	_ = tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "mergeSortMultipleSegments")
	defer span.End()

	log := log.With(zap.Int64("planID", plan.GetPlanID()))

	segIDAlloc := allocator.NewLocalAllocator(plan.GetPreAllocatedSegmentIDs().GetBegin(), plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(plan.GetBeginLogID(), math.MaxInt64)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)
	mWriter := NewMultiSegmentWriter(binlogIO, compAlloc, plan, maxRows, partitionID, collectionID)

	var (
		expiredRowCount int64 // the number of expired entities
		deletedRowCount int64
	)

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

	pkField, err := typeutil.GetPrimaryFieldSchema(plan.GetSchema())
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
	}

	//SegmentDeserializeReaderTest(binlogPaths, t.binlogIO, writer.GetPkID())
	segmentReaders := make([]*SegmentDeserializeReader, len(binlogs))
	for i, s := range binlogs {
		var binlogBatchCount int
		for _, b := range s.GetFieldBinlogs() {
			if b != nil {
				binlogBatchCount = len(b.GetBinlogs())
				break
			}
		}

		if binlogBatchCount == 0 {
			log.Warn("compacting empty segment", zap.Int64("segmentID", s.GetSegmentID()))
			continue
		}

		binlogPaths := make([][]string, binlogBatchCount)
		for idx := 0; idx < binlogBatchCount; idx++ {
			var batchPaths []string
			for _, f := range s.GetFieldBinlogs() {
				batchPaths = append(batchPaths, f.GetBinlogs()[idx].GetLogPath())
			}
			binlogPaths[idx] = batchPaths
		}
		segmentReaders[i] = NewSegmentDeserializeReader(ctx, binlogPaths, binlogIO, pkField.GetFieldID())
	}

	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	for i, r := range segmentReaders {
		if v, err := r.Next(); err == nil {
			heap.Push(&pq, &PQItem{
				Value: v,
				Index: i,
			})
		}
	}

	for pq.Len() > 0 {
		smallest := heap.Pop(&pq).(*PQItem)
		v := smallest.Value

		if isValueDeleted(v) {
			deletedRowCount++
			continue
		}

		// Filtering expired entity
		if isExpiredEntity(collectionTtl, currentTs, typeutil.Timestamp(v.Timestamp)) {
			expiredRowCount++
			continue
		}

		err := mWriter.Write(v)
		if err != nil {
			log.Warn("compact wrong, failed to writer row", zap.Error(err))
			return nil, err
		}

		v, err = segmentReaders[smallest.Index].Next()
		if err != nil && err != sio.EOF {
			return nil, err
		}
		if err == nil {
			next := &PQItem{
				Value: v,
				Index: smallest.Index,
			}
			heap.Push(&pq, next)
		}
	}

	res, err := mWriter.Finish()
	if err != nil {
		log.Warn("compact wrong, failed to finish writer", zap.Error(err))
		return nil, err
	}

	for _, seg := range res {
		seg.IsSorted = true
	}

	totalElapse := tr.RecordSpan()
	log.Info("compact mergeSortMultipleSegments end",
		zap.Int64s("mergeSplit to segments", lo.Keys(mWriter.cachedMeta)),
		zap.Int64("deleted row count", deletedRowCount),
		zap.Int64("expired entities", expiredRowCount),
		zap.Duration("total elapse", totalElapse))

	return res, nil
}
