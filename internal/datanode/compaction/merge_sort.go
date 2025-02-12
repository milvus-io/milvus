package compaction

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow/go/v12/arrow/array"
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
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type segmentWriterWrapper struct {
	*MultiSegmentWriter
}

var _ storage.RecordWriter = (*segmentWriterWrapper)(nil)

func (w *segmentWriterWrapper) GetWrittenUncompressed() uint64 {
	return 0
}

func (w *segmentWriterWrapper) Write(record storage.Record) error {
	return w.MultiSegmentWriter.WriteRecord(record)
}

func (w *segmentWriterWrapper) Close() error {
	return nil
}

func mergeSortMultipleSegments(ctx context.Context,
	plan *datapb.CompactionPlan,
	collectionID, partitionID, maxRows int64,
	binlogIO io.BinlogIO,
	binlogs []*datapb.CompactionSegmentBinlogs,
	tr *timerecord.TimeRecorder,
	currentTime time.Time,
	collectionTtl int64,
	bm25FieldIds []int64,
) ([]*datapb.CompactionSegment, error) {
	_ = tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "mergeSortMultipleSegments")
	defer span.End()

	log := log.With(zap.Int64("planID", plan.GetPlanID()))

	segIDAlloc := allocator.NewLocalAllocator(plan.GetPreAllocatedSegmentIDs().GetBegin(), plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(plan.GetBeginLogID(), math.MaxInt64)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)
	mWriter := NewMultiSegmentWriter(binlogIO, compAlloc, plan, maxRows, partitionID, collectionID, bm25FieldIds)
	writer := &segmentWriterWrapper{MultiSegmentWriter: mWriter}

	pkField, err := typeutil.GetPrimaryFieldSchema(plan.GetSchema())
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
	}

	segmentReaders := make([]storage.RecordReader, len(binlogs))
	segmentFilters := make([]*EntityFilter, len(binlogs))
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
		segmentReaders[i] = NewSegmentRecordReader(ctx, binlogPaths, binlogIO)
		deltalogPaths := make([]string, 0)
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				deltalogPaths = append(deltalogPaths, l.GetLogPath())
			}
		}
		delta, err := mergeDeltalogs(ctx, binlogIO, deltalogPaths)
		if err != nil {
			return nil, err
		}
		segmentFilters[i] = newEntityFilter(delta, collectionTtl, currentTime)
	}

	var predicate func(r storage.Record, ri, i int) bool
	switch pkField.DataType {
	case schemapb.DataType_Int64:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.Int64).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			return !segmentFilters[ri].Filtered(pk, uint64(ts))
		}
	case schemapb.DataType_VarChar:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.String).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			return !segmentFilters[ri].Filtered(pk, uint64(ts))
		}
	default:
		log.Warn("compaction only support int64 and varchar pk field")
	}

	if _, err = storage.MergeSort(plan.GetSchema(), segmentReaders, pkField.FieldID, writer, predicate); err != nil {
		return nil, err
	}

	res, err := mWriter.Finish()
	if err != nil {
		log.Warn("compact wrong, failed to finish writer", zap.Error(err))
		return nil, err
	}

	for _, seg := range res {
		seg.IsSorted = true
	}

	var (
		deletedRowCount            int
		expiredRowCount            int
		missingDeleteCount         int
		deltalogDeleteEntriesCount int
	)

	for _, filter := range segmentFilters {
		deletedRowCount += filter.GetDeletedCount()
		expiredRowCount += filter.GetExpiredCount()
		missingDeleteCount += filter.GetMissingDeleteCount()
		deltalogDeleteEntriesCount += filter.GetDeltalogDeleteCount()
	}

	totalElapse := tr.RecordSpan()
	log.Info("compact mergeSortMultipleSegments end",
		zap.Int64s("mergeSplit to segments", lo.Keys(mWriter.cachedMeta)),
		zap.Int("deleted row count", deletedRowCount),
		zap.Int("expired entities", expiredRowCount),
		zap.Int("missing deletes", missingDeleteCount),
		zap.Duration("total elapse", totalElapse))

	metrics.DataNodeCompactionDeleteCount.WithLabelValues(fmt.Sprint(collectionID)).Add(float64(deltalogDeleteEntriesCount))
	metrics.DataNodeCompactionMissingDeleteCount.WithLabelValues(fmt.Sprint(collectionID)).Add(float64(missingDeleteCount))

	return res, nil
}
