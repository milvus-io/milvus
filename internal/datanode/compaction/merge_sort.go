package compaction

import (
	"container/heap"
	"context"
	sio "io"
	"time"

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
	planID int64,
	binlogIO io.BinlogIO,
	allocator allocator.Interface,
	binlogs []*datapb.CompactionSegmentBinlogs,
	delta map[interface{}]typeutil.Timestamp,
	writer *storage.SegmentWriter,
	tr *timerecord.TimeRecorder,
	currentTs typeutil.Timestamp,
	collectionTtl int64,
) ([]*datapb.CompactionSegment, error) {
	_ = tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "mergeSortMultipleSegments")
	defer span.End()

	log := log.With(zap.Int64("planID", planID), zap.Int64("compactTo segment", writer.GetSegmentID()))

	var (
		syncBatchCount    int   // binlog batch count
		remainingRowCount int64 // the number of remaining entities
		expiredRowCount   int64 // the number of expired entities
		unflushedRowCount int64 = 0

		// All binlog meta of a segment
		allBinlogs = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
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

	downloadTimeCost := time.Duration(0)
	serWriteTimeCost := time.Duration(0)
	uploadTimeCost := time.Duration(0)

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
		segmentReaders[i] = NewSegmentDeserializeReader(binlogPaths, binlogIO, writer.GetPkID())
	}

	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	for i, r := range segmentReaders {
		if r.Next() == nil {
			heap.Push(&pq, &PQItem{
				Value: r.Value(),
				Index: i,
			})
		}
	}

	for pq.Len() > 0 {
		smallest := heap.Pop(&pq).(*PQItem)
		v := smallest.Value

		if isValueDeleted(v) {
			continue
		}

		// Filtering expired entity
		if isExpiredEntity(collectionTtl, currentTs, typeutil.Timestamp(v.Timestamp)) {
			expiredRowCount++
			continue
		}

		err := writer.Write(v)
		if err != nil {
			log.Warn("compact wrong, failed to writer row", zap.Error(err))
			return nil, err
		}
		unflushedRowCount++
		remainingRowCount++

		if (unflushedRowCount+1)%100 == 0 && writer.FlushAndIsFull() {
			serWriteStart := time.Now()
			kvs, partialBinlogs, err := serializeWrite(ctx, allocator, writer)
			if err != nil {
				log.Warn("compact wrong, failed to serialize writer", zap.Error(err))
				return nil, err
			}
			serWriteTimeCost += time.Since(serWriteStart)

			uploadStart := time.Now()
			if err := binlogIO.Upload(ctx, kvs); err != nil {
				log.Warn("compact wrong, failed to upload kvs", zap.Error(err))
			}
			uploadTimeCost += time.Since(uploadStart)
			mergeFieldBinlogs(allBinlogs, partialBinlogs)
			syncBatchCount++
			unflushedRowCount = 0
		}

		err = segmentReaders[smallest.Index].Next()
		if err != nil && err != sio.EOF {
			return nil, err
		}
		if err == nil {
			next := &PQItem{
				Value: segmentReaders[smallest.Index].Value(),
				Index: smallest.Index,
			}
			heap.Push(&pq, next)
		}
	}

	if !writer.FlushAndIsEmpty() {
		serWriteStart := time.Now()
		kvs, partialBinlogs, err := serializeWrite(ctx, allocator, writer)
		if err != nil {
			log.Warn("compact wrong, failed to serialize writer", zap.Error(err))
			return nil, err
		}
		serWriteTimeCost += time.Since(serWriteStart)

		uploadStart := time.Now()
		if err := binlogIO.Upload(ctx, kvs); err != nil {
			log.Warn("compact wrong, failed to upload kvs", zap.Error(err))
		}
		uploadTimeCost += time.Since(uploadStart)

		mergeFieldBinlogs(allBinlogs, partialBinlogs)
		syncBatchCount++
	}

	serWriteStart := time.Now()
	sPath, err := statSerializeWrite(ctx, binlogIO, allocator, writer)
	if err != nil {
		log.Warn("compact wrong, failed to serialize write segment stats",
			zap.Int64("remaining row count", remainingRowCount), zap.Error(err))
		return nil, err
	}
	serWriteTimeCost += time.Since(serWriteStart)

	pack := &datapb.CompactionSegment{
		SegmentID:           writer.GetSegmentID(),
		InsertLogs:          lo.Values(allBinlogs),
		Field2StatslogPaths: []*datapb.FieldBinlog{sPath},
		NumOfRows:           remainingRowCount,
		IsSorted:            true,
	}

	totalElapse := tr.RecordSpan()

	log.Info("mergeSortMultipleSegments merge end",
		zap.Int64("remaining row count", remainingRowCount),
		zap.Int64("expired entities", expiredRowCount),
		zap.Int("binlog batch count", syncBatchCount),
		zap.Duration("download binlogs elapse", downloadTimeCost),
		zap.Duration("upload binlogs elapse", uploadTimeCost),
		zap.Duration("serWrite elapse", serWriteTimeCost),
		zap.Duration("deRead elapse", totalElapse-serWriteTimeCost-downloadTimeCost-uploadTimeCost),
		zap.Duration("total elapse", totalElapse))

	return []*datapb.CompactionSegment{pack}, nil
}
