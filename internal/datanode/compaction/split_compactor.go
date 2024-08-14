package compaction

import (
	"context"
	"fmt"
	sio "io"
	"math"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type splitCompactionTask struct {
	allocator.Allocator
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

var _ Compactor = (*splitCompactionTask)(nil)

func NewSplitCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	alloc allocator.Allocator,
	plan *datapb.CompactionPlan,
) *splitCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &splitCompactionTask{
		ctx:       ctx1,
		cancel:    cancel,
		binlogIO:  binlogIO,
		Allocator: alloc,
		plan:      plan,
		tr:        timerecord.NewTimeRecorder("split compaction"),
		currentTs: tsoutil.GetCurrentTime(),
		done:      make(chan struct{}, 1),
	}
}

// preCompact exams whether its a valid compaction plan, and init the collectionID and partitionID
func (t *splitCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
	}

	if len(t.plan.GetSegmentBinlogs()) < 1 {
		return errors.Newf("compaction plan is illegal, there's no segments compaction plan, planID = %d", t.plan.GetPlanID())
	}

	// Only support m -> n  where m == 1, n > 1, m < n now.
	// TODO: enable m -> n where m > 1, n > 1, m < n
	// TODO: enable m -> n where m >= 1, n >= 1, and merge with mix_compactor
	if len(t.plan.GetSegmentBinlogs()) > 1 {
		return errors.Newf("compaction plan is illegal, unable to deal with m -> n cases while m > 1, planID = %d", t.plan.GetPlanID())
	}
	if t.plan.GetMaxSize() == 0 {
		return errors.New("compaction plan is illegal, empty maxSize")
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

				// TODO: dc needs to make sure MemorySize is correctly set.
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

func (t *splitCompactionTask) split(
	ctx context.Context,
	binlogPaths [][]string,
	delta map[interface{}]typeutil.Timestamp,
	mWriter *MultiSegmentWriter,
) ([]*datapb.CompactionSegment, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "CompactSplit")
	defer span.End()

	log := log.With(zap.Int64("planID", t.GetPlanID()))
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
	log.Info("compact split end",
		zap.Int64s("split to segments", lo.Keys(mWriter.cachedMeta)),
		zap.Int64("deleted row count", deletedRowCount),
		zap.Int64("expired entities", expiredRowCount),
		zap.Duration("total elapse", totalElapse))

	return res, nil
}

func (t *splitCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("SplitCompact-%d", t.GetPlanID()))
	defer span.End()
	compactStart := time.Now()

	if err := t.preCompact(); err != nil {
		log.Warn("compact wrong, failed to preCompact", zap.Error(err))
		return nil, err
	}

	writer := NewMultiSegmentWriter(t.binlogIO, t.Allocator, t.plan, t.maxRows, t.partitionID, t.collectionID)

	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))

	deltaPaths, allBatchPaths, err := composePaths(t.plan.GetSegmentBinlogs())
	if err != nil {
		log.Warn("compact wrong, failed to composePaths", zap.Error(err))
		return nil, err
	}

	deltaPk2Ts, err := mergeDeltalogs(ctx, t.binlogIO, deltaPaths)
	if err != nil {
		log.Warn("compact wrong, fail to merge deltalogs", zap.Error(err))
		return nil, err
	}

	res, err := t.split(ctx, allBatchPaths, deltaPk2Ts, writer)
	if err != nil {
		log.Warn("compact wrong, failed to split", zap.Error(err))
		return nil, err
	}

	log.Info("compact done",
		zap.Duration("compact elapse", time.Since(compactStart)),
	)

	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).Observe(float64(t.tr.ElapseSpan().Milliseconds()))

	planResult := &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Channel:  t.GetChannelName(),
		Segments: res,
		Type:     t.plan.GetType(),
	}
	return planResult, nil
}

func (t *splitCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *splitCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *splitCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *splitCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *splitCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}

func (t *splitCompactionTask) GetCollection() typeutil.UniqueID {
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *splitCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}
