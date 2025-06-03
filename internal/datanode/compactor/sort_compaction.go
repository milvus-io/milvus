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
	"runtime/debug"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type sortCompactionTask struct {
	binlogIO    io.BinlogIO
	currentTime time.Time

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	collectionID   int64
	partitionID    int64
	segmentID      int64
	deltaLogs      []string
	insertLogs     []*datapb.FieldBinlog
	storageVersion int64

	done chan struct{}
	tr   *timerecord.TimeRecorder

	compactionParams compaction.Params
}

var _ Compactor = (*sortCompactionTask)(nil)

func NewSortCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
) *sortCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &sortCompactionTask{
		ctx:         ctx1,
		cancel:      cancel,
		binlogIO:    binlogIO,
		plan:        plan,
		tr:          timerecord.NewTimeRecorder("sort compaction"),
		currentTime: time.Now(),
		done:        make(chan struct{}, 1),
	}
}

// preCompact examines whether it's a valid compaction plan, and initializes the collectionID and partitionID
func (t *sortCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
	}

	var err error
	t.compactionParams, err = compaction.ParseParamsFromJSON(t.plan.GetJsonParams())
	if err != nil {
		return err
	}

	if len(t.plan.GetSegmentBinlogs()) != 1 {
		return errors.Newf("sort compaction should handle exactly one segment, but got %d segments, planID = %d",
			len(t.plan.GetSegmentBinlogs()), t.GetPlanID())
	}

	segment := t.plan.GetSegmentBinlogs()[0]
	t.collectionID = segment.GetCollectionID()
	t.partitionID = segment.GetPartitionID()
	t.segmentID = segment.GetSegmentID()

	if err := binlog.DecompressBinLogWithRootPath(t.compactionParams.StorageConfig.GetRootPath(),
		storage.InsertBinlog, t.collectionID, t.partitionID,
		t.segmentID, segment.GetFieldBinlogs()); err != nil {
		log.Ctx(t.ctx).Warn("Decompress insert binlog error", zap.Error(err))
		return err
	}

	if err := binlog.DecompressBinLogWithRootPath(t.compactionParams.StorageConfig.GetRootPath(),
		storage.DeleteBinlog, t.collectionID, t.partitionID,
		t.segmentID, segment.GetDeltalogs()); err != nil {
		log.Ctx(t.ctx).Warn("Decompress delta binlog error", zap.Error(err))
		return err
	}

	for _, d := range segment.GetDeltalogs() {
		for _, l := range d.GetBinlogs() {
			t.deltaLogs = append(t.deltaLogs, l.GetLogPath())
		}
	}

	t.insertLogs = segment.GetFieldBinlogs()
	t.storageVersion = storage.StorageV1
	if t.compactionParams.EnableStorageV2 {
		t.storageVersion = storage.StorageV2
	}

	log.Info("preCompaction analyze",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.Int64("storageVersion", t.storageVersion),
	)

	return nil
}

func (t *sortCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	if !funcutil.CheckCtxValid(t.ctx) {
		return nil, t.ctx.Err()
	}
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("MixCompact-%d", t.GetPlanID()))
	defer span.End()
	compactStart := time.Now()

	if err := t.preCompact(); err != nil {
		log.Warn("failed to preCompact", zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}

	ctx, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()
	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
	)

	numRows := t.plan.GetTotalRows()
	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		return nil, err
	}

	alloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())
	targetSegmentID := t.plan.GetPreAllocatedSegmentIDs().GetBegin()

	srw, err := storage.NewBinlogRecordWriter(ctx,
		t.collectionID,
		t.partitionID,
		targetSegmentID,
		t.plan.GetSchema(),
		alloc,
		t.compactionParams.BinLogMaxSize,
		t.compactionParams.StorageConfig.GetBucketName(),
		t.compactionParams.StorageConfig.GetRootPath(),
		numRows,
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return t.binlogIO.Upload(ctx, kvs)
		}),
		storage.WithVersion(t.storageVersion),
	)
	if err != nil {
		log.Warn("sort segment wrong, unable to init segment writer",
			zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	deletePKs, err := compaction.ComposeDeleteFromDeltalogs(ctx, t.binlogIO, t.deltaLogs)
	if err != nil {
		log.Warn("load deletePKs failed", zap.Error(err))
		return nil, err
	}

	entityFilter := compaction.NewEntityFilter(deletePKs, t.plan.GetCollectionTtl(), t.currentTime)
	var predicate func(r storage.Record, ri, i int) bool
	switch pkField.DataType {
	case schemapb.DataType_Int64:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.Int64).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			return !entityFilter.Filtered(pk, uint64(ts))
		}
	case schemapb.DataType_VarChar:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.String).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			return !entityFilter.Filtered(pk, uint64(ts))
		}
	default:
		log.Warn("sort task only support int64 and varchar pk field")
	}

	rr, err := storage.NewBinlogRecordReader(ctx, t.insertLogs, t.plan.Schema,
		storage.WithVersion(t.storageVersion),
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithBucketName(t.compactionParams.StorageConfig.BucketName),
	)
	if err != nil {
		log.Warn("error creating insert binlog reader", zap.Error(err))
		return nil, err
	}
	rrs := []storage.RecordReader{rr}
	numValidRows, err := storage.Sort(t.compactionParams.BinLogMaxSize, t.plan.GetSchema(), rrs, srw, predicate)
	if err != nil {
		log.Warn("sort failed", zap.Error(err))
		return nil, err
	}
	if err := srw.Close(); err != nil {
		return nil, err
	}

	binlogs, stats, bm25stats := srw.GetLogs()
	insertLogs := lo.Values(binlogs)
	if err := binlog.CompressFieldBinlogs(insertLogs); err != nil {
		return nil, err
	}

	statsLogs := []*datapb.FieldBinlog{stats}
	if err := binlog.CompressFieldBinlogs(statsLogs); err != nil {
		return nil, err
	}

	bm25StatsLogs := lo.Values(bm25stats)
	if err := binlog.CompressFieldBinlogs(bm25StatsLogs); err != nil {
		return nil, err
	}

	debug.FreeOSMemory()
	log.Info("sort segment end",
		zap.Int64("target segmentID", targetSegmentID),
		zap.Int64("old rows", numRows),
		zap.Int("valid rows", numValidRows),
		zap.Int("deleted rows", entityFilter.GetDeletedCount()),
		zap.Int("expired rows", entityFilter.GetExpiredCount()),
		zap.Duration("total elapse", time.Since(compactStart)))

	res := []*datapb.CompactionSegment{
		{
			PlanID:              t.GetPlanID(),
			SegmentID:           targetSegmentID,
			NumOfRows:           int64(numValidRows),
			InsertLogs:          insertLogs,
			Field2StatslogPaths: statsLogs,
			Bm25Logs:            bm25StatsLogs,
			Channel:             t.GetChannelName(),
			IsSorted:            true,
			StorageVersion:      t.storageVersion,
		},
	}
	planResult := &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Channel:  t.GetChannelName(),
		Segments: res,
		Type:     t.plan.GetType(),
	}
	return planResult, nil
}

func (t *sortCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *sortCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *sortCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *sortCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *sortCompactionTask) GetCompactionType() datapb.CompactionType {
	return datapb.CompactionType_SortCompaction
}

func (t *sortCompactionTask) GetCollection() typeutil.UniqueID {
	return t.collectionID
}

func (t *sortCompactionTask) GetSlotUsage() int64 {
	return paramtable.Get().DataCoordCfg.SortCompactionSlotUsage.GetAsInt64()
}
