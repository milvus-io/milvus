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
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type sortCompactionTask struct {
	binlogIO    io.BinlogIO
	currentTime time.Time

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	collectionID          int64
	partitionID           int64
	segmentID             int64
	deltaLogs             []string
	insertLogs            []*datapb.FieldBinlog
	storageVersion        int64
	segmentStorageVersion int64

	done chan struct{}
	tr   *timerecord.TimeRecorder

	compactionParams compaction.Params
	sortByFieldIDs   []int64
}

var _ Compactor = (*sortCompactionTask)(nil)

func NewSortCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
	sortByFieldIDs []int64,
) *sortCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &sortCompactionTask{
		ctx:              ctx1,
		cancel:           cancel,
		binlogIO:         binlogIO,
		plan:             plan,
		tr:               timerecord.NewTimeRecorder("sort compaction"),
		currentTime:      time.Now(),
		done:             make(chan struct{}, 1),
		compactionParams: compactionParams,
		sortByFieldIDs:   sortByFieldIDs,
	}
}

// preCompact examines whether it's a valid compaction plan, and initializes the collectionID and partitionID
func (t *sortCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
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
	t.storageVersion = t.compactionParams.StorageVersion
	t.segmentStorageVersion = segment.GetStorageVersion()

	log.Ctx(t.ctx).Info("preCompaction analyze",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.Int64("storageVersion", t.storageVersion),
		zap.Any("compactionParams", t.compactionParams),
	)

	return nil
}

func (t *sortCompactionTask) sortSegment(ctx context.Context) (*datapb.CompactionPlanResult, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
	)
	sortStartTime := time.Now()
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
		numRows,
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return t.binlogIO.Upload(ctx, kvs)
		}),
		storage.WithVersion(t.storageVersion),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
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
		storage.WithVersion(t.segmentStorageVersion),
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithCollectionID(t.collectionID),
	)
	if err != nil {
		log.Warn("error creating insert binlog reader", zap.Error(err))
		return nil, err
	}
	defer rr.Close()
	rrs := []storage.RecordReader{rr}
	numValidRows, err := storage.Sort(t.compactionParams.BinLogMaxSize, t.plan.GetSchema(), rrs, srw, predicate, t.sortByFieldIDs)
	if err != nil {
		log.Warn("sort failed", zap.Error(err))
		return nil, err
	}
	if err := srw.Close(); err != nil {
		return nil, err
	}

	binlogs, stats, bm25stats := srw.GetLogs()
	insertLogs := storage.SortFieldBinlogs(binlogs)
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
		zap.Duration("total elapse", time.Since(sortStartTime)))

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

func (t *sortCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	if !funcutil.CheckCtxValid(t.ctx) {
		return nil, t.ctx.Err()
	}
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("MixCompact-%d", t.GetPlanID()))
	defer span.End()
	if err := t.preCompact(); err != nil {
		log.Warn("failed to preCompact", zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}

	compactStart := time.Now()

	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.Int64("totalRows", t.plan.GetTotalRows()),
		zap.Int64("slotUsage", t.plan.GetSlotUsage()))

	log.Info("compact start")

	res, err := t.sortSegment(ctx)
	if err != nil {
		log.Warn("failed to sort segment",
			zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}
	targetSegemntID := res.GetSegments()[0].GetSegmentID()
	insertLogs := res.GetSegments()[0].GetInsertLogs()
	if len(insertLogs) == 0 || res.GetSegments()[0].GetNumOfRows() == 0 {
		log.Info("compact done, but target segment is zero num rows",
			zap.Int64("targetSegmentID", targetSegemntID),
			zap.Duration("compact cost", time.Since(compactStart)))
		return res, nil
	}
	textStatsLogs, err := t.createTextIndex(ctx,
		t.collectionID, t.partitionID, targetSegemntID, t.GetPlanID(),
		res.GetSegments()[0].GetInsertLogs())
	if err != nil {
		log.Warn("failed to create text indexes", zap.Int64("targetSegmentID", targetSegemntID),
			zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}
	res.Segments[0].TextStatsLogs = textStatsLogs
	log.Info("compact done", zap.Int64("targetSegmentID", targetSegemntID),
		zap.Duration("compact cost", time.Since(compactStart)))
	return res, nil
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
	return t.plan.GetSlotUsage()
}

func (t *sortCompactionTask) createTextIndex(ctx context.Context,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	taskID int64,
	insertBinlogs []*datapb.FieldBinlog,
) (map[int64]*datapb.TextIndexStats, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
	)

	fieldBinlogs := lo.GroupBy(insertBinlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64) ([]string, error) {
		if t.storageVersion == storage.StorageV2 {
			return []string{}, nil
		}
		binlogs, ok := fieldBinlogs[fieldID]
		if !ok {
			return nil, fmt.Errorf("field binlog not found for field %d", fieldID)
		}
		result := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, file := range binlog.GetBinlogs() {
				result = append(result, metautil.BuildInsertLogPath(t.compactionParams.StorageConfig.GetRootPath(),
					collectionID, partitionID, segmentID, fieldID, file.GetLogID()))
			}
		}
		return result, nil
	}

	newStorageConfig, err := util.ParseStorageConfig(t.compactionParams.StorageConfig)
	if err != nil {
		return nil, err
	}

	textIndexLogs := make(map[int64]*datapb.TextIndexStats)
	for _, field := range t.plan.GetSchema().GetFields() {
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableMatch() {
			continue
		}
		log.Info("field enable match, ready to create text index", zap.Int64("field id", field.GetFieldID()))
		// create text index and upload the text index files.
		files, err := getInsertFiles(field.GetFieldID())
		if err != nil {
			return nil, err
		}

		buildIndexParams := &indexcgopb.BuildIndexInfo{
			BuildID:                   t.GetPlanID(),
			CollectionID:              collectionID,
			PartitionID:               partitionID,
			SegmentID:                 segmentID,
			IndexVersion:              0, // always zero
			InsertFiles:               files,
			FieldSchema:               field,
			StorageConfig:             newStorageConfig,
			CurrentScalarIndexVersion: t.plan.GetCurrentScalarIndexVersion(),
			StorageVersion:            t.storageVersion,
		}

		if t.storageVersion == storage.StorageV2 {
			buildIndexParams.SegmentInsertFiles = util.GetSegmentInsertFiles(
				insertBinlogs,
				t.compactionParams.StorageConfig,
				collectionID,
				partitionID,
				segmentID)
		}
		uploaded, err := indexcgowrapper.CreateTextIndex(ctx, buildIndexParams)
		if err != nil {
			return nil, err
		}
		textIndexLogs[field.GetFieldID()] = &datapb.TextIndexStats{
			FieldID: field.GetFieldID(),
			Version: 0,
			BuildID: taskID,
			Files:   lo.Keys(uploaded),
		}
		elapse := t.tr.RecordSpan()
		log.Info("field enable match, create text index done",
			zap.Int64("segmentID", segmentID),
			zap.Int64("field id", field.GetFieldID()),
			zap.Strings("files", lo.Keys(uploaded)),
			zap.Duration("elapse", elapse),
		)
	}
	return textIndexLogs, nil
}
