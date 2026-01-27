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
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	bm25FieldIDs []int64

	done chan struct{}
	tr   *timerecord.TimeRecorder

	compactionParams compaction.Params
	sortByFieldIDs   []int64

	ttlFieldID int64

	// lobContext holds LOB compaction strategy decisions for TEXT columns
	lobContext *compaction.LOBCompactionContext

	// estimatedOutputSegmentCount is the estimated number of output segments
	// computed during preCompact, used for LOB compaction strategy decision
	estimatedOutputSegmentCount int64
}

var _ Compactor = (*mixCompactionTask)(nil)

func NewMixCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
	sortByFieldIDs []int64,
) *mixCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &mixCompactionTask{
		ctx:              ctx1,
		cancel:           cancel,
		binlogIO:         binlogIO,
		plan:             plan,
		tr:               timerecord.NewTimeRecorder("mergeSplit compaction"),
		currentTime:      time.Now(),
		done:             make(chan struct{}, 1),
		compactionParams: compactionParams,
		sortByFieldIDs:   sortByFieldIDs,
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
	t.ttlFieldID = getTTLFieldID(t.plan.GetSchema())
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

	t.estimatedOutputSegmentCount = int64(math.Ceil(float64(currSize) / float64(t.targetSize)))
	log.Info("preCompaction analyze",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("inputSize", currSize),
		zap.Int64("targetSize", t.targetSize),
		zap.Int("inputSegmentCount", len(t.plan.GetSegmentBinlogs())),
		zap.Int64("estimatedOutputSegmentCount", t.estimatedOutputSegmentCount),
	)

	return nil
}

func (t *mixCompactionTask) mergeSplit(
	ctx context.Context,
) ([]*datapb.CompactionSegment, error) {
	_ = t.tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "MergeSplit")
	defer span.End()

	log := log.With(zap.Int64("planID", t.GetPlanID()))

	// initialize LOB compaction context for TEXT columns
	if err := t.initLOBCompactionContext(ctx); err != nil {
		log.Warn("failed to initialize LOB compaction context", zap.Error(err))
		// continue without REWRITE_ALL support, will fall back to REUSE_ALL only
	}

	segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)

	// build writer options
	writerOpts := []storage.RwOption{
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI),
	}

	// add TEXT column configs for REWRITE_ALL mode
	if t.lobContext != nil && t.lobContext.ShouldRewriteAnyField() {
		// generate LOB base path for new LOB files
		lobBasePath := t.compactionParams.StorageConfig.GetRootPath()
		textColumnConfigs := t.lobContext.GetTextColumnConfigs(
			lobBasePath,
			t.compactionParams.TextInlineThreshold,
			t.compactionParams.TextMaxLobFileBytes,
			t.compactionParams.TextFlushThresholdBytes,
		)
		if len(textColumnConfigs) > 0 {
			writerOpts = append(writerOpts, storage.WithTextColumnConfigs(textColumnConfigs))
			log.Info("TEXT column REWRITE_ALL mode enabled",
				zap.Int("rewriteFieldCount", len(textColumnConfigs)),
			)
		}
	}

	mWriter, err := NewMultiSegmentWriter(ctx,
		t.binlogIO, compAlloc, t.plan.GetMaxSize(), t.plan.GetSchema(),
		t.compactionParams, t.maxRows, t.partitionID, t.collectionID, t.GetChannelName(), 4096,
		writerOpts...,
	)
	if err != nil {
		return nil, err
	}

	deletedRowCount := int64(0)
	expiredRowCount := int64(0)

	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
	}

	for _, seg := range t.plan.GetSegmentBinlogs() {
		del, exp, err := t.writeSegment(ctx, seg, mWriter, pkField)
		if err != nil {
			mWriter.Close()
			return nil, err
		}
		deletedRowCount += del
		expiredRowCount += exp
	}
	if err := mWriter.Close(); err != nil {
		log.Warn("compact wrong, failed to finish writer", zap.Error(err))
		return nil, err
	}
	res := mWriter.GetCompactionSegments()
	if len(res) == 0 {
		// append an empty segment
		id, err := segIDAlloc.AllocOne()
		if err != nil {
			return nil, err
		}
		res = append(res, &datapb.CompactionSegment{
			SegmentID: id,
			NumOfRows: 0,
			Channel:   t.GetChannelName(),
		})
	}

	totalElapse := t.tr.RecordSpan()
	log.Info("compact mergeSplit end",
		zap.Int64("deleted row count", deletedRowCount),
		zap.Int64("expired entities", expiredRowCount),
		zap.Duration("total elapse", totalElapse))

	return res, nil
}

func (t *mixCompactionTask) writeSegment(ctx context.Context,
	seg *datapb.CompactionSegmentBinlogs,
	mWriter *MultiSegmentWriter, pkField *schemapb.FieldSchema,
) (deletedRowCount, expiredRowCount int64, err error) {
	deltaPaths := make([]string, 0)
	for _, fieldBinlog := range seg.GetDeltalogs() {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			deltaPaths = append(deltaPaths, binlog.GetLogPath())
		}
	}
	delta, err := compaction.ComposeDeleteFromDeltalogs(ctx, t.binlogIO, deltaPaths)
	if err != nil {
		log.Warn("compact wrong, fail to merge deltalogs", zap.Error(err))
		return
	}
	entityFilter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime)

	var reader storage.RecordReader
	if seg.GetManifest() != "" {
		reader, err = storage.NewManifestRecordReader(ctx,
			seg.GetManifest(),
			t.plan.GetSchema(),
			storage.WithCollectionID(t.collectionID),
			storage.WithDownloader(t.binlogIO.Download),
			storage.WithVersion(seg.GetStorageVersion()),
			storage.WithStorageConfig(t.compactionParams.StorageConfig),
		)
	} else {
		reader, err = storage.NewBinlogRecordReader(ctx,
			seg.GetFieldBinlogs(),
			t.plan.GetSchema(),
			storage.WithCollectionID(t.collectionID),
			storage.WithDownloader(t.binlogIO.Download),
			storage.WithVersion(seg.GetStorageVersion()),
			storage.WithStorageConfig(t.compactionParams.StorageConfig),
		)
	}
	if err != nil {
		log.Warn("compact wrong, failed to new insert binlogs reader", zap.Error(err))
		return
	}
	defer reader.Close()

	hasTTLField := t.ttlFieldID >= common.StartOfUserFieldID
	var totalRowsRead int64

	for {
		var r storage.Record
		r, err = reader.Next()
		if err != nil {
			if err == sio.EOF {
				err = nil
				break
			} else {
				log.Warn("compact wrong, failed to iter through data", zap.Error(err))
				return
			}
		}

		totalRowsRead += int64(r.Len())

		var (
			pkArray = r.Column(pkField.FieldID)
			tsArray = r.Column(common.TimeStampField).(*array.Int64)
			ttlArr  *array.Int64

			sliceStart = -1
			rb         *storage.RecordBuilder
		)

		if hasTTLField {
			ttlArr = r.Column(t.ttlFieldID).(*array.Int64)
		}

		for i := range r.Len() {
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
			expireTs := int64(-1)
			if hasTTLField {
				if ttlArr.IsValid(i) {
					expireTs = ttlArr.Value(i)
				}
			}
			if entityFilter.Filtered(pk, ts, expireTs) {
				if rb == nil {
					rb = storage.NewRecordBuilder(t.plan.GetSchema())
				}
				if sliceStart != -1 {
					rb.Append(r, sliceStart, i)
				}
				sliceStart = -1
				continue
			}

			if sliceStart == -1 {
				sliceStart = i
			}
		}

		if rb != nil {
			if sliceStart != -1 {
				rb.Append(r, sliceStart, r.Len())
			}
			if rb.GetRowNum() > 0 {
				err := func() error {
					rec := rb.Build()
					defer rec.Release()
					return mWriter.Write(rec)
				}()
				if err != nil {
					return 0, 0, err
				}
			}
		} else {
			err := mWriter.Write(r)
			if err != nil {
				return 0, 0, err
			}
		}
	}

	deltalogDeleteEntriesCount := len(delta)
	deletedRowCount = int64(entityFilter.GetDeletedCount())
	expiredRowCount = int64(entityFilter.GetExpiredCount())

	// track segment row statistics for LOB compaction in REUSE_ALL mode
	// this is used to update LOB file valid_rows based on per-segment deletion ratio
	if t.lobContext != nil && !t.lobContext.ShouldRewriteAnyField() {
		totalDeleted := deletedRowCount + expiredRowCount
		t.lobContext.SetSegmentRowStats(seg.GetSegmentID(), totalRowsRead, totalDeleted)
	}

	metrics.DataNodeCompactionDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(deltalogDeleteEntriesCount))
	metrics.DataNodeCompactionMissingDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(entityFilter.GetMissingDeleteCount()))

	return
}

func (t *mixCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	durInQueue := t.tr.RecordSpan()
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("MixCompact-%d", t.GetPlanID()))
	defer span.End()
	compactStart := time.Now()

	log.Info("compact start", zap.Any("compactionParams", t.compactionParams),
		zap.Any("plan", t.plan))

	if err := t.preCompact(); err != nil {
		log.Warn("compact wrong, failed to preCompact", zap.Error(err))
		return nil, err
	}

	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID))

	ctxTimeout, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	log.Info("compact start")
	// Decompress compaction binlogs first
	if err := binlog.DecompressCompactionBinlogsWithRootPath(t.compactionParams.StorageConfig.GetRootPath(), t.plan.SegmentBinlogs); err != nil {
		log.Warn("compact wrong, fail to decompress compaction binlogs", zap.Error(err))
		return nil, err
	}
	// Unable to deal with all empty segments cases, so return error
	isEmpty := lo.EveryBy(lo.FlatMap(t.plan.GetSegmentBinlogs(), func(seg *datapb.CompactionSegmentBinlogs, _ int) []*datapb.FieldBinlog {
		return seg.GetFieldBinlogs()
	}), func(field *datapb.FieldBinlog) bool {
		return len(field.GetBinlogs()) == 0
	})

	if isEmpty {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errors.New("illegal compaction plan")
	}

	sortMergeAppicable := t.compactionParams.UseMergeSort
	if sortMergeAppicable {
		for _, segment := range t.plan.GetSegmentBinlogs() {
			if !segment.GetIsSorted() {
				sortMergeAppicable = false
				break
			}
		}

		if len(t.plan.GetSegmentBinlogs()) > t.compactionParams.MaxSegmentMergeSort {
			// sort merge is not applicable if there is only one segment or too many segments
			sortMergeAppicable = false
		}
	}

	var res []*datapb.CompactionSegment
	var err error
	if sortMergeAppicable {
		log.Info("compact by merge sort")
		res, err = mergeSortMultipleSegments(ctxTimeout, t.plan, t.collectionID, t.partitionID, t.maxRows, t.binlogIO,
			t.plan.GetSegmentBinlogs(), t.tr, t.currentTime, t.plan.GetCollectionTtl(), t.compactionParams, t.sortByFieldIDs)
		if err != nil {
			log.Warn("compact wrong, fail to merge sort segments", zap.Error(err))
			return nil, err
		}
	} else {
		res, err = t.mergeSplit(ctxTimeout)
		if err != nil {
			log.Warn("compact wrong, failed to mergeSplit", zap.Error(err))
			return nil, err
		}
	}

	log.Info("compact done", zap.Duration("compact elapse", time.Since(compactStart)), zap.Any("res", res))

	// apply LOB compaction for TEXT columns (REUSE_ALL mode)
	if err := t.applyLOBCompaction(ctx, res); err != nil {
		return nil, err
	}

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

// applyLOBCompaction handles TEXT column LOB file merging for REUSE_ALL strategy.
// this is called after compaction completes to update output manifests with merged LOB file references.
// it uses t.lobContext which was initialized by initLOBCompactionContext before compaction.
//
// This function handles two scenarios:
//   - REUSE_ALL: all TEXT fields reuse existing LOB files, merge LOB file references
//   - REWRITE_ALL: all TEXT fields are rewritten, LOB files handled by segment writer
//
// NOTE: SetSegmentRowStats() must be called during compaction iteration to track deleted rows per segment.
func (t *mixCompactionTask) applyLOBCompaction(ctx context.Context, outputSegments []*datapb.CompactionSegment) error {
	if t.lobContext == nil {
		return nil
	}

	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()))

	if !t.lobContext.HasReuseAllFields() {
		log.Info("all TEXT fields use REWRITE_ALL, no LOB file merging needed")
		return nil
	}

	// merge LOB file references for REUSE_ALL fields to output manifests
	outputManifests := make(map[int64]string)
	for _, seg := range outputSegments {
		if seg.GetManifest() != "" {
			outputManifests[seg.GetSegmentID()] = seg.GetManifest()
		}
	}

	if len(outputManifests) == 0 {
		return nil
	}

	updatedManifests, err := compaction.ApplyLobCompactionToManifests(t.lobContext, outputManifests, t.compactionParams.StorageConfig)
	if err != nil {
		return err
	}

	// update output segments with new manifest paths (version changed after LOB commit)
	for _, seg := range outputSegments {
		if newManifest, ok := updatedManifests[seg.GetSegmentID()]; ok {
			seg.Manifest = newManifest
		}
	}

	reuseAllFieldIDs := t.lobContext.GetReuseAllFieldIDs()
	rewriteAllFieldIDs := t.lobContext.GetRewriteAllFieldIDs()
	log.Info("LOB file references merged to output manifests",
		zap.Int("outputSegmentCount", len(outputManifests)),
		zap.Int64s("reuseAllFieldIDs", reuseAllFieldIDs),
		zap.Int64s("rewriteAllFieldIDs", rewriteAllFieldIDs),
		zap.Any("updatedManifests", updatedManifests),
	)

	return nil
}

// initLOBCompactionContext initializes the LOB compaction context for TEXT columns.
// this is called before compaction starts to determine REUSE_ALL vs REWRITE_ALL strategy.
func (t *mixCompactionTask) initLOBCompactionContext(ctx context.Context) error {
	// check if there are TEXT fields in schema
	textFieldIDs := compaction.GetTEXTFieldIDsFromSchema(t.plan.GetSchema())
	if len(textFieldIDs) == 0 {
		return nil // no TEXT fields, nothing to do
	}

	// only apply for manifest-based storage (storage v2/v3)
	hasManifest := false
	for _, seg := range t.plan.GetSegmentBinlogs() {
		if seg.GetManifest() != "" {
			hasManifest = true
			break
		}
	}
	if !hasManifest {
		return nil // no manifest-based segments, nothing to do
	}

	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64s("textFieldIDs", textFieldIDs),
	)
	log.Info("initializing LOB compaction context for TEXT columns")

	// collect source segment manifests
	sourceManifests := make(map[int64]string)
	for _, seg := range t.plan.GetSegmentBinlogs() {
		if seg.GetManifest() != "" {
			sourceManifests[seg.GetSegmentID()] = seg.GetManifest()
		}
	}

	// collect LOB files from source manifests
	lobFilesBySegment, err := compaction.CollectLobFilesFromManifests(sourceManifests, t.compactionParams.StorageConfig)
	if err != nil {
		return err
	}

	// check if there are any LOB files
	hasLobFiles := false
	for _, files := range lobFilesBySegment {
		if len(files) > 0 {
			hasLobFiles = true
			break
		}
	}
	if !hasLobFiles {
		log.Info("no LOB files found in source segments")
		return nil
	}

	// create LOB compaction context and compute strategies
	t.lobContext = compaction.NewLOBCompactionContext()
	for segID, files := range lobFilesBySegment {
		t.lobContext.AddSegmentLobFiles(segID, files)
	}

	// set compaction type to check for forced strategy
	// for mix compaction:
	//   - if 1 segment splits into N segments: force REWRITE_ALL
	//   - otherwise: compute strategy based on hole ratio
	sourceSegmentCount := len(t.plan.GetSegmentBinlogs())
	targetSegmentCount := int(t.estimatedOutputSegmentCount)
	t.lobContext.SetCompactionType(datapb.CompactionType_MixCompaction, sourceSegmentCount, targetSegmentCount)

	t.lobContext.ComputeStrategies(textFieldIDs, t.compactionParams.LOBHoleRatioThreshold)

	// log strategy decisions
	for fieldID, decision := range t.lobContext.Decisions {
		log.Info("LOB compaction strategy decided",
			zap.Int64("fieldID", fieldID),
			zap.String("strategy", func() string {
				if decision.Strategy == compaction.LOBStrategyReuseAll {
					return "REUSE_ALL"
				}
				return "REWRITE_ALL"
			}()),
			zap.Bool("isForced", t.lobContext.IsForced),
			zap.Int("sourceSegmentCount", sourceSegmentCount),
			zap.Int("targetSegmentCount", targetSegmentCount),
			zap.Float64("holeRatio", decision.OverallHoleRatio),
			zap.Int64("validRows", decision.TotalValidRows),
			zap.Int64("totalRows", decision.TotalRows),
		)
	}

	return nil
}
