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
	"path"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type mixCompactionTask struct {
	binlogIO    io.BinlogIO
	cm          storage.ChunkManager
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
	lobContext            *compaction.LOBCompactionContext
	lobContextInitialized bool

	// estimatedOutputSegmentCount is the estimated number of output segments
	// computed during preCompact, used for LOB compaction strategy decision
	estimatedOutputSegmentCount int64
}

var _ Compactor = (*mixCompactionTask)(nil)

func NewMixCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
	sortByFieldIDs []int64,
) *mixCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &mixCompactionTask{
		ctx:              ctx1,
		cancel:           cancel,
		binlogIO:         binlogIO,
		cm:               cm,
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
		// The plan is produced by datacoord, so a malformed plan is an internal
		// protocol violation, not user input.
		return merr.WrapErrServiceInternalMsg("compaction plan is illegal, there's no segments in compaction plan, planID = %d", t.GetPlanID())
	}

	if t.plan.GetMaxSize() == 0 {
		return merr.WrapErrServiceInternalMsg("compaction plan is illegal, empty maxSize, planID = %d", t.GetPlanID())
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
	mlog.Info(context.TODO(), "preCompaction analyze",
		mlog.Int64("planID", t.GetPlanID()),
		mlog.Int64("inputSize", currSize),
		mlog.Int64("targetSize", t.targetSize),
		mlog.Int("inputSegmentCount", len(t.plan.GetSegmentBinlogs())),
		mlog.Int64("estimatedOutputSegmentCount", t.estimatedOutputSegmentCount),
	)

	return nil
}

func (t *mixCompactionTask) ensureLOBCompactionContext(ctx context.Context) error {
	if t.lobContextInitialized {
		return nil
	}
	if err := t.initLOBCompactionContext(ctx); err != nil {
		return err
	}
	t.lobContextInitialized = true
	return nil
}

func (t *mixCompactionTask) buildWriterOptions(ctx context.Context) []storage.RwOption {
	writerOpts := []storage.RwOption{
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI),
	}

	if t.lobContext != nil && t.lobContext.ShouldRewriteAnyField() {
		// LOB base path at partition level: {root}/insert_log/{coll}/{part}
		lobBasePath := path.Join(t.compactionParams.StorageConfig.GetRootPath(),
			common.SegmentInsertLogPath, metautil.JoinIDPath(t.collectionID, t.partitionID))
		textColumnConfigs := t.lobContext.GetTextColumnConfigs(
			lobBasePath,
			t.compactionParams.TextInlineThreshold,
			t.compactionParams.TextMaxLobFileBytes,
			t.compactionParams.TextFlushThresholdBytes,
		)
		if len(textColumnConfigs) > 0 {
			writerOpts = append(writerOpts, storage.WithTextColumnConfigs(textColumnConfigs))
			mlog.Info(ctx, "TEXT column REWRITE_ALL mode enabled",
				mlog.Int("rewriteFieldCount", len(textColumnConfigs)),
			)
		}
	}
	if t.lobContext != nil && t.lobContext.HasReuseAllFields() {
		writerOpts = append(writerOpts, storage.WithTextRefsAsBinary())
	}

	return writerOpts
}

func (t *mixCompactionTask) mergeSplit(
	ctx context.Context,
) ([]*datapb.CompactionSegment, error) {
	_ = t.tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "MergeSplit")
	defer span.End()

	if err := t.ensureLOBCompactionContext(ctx); err != nil {
		return nil, err
	}

	segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)

	writerSchema := t.plan.GetSchema()

	writerOpts := t.buildWriterOptions(ctx)

	mWriter, err := NewMultiSegmentWriter(ctx,
		t.binlogIO, compAlloc, t.plan.GetMaxSize(), writerSchema,
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
		mlog.Warn(context.TODO(), "failed to get pk field from schema")
		return nil, err
	}

	for _, seg := range t.plan.GetSegmentBinlogs() {
		del, exp, err := t.writeSegment(ctx, seg, mWriter, pkField, writerSchema)
		if err != nil {
			mWriter.Close()
			return nil, err
		}
		deletedRowCount += del
		expiredRowCount += exp
	}
	if err := mWriter.Close(); err != nil {
		mlog.Warn(context.TODO(), "compact wrong, failed to finish writer", mlog.Err(err))
		return nil, err
	}
	res := mWriter.GetCompactionSegments()
	if len(res) == 0 {
		// append an empty segment
		id, err := segIDAlloc.AllocOne()
		if err != nil {
			return nil, err
		}
		// Stats stays nil for the empty placeholder. The receiver's
		// NewSegmentInfo path tolerates nil Stats and populates it
		// from the (empty) arrays on first read; an empty CompactionSegment
		// has no aggregate footprint to report.
		res = append(res, &datapb.CompactionSegment{
			SegmentID: id,
			NumOfRows: 0,
			Channel:   t.GetChannelName(),
		})
	}

	totalElapse := t.tr.RecordSpan()
	mlog.Info(context.TODO(), "compact mergeSplit end",
		mlog.Int64("deleted row count", deletedRowCount),
		mlog.Int64("expired entities", expiredRowCount),
		mlog.Duration("total elapse", totalElapse))

	return res, nil
}

func (t *mixCompactionTask) writeSegment(ctx context.Context,
	seg *datapb.CompactionSegmentBinlogs,
	mWriter *MultiSegmentWriter, pkField *schemapb.FieldSchema,
	writerSchema *schemapb.CollectionSchema,
) (deletedRowCount, expiredRowCount int64, err error) {
	delta, err := compaction.ComposeDeleteFromDeltalogs(ctx, pkField.DataType, seg,
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		mlog.Warn(context.TODO(), "compact wrong, fail to merge deltalogs", mlog.Err(err))
		return
	}
	entityFilter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime, seg.GetCommitTimestamp())

	reader, existingFields, err := newCompactionSegmentRecordReader(ctx, seg, t.plan.GetSchema(), t.compactionParams.StorageConfig,
		storage.WithCollectionID(t.collectionID),
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithVersion(seg.GetStorageVersion()),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		mlog.Warn(context.TODO(), "compact wrong, failed to new insert binlogs reader", mlog.Err(err))
		return
	}
	defer reader.Close()

	materializer, err := NewRecordMaterializer(writerSchema, writerSchema.GetFunctions(), existingFields)
	if err != nil {
		mlog.Warn(ctx, "compact wrong, failed to init record materializer", mlog.Err(err))
		return
	}
	defer materializer.Close()

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
				mlog.Warn(context.TODO(), "compact wrong, failed to iter through data", mlog.Err(err))
				return
			}
		}

		baseRecord := r
		r, err = materializer.Wrap(baseRecord)
		if err != nil {
			baseRecord.Release()
			mlog.Warn(ctx, "compact wrong, failed to materialize record", mlog.Err(err))
			return
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
					rb = storage.NewRecordBuilder(writerSchema)
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
					out := overwriteRecordTimestamps(rec, seg.GetCommitTimestamp())
					if out != rec {
						defer out.Release()
					}
					return mWriter.Write(out)
				}()
				if err != nil {
					releaseWrappedRecord(r, baseRecord)
					return 0, 0, err
				}
			}
		} else {
			out := overwriteRecordTimestamps(r, seg.GetCommitTimestamp())
			err := mWriter.Write(out)
			if out != r {
				out.Release()
			}
			if err != nil {
				releaseWrappedRecord(r, baseRecord)
				return 0, 0, err
			}
		}
		releaseWrappedRecord(r, baseRecord)
	}

	deltalogDeleteEntriesCount := len(delta)
	deletedRowCount = int64(entityFilter.GetDeletedCount())
	expiredRowCount = int64(entityFilter.GetExpiredCount())

	// track segment row statistics for LOB compaction in REUSE_ALL mode
	// this is used to update LOB file valid_rows based on per-segment deletion ratio
	if t.lobContext != nil && t.lobContext.HasReuseAllFields() {
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

	mlog.Info(context.TODO(), "compact start", mlog.Any("compactionParams", t.compactionParams),
		mlog.Any("plan", t.plan))

	if err := t.preCompact(); err != nil {
		mlog.Warn(context.TODO(), "compact wrong, failed to preCompact", mlog.Err(err))
		return nil, err
	}

	ctxTimeout, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	mlog.Info(context.TODO(), "compact start")
	// Decompress compaction binlogs first
	if err := binlog.DecompressCompactionBinlogsWithRootPath(t.compactionParams.StorageConfig.GetRootPath(), t.plan.SegmentBinlogs); err != nil {
		mlog.Warn(context.TODO(), "compact wrong, fail to decompress compaction binlogs", mlog.Err(err))
		return nil, err
	}
	// Unable to deal with all empty segments cases, so return error
	isEmpty := lo.EveryBy(lo.FlatMap(t.plan.GetSegmentBinlogs(), func(seg *datapb.CompactionSegmentBinlogs, _ int) []*datapb.FieldBinlog {
		return seg.GetFieldBinlogs()
	}), func(field *datapb.FieldBinlog) bool {
		return len(field.GetBinlogs()) == 0
	})

	if isEmpty {
		mlog.Warn(context.TODO(), "compact wrong, all segments' binlogs are empty")
		return nil, merr.WrapErrServiceInternalMsg("illegal compaction plan")
	}

	if err := t.ensureLOBCompactionContext(ctx); err != nil {
		return nil, err
	}

	sortMergeAppicable := t.compactionParams.UseMergeSort
	if sortMergeAppicable {
		for _, segment := range t.plan.GetSegmentBinlogs() {
			if !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
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
		mlog.Info(context.TODO(), "compact by merge sort")
		writerOpts := t.buildWriterOptions(ctx)
		res, err = mergeSortMultipleSegments(ctxTimeout, t.plan, t.collectionID, t.partitionID, t.maxRows, t.binlogIO,
			t.plan.GetSegmentBinlogs(), t.tr, t.currentTime, t.plan.GetCollectionTtl(), t.compactionParams,
			writerOpts, t.lobContext, t.sortByFieldIDs)
		if err != nil {
			mlog.Warn(context.TODO(), "compact wrong, fail to merge sort segments", mlog.Err(err))
			return nil, err
		}
	} else {
		res, err = t.mergeSplit(ctxTimeout)
		if err != nil {
			mlog.Warn(context.TODO(), "compact wrong, failed to mergeSplit", mlog.Err(err))
			return nil, err
		}
	}

	mlog.Info(context.TODO(), "compact done", mlog.Duration("compact elapse", time.Since(compactStart)), mlog.Any("res", res))

	metrics.DataNodeCompactionLatency.WithLabelValues(paramtable.GetStringNodeID(), t.plan.GetType().String()).Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	metrics.DataNodeCompactionLatencyInQueue.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(durInQueue.Milliseconds()))

	// apply LOB compaction for TEXT columns (REUSE_ALL mode)
	if err := t.applyLOBCompaction(ctx, res); err != nil {
		return nil, err
	}

	// Build text index inline for each output segment so the segment arrives at
	// QueryNode with TextStatsLogs populated, avoiding the CGO_LOAD CreateTextIndex
	// fallback at load time. Mirrors sortCompaction.
	//
	// Only sorted outputs get an inline text index. Unsorted mix-compaction outputs
	// (from mergeSplit) are interim: a later sortcompaction will re-emit them as
	// sorted and build the text index inline at that step. Building here would be
	// discarded work. For non-external collections, stats_inspector also skips
	// unsorted segments in its TextIndexJob filter, so the index would never be
	// promoted to text_stats_logs in datacoord either. External collections are the
	// exception (allowUnsorted = collection.IsExternal() in stats_inspector.go),
	// where the async TextIndexJob still covers unsorted segments as a fallback.
	textIndexStart := time.Now()
	for _, resultSegment := range res {
		if resultSegment.GetNumOfRows() == 0 {
			continue
		}
		if !resultSegment.GetIsSorted() && !resultSegment.GetIsSortedByNamespace() {
			continue
		}
		textStatsLogs, err := t.createTextIndex(ctx, resultSegment)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to create text indexes",
				mlog.Int64("targetSegmentID", resultSegment.GetSegmentID()), mlog.Err(err))
			return nil, err
		}
		// For V3 segments, register text index stats in manifest.
		// TextStatsLogs already carries full object keys for mixed-version compatibility;
		// AddStatsToManifest stores the manifest-relative representation at commit time.
		if resultSegment.GetManifest() != "" && len(textStatsLogs) > 0 {
			statEntries := packed.TextIndexStatEntries(textStatsLogs, t.plan.GetCurrentScalarIndexVersion())
			newManifest, mErr := packed.AddStatsToManifest(
				resultSegment.GetManifest(), t.compactionParams.StorageConfig, statEntries)
			if mErr != nil {
				mlog.Warn(context.TODO(), "failed to add text index stats to manifest",
					mlog.Int64("targetSegmentID", resultSegment.GetSegmentID()), mlog.Err(mErr))
				return nil, mErr
			}
			resultSegment.Manifest = newManifest
			// Dual-write: V3 segments store text index stats in both manifest and segment metadata.
			// Manifest is the source of truth at load time; metadata acts as a placeholder so that
			// needDoTextIndex() in stats_inspector.go won't trigger a redundant TextIndexJob.
		}
		resultSegment.TextStatsLogs = textStatsLogs
	}
	createTextIndexCost := time.Since(textIndexStart)
	metrics.DataNodeCompactionStageLatency.
		WithLabelValues(paramtable.GetStringNodeID(), t.plan.GetType().String(), "create_text_index").
		Observe(float64(createTextIndexCost.Milliseconds()))
	mlog.Info(context.TODO(), "compact create text index done", mlog.Duration("createTextIndexCost", createTextIndexCost))

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

func (t *mixCompactionTask) GetStorageConfig() *indexpb.StorageConfig {
	return t.compactionParams.StorageConfig
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
// createTextIndex delegates to the shared package-level createTextIndex helper,
// sourcing collection/partition from the task and segment-specific fields from the
// output segment. Mirrors sortCompactionTask.createTextIndex.
func (t *mixCompactionTask) createTextIndex(ctx context.Context,
	segment *datapb.CompactionSegment,
) (map[int64]*datapb.TextIndexStats, error) {
	return createTextIndex(ctx, t.cm, t.plan, t.compactionParams, segment.GetStorageVersion(),
		t.collectionID, t.partitionID, segment.GetSegmentID(), t.GetPlanID(), segment)
}

// NOTE: SetSegmentRowStats() must be called during compaction iteration to track deleted rows per segment.
func (t *mixCompactionTask) applyLOBCompaction(ctx context.Context, outputSegments []*datapb.CompactionSegment) error {
	if t.lobContext == nil {
		return nil
	}

	if !t.lobContext.HasReuseAllFields() {
		mlog.Info(context.TODO(), "all TEXT fields use REWRITE_ALL, no LOB file merging needed")
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
	mlog.Info(context.TODO(), "LOB file references merged to output manifests",
		mlog.Int("outputSegmentCount", len(outputManifests)),
		mlog.Int64s("reuseAllFieldIDs", reuseAllFieldIDs),
		mlog.Int64s("rewriteAllFieldIDs", rewriteAllFieldIDs),
		mlog.Any("updatedManifests", updatedManifests),
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

	mlog.Info(context.TODO(), "initializing LOB compaction context for TEXT columns")

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
		mlog.Info(context.TODO(), "no LOB files found in source segments")
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
		mlog.Info(context.TODO(), "LOB compaction strategy decided",
			mlog.Int64("fieldID", fieldID),
			mlog.String("strategy", func() string {
				if decision.Strategy == compaction.LOBStrategyReuseAll {
					return "REUSE_ALL"
				}
				return "REWRITE_ALL"
			}()),
			mlog.Bool("isForced", t.lobContext.IsForced),
			mlog.Int("sourceSegmentCount", sourceSegmentCount),
			mlog.Int("targetSegmentCount", targetSegmentCount),
			mlog.Float64("holeRatio", decision.OverallHoleRatio),
			mlog.Int64("validRows", decision.TotalValidRows),
			mlog.Int64("totalRows", decision.TotalRows),
		)
	}

	return nil
}
