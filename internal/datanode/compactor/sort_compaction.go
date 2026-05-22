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
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type sortCompactionTask struct {
	binlogIO    io.BinlogIO
	cm          storage.ChunkManager
	currentTime time.Time

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	collectionID          int64
	partitionID           int64
	segmentID             int64
	insertLogs            []*datapb.FieldBinlog
	storageVersion        int64
	segmentStorageVersion int64
	manifest              string
	useLoonFFI            bool

	ttlFieldID int64

	done chan struct{}
	tr   *timerecord.TimeRecorder

	compactionParams compaction.Params
	sortByFieldIDs   []int64

	// lobContext holds LOB compaction strategy decisions for TEXT columns
	lobContext *compaction.LOBCompactionContext
}

var _ Compactor = (*sortCompactionTask)(nil)

func NewSortCompactionTask(
	ctx context.Context,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
	sortByFieldIDs []int64,
) *sortCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &sortCompactionTask{
		ctx:              ctx1,
		cancel:           cancel,
		binlogIO:         io.NewBinlogIO(cm),
		cm:               cm,
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
		// The plan is produced by datacoord, so a malformed plan is an internal
		// protocol violation, not user input.
		return merr.WrapErrServiceInternalMsg("sort compaction should handle exactly one segment, but got %d segments, planID = %d",
			len(t.plan.GetSegmentBinlogs()), t.GetPlanID())
	}

	segment := t.plan.GetSegmentBinlogs()[0]
	t.collectionID = segment.GetCollectionID()
	t.partitionID = segment.GetPartitionID()
	t.segmentID = segment.GetSegmentID()

	if err := binlog.DecompressBinLogWithRootPath(t.compactionParams.StorageConfig.GetRootPath(),
		storage.InsertBinlog, t.collectionID, t.partitionID,
		t.segmentID, segment.GetFieldBinlogs()); err != nil {
		mlog.Warn(t.ctx, "Decompress insert binlog error", zap.Error(err))
		return err
	}

	if err := binlog.DecompressBinLogWithRootPath(t.compactionParams.StorageConfig.GetRootPath(),
		storage.DeleteBinlog, t.collectionID, t.partitionID,
		t.segmentID, segment.GetDeltalogs()); err != nil {
		mlog.Warn(t.ctx, "Decompress delta binlog error", zap.Error(err))
		return err
	}

	t.insertLogs = segment.GetFieldBinlogs()
	t.storageVersion = t.compactionParams.StorageVersion
	t.segmentStorageVersion = segment.GetStorageVersion()
	t.manifest = segment.GetManifest()
	t.useLoonFFI = t.compactionParams.UseLoonFFI
	t.ttlFieldID = getTTLFieldID(t.plan.GetSchema())

	mlog.Info(t.ctx, "preCompaction analyze",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.Int64("storageVersion", t.storageVersion),
		zap.Bool("useLoonFFI", t.useLoonFFI),
		zap.Any("compactionParams", t.compactionParams),
	)

	return nil
}

func (t *sortCompactionTask) sortSegment(ctx context.Context) (*datapb.CompactionPlanResult, error) {
	log := mlog.With(
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

	phaseStart := time.Now()

	writerSchema := t.plan.GetSchema()

	srw, err := storage.NewBinlogRecordWriter(ctx,
		t.collectionID,
		t.partitionID,
		targetSegmentID,
		writerSchema,
		alloc,
		t.compactionParams.BinLogMaxSize,
		numRows,
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return t.binlogIO.Upload(ctx, kvs)
		}),
		storage.WithVersion(t.storageVersion),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithUseLoonFFI(t.useLoonFFI),
	)
	if err != nil {
		log.Warn(ctx, "sort segment wrong, unable to init segment writer",
			zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}
	initWriterCost := time.Since(phaseStart)

	phaseStart = time.Now()
	deletePKs, err := compaction.ComposeDeleteFromDeltalogs(ctx, pkField.DataType, t.plan.SegmentBinlogs[0],
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		log.Warn(ctx, "load deletePKs failed", zap.Error(err))
		srw.Close()
		return nil, err
	}
	loadDeltaCost := time.Since(phaseStart)
	hasTTLField := t.ttlFieldID >= common.StartOfUserFieldID

	entityFilter := compaction.NewEntityFilter(deletePKs, t.plan.GetCollectionTtl(), t.currentTime, t.plan.GetSegmentBinlogs()[0].GetCommitTimestamp())
	var predicate func(r storage.Record, ri, i int) bool
	switch pkField.DataType {
	case schemapb.DataType_Int64:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.Int64).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			expireTs := int64(-1)
			if hasTTLField {
				col := r.Column(t.ttlFieldID).(*array.Int64)
				if col.IsValid(i) {
					expireTs = col.Value(i)
				}
			}
			return !entityFilter.Filtered(pk, uint64(ts), expireTs)
		}
	case schemapb.DataType_VarChar:
		predicate = func(r storage.Record, ri, i int) bool {
			pk := r.Column(pkField.FieldID).(*array.String).Value(i)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(i)
			expireTs := int64(-1)
			if hasTTLField {
				col := r.Column(t.ttlFieldID).(*array.Int64)
				if col.IsValid(i) {
					expireTs = col.Value(i)
				}
			}
			return !entityFilter.Filtered(pk, uint64(ts), expireTs)
		}
	default:
		log.Warn(ctx, "sort task only support int64 and varchar pk field")
	}

	phaseStart = time.Now()
	rr, existingFields, err := newCompactionSegmentRecordReader(ctx, t.plan.GetSegmentBinlogs()[0], t.plan.Schema, t.compactionParams.StorageConfig,
		storage.WithVersion(t.segmentStorageVersion),
		storage.WithDownloader(t.binlogIO.Download),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithCollectionID(t.collectionID),
	)
	if err != nil {
		log.Warn(ctx, "error creating insert binlog reader", zap.Error(err))
		srw.Close()
		return nil, err
	}
	materializer, err := NewRecordMaterializer(writerSchema, writerSchema.GetFunctions(), existingFields)
	if err != nil {
		log.Warn("error creating record materializer", zap.Error(err))
		rr.Close()
		srw.Close()
		return nil, err
	}
	rr = newMaterializedRecordReader(rr, materializer)
	defer rr.Close()
	initReaderCost := time.Since(phaseStart)

	rr = wrapReaderWithTimestampOverwrite(rr, t.plan.GetSegmentBinlogs()[0].GetCommitTimestamp())
	rrs := []storage.RecordReader{rr}
	numValidRows, sortTimings, err := storage.Sort(t.compactionParams.BinLogMaxSize, writerSchema, rrs, srw, predicate, t.sortByFieldIDs)
	if err != nil {
		log.Warn(ctx, "sort failed", zap.Error(err))
		srw.Close()
		return nil, err
	}
	if sortTimings == nil {
		sortTimings = &storage.SortTimings{}
	}

	if t.lobContext != nil && t.lobContext.HasReuseAllFields() {
		t.lobContext.SetSegmentRowStats(t.segmentID, numRows, numRows-int64(numValidRows))
	}

	phaseStart = time.Now()
	if err := srw.Close(); err != nil {
		return nil, err
	}
	flushCost := time.Since(phaseStart)

	phaseStart = time.Now()
	binlogs, stats, bm25stats, manifest, expirQuantiles := srw.GetLogs()

	// For V3 segments, bloom filter and BM25 stats are already written to
	// basePath/_stats/ and registered in the manifest by writeStatsV3()
	// during PackedManifestRecordWriter.Close(). stats and bm25stats will
	// be nil; only the manifest carries stats information.
	if manifest != "" {
		stats = nil
		bm25stats = nil
	}

	insertLogs := storage.SortFieldBinlogs(binlogs)
	if err := binlog.CompressFieldBinlogs(insertLogs); err != nil {
		return nil, err
	}

	var statsLogs []*datapb.FieldBinlog
	if stats != nil {
		statsLogs = []*datapb.FieldBinlog{stats}
		if err := binlog.CompressFieldBinlogs(statsLogs); err != nil {
			return nil, err
		}
	}

	var bm25StatsLogs []*datapb.FieldBinlog
	if len(bm25stats) > 0 {
		bm25StatsLogs = lo.Values(bm25stats)
		if err := binlog.CompressFieldBinlogs(bm25StatsLogs); err != nil {
			return nil, err
		}
	}
	compressCost := time.Since(phaseStart)

	debug.FreeOSMemory()

	if numValidRows != int(numRows)-entityFilter.GetDeletedCount()-entityFilter.GetExpiredCount() {
		log.Warn(ctx, "unexpected row count after sort compaction",
			zap.Int64("target segmentID", targetSegmentID),
			zap.Int64("old rows", numRows),
			zap.Int("valid rows", numValidRows),
			zap.Int("deleted rows", entityFilter.GetDeletedCount()),
			zap.Int("expired rows", entityFilter.GetExpiredCount()))
		return nil, merr.WrapErrServiceInternal("unexpected row count")
	}

	log.Info(ctx, "sort segment end",
		zap.Int64("target segmentID", targetSegmentID),
		zap.Int64("old rows", numRows),
		zap.Int("valid rows", numValidRows),
		zap.Int("deleted rows", entityFilter.GetDeletedCount()),
		zap.Int("expired rows", entityFilter.GetExpiredCount()),
		zap.Int("deltaLogCount", len(t.plan.SegmentBinlogs[0].GetDeltalogs())),
		zap.Int("deletePKCount", len(deletePKs)),
		zap.Bool("useManifest", t.manifest != ""),
		zap.Duration("initWriterCost", initWriterCost),
		zap.Duration("loadDeltaCost", loadDeltaCost),
		zap.Duration("initReaderCost", initReaderCost),
		zap.Int("sortBatches", sortTimings.NumBatches),
		zap.Duration("sortReadCost", sortTimings.ReadCost),
		zap.Duration("sortSortCost", sortTimings.SortCost),
		zap.Duration("sortWriteCost", sortTimings.WriteCost),
		zap.Duration("flushCost", flushCost),
		zap.Duration("compressCost", compressCost),
		zap.Duration("total elapse", time.Since(sortStartTime)))

	nodeID := fmt.Sprint(paramtable.GetNodeID())
	compType := t.plan.GetType().String()
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "init_writer").Observe(float64(initWriterCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "load_delta").Observe(float64(loadDeltaCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "init_reader").Observe(float64(initReaderCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "sort_read").Observe(float64(sortTimings.ReadCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "sort_sort").Observe(float64(sortTimings.SortCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "sort_write").Observe(float64(sortTimings.WriteCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "flush").Observe(float64(flushCost.Milliseconds()))
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "compress").Observe(float64(compressCost.Milliseconds()))

	isNamespaceSorted := t.plan.GetSchema().GetEnableNamespace()
	isSorted := !isNamespaceSorted
	res := []*datapb.CompactionSegment{
		{
			PlanID:              t.GetPlanID(),
			SegmentID:           targetSegmentID,
			NumOfRows:           int64(numValidRows),
			InsertLogs:          insertLogs,
			Field2StatslogPaths: statsLogs,
			Bm25Logs:            bm25StatsLogs,
			Channel:             t.GetChannelName(),
			IsSorted:            isSorted,
			IsSortedByNamespace: isNamespaceSorted,
			StorageVersion:      t.storageVersion,
			Manifest:            manifest,
			ExpirQuantiles:      expirQuantiles,
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
		mlog.Warn(t.ctx, "failed to preCompact", zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}

	// init LOB compaction context for TEXT columns (if any)
	if err := t.initLOBCompactionContext(ctx); err != nil {
		mlog.Warn(ctx, "failed to init LOB compaction context", zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}

	compactStart := time.Now()

	log := mlog.With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.Int64("totalRows", t.plan.GetTotalRows()),
		zap.Int64("slotUsage", t.plan.GetSlotUsage()))

	log.Info(t.ctx, "compact start")

	stepStart := time.Now()
	res, err := t.sortSegment(ctx)
	if err != nil {
		log.Warn(t.ctx, "failed to sort segment",
			zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}
	sortSegmentCost := time.Since(stepStart)
	targetSegemntID := res.GetSegments()[0].GetSegmentID()
	insertLogs := res.GetSegments()[0].GetInsertLogs()
	if len(insertLogs) == 0 || res.GetSegments()[0].GetNumOfRows() == 0 {
		log.Info(t.ctx, "compact done, but target segment is zero num rows",
			zap.Int64("targetSegmentID", targetSegemntID),
			zap.Duration("sortSegmentCost", sortSegmentCost),
			zap.Duration("compact cost", time.Since(compactStart)))
		return res, nil
	}

	// apply LOB compaction: merge LOB file references to output manifest (REUSE_ALL)
	if err := t.applyLOBCompaction(ctx, res.GetSegments()); err != nil {
		log.Warn(t.ctx, "failed to apply LOB compaction", zap.Error(err))
		return &datapb.CompactionPlanResult{
			PlanID: t.GetPlanID(),
			State:  datapb.CompactionTaskState_failed,
		}, nil
	}

	stepStart = time.Now()
	for _, resultSegment := range res.GetSegments() {
		textStatsLogs, err := t.createTextIndex(ctx,
			t.collectionID, t.partitionID, targetSegemntID, t.GetPlanID(),
			resultSegment)
		if err != nil {
			log.Warn(t.ctx, "failed to create text indexes", zap.Int64("targetSegmentID", targetSegemntID),
				zap.Error(err))
			return &datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil
		}
		// For V3 segments, register text index stats in manifest.
		// TextStatsLogs already carries full object keys for mixed-version compatibility;
		// AddStatsToManifest stores the manifest-relative representation at commit time.
		if resultSegment.GetManifest() != "" && len(textStatsLogs) > 0 {
			statEntries := packed.TextIndexStatEntries(textStatsLogs, t.plan.GetCurrentScalarIndexVersion())
			newManifest, mErr := packed.AddStatsToManifest(
				resultSegment.GetManifest(), t.compactionParams.StorageConfig, statEntries)
			if mErr != nil {
				log.Warn(t.ctx, "failed to add text index stats to manifest",
					zap.Int64("targetSegmentID", targetSegemntID), zap.Error(mErr))
				return &datapb.CompactionPlanResult{
					PlanID: t.GetPlanID(),
					State:  datapb.CompactionTaskState_failed,
				}, nil
			}
			resultSegment.Manifest = newManifest
			// Dual-write: V3 segments store text index stats in both manifest and segment metadata.
			// Manifest is the source of truth at load time; metadata acts as a placeholder so that
			// needDoTextIndex() in stats_inspector.go won't trigger a redundant TextIndexJob.
		}
		resultSegment.TextStatsLogs = textStatsLogs
	}
	createTextIndexCost := time.Since(stepStart)

	totalCost := time.Since(compactStart)
	log.Info(t.ctx, "compact done", zap.Int64("targetSegmentID", targetSegemntID),
		zap.Duration("sortSegmentCost", sortSegmentCost),
		zap.Duration("createTextIndexCost", createTextIndexCost),
		zap.Duration("compact cost", totalCost))

	nodeID := fmt.Sprint(paramtable.GetNodeID())
	compType := t.plan.GetType().String()
	metrics.DataNodeCompactionStageLatency.WithLabelValues(nodeID, compType, "create_text_index").Observe(float64(createTextIndexCost.Milliseconds()))
	metrics.DataNodeCompactionLatency.WithLabelValues(nodeID, compType).Observe(float64(totalCost.Milliseconds()))
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

func (t *sortCompactionTask) GetStorageConfig() *indexpb.StorageConfig {
	return t.compactionParams.StorageConfig
}

func (t *sortCompactionTask) createTextIndex(ctx context.Context,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	taskID int64,
	segment *datapb.CompactionSegment,
) (map[int64]*datapb.TextIndexStats, error) {
	return createTextIndex(ctx, t.cm, t.plan, t.compactionParams, t.storageVersion, collectionID, partitionID, segmentID, taskID, segment)
}

// initLOBCompactionContext initializes the LOB compaction context for TEXT columns.
// For sort compaction, data is reordered but not redistributed, so TEXT columns
// use REUSE_ALL strategy (LOB references remain valid after reordering).
// The LOB file references need to be copied to the output segment's manifest.
func (t *sortCompactionTask) initLOBCompactionContext(ctx context.Context) error {
	// check if there are TEXT fields in schema
	textFieldIDs := compaction.GetTEXTFieldIDsFromSchema(t.plan.GetSchema())
	if len(textFieldIDs) == 0 {
		return nil // no TEXT fields, nothing to do
	}

	// only apply for manifest-based storage (storage v2/v3)
	if t.manifest == "" {
		return nil // no manifest-based segment, nothing to do
	}

	log := mlog.With(
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("segmentID", t.segmentID),
		zap.Int64s("textFieldIDs", textFieldIDs),
	)
	log.Info(ctx, "initializing LOB compaction context for TEXT columns (sort compaction)")

	// collect LOB files from source manifest
	sourceManifests := map[int64]string{t.segmentID: t.manifest}
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
		log.Info(ctx, "no LOB files found in source segment")
		return nil
	}

	// create LOB compaction context
	t.lobContext = compaction.NewLOBCompactionContext()
	for segID, files := range lobFilesBySegment {
		t.lobContext.AddSegmentLobFiles(segID, files)
	}

	// set compaction type - sort compaction forces REUSE_ALL
	// sort compaction always has 1 source segment and 1 output segment
	t.lobContext.SetCompactionType(datapb.CompactionType_SortCompaction, 1, 1)

	// compute strategies (will use forced REUSE_ALL for all TEXT fields)
	t.lobContext.ComputeStrategies(textFieldIDs, t.compactionParams.LOBHoleRatioThreshold)

	// log strategy decisions
	for fieldID, decision := range t.lobContext.Decisions {
		log.Info(ctx, "LOB compaction strategy decided",
			zap.Int64("fieldID", fieldID),
			zap.String("strategy", "REUSE_ALL"),
			zap.Bool("isForced", t.lobContext.IsForced),
			zap.Float64("holeRatio", decision.OverallHoleRatio),
		)
	}

	return nil
}

// applyLOBCompaction merges LOB file references from source segment to output segment manifests.
// Sort compaction uses REUSE_ALL strategy — LOB files are unchanged, only references need copying.
// SetSegmentRowStats is called to update valid_rows in case deleted rows were dropped during sort.
func (t *sortCompactionTask) applyLOBCompaction(ctx context.Context, outputSegments []*datapb.CompactionSegment) error {
	if t.lobContext == nil {
		return nil
	}

	if !t.lobContext.HasReuseAllFields() {
		return nil
	}

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

	for _, seg := range outputSegments {
		if newManifest, ok := updatedManifests[seg.GetSegmentID()]; ok {
			seg.Manifest = newManifest
		}
	}

	mlog.Info(ctx, "sort compaction: LOB file references merged to output manifest",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int("outputSegmentCount", len(outputManifests)),
	)

	return nil
}
