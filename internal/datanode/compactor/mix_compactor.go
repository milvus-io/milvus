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
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
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
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
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

	// LOB compaction support
	lobHelper         *LOBCompactionHelper
	sourceLOBMetadata []*storage.LOBSegmentMetadata
	lobMode           CompactionLOBMode

	// LOB readers/writers for SmartRewrite mode
	// lobReaders: fieldID -> (lobFileID -> LOBReaderInfo)
	lobReaders map[int64]map[uint64]*lobReaderInfo
	// lobWriters: fieldID -> LOB writer for target segment
	lobWriters map[int64]*storage.LOBWriter

	// LOB reference tracking for ReferenceOnly mode
	// map structure: fieldID -> map of lob_file_id -> reference count
	usedLOBFileIDs map[int64]map[int64]int64
	textFieldIDs   map[int64]struct{}
	lobTrackMutex  sync.Mutex
}

// lobReaderInfo holds LOB reader and associated file path
type lobReaderInfo struct {
	reader   *storage.LOBReader
	filePath string
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

	lobHelper := NewLOBCompactionHelper(compactionParams.StorageConfig)

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
		lobHelper:        lobHelper,
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
		zap.Int64("inputSize", currSize),
		zap.Int64("targetSize", t.targetSize),
		zap.Int("inputSegmentCount", len(t.plan.GetSegmentBinlogs())),
		zap.Int64("estimatedOutputSegmentCount", outputSegmentCount),
	)

	// LOB compaction preparation
	if t.lobHelper != nil {
		// get LOB metadata from source segments (from segment's stored metadata)
		t.sourceLOBMetadata = make([]*storage.LOBSegmentMetadata, 0)
		for _, segBinlog := range t.plan.GetSegmentBinlogs() {
			lobMeta := t.lobHelper.ConvertProtoLOBMetadata(
				segBinlog.GetLobMetadata(),
				segBinlog.GetSegmentID(),
				t.collectionID,
				t.partitionID,
			)
			if lobMeta != nil && lobMeta.HasLOBFields() {
				t.sourceLOBMetadata = append(t.sourceLOBMetadata, lobMeta)
			}
		}

		// decide LOB compaction mode if we have LOB data
		if len(t.sourceLOBMetadata) > 0 {
			lobGarbageRatio := t.lobHelper.CalculateLOBGarbageRatio(t.sourceLOBMetadata)

			t.lobMode = t.lobHelper.DecideCompactionMode(
				t.plan.GetType(),
				lobGarbageRatio,
				len(t.plan.GetSegmentBinlogs()),
			)

			log.Info("LOB compaction mode decided",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int("sourceLOBCount", len(t.sourceLOBMetadata)),
				zap.Float64("lobGarbageRatio", lobGarbageRatio),
				zap.String("mode", t.getLOBModeName(t.lobMode)),
			)

			// initialize TEXT field IDs and LOB tracking for ReferenceOnly mode
			if t.lobMode == CompactionLOBModeReferenceOnly {
				t.textFieldIDs = make(map[int64]struct{})
				for _, field := range t.plan.GetSchema().GetFields() {
					if field.GetDataType() == schemapb.DataType_Text {
						t.textFieldIDs[field.GetFieldID()] = struct{}{}
					}
				}
				t.usedLOBFileIDs = make(map[int64]map[int64]int64)
			}
		}
	}

	return nil
}

// getLOBModeName returns the string representation of LOB compaction mode
func (t *mixCompactionTask) getLOBModeName(mode CompactionLOBMode) string {
	switch mode {
	case CompactionLOBModeReferenceOnly:
		return "Reference-Only"
	case CompactionLOBModeSmartRewrite:
		return "SmartRewrite"
	case CompactionLOBModeSkip:
		return "Skip"
	default:
		return "Unknown"
	}
}

// trackLOBReferencesInRecord extracts and tracks LOB file references from TEXT fields in a record.
// used in ReferenceOnly mode to count actual references for each LOB file.
func (t *mixCompactionTask) trackLOBReferencesInRecord(r storage.Record) {
	if len(t.textFieldIDs) == 0 || t.usedLOBFileIDs == nil {
		return
	}

	t.lobTrackMutex.Lock()
	defer t.lobTrackMutex.Unlock()

	for textFieldID := range t.textFieldIDs {
		col := r.Column(storage.FieldID(textFieldID))
		if col == nil {
			continue
		}

		strArray, ok := col.(*array.String)
		if !ok {
			continue
		}

		for i := 0; i < strArray.Len(); i++ {
			if strArray.IsNull(i) {
				continue
			}

			strVal := strArray.Value(i)
			if !storage.IsLOBReference([]byte(strVal)) {
				continue
			}

			ref, err := storage.DecodeLOBReference([]byte(strVal))
			if err != nil {
				continue
			}

			if t.usedLOBFileIDs[textFieldID] == nil {
				t.usedLOBFileIDs[textFieldID] = make(map[int64]int64)
			}
			// increment reference count for this LOB file
			t.usedLOBFileIDs[textFieldID][int64(ref.LobFileID)]++
		}
	}
}

func (t *mixCompactionTask) mergeSplit(
	ctx context.Context,
) ([]*datapb.CompactionSegment, error) {
	_ = t.tr.RecordSpan()

	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "MergeSplit")
	defer span.End()

	log := log.With(zap.Int64("planID", t.GetPlanID()))

	segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
	logIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)
	mWriter, err := NewMultiSegmentWriter(ctx, t.binlogIO, compAlloc, t.plan.GetMaxSize(), t.plan.GetSchema(), t.compactionParams, t.maxRows, t.partitionID, t.collectionID, t.GetChannelName(), 4096, storage.WithStorageConfig(t.compactionParams.StorageConfig), storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI))
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

	// check if we need to process LOB fields (SmartRewrite mode)
	needProcessLOB := t.lobMode == CompactionLOBModeSmartRewrite && len(t.lobWriters) > 0

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

		// if SmartRewrite mode, wrap record with LOB processing
		if needProcessLOB {
			r = t.wrapRecordWithLOBProcessing(ctx, r)
		}

		var (
			pkArray    = r.Column(pkField.FieldID)
			tsArray    = r.Column(common.TimeStampField).(*array.Int64)
			sliceStart = -1
			rb         *storage.RecordBuilder
		)

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
			if entityFilter.Filtered(pk, ts) {
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
					if t.lobMode == CompactionLOBModeReferenceOnly {
						t.trackLOBReferencesInRecord(rec)
					}
					return mWriter.Write(rec)
				}()
				if err != nil {
					return 0, 0, err
				}
			}
		} else {
			if t.lobMode == CompactionLOBModeReferenceOnly {
				t.trackLOBReferencesInRecord(r)
			}
			err := mWriter.Write(r)
			if err != nil {
				return 0, 0, err
			}
		}
	}

	deltalogDeleteEntriesCount := len(delta)
	deletedRowCount = int64(entityFilter.GetDeletedCount())
	expiredRowCount = int64(entityFilter.GetExpiredCount())

	metrics.DataNodeCompactionDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(deltalogDeleteEntriesCount))
	metrics.DataNodeCompactionMissingDeleteCount.WithLabelValues(fmt.Sprint(t.collectionID)).Add(float64(entityFilter.GetMissingDeleteCount()))

	return
}

// lobProcessingRecord is a wrapper that lazily processes LOB references when columns are accessed
type lobProcessingRecord struct {
	originalRecord storage.Record
	task           *mixCompactionTask
	ctx            context.Context
	processedCols  map[int64]arrow.Array
	mu             sync.Mutex
}

// wrapRecordWithLOBProcessing wraps a record to process LOB references lazily
func (t *mixCompactionTask) wrapRecordWithLOBProcessing(ctx context.Context, rec storage.Record) storage.Record {
	return &lobProcessingRecord{
		originalRecord: rec,
		task:           t,
		ctx:            ctx,
		processedCols:  make(map[int64]arrow.Array),
	}
}

func (r *lobProcessingRecord) Column(fieldID storage.FieldID) arrow.Array {
	r.mu.Lock()
	defer r.mu.Unlock()

	// check if we already processed this column
	if processedCol, exists := r.processedCols[fieldID]; exists {
		return processedCol
	}

	// get original column
	origCol := r.originalRecord.Column(fieldID)
	if origCol == nil {
		return nil
	}

	// check if this is a LOB field that needs processing
	lobWriter, hasWriter := r.task.lobWriters[fieldID]
	if !hasWriter {
		// not a LOB field, return original
		return origCol
	}

	lobReaders, hasReaders := r.task.lobReaders[fieldID]
	if !hasReaders {
		return origCol
	}

	// this is a LOB field - process all references in the column
	binaryArray, ok := origCol.(*array.Binary)
	if !ok {
		// not a binary array, return original
		return origCol
	}

	// process all references in this column (batch processing like insert)
	newRefBytes := make([][]byte, binaryArray.Len())
	for rowIdx := 0; rowIdx < binaryArray.Len(); rowIdx++ {
		if binaryArray.IsNull(rowIdx) {
			newRefBytes[rowIdx] = nil
			continue
		}

		oldRefBytes := binaryArray.Value(rowIdx)

		// check if this is a LOB reference (by magic number) or inline data
		if !storage.IsLOBReference(oldRefBytes) {
			// not a LOB reference (inline data), keep original
			newRefBytes[rowIdx] = oldRefBytes
			continue
		}

		// decode old reference
		ref, err := storage.DecodeLOBReference(oldRefBytes)
		if err != nil {
			// decode failed, keep original
			newRefBytes[rowIdx] = oldRefBytes
			continue
		}

		// get LOB reader for this file
		lobReaderInfo, exists := lobReaders[ref.LobFileID]
		if !exists {
			log.Warn("LOB reader not found, keeping original reference",
				zap.Int64("fieldID", fieldID),
				zap.Uint64("lobFileID", ref.LobFileID))
			newRefBytes[rowIdx] = oldRefBytes
			continue
		}

		// read LOB data from source file
		lobData, err := lobReaderInfo.reader.ReadText(r.ctx, ref, lobReaderInfo.filePath)
		if err != nil {
			log.Warn("failed to read LOB data, keeping original reference",
				zap.Int64("fieldID", fieldID),
				zap.Uint64("lobFileID", ref.LobFileID),
				zap.Uint32("rowOffset", ref.RowOffset),
				zap.Error(err))
			newRefBytes[rowIdx] = oldRefBytes
			continue
		}

		// write LOB data to target file (like insert)
		newRef, err := lobWriter.WriteText(r.ctx, lobData)
		if err != nil {
			log.Warn("failed to write LOB data, keeping original reference",
				zap.Int64("fieldID", fieldID),
				zap.Error(err))
			newRefBytes[rowIdx] = oldRefBytes
			continue
		}

		// encode new reference
		newRefBytes[rowIdx] = storage.EncodeLOBReference(newRef)
	}

	// build new binary array with updated references
	binaryBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	for _, refBytes := range newRefBytes {
		if refBytes == nil {
			binaryBuilder.AppendNull()
		} else {
			binaryBuilder.Append(refBytes)
		}
	}
	processedCol := binaryBuilder.NewBinaryArray()
	binaryBuilder.Release()

	// cache the processed column
	r.processedCols[fieldID] = processedCol

	return processedCol
}

func (r *lobProcessingRecord) Len() int {
	return r.originalRecord.Len()
}

func (r *lobProcessingRecord) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// release all processed columns
	for _, col := range r.processedCols {
		col.Release()
	}
	r.processedCols = nil

	// release original record
	r.originalRecord.Release()
}

func (r *lobProcessingRecord) Retain() {
	r.originalRecord.Retain()

	r.mu.Lock()
	defer r.mu.Unlock()

	// retain all processed columns
	for _, col := range r.processedCols {
		col.Retain()
	}
}

// processLOBReferenceForRow processes a single row's LOB fields during compaction
// it reads LOB data from source files, writes to target files, and returns updated reference bytes
func (t *mixCompactionTask) processLOBReferenceForRow(
	ctx context.Context,
	fieldID int64,
	refBytes []byte,
) ([]byte, error) {
	if len(refBytes) != storage.LOBReferenceSize {
		return refBytes, nil
	}

	lobWriter, hasWriter := t.lobWriters[fieldID]
	if !hasWriter {
		return refBytes, errors.Newf("no LOB writer found for field %d", fieldID)
	}

	lobReaders, hasReaders := t.lobReaders[fieldID]
	if !hasReaders {
		return refBytes, errors.Newf("no LOB readers found for field %d", fieldID)
	}

	ref, err := storage.DecodeLOBReference(refBytes)
	if err != nil {
		log.Warn("failed to decode LOB reference, treating as regular data",
			zap.Int64("fieldID", fieldID),
			zap.Error(err))
		return refBytes, nil
	}

	lobReaderInfo, exists := lobReaders[ref.LobFileID]
	if !exists {
		return nil, errors.Newf("LOB reader not found for file ID %d field %d", ref.LobFileID, fieldID)
	}

	lobData, err := lobReaderInfo.reader.ReadText(ctx, ref, lobReaderInfo.filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read LOB data from file %d offset %d", ref.LobFileID, ref.RowOffset)
	}

	newRef, err := lobWriter.WriteText(ctx, lobData)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to write LOB data for field %d", fieldID)
	}

	newRefBytes := storage.EncodeLOBReference(newRef)
	return newRefBytes, nil
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

	// for SmartRewrite mode, initialize LOB readers/writers before compaction
	var lobFileIDAlloc allocator.Interface
	if t.lobMode == CompactionLOBModeSmartRewrite && t.lobHelper != nil && len(t.sourceLOBMetadata) > 0 {
		log.Info("initializing LOB readers/writers for SmartRewrite mode")

		if err := t.initLOBReaders(ctxTimeout); err != nil {
			log.Warn("failed to initialize LOB readers", zap.Error(err))
			return nil, err
		}
		defer t.closeLOBReaders(ctxTimeout)

		segIDAlloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedSegmentIDs().GetBegin(), t.plan.GetPreAllocatedSegmentIDs().GetEnd())
		targetSegmentID, err := segIDAlloc.AllocOne()
		if err != nil {
			log.Warn("failed to allocate target segment ID for LOB", zap.Error(err))
			return nil, err
		}

		lobFileIDAlloc = allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())

		if err := t.initLOBWriters(ctxTimeout, targetSegmentID, lobFileIDAlloc); err != nil {
			log.Warn("failed to initialize LOB writers", zap.Error(err))
			return nil, err
		}
		defer func() {
			if lobMeta, err := t.closeLOBWriters(ctxTimeout); err != nil {
				log.Warn("failed to close LOB writers", zap.Error(err))
			} else if lobMeta != nil && lobMeta.HasLOBFields() {
				if len(res) > 0 {
					protoMeta := t.lobHelper.ConvertToProtoLOBMetadata(lobMeta)
					res[0].LobMetadata = protoMeta
					log.Info("attached LOB metadata to compaction result",
						zap.Int("lobFiles", lobMeta.TotalLOBFiles),
						zap.Int64("lobRecords", lobMeta.TotalLOBRecords))
				}
			}
		}()
	}

	// use normal compaction path for all modes
	if sortMergeAppicable {
		log.Info("compact by merge sort")
		res, err = mergeSortMultipleSegments(ctxTimeout, t.plan, t.collectionID, t.partitionID, t.maxRows, t.binlogIO,
			t.plan.GetSegmentBinlogs(), t.tr, t.currentTime, t.plan.GetCollectionTtl(), t.compactionParams, t.sortByFieldIDs)
	} else {
		res, err = t.mergeSplit(ctxTimeout)
	}
	if err != nil {
		log.Warn("compact wrong, failed to compact", zap.Error(err))
		return nil, err
	}

	log.Info("compact done", zap.Duration("compact elapse", time.Since(compactStart)), zap.Any("res", res))

	// post-process LOB files based on mode
	if t.lobHelper != nil && len(t.sourceLOBMetadata) > 0 && len(res) > 0 {
		lobCompactStart := time.Now()

		switch t.lobMode {
		case CompactionLOBModeReferenceOnly:
			log.Info("post-processing LOB metadata in Reference-Only mode",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int("sourceSegments", len(t.sourceLOBMetadata)))

			err = t.mergeLOBMetadataOnly(res)
			if err != nil {
				log.Warn("failed to merge LOB metadata", zap.Error(err))
			} else {
				log.Info("LOB metadata merged successfully",
					zap.Duration("lobMetadataMergeElapse", time.Since(lobCompactStart)))
			}

		case CompactionLOBModeSmartRewrite:
			// SmartRewrite mode: LOB processing was done in-flight during batch loop
			// LOB readers/writers were initialized before compaction (lines 720-757)
			// and closed after compaction with metadata attached to result
			log.Info("SmartRewrite mode completed in-flight, no post-processing needed")

		default:
			// no LOB post-processing needed
			log.Debug("no LOB post-processing needed", zap.String("mode", t.getLOBModeName(t.lobMode)))
		}
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

// initLOBReaders initializes LOB readers for reading source LOB files
func (t *mixCompactionTask) initLOBReaders(ctx context.Context) error {
	t.lobReaders = make(map[int64]map[uint64]*lobReaderInfo)

	log.Info("initializing LOB readers",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int("sourceSegmentCount", len(t.sourceLOBMetadata)))

	rootPath := t.compactionParams.StorageConfig.GetRootPath()
	for _, sourceMeta := range t.sourceLOBMetadata {
		for fieldID, fieldMeta := range sourceMeta.LOBFields {
			if t.lobReaders[fieldID] == nil {
				t.lobReaders[fieldID] = make(map[uint64]*lobReaderInfo)
			}

			for _, lobFileInfo := range fieldMeta.LOBFiles {
				lobFileID := uint64(lobFileInfo.LobFileID)

				// reconstruct full path using partition-level LOB storage
				lobFilePath := metautil.BuildLOBLogPath(rootPath, sourceMeta.CollectionID, sourceMeta.PartitionID, fieldID, int64(lobFileID))

				// create LOB reader with storage config
				reader := storage.NewLOBReader(t.lobHelper.GetStorageConfig())

				t.lobReaders[fieldID][lobFileID] = &lobReaderInfo{
					reader:   reader,
					filePath: lobFilePath,
				}

				log.Debug("created LOB reader",
					zap.Int64("fieldID", fieldID),
					zap.Uint64("lobFileID", lobFileID),
					zap.String("path", lobFilePath))
			}
		}
	}

	log.Info("LOB readers initialized",
		zap.Int("fieldCount", len(t.lobReaders)))

	return nil
}

// initLOBWriters initializes LOB writers for writing target LOB files
func (t *mixCompactionTask) initLOBWriters(ctx context.Context, targetSegmentID int64, lobFileIDAlloc allocator.Interface) error {
	t.lobWriters = make(map[int64]*storage.LOBWriter)

	log.Info("initializing LOB writers",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("targetSegmentID", targetSegmentID))

	for _, field := range t.plan.GetSchema().GetFields() {
		if field.GetDataType() != schemapb.DataType_Text {
			continue
		}

		hasLOBData := false
		for _, sourceMeta := range t.sourceLOBMetadata {
			if _, exists := sourceMeta.LOBFields[field.GetFieldID()]; exists {
				hasLOBData = true
				break
			}
		}

		if !hasLOBData {
			continue
		}

		writer, err := storage.NewLOBWriter(
			targetSegmentID,
			t.partitionID,
			field.GetFieldID(),
			t.collectionID,
			lobFileIDAlloc, // use allocator for LOB file IDs
			t.compactionParams.StorageConfig,
			storage.WithMaxLOBFileSize(t.compactionParams.LOBMaxFileSize),
		)
		if err != nil {
			return errors.Wrapf(err, "failed to create LOB writer for field %d", field.GetFieldID())
		}

		t.lobWriters[field.GetFieldID()] = writer

		log.Debug("created LOB writer",
			zap.Int64("fieldID", field.GetFieldID()),
			zap.Int64("targetSegmentID", targetSegmentID))
	}

	log.Info("LOB writers initialized",
		zap.Int("fieldCount", len(t.lobWriters)))

	return nil
}

// closeLOBReaders closes all LOB readers
func (t *mixCompactionTask) closeLOBReaders(ctx context.Context) {
	t.lobReaders = nil
}

// closeLOBWriters closes all LOB writers and collects their metadata
func (t *mixCompactionTask) closeLOBWriters(ctx context.Context) (*storage.LOBSegmentMetadata, error) {
	targetMetadata := storage.NewLOBSegmentMetadata()

	for fieldID, writer := range t.lobWriters {
		if writer != nil {
			if err := writer.Close(ctx); err != nil {
				log.Error("failed to close LOB writer",
					zap.Int64("fieldID", fieldID),
					zap.Error(err))
				return nil, errors.Wrapf(err, "failed to close LOB writer for field %d", fieldID)
			}

			stats := writer.GetStatistics()
			recordCount, _ := stats["total_lob_records"].(int64)
			totalBytes, _ := stats["total_lob_bytes"].(int64)
			fieldMeta := &storage.LOBFieldMetadata{
				FieldID:       fieldID,
				LOBFiles:      writer.GetLOBFileInfos(),
				SizeThreshold: writer.GetLOBSizeThreshold(),
				RecordCount:   recordCount,
				TotalBytes:    totalBytes,
			}

			targetMetadata.LOBFields[fieldID] = fieldMeta
			targetMetadata.TotalLOBFiles += len(fieldMeta.LOBFiles)
			targetMetadata.TotalLOBRecords += fieldMeta.RecordCount
			targetMetadata.TotalLOBBytes += fieldMeta.TotalBytes

			log.Info("closed LOB writer",
				zap.Int64("fieldID", fieldID),
				zap.Int("lobFiles", len(fieldMeta.LOBFiles)),
				zap.Int64("recordCount", fieldMeta.RecordCount),
				zap.Int64("totalBytes", fieldMeta.TotalBytes))
		}
	}

	t.lobWriters = nil
	return targetMetadata, nil
}

// mergeLOBMetadataOnly merges LOB metadata from source segments (Reference-Only mode)
// LOB files stay in their original locations, but metadata is filtered to only include
// files that are actually referenced, with accurate valid_record_count.
func (t *mixCompactionTask) mergeLOBMetadataOnly(
	resultSegments []*datapb.CompactionSegment,
) error {
	if len(resultSegments) == 0 {
		return errors.New("no result segments to merge LOB metadata")
	}

	log.Info("merging LOB metadata only",
		zap.Int64("planID", t.GetPlanID()),
		zap.Int("sourceSegmentCount", len(t.sourceLOBMetadata)))

	mergedMetadata := MergeLOBMetadata(t.sourceLOBMetadata)

	if mergedMetadata != nil && mergedMetadata.HasLOBFields() {
		var finalMetadata *storage.LOBSegmentMetadata

		// filter LOB metadata using tracked reference counts
		if len(t.usedLOBFileIDs) > 0 {
			finalMetadata = FilterLOBMetadataByUsedFiles(mergedMetadata, t.usedLOBFileIDs)
			log.Info("filtered LOB metadata by used files",
				zap.Int("originalLOBFiles", mergedMetadata.TotalLOBFiles),
				zap.Int("filteredLOBFiles", finalMetadata.TotalLOBFiles),
				zap.Int64("originalRecords", mergedMetadata.TotalLOBRecords),
				zap.Int64("filteredRecords", finalMetadata.TotalLOBRecords))
		} else {
			// no tracking available, use merged metadata as-is
			finalMetadata = mergedMetadata
		}

		if finalMetadata != nil && finalMetadata.HasLOBFields() {
			protoLOBMeta := t.lobHelper.ConvertToProtoLOBMetadata(finalMetadata)
			resultSegments[0].LobMetadata = protoLOBMeta

			log.Info("LOB metadata merged",
				zap.Int("lobFiles", finalMetadata.TotalLOBFiles),
				zap.Int64("lobRecords", finalMetadata.TotalLOBRecords),
				zap.Int64("lobBytes", finalMetadata.TotalLOBBytes))
		}
	}

	return nil
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
