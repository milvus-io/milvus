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
	"encoding/json"
	"fmt"
	sio "io"
	"path"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type bumpSchemaVersionCompactionTask struct {
	ctx              context.Context
	cancel           context.CancelFunc
	plan             *datapb.CompactionPlan
	compactionParams compaction.Params
	done             chan struct{}
	logIDAlloc       allocator.Interface
	chunkManager     storage.ChunkManager
	currentTime      time.Time
}

func (t *bumpSchemaVersionCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	if !funcutil.CheckCtxValid(t.ctx) {
		return nil, t.ctx.Err()
	}
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("BumpSchemaVersionCompact-%d", t.GetPlanID()))
	defer span.End()

	if err := t.preCompact(); err != nil {
		log.Ctx(ctx).Warn("failed to preCompact", zap.Error(err))
		return nil, err
	}
	compactStart := time.Now()
	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.GetCollection()),
	)

	missingFunctions, droppedFieldIDs, existingFields, err := t.schemaBumpDecision()
	if err != nil {
		log.Warn("failed to decide schema bump action", zap.Error(err))
		return nil, err
	}

	log.Info("schema bump compact start",
		zap.Int64("segmentID", t.plan.GetSegmentBinlogs()[0].GetSegmentID()),
		zap.Int32("collectionSchemaVersion", t.plan.GetSchema().GetVersion()),
		zap.Int("missingFunctionCount", len(missingFunctions)),
		zap.Int64s("droppedFieldIDs", droppedFieldIDs),
	)

	var result *datapb.CompactionPlanResult
	if len(missingFunctions) == 0 && len(droppedFieldIDs) == 0 {
		result = t.runSchemaVersionBumpOnly()
	} else if len(droppedFieldIDs) > 0 {
		result, err = t.runFullSchemaRewrite(existingFields)
	} else {
		result, err = t.runMissingFunctionMaterialization(ctx, missingFunctions, existingFields)
	}
	if err != nil {
		log.Warn("schema bump compact failed", zap.Error(err), zap.Duration("compact cost", time.Since(compactStart)))
		return nil, err
	}

	log.Info("schema bump compact done", zap.Duration("compact cost", time.Since(compactStart)))
	return result, nil
}

// preCompact validates the compaction plan and checks context.
func (t *bumpSchemaVersionCompactionTask) preCompact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		return t.ctx.Err()
	}

	// Check segment binlogs: must have exactly one segment.
	// The plan is produced by datacoord, so a malformed plan is an internal
	// protocol violation, not user input.
	if len(t.plan.GetSegmentBinlogs()) != 1 {
		return merr.WrapErrServiceInternalMsg("schema bump compaction plan is illegal, must have exactly one segment, but got %d segments, planID = %d", len(t.plan.GetSegmentBinlogs()), t.GetPlanID())
	}

	segment := t.plan.GetSegmentBinlogs()[0]
	if segment.GetStorageVersion() < storage.StorageV3 || segment.GetManifest() == "" {
		return merr.WrapErrServiceInternalMsg("schema bump compaction requires a StorageV3 segment with manifest, planID = %d, segmentID = %d, storageVersion = %d", t.GetPlanID(), segment.GetSegmentID(), segment.GetStorageVersion())
	}

	return nil
}

func (t *bumpSchemaVersionCompactionTask) missingFunctionInputSchema(missingFunctions []*schemapb.FunctionSchema) (*schemapb.CollectionSchema, []int64, error) {
	schema := t.plan.GetSchema()
	seen := make(map[int64]struct{})
	var fields []*schemapb.FieldSchema
	var fieldIDs []int64
	addInputField := func(field *schemapb.FieldSchema) {
		fieldID := field.GetFieldID()
		if _, ok := seen[fieldID]; ok {
			return
		}
		seen[fieldID] = struct{}{}
		fields = append(fields, field)
		fieldIDs = append(fieldIDs, fieldID)
	}
	for _, functionSchema := range missingFunctions {
		if err := validateSupportedMissingFunctionMaterialization(functionSchema); err != nil {
			return nil, nil, err
		}
		for _, inputFieldID := range functionSchema.GetInputFieldIds() {
			inputField := typeutil.GetField(schema, inputFieldID)
			if inputField == nil {
				return nil, nil, merr.WrapErrParameterInvalidMsg("input field not found in schema")
			}
			if err := validateMaterializationInputField(functionSchema, inputField); err != nil {
				return nil, nil, err
			}
			addInputField(inputField)

			additionalFields, err := additionalFunctionInputFields(schema, functionSchema, inputField)
			if err != nil {
				return nil, nil, err
			}
			for _, additionalField := range additionalFields {
				if err := validateMaterializationInputField(functionSchema, additionalField); err != nil {
					return nil, nil, err
				}
				addInputField(additionalField)
			}
		}
	}
	return &schemapb.CollectionSchema{
		Name:               schema.GetName(),
		Description:        schema.GetDescription(),
		Fields:             fields,
		EnableDynamicField: schema.GetEnableDynamicField(),
		Properties:         schema.GetProperties(),
	}, fieldIDs, nil
}

func additionalFunctionInputFields(schema *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema, inputField *schemapb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		return bm25AdditionalInputFields(schema, inputField)
	default:
		return nil, nil
	}
}

func bm25AdditionalInputFields(schema *schemapb.CollectionSchema, inputField *schemapb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	params, ok := typeutil.CreateFieldSchemaHelper(inputField).GetMultiAnalyzerParams()
	if !ok {
		return nil, nil
	}
	var multiAnalyzerParams struct {
		ByField string `json:"by_field"`
	}
	if err := json.Unmarshal([]byte(params), &multiAnalyzerParams); err != nil {
		return nil, err
	}
	if multiAnalyzerParams.ByField == "" {
		return nil, merr.WrapErrParameterInvalidMsg("multi_analyzer_params missing required 'by_field' key")
	}
	byField := typeutil.GetFieldByName(schema, multiAnalyzerParams.ByField)
	if byField == nil {
		return nil, merr.WrapErrParameterInvalidMsg("input field not found in schema")
	}
	return []*schemapb.FieldSchema{byField}, nil
}

func (t *bumpSchemaVersionCompactionTask) missingFunctionOutputFields(missingFunctions []*schemapb.FunctionSchema, existingFields map[int64]struct{}) ([]*schemapb.FieldSchema, []int64, error) {
	schema := t.plan.GetSchema()
	var fields []*schemapb.FieldSchema
	var fieldIDs []int64
	for _, functionSchema := range missingFunctions {
		if err := validateSupportedMissingFunctionMaterialization(functionSchema); err != nil {
			return nil, nil, err
		}
		for _, outputIndex := range functionOutputIndexesToMaterialize(functionSchema, existingFields) {
			outputFieldID := functionSchema.GetOutputFieldIds()[outputIndex]
			outputField := typeutil.GetField(schema, outputFieldID)
			if outputField == nil {
				return nil, nil, merr.WrapErrParameterInvalidMsg("output field not found in schema")
			}
			if err := validateMaterializationOutputField(functionSchema, outputField); err != nil {
				return nil, nil, err
			}
			fields = append(fields, outputField)
			fieldIDs = append(fieldIDs, outputFieldID)
		}
	}
	if len(fields) == 0 {
		return nil, nil, merr.WrapErrParameterInvalidMsg("no missing function output fields")
	}
	return fields, fieldIDs, nil
}

func validateSupportedMissingFunctionMaterialization(functionSchema *schemapb.FunctionSchema) error {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if len(functionSchema.GetInputFieldIds()) == 0 {
			return merr.WrapErrParameterInvalidMsg("bm25 function should have input fields")
		}
		if len(functionSchema.GetOutputFieldIds()) == 0 {
			return merr.WrapErrParameterInvalidMsg("bm25 function should have output fields")
		}
		return nil
	case schemapb.FunctionType_MinHash:
		if len(functionSchema.GetInputFieldIds()) == 0 {
			return merr.WrapErrParameterInvalidMsg("minhash function should have input fields")
		}
		if len(functionSchema.GetOutputFieldIds()) == 0 {
			return merr.WrapErrParameterInvalidMsg("minhash function should have output fields")
		}
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported function type")
	}
}

func validateMaterializationInputField(functionSchema *schemapb.FunctionSchema, field *schemapb.FieldSchema) error {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_Text {
			return merr.WrapErrParameterInvalidMsg("input field data type must be varchar or text for bm25 materialization")
		}
	case schemapb.FunctionType_MinHash:
		if field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_Text {
			return merr.WrapErrParameterInvalidMsg("input field data type must be varchar or text for minhash materialization")
		}
	}
	return nil
}

func validateMaterializationOutputField(functionSchema *schemapb.FunctionSchema, field *schemapb.FieldSchema) error {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if field.GetDataType() != schemapb.DataType_SparseFloatVector {
			return merr.WrapErrParameterInvalidMsg("output field data type must be sparse float vector for bm25 materialization")
		}
	case schemapb.FunctionType_MinHash:
		if field.GetDataType() != schemapb.DataType_BinaryVector {
			return merr.WrapErrParameterInvalidMsg("output field data type must be binary vector for minhash materialization")
		}
	}
	return nil
}

func partialMaterializerExistingFields(schema *schemapb.CollectionSchema, missingFunctions []*schemapb.FunctionSchema, existingFields map[int64]struct{}) map[int64]struct{} {
	fields := collectionSchemaFields(schema)
	for _, functionSchema := range missingFunctions {
		for _, outputIndex := range functionOutputIndexesToMaterialize(functionSchema, existingFields) {
			delete(fields, functionSchema.GetOutputFieldIds()[outputIndex])
		}
	}
	return fields
}

func (t *bumpSchemaVersionCompactionTask) openRecordReader(segment *datapb.CompactionSegmentBinlogs, schema *schemapb.CollectionSchema) (storage.RecordReader, map[int64]struct{}, error) {
	reader, existingFields, err := newCompactionSegmentRecordReader(t.ctx, segment, schema, t.compactionParams.StorageConfig,
		storage.WithCollectionID(segment.GetCollectionID()),
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithDownloader(t.chunkManager.MultiRead),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		log.Ctx(t.ctx).Warn("failed to open compaction segment reader", zap.Error(err))
	}
	return reader, existingFields, err
}

func (t *bumpSchemaVersionCompactionTask) schemaBumpDecision() ([]*schemapb.FunctionSchema, []int64, map[int64]struct{}, error) {
	segment := t.plan.GetSegmentBinlogs()[0]
	existingFields, err := compactionSegmentStorageFields(segment, t.compactionParams.StorageConfig)
	if err != nil {
		return nil, nil, nil, err
	}
	return missingSchemaFunctions(t.plan.GetSchema(), existingFields), droppedSchemaFieldIDs(t.plan.GetSchema(), existingFields), existingFields, nil
}

func (t *bumpSchemaVersionCompactionTask) fullRewriteSegmentID() (int64, error) {
	idRange := t.plan.GetPreAllocatedSegmentIDs()
	if idRange == nil || idRange.GetBegin() >= idRange.GetEnd() {
		return 0, merr.WrapErrServiceInternal("schema bump full rewrite requires a pre-allocated segment ID")
	}
	return idRange.GetBegin(), nil
}

func selectFullRewriteRecord(record storage.Record, pkField *schemapb.FieldSchema, entityFilter compaction.EntityFilter, ttlFieldID int64, useTTLField bool, ttlValues []int64) (*recordSelection, []int64, error) {
	pkArray := record.Column(pkField.GetFieldID())
	var pkAt func(int) any
	switch pkField.GetDataType() {
	case schemapb.DataType_Int64:
		int64Array, ok := pkArray.(*array.Int64)
		if !ok {
			return nil, nil, merr.WrapErrServiceInternal("int64 primary key field not found in full schema rewrite record")
		}
		pkAt = func(i int) any { return int64Array.Value(i) }
	case schemapb.DataType_VarChar:
		stringArray, ok := pkArray.(*array.String)
		if !ok {
			return nil, nil, merr.WrapErrServiceInternal("varchar primary key field not found in full schema rewrite record")
		}
		pkAt = func(i int) any { return stringArray.Value(i) }
	default:
		return nil, nil, merr.WrapErrServiceInternal("invalid primary key data type for full schema rewrite")
	}

	timestampArray, ok := record.Column(common.TimeStampField).(*array.Int64)
	if !ok {
		return nil, nil, merr.WrapErrServiceInternal("timestamp field not found in full schema rewrite record")
	}
	var ttlArray *array.Int64
	if useTTLField {
		ttlArray, ok = record.Column(ttlFieldID).(*array.Int64)
		if !ok {
			return nil, nil, merr.WrapErrServiceInternal("TTL field not found in full schema rewrite record")
		}
	}

	selection := &recordSelection{}
	sliceStart := -1
	filteredRows := 0
	for i := range record.Len() {
		expireTs := int64(-1)
		if useTTLField && ttlArray.IsValid(i) {
			expireTs = ttlArray.Value(i)
		}
		if entityFilter.Filtered(pkAt(i), typeutil.Timestamp(timestampArray.Value(i)), expireTs) {
			if sliceStart != -1 {
				selection.ranges = append(selection.ranges, rowRange{start: sliceStart, end: i})
				selection.length += i - sliceStart
			}
			sliceStart = -1
			filteredRows++
			continue
		}

		if useTTLField && expireTs > 0 {
			ttlValues = append(ttlValues, expireTs)
		}
		if sliceStart == -1 {
			sliceStart = i
		}
	}
	if sliceStart != -1 {
		selection.ranges = append(selection.ranges, rowRange{start: sliceStart, end: record.Len()})
		selection.length += record.Len() - sliceStart
	}
	if filteredRows == 0 {
		return nil, ttlValues, nil
	}
	return selection, ttlValues, nil
}

func (t *bumpSchemaVersionCompactionTask) runSchemaVersionBumpOnly() *datapb.CompactionPlanResult {
	segment := t.plan.GetSegmentBinlogs()[0]
	return &datapb.CompactionPlanResult{
		PlanID: t.plan.GetPlanID(),
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           segment.GetSegmentID(),
				NumOfRows:           t.plan.GetTotalRows(),
				InsertLogs:          segment.GetFieldBinlogs(),
				Field2StatslogPaths: segment.GetField2StatslogPaths(),
				Deltalogs:           segment.GetDeltalogs(),
				Channel:             segment.GetInsertChannel(),
				StorageVersion:      segment.GetStorageVersion(),
				Manifest:            segment.GetManifest(),
				ExpirQuantiles:      segment.GetExpirQuantiles(),
				// Schema-version-bump-only path doesn't rewrite data —
				// the receiver Clones oldSegment and only updates
				// SchemaVersion/Binlogs/StorageVersion/ManifestPath, so
				// the existing oldSegment.Stats is preserved. Leaving
				// Stats nil here documents that the receiver does not
				// read it for this path.
			},
		},
		Type: t.plan.GetType(),
	}
}

func (t *bumpSchemaVersionCompactionTask) runFullSchemaRewrite(existingFields map[int64]struct{}) (*datapb.CompactionPlanResult, error) {
	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	newSegmentID, err := t.fullRewriteSegmentID()
	if err != nil {
		return nil, err
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(t.plan.GetSchema())
	if err != nil {
		return nil, err
	}
	delta, err := compaction.ComposeDeleteFromDeltalogs(t.ctx, pkField.GetDataType(), segment,
		storage.WithDownloader(t.chunkManager.MultiRead),
		storage.WithStorageConfig(t.compactionParams.StorageConfig))
	if err != nil {
		return nil, err
	}
	entityFilter := compaction.NewEntityFilter(delta, t.plan.GetCollectionTtl(), t.currentTime, segment.GetCommitTimestamp())
	ttlFieldID := getTTLFieldID(t.plan.GetSchema())
	reader, _, err := newCompactionSegmentRecordReaderWithFields(t.ctx, segment, t.plan.GetSchema(), t.compactionParams.StorageConfig, existingFields,
		storage.WithCollectionID(collectionID),
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithDownloader(t.chunkManager.MultiRead),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	materializer, err := NewRecordMaterializer(t.plan.GetSchema(), t.plan.GetSchema().GetFunctions(), existingFields)
	if err != nil {
		return nil, err
	}
	defer materializer.Close()

	alloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())
	// TODO(#50021): Support full TEXT LOB rewrite for schema-bump compaction.
	writer, err := storage.NewBinlogRecordWriter(t.ctx,
		collectionID,
		segment.GetPartitionID(),
		newSegmentID,
		t.plan.GetSchema(),
		alloc,
		t.compactionParams.BinLogMaxSize,
		t.plan.GetTotalRows(),
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return t.chunkManager.MultiWrite(ctx, kvs)
		}),
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithCollectionID(collectionID),
		storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI),
	)
	if err != nil {
		return nil, err
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writer.Close()
		}
	}()

	var totalRows int64
	for {
		record, err := reader.Next()
		if err != nil {
			if err == sio.EOF {
				break
			}
			return nil, err
		}

		sourceHasTTLField := ttlFieldID >= common.StartOfUserFieldID && record.Column(ttlFieldID) != nil
		preMaterializeFilter := len(delta) > 0 || t.plan.GetCollectionTtl() > 0 || sourceHasTTLField
		var selection *recordSelection
		if preMaterializeFilter {
			selection, _, err = selectFullRewriteRecord(record, pkField, entityFilter, ttlFieldID, sourceHasTTLField, nil)
			if err != nil {
				record.Release()
				return nil, err
			}
			if selection != nil && selection.Len() == 0 {
				record.Release()
				continue
			}
		}

		wrapped, err := materializer.WrapWithSelection(record, selection)
		if err != nil {
			if selection == nil {
				record.Release()
			}
			return nil, err
		}
		out := overwriteRecordTimestamps(wrapped, segment.GetCommitTimestamp())
		if err := writer.Write(out); err != nil {
			if out != wrapped {
				out.Release()
			}
			releaseWrappedRecord(wrapped, record)
			return nil, err
		}
		if out != wrapped {
			out.Release()
		}

		totalRows += int64(wrapped.Len())
		releaseWrappedRecord(wrapped, record)
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	writerClosed = true
	insertLogs, statsLog, bm25StatsLogs, manifestPath, expirQuantiles := writer.GetLogs()
	if totalRows > 0 && manifestPath == "" {
		return nil, merr.WrapErrServiceInternal("schema bump full rewrite produced empty manifest")
	}
	statEntries := make([]packed.StatEntry, 0, len(bm25StatsLogs)+1)
	if statsLog != nil {
		statEntries = append(statEntries, packed.FieldBinlogStatEntry("bloom_filter", statsLog.GetFieldID(), statsLog))
	}
	for fieldID, bm25StatsLog := range bm25StatsLogs {
		statEntries = append(statEntries, packed.FieldBinlogStatEntry("bm25", fieldID, bm25StatsLog))
	}
	if len(statEntries) > 0 {
		if manifestPath == "" {
			return nil, merr.WrapErrServiceInternal("schema bump full rewrite produced stats without manifest")
		}
		manifestPath, err = packed.AddStatsToManifest(manifestPath, t.compactionParams.StorageConfig, statEntries)
		if err != nil {
			return nil, merr.Wrap(err, "failed to add writer stats to schema bump full rewrite manifest")
		}
	}
	sortedInsertLogs := storage.SortFieldBinlogs(insertLogs)
	if err := binlog.CompressFieldBinlogs(sortedInsertLogs); err != nil {
		return nil, err
	}

	resultSegment := &datapb.CompactionSegment{
		SegmentID:           newSegmentID,
		NumOfRows:           totalRows,
		InsertLogs:          sortedInsertLogs,
		Channel:             segment.GetInsertChannel(),
		StorageVersion:      segment.GetStorageVersion(),
		Manifest:            manifestPath,
		ExpirQuantiles:      expirQuantiles,
		IsSorted:            segment.GetIsSorted(),
		IsSortedByNamespace: segment.GetIsSortedByNamespace(),
		// Stats: insert aggregates from the freshly emitted insert logs,
		// stats footprint from the writer's tracked counter. V3 writers
		// leave statsLog/bm25StatsLogs nil because stats live in the
		// manifest — the counter is the only correct source.
		Stats: buildCompactionOutputStats(sortedInsertLogs, nil, writer.GetStatsBlobSize()),
	}
	if totalRows > 0 {
		// Text stats are built explicitly, matching sort compaction.
		textStatsLogs, err := createTextIndex(t.ctx, t.chunkManager, t.plan, t.compactionParams, segment.GetStorageVersion(), collectionID, segment.GetPartitionID(), newSegmentID, t.plan.GetPlanID(), resultSegment)
		if err != nil {
			return nil, err
		}
		if resultSegment.GetManifest() != "" && len(textStatsLogs) > 0 {
			basePath, _, err := packed.UnmarshalManifestPath(resultSegment.GetManifest())
			if err != nil {
				return nil, err
			}
			for _, stats := range textStatsLogs {
				prefix := fmt.Sprintf("%s/_stats/text_index.%d", basePath, stats.GetFieldID())
				for i, f := range stats.GetFiles() {
					stats.Files[i] = prefix + "/" + f
				}
			}
			newManifest, err := packed.AddStatsToManifest(resultSegment.GetManifest(), t.compactionParams.StorageConfig, packed.TextIndexStatEntries(textStatsLogs, t.plan.GetCurrentScalarIndexVersion()))
			if err != nil {
				return nil, err
			}
			resultSegment.Manifest = newManifest
		}
		resultSegment.TextStatsLogs = textStatsLogs
	}

	return &datapb.CompactionPlanResult{
		PlanID:   t.plan.GetPlanID(),
		State:    datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{resultSegment},
		Type:     t.plan.GetType(),
	}, nil
}

func appendBM25StatsFromArrowArray(stats *storage.BM25Stats, arr arrow.Array) (int, error) {
	binaryArray, ok := arr.(*array.Binary)
	if !ok {
		return 0, merr.WrapErrParameterInvalidMsg("bm25 output field must be arrow binary array, got %T", arr)
	}
	memorySize := 0
	for i := 0; i < binaryArray.Len(); i++ {
		if binaryArray.IsNull(i) {
			continue
		}
		value := binaryArray.Value(i)
		stats.AppendBytes(value)
		memorySize += len(value)
	}
	return memorySize, nil
}

func isBM25MaterializedOutputField(schema *schemapb.CollectionSchema, fieldID int64) bool {
	for _, functionSchema := range schema.GetFunctions() {
		if functionSchema.GetType() != schemapb.FunctionType_BM25 {
			continue
		}
		for _, outputFieldID := range functionSchema.GetOutputFieldIds() {
			if outputFieldID == fieldID {
				return true
			}
		}
	}
	return false
}

func arrowArrayMemorySize(arr arrow.Array) int {
	if arr == nil || arr.Data() == nil {
		return 0
	}
	return int(storage.ActualSizeInBytes(arr.Data()))
}

type bumpSchemaVersionBatchWriter interface {
	Write(storage.Record) error
	GetWrittenUncompressed() uint64
	AsNewColumnGroups()
	Close() (packed.WriterOutput, error)
}

type bumpSchemaVersionWriterResult struct {
	writer         bumpSchemaVersionBatchWriter
	columnGroups   []storagecommon.ColumnGroup
	storageVersion int64
	basePath       string
	baseVersion    int64
	v3Stats        []packed.StatEntry
	// statsBlobSize tracks the cumulative bloom-filter + BM25 blob memory
	// committed via addV3Stats. Reported to DataCoord on
	// CompactionSegment.Stats.StatsBinlogSize.
	statsBlobSize int64
}

func (t *bumpSchemaVersionCompactionTask) setupWriter(outputFields []*schemapb.FieldSchema, segment *datapb.CompactionSegmentBinlogs, collectionID int64) (*bumpSchemaVersionWriterResult, error) {
	outputSchema := &schemapb.CollectionSchema{Fields: outputFields}
	newColumnGroups := make([]storagecommon.ColumnGroup, 0, len(outputFields))
	for i, outputField := range outputFields {
		outputFieldID := outputField.GetFieldID()
		newColumnGroups = append(newColumnGroups, storagecommon.ColumnGroup{
			GroupID: outputFieldID,
			Columns: []int{i},
			Fields:  []int64{outputFieldID},
		})
	}

	basePath, existingVersion, err := packed.UnmarshalManifestPath(segment.GetManifest())
	if err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("failed to parse existing manifest for schema bump: %s", err.Error())
	}
	writerResult, err := t.newV3WriterResult(outputSchema, newColumnGroups, segment, collectionID, basePath, existingVersion)
	if err != nil {
		return nil, err
	}
	writerResult.writer.AsNewColumnGroups()
	log.Ctx(t.ctx).Info("[schema-bump-partial-writer] writer setup",
		zap.Int64("baseVersion", existingVersion),
		zap.String("basePath", basePath),
		zap.Int("outputFieldCount", len(outputFields)),
		zap.Int("columnGroupCount", len(newColumnGroups)),
	)
	return writerResult, nil
}

func (t *bumpSchemaVersionCompactionTask) newV3WriterResult(schema *schemapb.CollectionSchema, columnGroups []storagecommon.ColumnGroup, segment *datapb.CompactionSegmentBinlogs, collectionID int64, basePath string, baseVersion int64) (*bumpSchemaVersionWriterResult, error) {
	if segment.GetStorageVersion() < storage.StorageV3 || segment.GetManifest() == "" {
		return nil, merr.WrapErrParameterInvalidMsg("schema bump compaction requires a StorageV3 segment with manifest")
	}

	pluginContext, err := hookutil.GetCPluginContext(t.plan.GetPluginContext(), collectionID)
	if err != nil {
		return nil, err
	}

	writerFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	columnGroups = storagecommon.FillColumnGroupFormats(columnGroups, writerFormat)
	schemaBasedFormats := storagecommon.ColumnGroupFormats(columnGroups, writerFormat)
	writer, err := storage.NewPartialPackedRecordBatchWriter(basePath, schema, int64(t.compactionParams.BinLogMaxSize), packed.DefaultMultiPartUploadSize, columnGroups, t.compactionParams.StorageConfig, pluginContext, writerFormat, schemaBasedFormats)
	if err != nil {
		return nil, err
	}
	log.Ctx(t.ctx).Info("[schema-bump-partial-writer] partial manifest writer created",
		zap.Int64("baseVersion", baseVersion),
		zap.Int("schemaFieldCount", len(schema.GetFields())),
		zap.Int("columnGroupCount", len(columnGroups)),
	)

	return &bumpSchemaVersionWriterResult{
		writer:         writer,
		columnGroups:   columnGroups,
		storageVersion: segment.GetStorageVersion(),
		basePath:       basePath,
		baseVersion:    baseVersion,
	}, nil
}

func (t *bumpSchemaVersionCompactionTask) updateStats(stats *storage.BM25Stats, outputFieldID int64, writerResult *bumpSchemaVersionWriterResult) error {
	bytes, err := stats.Serialize()
	if err != nil {
		return err
	}
	return t.addV3Stats("bm25", outputFieldID, bytes, int64(len(bytes)), writerResult)
}

func (t *bumpSchemaVersionCompactionTask) addV3Stats(prefix string, fieldID int64, bytes []byte, memorySize int64, writerResult *bumpSchemaVersionWriterResult) error {
	statsID, _, err := t.logIDAlloc.Alloc(uint32(1))
	if err != nil {
		return err
	}

	statsRelPath := fmt.Sprintf("_stats/%s.%d/%d", prefix, fieldID, statsID)
	absStatsPath := path.Join(writerResult.basePath, statsRelPath)
	if err := packed.WriteFile(t.compactionParams.StorageConfig, absStatsPath, bytes); err != nil {
		return merr.Wrap(err, "failed to write V3 stats")
	}
	writerResult.v3Stats = append(writerResult.v3Stats, packed.FieldBinlogStatEntry(prefix, fieldID, &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: []*datapb.Binlog{{
			LogPath:    absStatsPath,
			MemorySize: memorySize,
		}},
	}))
	writerResult.statsBlobSize += memorySize
	return nil
}

func (t *bumpSchemaVersionCompactionTask) buildMergedLogsV3(segment *datapb.CompactionSegmentBinlogs, writerResult *bumpSchemaVersionWriterResult, sparseFieldMemorySizes map[int64]int, totalRows int64) ([]*datapb.FieldBinlog, error) {
	logIDStart, _, err := t.logIDAlloc.Alloc(uint32(len(writerResult.columnGroups)))
	if err != nil {
		return nil, err
	}
	newInsertLogs := make(map[int64]*datapb.FieldBinlog)
	for i, columnGroup := range writerResult.columnGroups {
		fieldID := columnGroup.GroupID
		fieldBinlog := &datapb.FieldBinlog{
			FieldID:     fieldID,
			ChildFields: columnGroup.Fields,
			Format:      columnGroup.Format,
			Binlogs: []*datapb.Binlog{
				{
					MemorySize: int64(sparseFieldMemorySizes[fieldID]),
					EntriesNum: totalRows,
					LogID:      logIDStart + int64(i),
				},
			},
		}
		newInsertLogs[fieldID] = fieldBinlog
	}

	return t.finalizeMergedLogs(segment, newInsertLogs)
}

func (t *bumpSchemaVersionCompactionTask) preserveDeltaLogsV3(segment *datapb.CompactionSegmentBinlogs, manifestPath string) (string, error) {
	deltaPaths, err := packed.GetDeltaLogPathsFromManifest(segment.GetManifest(), t.compactionParams.StorageConfig)
	if err != nil {
		return "", merr.Wrap(err, "failed to read V3 delta logs from existing manifest")
	}
	if len(deltaPaths) == 0 {
		return manifestPath, nil
	}

	deltaSummaries := make([]*datapb.Binlog, 0, len(deltaPaths))
	for _, fieldBinlog := range segment.GetDeltalogs() {
		deltaSummaries = append(deltaSummaries, fieldBinlog.GetBinlogs()...)
	}
	if len(deltaSummaries) != len(deltaPaths) {
		return "", merr.WrapErrServiceInternalMsg("V3 delta manifest path count %d does not match segment delta summary count %d", len(deltaPaths), len(deltaSummaries))
	}

	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", merr.WrapErrDataIntegrityMsg("failed to parse new V3 manifest for delta preservation: %s", err.Error())
	}

	deltaEntries := make([]packed.DeltaLogEntry, 0, len(deltaPaths))
	for i, deltaPath := range deltaPaths {
		newDeltaPath := metautil.BuildDeltaLogPathV3(basePath, deltaSummaries[i].GetLogID())
		if newDeltaPath != deltaPath {
			if err := t.chunkManager.Copy(t.ctx, deltaPath, newDeltaPath); err != nil {
				return "", merr.Wrap(err, "failed to copy V3 delta log for schema bump full rewrite")
			}
		}
		deltaEntries = append(deltaEntries, packed.DeltaLogEntry{
			Path:       newDeltaPath,
			NumEntries: deltaSummaries[i].GetEntriesNum(),
		})
	}
	newManifest, err := packed.AddDeltaLogsToManifest(manifestPath, t.compactionParams.StorageConfig, deltaEntries)
	if err != nil {
		return "", merr.Wrap(err, "failed to preserve V3 delta logs in full rewrite manifest")
	}
	return newManifest, nil
}

func (t *bumpSchemaVersionCompactionTask) finalizeMergedLogs(segment *datapb.CompactionSegmentBinlogs, newInsertLogs map[int64]*datapb.FieldBinlog) ([]*datapb.FieldBinlog, error) {
	newFieldIDs := make(map[int64]struct{}, len(newInsertLogs)*2)
	for _, fb := range newInsertLogs {
		newFieldIDs[fb.GetFieldID()] = struct{}{}
		for _, childID := range fb.GetChildFields() {
			newFieldIDs[childID] = struct{}{}
		}
	}
	mergedInsertLogs := make(map[int64]*datapb.FieldBinlog, len(segment.GetFieldBinlogs())+len(newInsertLogs))
	for _, fb := range segment.GetFieldBinlogs() {
		if compactionFieldBinlogReadable(fb, newFieldIDs) {
			continue
		}
		mergedInsertLogs[fb.GetFieldID()] = fb
	}
	for fieldID, fb := range newInsertLogs {
		mergedInsertLogs[fieldID] = fb
	}

	return storage.SortFieldBinlogs(mergedInsertLogs), nil
}

func (t *bumpSchemaVersionCompactionTask) runMissingFunctionMaterialization(ctx context.Context, missingFunctions []*schemapb.FunctionSchema, existingFields map[int64]struct{}) (*datapb.CompactionPlanResult, error) {
	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	partitionID := segment.GetPartitionID()
	segmentID := segment.GetSegmentID()

	inputSchema, inputFieldIDs, err := t.missingFunctionInputSchema(missingFunctions)
	if err != nil {
		return nil, err
	}
	outputFields, outputFieldIDs, err := t.missingFunctionOutputFields(missingFunctions, existingFields)
	if err != nil {
		return nil, err
	}

	log := log.Ctx(ctx).With(
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
		zap.Int64s("inputFieldIDs", inputFieldIDs),
		zap.Int64s("outputFieldIDs", outputFieldIDs),
	)

	var readDuration, computeDuration, writeDuration, updateStatsDuration time.Duration

	reader, _, err := t.openRecordReader(segment, inputSchema)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	writerResult, err := t.setupWriter(outputFields, segment, collectionID)
	if err != nil {
		return nil, err
	}

	log.Info("schema bump writer setup",
		zap.Int64("effectiveStorageVersion", writerResult.storageVersion),
		zap.Int64("clusterStorageVersion", t.compactionParams.StorageVersion),
		zap.Bool("segmentHasManifest", segment.GetManifest() != ""),
		zap.Int64s("inputFieldIDs", inputFieldIDs),
		zap.Int64s("outputFieldIDs", outputFieldIDs),
	)
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BumpSchemaVersionCompact.batchProcess")
	statsByField := make(map[int64]*storage.BM25Stats)
	fieldMemorySizeByField := make(map[int64]int, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		if isBM25MaterializedOutputField(t.plan.GetSchema(), outputFieldID) {
			statsByField[outputFieldID] = storage.NewBM25Stats()
		}
	}
	existingFields = partialMaterializerExistingFields(t.plan.GetSchema(), missingFunctions, existingFields)
	materializer, err := NewRecordMaterializer(t.plan.GetSchema(), missingFunctions, existingFields)
	if err != nil {
		span.End()
		return nil, err
	}
	defer materializer.Close()

	var totalRows int64
	for {
		readStart := time.Now()
		record, err := reader.Next()
		if err != nil {
			if err == sio.EOF {
				readDuration += time.Since(readStart)
				break
			}
			span.End()
			return nil, err
		}
		readDuration += time.Since(readStart)

		computeStart := time.Now()
		wrapped, err := materializer.Wrap(record)
		computeDuration += time.Since(computeStart)
		if err != nil {
			record.Release()
			span.End()
			return nil, err
		}

		for _, outputField := range outputFields {
			outputFieldID := outputField.GetFieldID()
			outputCol := wrapped.Column(outputFieldID)
			if outputCol == nil {
				releaseWrappedRecord(wrapped, record)
				span.End()
				return nil, merr.WrapErrServiceInternalMsg("output field %d not found in materialized record", outputFieldID)
			}
			batchMemSize := arrowArrayMemorySize(outputCol)
			if stats, ok := statsByField[outputFieldID]; ok {
				bm25MemSize, err := appendBM25StatsFromArrowArray(stats, outputCol)
				if err != nil {
					releaseWrappedRecord(wrapped, record)
					span.End()
					return nil, err
				}
				batchMemSize = bm25MemSize
			}
			fieldMemorySizeByField[outputFieldID] += batchMemSize
		}

		writeStart := time.Now()
		if err := writerResult.writer.Write(wrapped); err != nil {
			releaseWrappedRecord(wrapped, record)
			span.End()
			return nil, err
		}
		writeDuration += time.Since(writeStart)
		log.Info("[schema-bump-partial-writer] record batch written",
			zap.Int("rows", wrapped.Len()),
			zap.Int64("totalRowsBeforeBatch", totalRows),
			zap.Int("outputFieldCount", len(outputFields)),
		)

		totalRows += int64(wrapped.Len())
		releaseWrappedRecord(wrapped, record)
	}
	span.End()

	_, span2 := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BumpSchemaVersionCompact.updateStats")
	for outputFieldID, stats := range statsByField {
		startTime := time.Now()
		err := t.updateStats(stats, outputFieldID, writerResult)
		updateStatsDuration += time.Since(startTime)
		if err != nil {
			span2.End()
			return nil, err
		}
	}
	span2.End()

	log.Info("schema bump bm25 stats written",
		zap.Int("bm25StatsFieldCount", len(outputFieldIDs)),
		zap.Int64("storageVersion", writerResult.storageVersion),
		zap.Int("v3StatsCount", len(writerResult.v3Stats)),
	)

	mergedInsertLogs, err := t.buildMergedLogsV3(segment, writerResult, fieldMemorySizeByField, totalRows)
	if err != nil {
		return nil, err
	}

	writerOutput, err := writerResult.writer.Close()
	if err != nil {
		return nil, err
	}
	if writerOutput != nil {
		defer writerOutput.Destroy()
	}

	manifestPath, err := packed.CommitManifestUpdates(
		writerResult.basePath,
		writerResult.baseVersion,
		t.compactionParams.StorageConfig,
		&packed.ManifestUpdates{
			NewFiles: writerOutput,
			Stats:    writerResult.v3Stats,
		},
	)
	if err != nil {
		return nil, merr.Wrap(err, "failed to commit schema bump V3 manifest")
	}
	log.Info("[schema-bump-partial-writer] writer output and bm25 stats committed",
		zap.String("manifestPath", manifestPath),
		zap.Int64("totalRows", totalRows),
		zap.Int("mergedInsertLogsCount", len(mergedInsertLogs)),
		zap.Int("v3StatsCount", len(writerResult.v3Stats)),
	)

	ret := &datapb.CompactionPlanResult{
		PlanID: t.plan.GetPlanID(),
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           segmentID,
				NumOfRows:           totalRows,
				InsertLogs:          mergedInsertLogs,
				Field2StatslogPaths: segment.GetField2StatslogPaths(),
				Deltalogs:           segment.GetDeltalogs(),
				Channel:             segment.GetInsertChannel(),
				StorageVersion:      writerResult.storageVersion,
				Manifest:            manifestPath,
				// Partial-writer path merges new inserts with the
				// input's existing statslogs and deltalogs. statsBlobSize
				// comes from the freshly committed V3 stats (tracked on
				// writerResult); insert/delta aggregates come from the
				// merged arrays.
				Stats: buildCompactionOutputStats(
					mergedInsertLogs,
					segment.GetDeltalogs(),
					writerResult.statsBlobSize,
				),
			},
		},
		Type: t.plan.GetType(),
	}
	log.Info("[schema-bump-partial-writer] compaction completed",
		zap.Int64("numOfRows", totalRows),
		zap.Int("mergedInsertLogsCount", len(mergedInsertLogs)),
		zap.String("manifestPath", manifestPath),
		zap.Int64("effectiveStorageVersion", writerResult.storageVersion),
		zap.Int32("collectionSchemaVersion", t.plan.GetSchema().GetVersion()),
		zap.Duration("readDuration", readDuration),
		zap.Duration("computeDuration", computeDuration),
		zap.Duration("writeDuration", writeDuration),
		zap.Duration("updateStatsDuration", updateStatsDuration),
	)
	return ret, nil
}

func (t *bumpSchemaVersionCompactionTask) Complete() {
	if t.done != nil {
		select {
		case t.done <- struct{}{}:
		default:
		}
	}
}

func (t *bumpSchemaVersionCompactionTask) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
	if t.done != nil {
		<-t.done
	}
}

func (t *bumpSchemaVersionCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *bumpSchemaVersionCompactionTask) GetCollection() typeutil.UniqueID {
	// Get collection ID from the first segment binlog
	if len(t.plan.GetSegmentBinlogs()) > 0 {
		return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
	}
	return 0
}

func (t *bumpSchemaVersionCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *bumpSchemaVersionCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}

func (t *bumpSchemaVersionCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}

func (t *bumpSchemaVersionCompactionTask) GetStorageConfig() *indexpb.StorageConfig {
	return t.compactionParams.StorageConfig
}

var _ Compactor = (*bumpSchemaVersionCompactionTask)(nil)

func NewBumpSchemaVersionCompactionTask(ctx context.Context, cm storage.ChunkManager, plan *datapb.CompactionPlan, compactionParams compaction.Params) *bumpSchemaVersionCompactionTask {
	ctx, cancel := context.WithCancel(ctx)
	return &bumpSchemaVersionCompactionTask{
		ctx:              ctx,
		cancel:           cancel,
		plan:             plan,
		compactionParams: compactionParams,
		done:             make(chan struct{}, 1),
		logIDAlloc:       allocator.NewLocalAllocator(plan.GetPreAllocatedLogIDs().GetBegin(), plan.GetPreAllocatedLogIDs().GetEnd()),
		chunkManager:     cm,
		currentTime:      time.Now(),
	}
}
