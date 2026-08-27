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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// lobContext holds LOB (TEXT) compaction strategy decisions for the full-rewrite
	// path. A schema bump is 1->1 and never changes existing TEXT LOB data, so it
	// always uses REUSE_ALL: the existing LOB files are unchanged and only their
	// references are merged into the output manifest (mirrors sort compaction).
	lobContext *compaction.LOBCompactionContext
}

// schemaBumpPhysicalDiff describes the difference between the target schema and
// the columns physically present in the source manifest. existingFields always
// comes from the manifest; target-schema membership must never be used as a
// substitute for physical presence.
type schemaBumpPhysicalDiff struct {
	existingFields       map[int64]struct{}
	droppedFieldIDs      []int64
	absentOrdinaryFields []*schemapb.FieldSchema
	missingOutputFields  []*schemapb.FieldSchema
	missingFunctions     []*schemapb.FunctionSchema
}

func (d *schemaBumpPhysicalDiff) appendedFields() []*schemapb.FieldSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(d.absentOrdinaryFields)+len(d.missingOutputFields))
	fields = append(fields, d.absentOrdinaryFields...)
	fields = append(fields, d.missingOutputFields...)
	return fields
}

func (t *bumpSchemaVersionCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	if !funcutil.CheckCtxValid(t.ctx) {
		return nil, t.ctx.Err()
	}
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("BumpSchemaVersionCompact-%d", t.GetPlanID()))
	defer span.End()

	if err := t.preCompact(); err != nil {
		mlog.Warn(ctx, "failed to preCompact", mlog.Err(err))
		return nil, err
	}
	compactStart := time.Now()
	log := mlog.With(
		mlog.Int64("planID", t.GetPlanID()),
		mlog.FieldCollectionID(t.GetCollection()),
	)

	diff, err := t.schemaBumpDecision()
	if err != nil {
		log.Warn(ctx, "failed to decide schema bump action", mlog.Err(err))
		return nil, err
	}

	log.Info(ctx, "schema bump compact start",
		mlog.FieldSegmentID(t.plan.GetSegmentBinlogs()[0].GetSegmentID()),
		mlog.Int32("collectionSchemaVersion", t.plan.GetSchema().GetVersion()),
		mlog.Int("missingFunctionCount", len(diff.missingFunctions)),
		mlog.Int("absentOrdinaryFieldCount", len(diff.absentOrdinaryFields)),
		mlog.Int("missingOutputFieldCount", len(diff.missingOutputFields)),
		mlog.Int64s("droppedFieldIDs", diff.droppedFieldIDs),
	)

	var result *datapb.CompactionPlanResult
	// Dropped physical fields always require a replacement rewrite. Zero-row
	// segments still route to additive reconciliation, which rejects them as a
	// data-integrity error rather than writing an empty materialized record.
	if len(diff.droppedFieldIDs) > 0 {
		result, err = t.runFullSchemaRewrite(diff.existingFields)
	} else if len(diff.absentOrdinaryFields) == 0 && len(diff.missingOutputFields) == 0 {
		result = t.runSchemaVersionBumpOnly()
	} else {
		result, err = t.runAdditivePhysicalReconciliation(ctx, diff)
	}
	if err != nil {
		log.Warn(ctx, "schema bump compact failed", mlog.Err(err), mlog.Duration("compact cost", time.Since(compactStart)))
		return nil, err
	}

	log.Info(ctx, "schema bump compact done", mlog.Duration("compact cost", time.Since(compactStart)))
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
	// Inputs were validated by schemaBumpDecision (missingFunctionMaterializations);
	// this only assembles the read schema.
	for _, functionSchema := range missingFunctions {
		for _, inputFieldID := range functionSchema.GetInputFieldIds() {
			inputField := typeutil.GetField(schema, inputFieldID)
			if inputField == nil {
				return nil, nil, merr.WrapErrDataIntegrityMsg(
					"function %s input field %d not found in persisted schema",
					functionSchema.GetName(), inputFieldID)
			}
			addInputField(inputField)

			if err := addBM25MultiAnalyzerInputFields(schema, functionSchema, inputField, addInputField); err != nil {
				return nil, nil, err
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

// addBM25MultiAnalyzerInputFields adds the by_field analyzer-selector column of
// a BM25 multi-analyzer input to the physical read schema; the selector is the
// runner's per-row analyzer name, not a function text input, so it is resolved
// and VarChar-validated in bm25AdditionalInputFields instead of the generic
// input validator. No-op for other function types.
func addBM25MultiAnalyzerInputFields(schema *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema, inputField *schemapb.FieldSchema, addInputField func(*schemapb.FieldSchema)) error {
	if functionSchema.GetType() != schemapb.FunctionType_BM25 {
		return nil
	}
	byFields, err := bm25AdditionalInputFields(schema, functionSchema, inputField)
	if err != nil {
		return err
	}
	for _, byField := range byFields {
		addInputField(byField)
	}
	return nil
}

func bm25AdditionalInputFields(schema *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema, inputField *schemapb.FieldSchema) ([]*schemapb.FieldSchema, error) {
	params, ok := typeutil.CreateFieldSchemaHelper(inputField).GetMultiAnalyzerParams()
	if !ok {
		return nil, nil
	}
	var multiAnalyzerParams struct {
		ByField string `json:"by_field"`
	}
	// The params were validated at DDL time; failing to parse or resolve them
	// here means the persisted schema itself is inconsistent, not that the
	// compaction request is malformed.
	if err := json.Unmarshal([]byte(params), &multiAnalyzerParams); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "failed to parse multi_analyzer_params for function %s", functionSchema.GetName())
	}
	if multiAnalyzerParams.ByField == "" {
		return nil, merr.WrapErrDataIntegrityMsg("multi_analyzer_params for function %s is missing required 'by_field' key", functionSchema.GetName())
	}
	byField := typeutil.GetFieldByName(schema, multiAnalyzerParams.ByField)
	if byField == nil {
		return nil, merr.WrapErrDataIntegrityMsg("multi_analyzer_params for function %s references missing input field %s", functionSchema.GetName(), multiAnalyzerParams.ByField)
	}
	// DDL only admits a VarChar by_field selector (validateMultiAnalyzerParams).
	// A resolved selector of any other type is the same class of persisted-schema
	// corruption as the branches above, so classify it identically instead of
	// letting it degrade into an ErrParameterInvalid downstream.
	if byField.GetDataType() != schemapb.DataType_VarChar {
		return nil, merr.WrapErrDataIntegrityMsg("multi_analyzer_params for function %s references by_field %s of type %s, but only VarChar is allowed", functionSchema.GetName(), multiAnalyzerParams.ByField, byField.GetDataType())
	}
	return []*schemapb.FieldSchema{byField}, nil
}

func validateSupportedMissingFunctionMaterialization(functionSchema *schemapb.FunctionSchema) error {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if len(functionSchema.GetInputFieldIds()) == 0 {
			return merr.WrapErrDataIntegrityMsg("persisted bm25 function %s has no input fields", functionSchema.GetName())
		}
		if len(functionSchema.GetOutputFieldIds()) == 0 {
			return merr.WrapErrDataIntegrityMsg("persisted bm25 function %s has no output fields", functionSchema.GetName())
		}
		return nil
	case schemapb.FunctionType_MinHash:
		if len(functionSchema.GetInputFieldIds()) == 0 {
			return merr.WrapErrDataIntegrityMsg("persisted minhash function %s has no input fields", functionSchema.GetName())
		}
		if len(functionSchema.GetOutputFieldIds()) == 0 {
			return merr.WrapErrDataIntegrityMsg("persisted minhash function %s has no output fields", functionSchema.GetName())
		}
		return nil
	default:
		return merr.WrapErrDataIntegrityMsg(
			"persisted function %s has unsupported materialization type %s",
			functionSchema.GetName(), functionSchema.GetType())
	}
}

// validateMaterializationInputField enforces the schema-bump runtime contract on
// persisted function inputs. It is narrower than the collection-creation
// validator: a sealed StorageV3 Text column is read back as encoded LOB
// references, which stringInputsFromRecord cannot consume without LOB decoding,
// so only VarChar is an acceptable persisted input here.
func validateMaterializationInputField(functionSchema *schemapb.FunctionSchema, field *schemapb.FieldSchema) error {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if field.GetDataType() != schemapb.DataType_VarChar {
			return merr.WrapErrDataIntegrityMsg(
				"persisted bm25 input field %d must be VarChar for schema-bump materialization (Text would require LOB decoding), got %s",
				field.GetFieldID(), field.GetDataType())
		}
	case schemapb.FunctionType_MinHash:
		if field.GetDataType() != schemapb.DataType_VarChar {
			return merr.WrapErrDataIntegrityMsg(
				"persisted minhash input field %d must be VarChar for schema-bump materialization (Text would require LOB decoding), got %s",
				field.GetFieldID(), field.GetDataType())
		}
	}
	return nil
}

func validateMaterializationOutputField(functionSchema *schemapb.FunctionSchema, field *schemapb.FieldSchema) error {
	switch functionSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if field.GetDataType() != schemapb.DataType_SparseFloatVector {
			return merr.WrapErrDataIntegrityMsg(
				"persisted bm25 output field %d must be SparseFloatVector, got %s",
				field.GetFieldID(), field.GetDataType())
		}
	case schemapb.FunctionType_MinHash:
		if field.GetDataType() != schemapb.DataType_BinaryVector {
			return merr.WrapErrDataIntegrityMsg(
				"persisted minhash output field %d must be BinaryVector, got %s",
				field.GetFieldID(), field.GetDataType())
		}
	}
	return nil
}

func (t *bumpSchemaVersionCompactionTask) openRecordReader(segment *datapb.CompactionSegmentBinlogs, schema *schemapb.CollectionSchema) (storage.RecordReader, map[int64]struct{}, error) {
	reader, existingFields, err := newCompactionSegmentRecordReader(t.ctx, segment, schema, t.compactionParams.StorageConfig,
		storage.WithCollectionID(segment.GetCollectionID()),
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithDownloader(t.chunkManager.MultiRead),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
	)
	if err != nil {
		mlog.Warn(t.ctx, "failed to open compaction segment reader", mlog.Err(err))
	}
	return reader, existingFields, err
}

// schemaBumpDecision diffs the target schema against the manifest-backed
// physical field set and classifies every gap the bump has to close.
func (t *bumpSchemaVersionCompactionTask) schemaBumpDecision() (*schemaBumpPhysicalDiff, error) {
	segment := t.plan.GetSegmentBinlogs()[0]
	existingFields, err := compactionSegmentStorageFields(segment, t.compactionParams.StorageConfig)
	if err != nil {
		return nil, err
	}
	schema := t.plan.GetSchema()

	functionOutputFields, err := declaredFunctionOutputFields(schema)
	if err != nil {
		return nil, err
	}
	missingFunctions, missingOutputFields, err := missingFunctionMaterializations(schema, existingFields)
	if err != nil {
		return nil, err
	}

	return &schemaBumpPhysicalDiff{
		existingFields:       existingFields,
		droppedFieldIDs:      droppedSchemaFieldIDs(schema, existingFields),
		absentOrdinaryFields: absentOrdinarySchemaFields(schema, existingFields, functionOutputFields),
		missingOutputFields:  missingOutputFields,
		missingFunctions:     missingFunctions,
	}, nil
}

// declaredFunctionOutputFields returns the field IDs declared as outputs by the
// schema's functions. A field marked IsFunctionOutput that no function declares
// is persisted-schema corruption: fail instead of silently backfilling a
// non-nullable vector as an ordinary field.
func declaredFunctionOutputFields(schema *schemapb.CollectionSchema) (map[int64]struct{}, error) {
	declared := make(map[int64]struct{})
	for _, functionSchema := range schema.GetFunctions() {
		for _, outputFieldID := range functionSchema.GetOutputFieldIds() {
			declared[outputFieldID] = struct{}{}
		}
	}
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetIsFunctionOutput() {
			if _, hasProducer := declared[field.GetFieldID()]; !hasProducer {
				return nil, merr.WrapErrDataIntegrityMsg(
					"function output field %d has no producing function", field.GetFieldID())
			}
		}
	}
	return declared, nil
}

// missingFunctionMaterializations derives, from the manifest-backed
// existingFields, the functions whose outputs must be materialized by this bump
// together with those output fields. Missing functions are defined by their
// physically absent outputs, so both results are non-empty or both empty by
// construction. The materialization contract is enforced here, upfront for
// every route: supported function type, all-or-none output presence, output
// field types, and input fields that exist and satisfy the schema-bump input
// contract.
func missingFunctionMaterializations(schema *schemapb.CollectionSchema, existingFields map[int64]struct{}) ([]*schemapb.FunctionSchema, []*schemapb.FieldSchema, error) {
	var missingFunctions []*schemapb.FunctionSchema
	var missingOutputFields []*schemapb.FieldSchema
	for _, functionSchema := range schema.GetFunctions() {
		outputFieldIDs := functionSchema.GetOutputFieldIds()
		// A persisted function with no output fields is schema corruption (DDL
		// rejects it); reject before the all-present early-continue treats the
		// empty set as "nothing missing" and silently drops it.
		if len(outputFieldIDs) == 0 {
			return nil, nil, merr.WrapErrDataIntegrityMsg("persisted function %s has no output fields", functionSchema.GetName())
		}
		presentCount := 0
		for _, outputFieldID := range outputFieldIDs {
			if _, present := existingFields[outputFieldID]; present {
				presentCount++
			}
		}
		if presentCount == len(outputFieldIDs) {
			continue
		}
		if err := validateSupportedMissingFunctionMaterialization(functionSchema); err != nil {
			return nil, nil, err
		}
		if presentCount != 0 {
			return nil, nil, merr.WrapErrDataIntegrityMsg(
				"function %s has partially materialized output fields: %d of %d are physically present",
				functionSchema.GetName(), presentCount, len(outputFieldIDs))
		}
		for _, outputFieldID := range outputFieldIDs {
			outputField := typeutil.GetField(schema, outputFieldID)
			if outputField == nil {
				return nil, nil, merr.WrapErrDataIntegrityMsg(
					"function %s output field %d not found in persisted schema",
					functionSchema.GetName(), outputFieldID)
			}
			if err := validateMaterializationOutputField(functionSchema, outputField); err != nil {
				return nil, nil, err
			}
			missingOutputFields = append(missingOutputFields, outputField)
		}
		if err := validateFunctionInputFields(schema, functionSchema); err != nil {
			return nil, nil, err
		}
		missingFunctions = append(missingFunctions, functionSchema)
	}
	return missingFunctions, missingOutputFields, nil
}

// validateFunctionInputFields checks that every declared input of a
// to-be-materialized function exists in the persisted schema and satisfies the
// schema-bump materialization input contract.
func validateFunctionInputFields(schema *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema) error {
	for _, inputFieldID := range functionSchema.GetInputFieldIds() {
		inputField := typeutil.GetField(schema, inputFieldID)
		if inputField == nil {
			return merr.WrapErrDataIntegrityMsg(
				"function %s input field %d not found in persisted schema",
				functionSchema.GetName(), inputFieldID)
		}
		if err := validateMaterializationInputField(functionSchema, inputField); err != nil {
			return err
		}
	}
	return nil
}

// absentOrdinarySchemaFields returns the non-system, non-function-output schema
// fields that are physically absent from the manifest.
func absentOrdinarySchemaFields(schema *schemapb.CollectionSchema, existingFields, functionOutputFields map[int64]struct{}) []*schemapb.FieldSchema {
	absent := make([]*schemapb.FieldSchema, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fieldID := field.GetFieldID()
		if common.IsSystemField(fieldID) {
			continue
		}
		if _, isFunctionOutput := functionOutputFields[fieldID]; isFunctionOutput {
			continue
		}
		if _, present := existingFields[fieldID]; !present {
			absent = append(absent, field)
		}
	}
	return absent
}

func (t *bumpSchemaVersionCompactionTask) additiveReadSchema(diff *schemaBumpPhysicalDiff) (*schemapb.CollectionSchema, []int64, error) {
	inputSchema, logicalInputFieldIDs, err := t.missingFunctionInputSchema(diff.missingFunctions)
	if err != nil {
		return nil, nil, err
	}

	readFields := make([]*schemapb.FieldSchema, 0, len(inputSchema.GetFields())+1)
	seen := make(map[int64]struct{}, len(inputSchema.GetFields())+1)
	// Appends a field once (dedup by ID); absent fields stay in the read schema
	// for the reader to fill per its contract.
	addReadField := func(field *schemapb.FieldSchema) {
		if field == nil {
			return
		}
		fieldID := field.GetFieldID()
		if _, ok := seen[fieldID]; ok {
			return
		}
		seen[fieldID] = struct{}{}
		readFields = append(readFields, field)
	}
	// Every additive pass reads one stable system column so batches retain row
	// cardinality even when all logical function inputs are newly added fields.
	anchor := typeutil.GetField(t.plan.GetSchema(), common.RowIDField)
	if _, ok := diff.existingFields[common.RowIDField]; !ok || anchor == nil {
		anchor = nil
		if _, ok := diff.existingFields[common.TimeStampField]; ok {
			anchor = typeutil.GetField(t.plan.GetSchema(), common.TimeStampField)
		}
	}
	if anchor == nil {
		return nil, nil, merr.WrapErrDataIntegrityMsg(
			"schema bump additive reconciliation requires a physical RowID or Timestamp anchor")
	}
	// Anchor was selected from existingFields above, so it is physically present.
	addReadField(anchor)
	for _, field := range inputSchema.GetFields() {
		addReadField(field)
	}
	for _, structField := range inputSchema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			addReadField(field)
		}
	}

	// Absent ordinary fields (including absent function inputs) enter the read
	// schema so the reader fills them per its contract; the writer then takes
	// the appended columns straight from the read record, and functions see
	// target-schema values for their inputs.
	for _, field := range diff.absentOrdinaryFields {
		addReadField(field)
	}

	schema := t.plan.GetSchema()
	return &schemapb.CollectionSchema{
		Name:               schema.GetName(),
		Description:        schema.GetDescription(),
		Fields:             readFields,
		EnableDynamicField: schema.GetEnableDynamicField(),
		Properties:         schema.GetProperties(),
	}, logicalInputFieldIDs, nil
}

func (t *bumpSchemaVersionCompactionTask) fullRewriteSegmentID() (int64, error) {
	idRange := t.plan.GetPreAllocatedSegmentIDs()
	if idRange == nil || idRange.GetBegin() >= idRange.GetEnd() {
		return 0, merr.WrapErrServiceInternal("schema bump full rewrite requires a pre-allocated segment ID")
	}
	return idRange.GetBegin(), nil
}

func selectFullRewriteRecord(record storage.Record, pkField *schemapb.FieldSchema, entityFilter compaction.EntityFilter, ttlFieldID int64, useTTLField bool) (*recordSelection, error) {
	pkArray := record.Column(pkField.GetFieldID())
	var pkAt func(int) any
	switch pkField.GetDataType() {
	case schemapb.DataType_Int64:
		int64Array, ok := pkArray.(*array.Int64)
		if !ok {
			return nil, merr.WrapErrServiceInternal("int64 primary key field not found in full schema rewrite record")
		}
		pkAt = func(i int) any { return int64Array.Value(i) }
	case schemapb.DataType_VarChar:
		stringArray, ok := pkArray.(*array.String)
		if !ok {
			return nil, merr.WrapErrServiceInternal("varchar primary key field not found in full schema rewrite record")
		}
		pkAt = func(i int) any { return stringArray.Value(i) }
	case schemapb.DataType_UUID:
		if fbArr, ok := pkArray.(*array.FixedSizeBinary); ok {
			if dt, ok := fbArr.DataType().(*arrow.FixedSizeBinaryType); ok && dt.ByteWidth == 16 {
				pkAt = func(i int) any {
					var u [16]byte
					copy(u[:], fbArr.Value(i))
					return u
				}
				break
			}
		}
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("uuid primary key field not found in full schema rewrite record: expected FixedSizeBinary(16), got %T", pkArray))
	default:
		return nil, merr.WrapErrServiceInternal("invalid primary key data type for full schema rewrite")
	}

	timestampArray, ok := record.Column(common.TimeStampField).(*array.Int64)
	if !ok {
		return nil, merr.WrapErrServiceInternal("timestamp field not found in full schema rewrite record")
	}
	var ttlArray *array.Int64
	if useTTLField {
		ttlArray, ok = record.Column(ttlFieldID).(*array.Int64)
		if !ok {
			return nil, merr.WrapErrServiceInternal("TTL field not found in full schema rewrite record")
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

		if sliceStart == -1 {
			sliceStart = i
		}
	}
	if sliceStart != -1 {
		selection.ranges = append(selection.ranges, rowRange{start: sliceStart, end: record.Len()})
		selection.length += record.Len() - sliceStart
	}
	if filteredRows == 0 {
		return nil, nil
	}
	return selection, nil
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
				BaseManifest:        segment.GetManifest(),
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
	_, ttlFieldPhysicallyPresent := existingFields[ttlFieldID]
	sourceHasTTLField := ttlFieldID >= common.StartOfUserFieldID && ttlFieldPhysicallyPresent
	preMaterializeFilter := len(delta) > 0 || t.plan.GetCollectionTtl() > 0 || sourceHasTTLField
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

	// Prepare LOB (TEXT) handling: a schema bump keeps existing TEXT LOB data
	// unchanged, so REUSE_ALL — the writer preserves the encoded LOB reference
	// bytes (WithTextRefsAsBinary) and the source LOB files are merged into the
	// output manifest afterwards (applyLOBCompaction). Mirrors sort compaction.
	if err := t.initLOBCompactionContext(t.ctx); err != nil {
		return nil, err
	}

	alloc := allocator.NewLocalAllocator(t.plan.GetPreAllocatedLogIDs().GetBegin(), t.plan.GetPreAllocatedLogIDs().GetEnd())
	writerOpts := []storage.RwOption{
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return t.chunkManager.MultiWrite(ctx, kvs)
		}),
		storage.WithVersion(segment.GetStorageVersion()),
		storage.WithStorageConfig(t.compactionParams.StorageConfig),
		storage.WithCollectionID(collectionID),
		storage.WithUseLoonFFI(t.compactionParams.UseLoonFFI),
	}
	if t.lobContext != nil && t.lobContext.HasReuseAllFields() {
		// Existing TEXT columns arrive from the reader as encoded binary LOB refs;
		// tell the writer to write them via the physical binary schema unchanged.
		writerOpts = append(writerOpts, storage.WithTextRefsAsBinary())
	}
	writer, err := storage.NewBinlogRecordWriter(t.ctx,
		collectionID,
		segment.GetPartitionID(),
		newSegmentID,
		t.plan.GetSchema(),
		alloc,
		t.compactionParams.BinLogMaxSize,
		t.plan.GetTotalRows(),
		writerOpts...,
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

		var selection *recordSelection
		if preMaterializeFilter {
			selection, err = selectFullRewriteRecord(record, pkField, entityFilter, ttlFieldID, sourceHasTTLField)
			if err != nil {
				return nil, err
			}
			if selection != nil && selection.Len() == 0 {
				continue
			}
		}

		// record stays owned by the reader (released on its next Next/Close);
		// only the derived arrays of the wrapped record are cleaned up here.
		wrapped, err := materializer.WrapWithSelection(record, selection)
		if err != nil {
			return nil, err
		}
		out := overwriteRecordTimestamps(wrapped, segment.GetCommitTimestamp())
		if err := writer.Write(out); err != nil {
			if out != wrapped {
				out.Release()
			}
			cleanupMaterializedRecord(wrapped)
			return nil, err
		}
		if out != wrapped {
			out.Release()
		}

		totalRows += int64(wrapped.Len())
		cleanupMaterializedRecord(wrapped)
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	writerClosed = true

	// Update per-LOB-file valid_rows for REUSE_ALL fields: rows dropped by
	// delete/TTL during the rewrite reduce the number of live LOB references.
	if t.lobContext != nil && t.lobContext.HasReuseAllFields() {
		inputRows := t.plan.GetTotalRows()
		deletedRows := inputRows - totalRows
		if deletedRows < 0 {
			deletedRows = 0
		}
		t.lobContext.SetSegmentRowStats(segment.GetSegmentID(), inputRows, deletedRows)
	}

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

	// Merge the source segment's TEXT LOB file references into the output manifest
	// (REUSE_ALL) BEFORE building the text-match index. createTextIndex reads the TEXT
	// column through this manifest, so the carried LOB files must already be referenced
	// -- otherwise the match index for an out-of-line (>=64KB) TEXT field is built against
	// a manifest that does not yet list those LOB files, silently missing that data.
	// Mirrors sort/mix (applyLOBCompaction before createTextIndex); AddStatsToManifest
	// below then chains its text stats onto the LOB-merged manifest.
	if err := t.applyLOBCompaction(t.ctx, resultSegment); err != nil {
		return nil, err
	}

	if totalRows > 0 {
		// Text stats are built explicitly, matching sort compaction.
		textStatsLogs, err := createTextIndex(t.ctx, t.chunkManager, t.plan, t.compactionParams, segment.GetStorageVersion(), collectionID, segment.GetPartitionID(), newSegmentID, t.plan.GetPlanID(), resultSegment)
		if err != nil {
			return nil, err
		}
		if resultSegment.GetManifest() != "" && len(textStatsLogs) > 0 {
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

// initLOBCompactionContext prepares REUSE_ALL LOB handling for the (single) input
// segment's TEXT columns. A schema bump is 1->1 and never changes existing TEXT LOB
// data, so all TEXT fields are forced REUSE_ALL (GetForcedStrategy): the source LOB
// files stay in place and only their references are merged into the output manifest.
// No-op when the schema has no TEXT fields. Mirrors sort compaction.
func (t *bumpSchemaVersionCompactionTask) initLOBCompactionContext(ctx context.Context) error {
	textFieldIDs := compaction.GetTEXTFieldIDsFromSchema(t.plan.GetSchema())
	if len(textFieldIDs) == 0 {
		return nil
	}

	// preCompact already guarantees a StorageV3 segment with a non-empty manifest,
	// and this only runs from runFullSchemaRewrite (after preCompact), so no manifest guard.
	segment := t.plan.GetSegmentBinlogs()[0]
	sourceManifests := map[int64]string{segment.GetSegmentID(): segment.GetManifest()}
	lobFilesBySegment, err := compaction.CollectLobFilesFromManifests(sourceManifests, t.compactionParams.StorageConfig)
	if err != nil {
		return err
	}

	t.lobContext = compaction.NewLOBCompactionContext()
	for segID, files := range lobFilesBySegment {
		t.lobContext.AddSegmentLobFiles(segID, files)
	}
	// schema-bump is always 1 source -> 1 output; forced REUSE_ALL.
	t.lobContext.SetCompactionType(datapb.CompactionType_BumpSchemaVersionCompaction, 1, 1)
	t.lobContext.ComputeStrategies(textFieldIDs, t.compactionParams.LOBHoleRatioThreshold)
	mlog.Info(ctx, "schema bump: initialized LOB compaction context (REUSE_ALL)",
		mlog.Int64("planID", t.GetPlanID()),
		mlog.FieldSegmentID(segment.GetSegmentID()),
		mlog.Int64s("textFieldIDs", textFieldIDs),
	)
	return nil
}

// applyLOBCompaction merges the source segment's TEXT LOB file references into the
// output segment's manifest (REUSE_ALL): LOB files are unchanged, only their
// references (and valid_rows) are copied over. Mirrors sort compaction.
func (t *bumpSchemaVersionCompactionTask) applyLOBCompaction(ctx context.Context, outputSegment *datapb.CompactionSegment) error {
	if t.lobContext == nil || !t.lobContext.HasReuseAllFields() {
		return nil
	}
	if outputSegment.GetManifest() == "" {
		return nil
	}

	outputManifests := map[int64]string{outputSegment.GetSegmentID(): outputSegment.GetManifest()}
	updatedManifests, err := compaction.ApplyLobCompactionToManifests(t.lobContext, outputManifests, t.compactionParams.StorageConfig)
	if err != nil {
		return err
	}
	if newManifest, ok := updatedManifests[outputSegment.GetSegmentID()]; ok {
		outputSegment.Manifest = newManifest
	}
	mlog.Info(ctx, "schema bump: merged TEXT LOB references into output manifest",
		mlog.Int64("planID", t.GetPlanID()),
		mlog.FieldSegmentID(outputSegment.GetSegmentID()),
	)
	return nil
}

func appendBM25StatsFromArrowArray(stats *storage.BM25Stats, arr arrow.Array) (int, error) {
	binaryArray, ok := arr.(*array.Binary)
	if !ok {
		return 0, merr.WrapErrFunctionFailedMsg("bm25 output field must be arrow binary array, got %T", arr)
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
	Abort()
}

type bumpSchemaVersionWriterLease struct {
	writer    bumpSchemaVersionBatchWriter
	exhausted bool
}

func (l *bumpSchemaVersionWriterLease) Close() (packed.WriterOutput, error) {
	if l.exhausted {
		return nil, merr.WrapErrServiceInternalMsg("schema bump writer lease already consumed")
	}
	out, err := l.writer.Close()
	if err != nil {
		// Keep the lease unconsumed so the deferred Cleanup aborts the writer;
		// do not rely on the underlying Close to release native resources.
		return nil, err
	}
	l.exhausted = true
	return out, nil
}

func (l *bumpSchemaVersionWriterLease) Cleanup() {
	if l == nil || l.exhausted {
		return
	}
	l.exhausted = true
	l.writer.Abort()
}

type bumpSchemaVersionWriterResult struct {
	writer         bumpSchemaVersionBatchWriter
	columnGroups   []storagecommon.ColumnGroup
	storageVersion int64
	basePath       string
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
		return nil, merr.WrapErrDataIntegrity(err, "failed to parse existing manifest for schema bump")
	}
	writerResult, err := t.newV3WriterResult(outputSchema, newColumnGroups, segment, collectionID, basePath, existingVersion)
	if err != nil {
		return nil, err
	}
	writerResult.writer.AsNewColumnGroups()
	mlog.Info(t.ctx, "schema bump partial writer setup",
		mlog.Int64("baseVersion", existingVersion),
		mlog.String("basePath", basePath),
		mlog.Int("outputFieldCount", len(outputFields)),
		mlog.Int("columnGroupCount", len(newColumnGroups)),
	)
	return writerResult, nil
}

func (t *bumpSchemaVersionCompactionTask) newV3WriterResult(schema *schemapb.CollectionSchema, columnGroups []storagecommon.ColumnGroup, segment *datapb.CompactionSegmentBinlogs, collectionID int64, basePath string, baseVersion int64) (*bumpSchemaVersionWriterResult, error) {
	if segment.GetStorageVersion() < storage.StorageV3 || segment.GetManifest() == "" {
		return nil, merr.WrapErrServiceInternalMsg("schema bump compaction requires a StorageV3 segment with manifest")
	}

	pluginContext, err := hookutil.GetCPluginContext(t.plan.GetPluginContext(), collectionID)
	if err != nil {
		return nil, err
	}

	writerFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	columnGroups = storagecommon.FillColumnGroupFormats(columnGroups, writerFormat)
	schemaBasedFormats := storagecommon.ColumnGroupFormats(columnGroups, writerFormat)
	var writer bumpSchemaVersionBatchWriter
	if schemaContainsTextField(schema) {
		writer, err = storage.NewPartialPackedRecordBatchWriterWithTextRefsAsBinary(basePath, schema, int64(t.compactionParams.BinLogMaxSize), packed.DefaultMultiPartUploadSize, columnGroups, t.compactionParams.StorageConfig, pluginContext, writerFormat, schemaBasedFormats)
	} else {
		writer, err = storage.NewPartialPackedRecordBatchWriter(basePath, schema, int64(t.compactionParams.BinLogMaxSize), packed.DefaultMultiPartUploadSize, columnGroups, t.compactionParams.StorageConfig, pluginContext, writerFormat, schemaBasedFormats)
	}
	if err != nil {
		return nil, err
	}
	mlog.Info(t.ctx, "schema bump partial manifest writer created",
		mlog.Int64("baseVersion", baseVersion),
		mlog.Int("schemaFieldCount", len(schema.GetFields())),
		mlog.Int("columnGroupCount", len(columnGroups)),
	)

	return &bumpSchemaVersionWriterResult{
		writer:         writer,
		columnGroups:   columnGroups,
		storageVersion: segment.GetStorageVersion(),
		basePath:       basePath,
	}, nil
}

func schemaContainsTextField(schema *schemapb.CollectionSchema) bool {
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetDataType() == schemapb.DataType_Text {
			return true
		}
	}
	return false
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

// buildNewInsertLogsV3 returns FieldBinlogs for the column groups this run
// wrote, and nothing else. It deliberately does not merge the input segment's
// FieldBinlogs: those come from the compaction plan, which copies them off
// SegmentInfo, and for a StorageV3 segment recovered after a DataCoord restart
// they are empty (#50410 stopped persisting per-field binlog KVs for V3).
// The receiver (completeBumpSchemaVersionCompactionMutation in
// internal/datacoord/meta.go) merges this array onto SegmentInfo.Binlogs,
// which is where the materialized field's data becomes visible to the rest of
// DataCoord — including index building — without replacing untouched groups.
// Index eligibility is gated separately by the segment schema version.
// This result is therefore load-bearing: dropping it silently makes the
// materialized field permanently un-indexable.
func (t *bumpSchemaVersionCompactionTask) buildNewInsertLogsV3(writerResult *bumpSchemaVersionWriterResult, fieldMemorySizes map[int64]int, totalRows int64) ([]*datapb.FieldBinlog, error) {
	logIDStart, _, err := t.logIDAlloc.Alloc(uint32(len(writerResult.columnGroups)))
	if err != nil {
		return nil, err
	}
	newInsertLogs := make(map[int64]*datapb.FieldBinlog, len(writerResult.columnGroups))
	for i, columnGroup := range writerResult.columnGroups {
		fieldID := columnGroup.GroupID
		newInsertLogs[fieldID] = &datapb.FieldBinlog{
			FieldID:     fieldID,
			ChildFields: columnGroup.Fields,
			Format:      columnGroup.Format,
			Binlogs: []*datapb.Binlog{
				{
					MemorySize: int64(fieldMemorySizes[fieldID]),
					EntriesNum: totalRows,
					LogID:      logIDStart + int64(i),
				},
			},
		}
	}

	return storage.SortFieldBinlogs(newInsertLogs), nil
}

func (t *bumpSchemaVersionCompactionTask) runAdditivePhysicalReconciliation(ctx context.Context, diff *schemaBumpPhysicalDiff) (*datapb.CompactionPlanResult, error) {
	// Additive reconciliation reports a Statistics INCREMENT, which is only
	// valid because this path appends columns and never removes them. Dropped
	// fields must go to runFullSchemaRewrite, which ships absolute statistics.
	if len(diff.droppedFieldIDs) > 0 {
		return nil, merr.WrapErrServiceInternalMsg(
			"schema bump additive reconciliation cannot run with dropped fields, planID = %d, droppedFieldIDs = %v",
			t.GetPlanID(), diff.droppedFieldIDs)
	}

	segment := t.plan.GetSegmentBinlogs()[0]
	collectionID := segment.GetCollectionID()
	partitionID := segment.GetPartitionID()
	segmentID := segment.GetSegmentID()

	inputSchema, inputFieldIDs, err := t.additiveReadSchema(diff)
	if err != nil {
		return nil, err
	}
	outputFields := diff.appendedFields()
	if len(outputFields) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("schema bump additive reconciliation has no fields to append")
	}
	outputFieldIDs := make([]int64, 0, len(outputFields))
	for _, field := range outputFields {
		outputFieldIDs = append(outputFieldIDs, field.GetFieldID())
	}

	log := mlog.With(
		mlog.Int64("planID", t.plan.GetPlanID()),
		mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(partitionID),
		mlog.FieldSegmentID(segmentID),
		mlog.Int64s("inputFieldIDs", inputFieldIDs),
		mlog.Int64s("outputFieldIDs", outputFieldIDs),
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
	writerLease := &bumpSchemaVersionWriterLease{writer: writerResult.writer}
	defer writerLease.Cleanup()

	log.Info(ctx, "schema bump writer setup",
		mlog.Int64("effectiveStorageVersion", writerResult.storageVersion),
		mlog.Int64("clusterStorageVersion", t.compactionParams.StorageVersion),
		mlog.Bool("segmentHasManifest", segment.GetManifest() != ""),
	)
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "BumpSchemaVersionCompact.batchProcess")
	statsByField := make(map[int64]*storage.BM25Stats)
	fieldMemorySizeByField := make(map[int64]int, len(outputFieldIDs))
	fieldNullCountByField := make(map[int64]int64, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		if isBM25MaterializedOutputField(t.plan.GetSchema(), outputFieldID) {
			statsByField[outputFieldID] = storage.NewBM25Stats()
		}
	}
	materializer, err := NewRecordMaterializer(t.plan.GetSchema(), diff.missingFunctions, diff.existingFields)
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
		// record stays owned by the reader (released on its next Next/Close);
		// only the derived arrays of the wrapped record are cleaned up here.
		wrapped, err := materializer.Wrap(record)
		computeDuration += time.Since(computeStart)
		if err != nil {
			span.End()
			return nil, err
		}

		for _, outputField := range outputFields {
			outputFieldID := outputField.GetFieldID()
			outputCol := wrapped.Column(outputFieldID)
			if outputCol == nil {
				cleanupMaterializedRecord(wrapped)
				span.End()
				if outputField.GetIsFunctionOutput() {
					return nil, merr.WrapErrFunctionFailedMsg("function output field %d not found in materialized record", outputFieldID)
				}
				return nil, merr.WrapErrServiceInternalMsg("ordinary output field %d not found in materialized record", outputFieldID)
			}
			batchMemSize := arrowArrayMemorySize(outputCol)
			if stats, ok := statsByField[outputFieldID]; ok {
				bm25MemSize, err := appendBM25StatsFromArrowArray(stats, outputCol)
				if err != nil {
					cleanupMaterializedRecord(wrapped)
					span.End()
					return nil, err
				}
				batchMemSize = bm25MemSize
			}
			fieldMemorySizeByField[outputFieldID] += batchMemSize
			fieldNullCountByField[outputFieldID] += int64(outputCol.NullN())
		}

		writeStart := time.Now()
		if err := writerResult.writer.Write(wrapped); err != nil {
			cleanupMaterializedRecord(wrapped)
			span.End()
			return nil, err
		}
		writeDuration += time.Since(writeStart)

		totalRows += int64(wrapped.Len())
		cleanupMaterializedRecord(wrapped)
	}
	span.End()
	if totalRows != t.plan.GetTotalRows() {
		return nil, merr.WrapErrDataIntegrityMsg(
			"schema bump additive reconciliation read %d rows, expected %d",
			totalRows, t.plan.GetTotalRows())
	}
	// A zero-row materialized write would manufacture physical completeness for
	// an empty segment; fail and let the deferred lease Abort the writer.
	if totalRows == 0 {
		return nil, merr.WrapErrDataIntegrityMsg(
			"schema bump additive reconciliation observed zero rows for segment %d, refusing to write an empty materialized record", segmentID)
	}

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

	log.Info(ctx, "schema bump bm25 stats written",
		mlog.Int("bm25StatsFieldCount", len(statsByField)),
		mlog.Int64("storageVersion", writerResult.storageVersion),
		mlog.Int("v3StatsCount", len(writerResult.v3Stats)),
	)

	newInsertLogs, err := t.buildNewInsertLogsV3(writerResult, fieldMemorySizeByField, totalRows)
	if err != nil {
		return nil, err
	}

	writerOutput, err := writerLease.Close()
	if writerOutput != nil {
		defer writerOutput.Destroy()
	}
	if err != nil {
		return nil, err
	}

	// The heavy data files are already written to object storage by the writer
	// above. Instead of committing the manifest transaction here — on the base
	// version pinned when the plan was built — ship the transaction INPUT (the
	// serializable descriptors of those files plus the bm25 stat entries) to
	// DataCoord and let CommitSegmentManifest run the transaction on the
	// segment's CURRENT manifest. That rebases the column-group / stat adds
	// against any concurrent commit (e.g. an L0 deltalog add) instead of losing
	// to it on a stale-base CAS and forcing a full re-materialization.
	describer, ok := writerOutput.(interface {
		ColumnGroupEntries() ([]packed.ColumnGroupEntry, error)
	})
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("schema bump materialization writer output does not expose column group entries")
	}
	columnGroupEntries, err := describer.ColumnGroupEntries()
	if err != nil {
		return nil, merr.Wrap(err, "failed to read schema bump column group descriptors")
	}
	if len(columnGroupEntries) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("schema bump materialization produced no column groups, planID = %d, segmentID = %d", t.GetPlanID(), segmentID)
	}
	manifestDelta := &datapb.SegmentManifestDelta{
		ColumnGroups: packed.ColumnGroupEntriesToProto(columnGroupEntries),
		Stats:        packed.StatEntriesToProto(writerResult.v3Stats),
	}
	log.Info(ctx, "[schema-bump-partial-writer] writer output prepared as manifest delta",
		mlog.Int64("totalRows", totalRows),
		mlog.Int("newInsertLogsCount", len(newInsertLogs)),
		mlog.Int("columnGroupCount", len(manifestDelta.GetColumnGroups())),
		mlog.Int("v3StatsCount", len(writerResult.v3Stats)),
	)

	ret := &datapb.CompactionPlanResult{
		PlanID: t.plan.GetPlanID(),
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:      segmentID,
				NumOfRows:      totalRows,
				InsertLogs:     newInsertLogs,
				Channel:        segment.GetInsertChannel(),
				StorageVersion: writerResult.storageVersion,
				// Manifest is intentionally empty: with ManifestDelta set,
				// DataCoord generates the new manifest revision itself from the
				// shipped descriptors, on the segment's current base.
				ManifestDelta: manifestDelta,
				BaseManifest:  segment.GetManifest(),
				// In-place materialization ships an INCREMENT, not an absolute
				// footprint: DataCoord adds it onto the segment's existing
				// Stats. Field2StatslogPaths and Deltalogs are deliberately
				// absent — they were echoes of the plan that the in-place
				// receiver never reads, and the plan's arrays are empty for a
				// recovered StorageV3 segment anyway.
				Stats: buildMaterializationStatsDelta(
					writerResult.columnGroups,
					fieldMemorySizeByField,
					fieldNullCountByField,
					writerResult.statsBlobSize,
				),
			},
		},
		Type: t.plan.GetType(),
	}
	log.Info(ctx, "schema bump additive reconciliation completed",
		mlog.Int64("numOfRows", totalRows),
		mlog.Int("newInsertLogsCount", len(newInsertLogs)),
		mlog.Int("columnGroupCount", len(manifestDelta.GetColumnGroups())),
		mlog.Int64("effectiveStorageVersion", writerResult.storageVersion),
		mlog.Int32("collectionSchemaVersion", t.plan.GetSchema().GetVersion()),
		mlog.Duration("readDuration", readDuration),
		mlog.Duration("computeDuration", computeDuration),
		mlog.Duration("writeDuration", writeDuration),
		mlog.Duration("updateStatsDuration", updateStatsDuration),
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
