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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// ============================================================================
// Unit layer: ctor guards, runner errors, bad outputs, lifecycle (U series)
// ============================================================================
// fakeFunctionRunner is a controllable function.FunctionRunner for exercising
// materializer constructor guards and bad-output handling without cgo runners.
type fakeFunctionRunner struct {
	schema     *schemapb.FunctionSchema
	inputs     []*schemapb.FieldSchema
	outputs    []any
	runErr     error
	closeCount int
}

func (r *fakeFunctionRunner) BatchRun(...any) ([]any, error) {
	if r.runErr != nil {
		return nil, r.runErr
	}
	return r.outputs, nil
}
func (r *fakeFunctionRunner) GetSchema() *schemapb.FunctionSchema      { return r.schema }
func (r *fakeFunctionRunner) GetOutputFields() []*schemapb.FieldSchema { return nil }
func (r *fakeFunctionRunner) GetInputFields() []*schemapb.FieldSchema  { return r.inputs }
func (r *fakeFunctionRunner) Close()                                   { r.closeCount++ }

var _ function.FunctionRunner = (*fakeFunctionRunner)(nil)

func rmSchemaBM25() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}}
}

func rmSchemaMinHash() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{
			FieldID: 102, Name: "mh", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "32"}},
		},
	}}
}

// ---------------------------------------------------------------------------
// U2: function-materializer constructor guards (table-driven, fake runner)
// ---------------------------------------------------------------------------

// [U2][S3] every schema-integrity guard in the BM25/MinHash materializer
// constructors fires with the right message.
func TestRMFunctionMaterializerCtorGuards(t *testing.T) {
	varcharIn := &schemapb.FieldSchema{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar}
	int64In := &schemapb.FieldSchema{FieldID: 101, Name: "text", DataType: schemapb.DataType_Int64}
	ghostIn := &schemapb.FieldSchema{FieldID: 999, Name: "ghost", DataType: schemapb.DataType_VarChar}

	cases := []struct {
		name    string
		fnType  schemapb.FunctionType
		schema  *schemapb.CollectionSchema
		mutate  func(r *fakeFunctionRunner, s *schemapb.CollectionSchema)
		wantErr string
	}{
		{"bm25-no-inputs", schemapb.FunctionType_BM25, rmSchemaBM25(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.inputs = nil
		}, "should have input fields"},
		{"bm25-input-not-in-schema", schemapb.FunctionType_BM25, rmSchemaBM25(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.inputs = []*schemapb.FieldSchema{ghostIn}
		}, "input field not found in schema"},
		{"bm25-input-wrong-type", schemapb.FunctionType_BM25, rmSchemaBM25(), func(r *fakeFunctionRunner, s *schemapb.CollectionSchema) {
			s.Fields[1].DataType = schemapb.DataType_Int64
			r.inputs = []*schemapb.FieldSchema{int64In}
		}, "must be varchar or text"},
		{"bm25-no-outputs", schemapb.FunctionType_BM25, rmSchemaBM25(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.schema.OutputFieldIds = nil
		}, "should have output fields"},
		{"bm25-output-not-in-schema", schemapb.FunctionType_BM25, rmSchemaBM25(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.schema.OutputFieldIds = []int64{888}
		}, "output field not found in schema"},
		{"bm25-output-wrong-type", schemapb.FunctionType_BM25, rmSchemaBM25(), func(_ *fakeFunctionRunner, s *schemapb.CollectionSchema) {
			s.Fields[2].DataType = schemapb.DataType_FloatVector
		}, "must be sparse float vector"},
		{"bm25-output-nullable", schemapb.FunctionType_BM25, rmSchemaBM25(), func(_ *fakeFunctionRunner, s *schemapb.CollectionSchema) {
			s.Fields[2].Nullable = true
		}, "cannot be nullable"},
		{"minhash-no-inputs", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.inputs = nil
		}, "should have input fields"},
		{"minhash-input-not-in-schema", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.inputs = []*schemapb.FieldSchema{ghostIn}
		}, "input field not found in schema"},
		{"minhash-input-wrong-type", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(r *fakeFunctionRunner, s *schemapb.CollectionSchema) {
			s.Fields[1].DataType = schemapb.DataType_Int64
			r.inputs = []*schemapb.FieldSchema{int64In}
		}, "must be varchar or text"},
		{"minhash-no-outputs", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.schema.OutputFieldIds = nil
		}, "should have output fields"},
		{"minhash-output-not-in-schema", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(r *fakeFunctionRunner, _ *schemapb.CollectionSchema) {
			r.schema.OutputFieldIds = []int64{888}
		}, "output field not found in schema"},
		{"minhash-output-wrong-type", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(_ *fakeFunctionRunner, s *schemapb.CollectionSchema) {
			s.Fields[2].DataType = schemapb.DataType_SparseFloatVector
			s.Fields[2].TypeParams = nil
		}, "must be binary vector"},
		{"minhash-output-nullable", schemapb.FunctionType_MinHash, rmSchemaMinHash(), func(_ *fakeFunctionRunner, s *schemapb.CollectionSchema) {
			s.Fields[2].Nullable = true
		}, "cannot be nullable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeFunctionRunner{
				schema: &schemapb.FunctionSchema{
					Name: "f", Type: tc.fnType,
					InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
				},
				inputs: []*schemapb.FieldSchema{varcharIn},
			}
			tc.mutate(runner, tc.schema)
			_, err := newFunctionMaterializer(tc.schema, runner, []int{0}, true)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// ---------------------------------------------------------------------------
// U3: NewRecordMaterializer error chain
// ---------------------------------------------------------------------------

// [U3][S3] runner construction failures propagate and never leak an already
// constructed runner.
func TestRMNewRecordMaterializerRunnerErrors(t *testing.T) {
	schema := rmSchemaBM25()
	fn := &schemapb.FunctionSchema{
		Name: "f", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
	}
	existing := map[int64]struct{}{100: {}, 101: {}}

	// runner factory error
	errPatch := mockey.Mock(function.NewFunctionRunner).
		Return(nil, errors.New("rm-ut runner factory failed")).Build()
	_, err := NewRecordMaterializer(schema, []*schemapb.FunctionSchema{fn}, existing)
	errPatch.UnPatch()
	require.ErrorContains(t, err, "runner factory failed")

	// nil runner without error
	nilPatch := mockey.Mock(function.NewFunctionRunner).
		Return(nil, nil).Build()
	_, err = NewRecordMaterializer(schema, []*schemapb.FunctionSchema{fn}, existing)
	nilPatch.UnPatch()
	require.ErrorContains(t, err, "failed to set up function runner")

	// materializer construction fails after a live runner: the runner must be closed.
	runner := &fakeFunctionRunner{
		schema: &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_Unknown},
	}
	livePatch := mockey.Mock(function.NewFunctionRunner).
		Return(runner, nil).Build()
	_, err = NewRecordMaterializer(schema, []*schemapb.FunctionSchema{fn}, existing)
	livePatch.UnPatch()
	require.ErrorContains(t, err, "unsupported function type")
	require.Equal(t, 1, runner.closeCount, "runner must be closed when materializer setup fails")
}

// ---------------------------------------------------------------------------
// U4: bad runner outputs at Materialize time
// ---------------------------------------------------------------------------

func rmVarcharRecord(t *testing.T, values []string) *materializerTestRecord {
	arr := bumpUTStringArray(values)
	t.Cleanup(arr.Release)
	return &materializerTestRecord{len: len(values), columns: map[storage.FieldID]arrow.Array{101: arr}}
}

// [U4][S3] minhash materializer rejects malformed runner outputs instead of
// writing garbage.
func TestRMMinHashMaterializeBadOutputs(t *testing.T) {
	newMH := func(outputs []any, runErr error) FunctionMaterializer {
		runner := &fakeFunctionRunner{
			schema: &schemapb.FunctionSchema{
				Name: "f", Type: schemapb.FunctionType_MinHash,
				InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
			},
			inputs:  []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar}},
			outputs: outputs,
			runErr:  runErr,
		}
		m, err := newFunctionMaterializer(rmSchemaMinHash(), runner, []int{0}, true)
		require.NoError(t, err)
		return m
	}
	rec := rmVarcharRecord(t, []string{"a", "b"})

	_, err := newMH(nil, errors.New("rm-ut run failed")).Materialize(rec)
	require.ErrorContains(t, err, "run failed")

	_, err = newMH([]any{}, nil).Materialize(rec)
	require.ErrorContains(t, err, "expects 1 outputs, got 0")

	_, err = newMH([]any{"not-field-data"}, nil).Materialize(rec)
	require.ErrorContains(t, err, "expected FieldData")

	_, err = newMH([]any{&schemapb.FieldData{}}, nil).Materialize(rec)
	require.ErrorContains(t, err, "expected binary vector field data")

	short := &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
		Dim: 32, Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 4)}, // 1 row, record has 2
	}}}
	_, err = newMH([]any{short}, nil).Materialize(rec)
	require.ErrorContains(t, err, "row count mismatch")
}

// [U4b][S3] bm25 materializer rejects wrong output arity and type.
func TestRMBM25MaterializeBadOutputs(t *testing.T) {
	newBM := func(outputs []any) FunctionMaterializer {
		runner := &fakeFunctionRunner{
			schema: &schemapb.FunctionSchema{
				Name: "f", Type: schemapb.FunctionType_BM25,
				InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
			},
			inputs:  []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar}},
			outputs: outputs,
		}
		m, err := newFunctionMaterializer(rmSchemaBM25(), runner, []int{0}, true)
		require.NoError(t, err)
		return m
	}
	rec := rmVarcharRecord(t, []string{"a", "b"})

	_, err := newBM([]any{}).Materialize(rec)
	require.ErrorContains(t, err, "expects 1 outputs, got 0")

	_, err = newBM([]any{"nope"}).Materialize(rec)
	require.ErrorContains(t, err, "expected SparseFloatArray")
}

// ---------------------------------------------------------------------------
// U5/U6: synthesis failure release + reader wrap failure
// ---------------------------------------------------------------------------

// [U5][S1] when synthesizing the second missing field fails, the first field's
// already-built array is released (no leak) and the error propagates.
func TestRMWrapSynthesisPartialFailureReleases(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	original := memory.DefaultAllocator
	memory.DefaultAllocator = alloc
	defer func() { memory.DefaultAllocator = original }()

	func() {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "ok_nullable", DataType: schemapb.DataType_Int64, Nullable: true},
			{FieldID: 104, Name: "bad_required", DataType: schemapb.DataType_Int64}, // non-nullable, no default
		}}
		materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
		require.NoError(t, err)
		defer materializer.Close()

		builder := array.NewInt64Builder(alloc)
		defer builder.Release()
		builder.AppendValues([]int64{1, 2, 3}, nil)
		pk := builder.NewInt64Array()
		defer pk.Release()

		record := &materializerTestRecord{len: 3, columns: map[storage.FieldID]arrow.Array{100: pk}}
		wrapped, err := materializer.Wrap(record)
		require.Error(t, err)
		require.Nil(t, wrapped)
	}()
	alloc.AssertSize(t, 0)
}

// [U6][S1] materializedRecordReader propagates Wrap failures, cleans the
// previous record, and leaves the base record reader-owned.
func TestRMMaterializedReaderWrapFailure(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
		{FieldID: 104, Name: "bad_required", DataType: schemapb.DataType_Int64},
	}}
	materializer, err := NewRecordMaterializer(schema, nil, map[int64]struct{}{100: {}})
	require.NoError(t, err)

	ints := newInt64Array(t, []int64{1, 2})
	defer ints.Release()
	base := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: ints}}
	reader := newMaterializedRecordReader(&stubRecordReader{records: []storage.Record{base}}, materializer)

	_, err = reader.Next()
	require.Error(t, err)
	require.Zero(t, base.releaseCount, "base record stays owned by its reader")
	require.NoError(t, reader.Close())
}

// stubRecordReader hands out a fixed record sequence.
type stubRecordReader struct {
	records []storage.Record
	pos     int
}

func (r *stubRecordReader) Next() (storage.Record, error) {
	if r.pos >= len(r.records) {
		return nil, errStubEOF
	}
	rec := r.records[r.pos]
	r.pos++
	return rec, nil
}
func (r *stubRecordReader) Close() error { return nil }

var errStubEOF = errors.New("stub EOF")

// ---------------------------------------------------------------------------
// U7: small guards
// ---------------------------------------------------------------------------

// [U7][S3] tiny guards: nil selection length, nil materializer close,
// mismatched selection column build.
func TestRMSmallGuards(t *testing.T) {
	var nilSelection *recordSelection
	require.Zero(t, nilSelection.Len())

	var nilMaterializer *RecordMaterializer
	nilMaterializer.Close() // must not panic

	// buildSelectedColumn: schema/builder mismatch surfaces as an error, not a
	// silent wrong column (field absent from the built record).
	ints := newInt64Array(t, []int64{1, 2})
	defer ints.Release()
	rec := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{100: ints}}
	_, err := buildSelectedColumn(rec, &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar},
		&recordSelection{ranges: []rowRange{{start: 0, end: 1}}, length: 1})
	require.Error(t, err)
}

// ============================================================================
// Consumer goldens: sort / merge-sort / mix / clustering / invariance (C series)
// ============================================================================
// Consumer-level golden tests: RecordMaterializer runs inside every compaction
// task, so each consumer's usage pattern (sort's retained reader records,
// merge-sort's K concurrent readers, mix's Wrap + commit-ts overwrite chain)
// must be verified against per-row data, not just metadata counts. These tests
// drive the real Compact() of each consumer over V1/V2 binlog segments whose
// materialized columns (added default + BM25 output) are absent from storage.

const (
	rmcPKField     = int64(100)
	rmcTextField   = int64(101)
	rmcSparseField = int64(102)
	rmcAddedField  = int64(103)
	rmcAddedValue  = int64(777)
)

func rmcSourceSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: rmcPKField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: rmcTextField, Name: "text", DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "256"}},
		},
	}}
}

func rmcTargetSchema() *schemapb.CollectionSchema {
	schema := rmcSourceSchema()
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{
			FieldID: rmcAddedField, Name: "added_default", DataType: schemapb.DataType_Int64, Nullable: true,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: rmcAddedValue}},
		},
		&schemapb.FieldSchema{
			FieldID: rmcSparseField, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true,
		},
	)
	schema.Functions = []*schemapb.FunctionSchema{{
		Name: "bm25", Id: 100, Type: schemapb.FunctionType_BM25,
		InputFieldNames: []string{"text"}, InputFieldIds: []int64{rmcTextField},
		OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{rmcSparseField},
	}}
	return schema
}

func rmcText(segID int64, i int) string { return fmt.Sprintf("seg%d word%d common", segID, i) }

// rmcWriteSourceSegment writes rows through the real V1 segment writer using
// the SOURCE schema (added + sparse fields physically absent) and returns the
// serialized blobs. Rows are written in ascending pk order so segments qualify
// as sorted inputs for the merge-sort path.
func rmcWriteSourceSegment(t *testing.T, segID int64, rows int, logIDStart int64) (map[string][]byte, []*datapb.FieldBinlog) {
	writer, err := NewSegmentWriter(rmcSourceSchema(), int64(rows), compactionBatchSize, segID, PartitionID, CollectionID, nil)
	require.NoError(t, err)
	for i := 0; i < rows; i++ {
		pk := segID*100000 + int64(i)
		ts := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(0))
		err := writer.Write(&storage.Value{
			PK:        storage.NewInt64PrimaryKey(pk),
			Timestamp: int64(ts),
			Value: map[int64]interface{}{
				common.RowIDField:     pk,
				common.TimeStampField: int64(ts),
				rmcPKField:            pk,
				rmcTextField:          rmcText(segID, i),
			},
		})
		require.NoError(t, err)
	}
	writer.FlushAndIsFull()
	alloc := allocator.NewLocalAllocator(logIDStart, math.MaxInt64)
	kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, writer)
	require.NoError(t, err)
	return kvs, lo.Values(fBinlogs)
}

// rmcBinlogIO wires Download over the source blobs; compaction outputs are
// written by the V2 storage writer straight to the local root path, so reading
// them back goes through the filesystem (rmcReadOutputValues), not Upload.
func rmcBinlogIO(t *testing.T, sources map[string][]byte) *mock_util.MockBinlogIO {
	binlogIO := mock_util.NewMockBinlogIO(t)
	binlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	binlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, paths []string) ([][]byte, error) {
			out := make([][]byte, 0, len(paths))
			for _, p := range paths {
				data, ok := sources[p]
				require.True(t, ok, "unknown binlog path %s", p)
				out = append(out, data)
			}
			return out, nil
		}).Maybe()
	return binlogIO
}

// rmcReadOutputColumns reads every result segment back through the package's
// own segment reader (same storage stack that wrote it) and accumulates the
// requested columns row-by-row in output order.
func rmcReadOutputColumns(t *testing.T, cfg *indexpb.StorageConfig, schema *schemapb.CollectionSchema, segments []*datapb.CompactionSegment) (map[int64][]any, int) {
	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
	}}
	for _, f := range schema.GetFields() {
		if !common.IsSystemField(f.GetFieldID()) {
			readSchema.Fields = append(readSchema.Fields, f)
		}
	}
	out := make(map[int64][]any)
	total := 0
	for _, seg := range segments {
		segBinlogs := &datapb.CompactionSegmentBinlogs{
			SegmentID:      seg.GetSegmentID(),
			CollectionID:   CollectionID,
			PartitionID:    PartitionID,
			FieldBinlogs:   seg.GetInsertLogs(),
			StorageVersion: storage.StorageV2,
		}
		reader, _, err := newCompactionSegmentRecordReader(context.Background(), segBinlogs, readSchema, cfg,
			storage.WithCollectionID(CollectionID),
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(cfg),
		)
		require.NoError(t, err)
		for {
			rec, err := reader.Next()
			if err == sio.EOF {
				break
			}
			require.NoError(t, err)
			total += rec.Len()
			for _, field := range readSchema.GetFields() {
				col := rec.Column(field.GetFieldID())
				require.NotNil(t, col, "output field %d missing", field.GetFieldID())
				out[field.GetFieldID()] = appendArrowValues(t, out[field.GetFieldID()], col)
			}
		}
		require.NoError(t, reader.Close())
	}
	return out, total
}

// rmcAssertGolden checks every output row against the deterministic source
// formulas: original columns intact, added column = declared default, sparse
// output present and non-empty. Returns sparse bytes keyed by pk.
func rmcAssertGolden(t *testing.T, cols map[int64][]any, total, wantRows int) map[int64][]byte {
	require.Equal(t, wantRows, total)
	sparseByPK := make(map[int64][]byte, total)
	for i := 0; i < total; i++ {
		pk := cols[rmcPKField][i].(int64)
		segID, row := pk/100000, int(pk%100000)
		require.Equal(t, rmcText(segID, row), cols[rmcTextField][i], "text intact for pk %d", pk)
		require.Equal(t, rmcAddedValue, cols[rmcAddedField][i], "default materialized for pk %d", pk)
		sparse, ok := cols[rmcSparseField][i].([]byte)
		require.True(t, ok, "sparse output missing for pk %d", pk)
		require.NotEmpty(t, sparse, "sparse output empty for pk %d", pk)
		sparseByPK[pk] = sparse
	}
	// W1 oracle: recompute the expected sparse bytes from the deterministic
	// text formula through a real BM25 runner and byte-compare per row —
	// non-emptiness cannot see rotation/misalignment corruption.
	pks := make([]int64, 0, total)
	texts := make([]string, 0, total)
	for i := 0; i < total; i++ {
		pk := cols[rmcPKField][i].(int64)
		pks = append(pks, pk)
		texts = append(texts, rmcText(pk/100000, int(pk%100000)))
	}
	schema := rmcTargetSchema()
	expected := expectedBM25SparseRows(t, schema, schema.GetFunctions()[0], texts)
	for i, pk := range pks {
		require.Equal(t, expected[i], sparseByPK[pk], "sparse content<->row alignment for pk %d", pk)
	}
	return sparseByPK
}

func rmcPKsSorted(cols map[int64][]any, total int) bool {
	for i := 1; i < total; i++ {
		if cols[rmcPKField][i-1].(int64) > cols[rmcPKField][i].(int64) {
			return false
		}
	}
	return true
}

func rmcSortPlan(schema *schemapb.CollectionSchema, segments []*datapb.CompactionSegmentBinlogs, totalRows int64, compactionType datapb.CompactionType) *datapb.CompactionPlan {
	jsonParams, err := compaction.GenerateJSONParams(schema)
	if err != nil {
		panic(err)
	}
	return &datapb.CompactionPlan{
		PlanID:                 999,
		Type:                   compactionType,
		SegmentBinlogs:         segments,
		Schema:                 schema,
		TotalRows:              totalRows,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 50000, End: math.MaxInt64},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             jsonParams,
		Channel:                "rmc_channel",
	}
}

// [C1][S0] sort consumer: materialized columns survive the sort pipeline
// (reader-wrapper + storage.Sort record retention) row-for-row, and the output
// order is by pk.
func TestRMConsumerSortGolden(t *testing.T) {
	setupBumpUTEnv(t)
	const rows = 120
	kvs, fBinlogs := rmcWriteSourceSegment(t, 5, rows, 40000)
	binlogIO := rmcBinlogIO(t, kvs)

	plan := rmcSortPlan(rmcTargetSchema(), []*datapb.CompactionSegmentBinlogs{{
		SegmentID: 5, CollectionID: CollectionID, PartitionID: PartitionID, FieldBinlogs: fBinlogs,
	}}, rows, datapb.CompactionType_SortCompaction)

	params := compaction.GenParams()
	task := NewSortCompactionTask(context.Background(), mocks.NewChunkManager(t), plan, params, []int64{rmcPKField})
	task.binlogIO = binlogIO

	result, err := task.Compact()
	require.NoError(t, err)
	require.Equal(t, datapb.CompactionTaskState_completed, result.GetState())
	require.Len(t, result.GetSegments(), 1)
	require.True(t, result.GetSegments()[0].GetIsSorted())

	cols, total := rmcReadOutputColumns(t, params.StorageConfig, rmcTargetSchema(), result.GetSegments())
	rmcAssertGolden(t, cols, total, rows)
	require.True(t, rmcPKsSorted(cols, total), "sort output must be pk-ordered")
}

// [C2][S0] merge-sort consumer: three sorted segments, each with its own
// materializer instance, merge into one globally ordered golden output with no
// cross-segment value bleed.
func TestRMConsumerMergeSortGolden(t *testing.T) {
	setupBumpUTEnv(t)
	const perSeg = 50
	segIDs := []int64{5, 6, 7}
	sources := make(map[string][]byte)
	segments := make([]*datapb.CompactionSegmentBinlogs, 0, len(segIDs))
	for idx, segID := range segIDs {
		kvs, fBinlogs := rmcWriteSourceSegment(t, segID, perSeg, 40000+int64(idx)*1000)
		for k, v := range kvs {
			sources[k] = v
		}
		segments = append(segments, &datapb.CompactionSegmentBinlogs{
			SegmentID: segID, CollectionID: CollectionID, PartitionID: PartitionID,
			FieldBinlogs: fBinlogs, IsSorted: true,
		})
	}
	binlogIO := rmcBinlogIO(t, sources)

	plan := rmcSortPlan(rmcTargetSchema(), segments, int64(perSeg*len(segIDs)), datapb.CompactionType_MixCompaction)
	params := compaction.GenParams()
	params.UseMergeSort = true
	params.MaxSegmentMergeSort = 8
	task := NewMixCompactionTask(context.Background(), binlogIO, mocks.NewChunkManager(t), plan, params, []int64{rmcPKField})

	result, err := task.Compact()
	require.NoError(t, err)
	require.Equal(t, datapb.CompactionTaskState_completed, result.GetState())

	cols, total := rmcReadOutputColumns(t, params.StorageConfig, rmcTargetSchema(), result.GetSegments())
	rmcAssertGolden(t, cols, total, perSeg*len(segIDs))
	require.True(t, rmcPKsSorted(cols, total), "merge-sort output must be globally pk-ordered")

	// [TS4][S0] merge-sort must carry the row_id system column through: every
	// row keeps a unique row_id (no zeroed/duplicated system column).
	rowIDs := make(map[int64]struct{}, total)
	for i := 0; i < total; i++ {
		id := cols[common.RowIDField][i].(int64)
		_, dup := rowIDs[id]
		require.False(t, dup, "duplicate row_id %d at row %d", id, i)
		rowIDs[id] = struct{}{}
	}
}

// [C3][S0] mix consumer: unsorted merge with a per-segment commit timestamp —
// materialization and the ts-overwrite chain must compose, and rows from the
// commit-ts segment must read back with the normalized timestamp.
func TestRMConsumerMixCommitTsGolden(t *testing.T) {
	setupBumpUTEnv(t)
	const perSeg = 40
	commitTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(24 * 3600 * 1e9))
	kvsA, fBinlogsA := rmcWriteSourceSegment(t, 5, perSeg, 40000)
	kvsB, fBinlogsB := rmcWriteSourceSegment(t, 6, perSeg, 41000)
	sources := make(map[string][]byte)
	for k, v := range kvsA {
		sources[k] = v
	}
	for k, v := range kvsB {
		sources[k] = v
	}
	binlogIO := rmcBinlogIO(t, sources)

	plan := rmcSortPlan(rmcTargetSchema(), []*datapb.CompactionSegmentBinlogs{
		{SegmentID: 5, CollectionID: CollectionID, PartitionID: PartitionID, FieldBinlogs: fBinlogsA},
		{SegmentID: 6, CollectionID: CollectionID, PartitionID: PartitionID, FieldBinlogs: fBinlogsB, CommitTimestamp: commitTs},
	}, int64(perSeg*2), datapb.CompactionType_MixCompaction)

	params := compaction.GenParams()
	task := NewMixCompactionTask(context.Background(), binlogIO, mocks.NewChunkManager(t), plan, params, []int64{rmcPKField})

	result, err := task.Compact()
	require.NoError(t, err)
	require.Equal(t, datapb.CompactionTaskState_completed, result.GetState())

	cols, total := rmcReadOutputColumns(t, params.StorageConfig, rmcTargetSchema(), result.GetSegments())
	rmcAssertGolden(t, cols, total, perSeg*2)
	for i := 0; i < total; i++ {
		pk := cols[rmcPKField][i].(int64)
		ts := cols[common.TimeStampField][i].(int64)
		if pk >= 600000 { // rows of segment 6 carry the normalized commit ts
			require.Equal(t, int64(commitTs), ts, "commit-ts normalization for pk %d", pk)
		} else {
			rawTs := int64(tsoutil.ComposeTSByTime(getMilvusBirthday()))
			require.Equal(t, rawTs, ts, "segment 5 keeps raw ts for pk %d", pk)
		}
	}
}

// [C5][S0] cross-consumer invariance: the same source rows materialized through
// sort and through mix must yield identical added-column values and identical
// BM25 sparse bytes per pk — the materializer's semantics must not depend on
// its consumer.
func TestRMConsumerCrossConsumerInvariance(t *testing.T) {
	const rows = 60

	runSort := func(t *testing.T) map[int64][]byte {
		setupBumpUTEnv(t)
		kvs, fBinlogs := rmcWriteSourceSegment(t, 5, rows, 40000)
		binlogIO := rmcBinlogIO(t, kvs)
		plan := rmcSortPlan(rmcTargetSchema(), []*datapb.CompactionSegmentBinlogs{{
			SegmentID: 5, CollectionID: CollectionID, PartitionID: PartitionID, FieldBinlogs: fBinlogs,
		}}, rows, datapb.CompactionType_SortCompaction)
		params := compaction.GenParams()
		task := NewSortCompactionTask(context.Background(), mocks.NewChunkManager(t), plan, params, []int64{rmcPKField})
		task.binlogIO = binlogIO
		result, err := task.Compact()
		require.NoError(t, err)
		cols, total := rmcReadOutputColumns(t, params.StorageConfig, rmcTargetSchema(), result.GetSegments())
		return rmcAssertGolden(t, cols, total, rows)
	}
	runMix := func(t *testing.T) map[int64][]byte {
		setupBumpUTEnv(t)
		kvs, fBinlogs := rmcWriteSourceSegment(t, 5, rows, 40000)
		binlogIO := rmcBinlogIO(t, kvs)
		plan := rmcSortPlan(rmcTargetSchema(), []*datapb.CompactionSegmentBinlogs{{
			SegmentID: 5, CollectionID: CollectionID, PartitionID: PartitionID, FieldBinlogs: fBinlogs,
		}}, rows, datapb.CompactionType_MixCompaction)
		params := compaction.GenParams()
		task := NewMixCompactionTask(context.Background(), binlogIO, mocks.NewChunkManager(t), plan, params, []int64{rmcPKField})
		result, err := task.Compact()
		require.NoError(t, err)
		cols, total := rmcReadOutputColumns(t, params.StorageConfig, rmcTargetSchema(), result.GetSegments())
		return rmcAssertGolden(t, cols, total, rows)
	}

	sortSparse := runSort(t)
	mixSparse := runMix(t)
	require.Equal(t, len(sortSparse), len(mixSparse))
	for pk, sparse := range sortSparse {
		require.Equal(t, sparse, mixSparse[pk], "sparse bytes must be consumer-independent for pk %d", pk)
	}
}

// [C4][S0] clustering consumer: buckets re-partition rows after
// materialization; every output bucket's rows keep golden sparse outputs and
// the input pk set is preserved exactly.
func TestRMConsumerClusteringGolden(t *testing.T) {
	s := &ClusteringCompactionTaskSuite{}
	s.SetT(t)
	s.SetupSuite()
	s.SetupTest()
	t.Cleanup(s.TearDownTest)

	const rows = 30
	s.prepareCompactionWithMissingBM25OutputTask(rows)

	result, err := s.task.Compact()
	require.NoError(t, err)
	require.NotNil(t, result)

	cols, total := rmcReadOutputColumns(t, s.task.compactionParams.StorageConfig, s.task.plan.GetSchema(), result.GetSegments())
	require.Equal(t, rows, total)
	seenPKs := make(map[int64]struct{}, rows)
	texts := make([]string, total)
	for i := 0; i < total; i++ {
		pk := cols[100][i].(int64)
		seenPKs[pk] = struct{}{}
		texts[i] = cols[101][i].(string)
	}
	require.Len(t, seenPKs, rows, "clustering must preserve the exact pk set")
	// W1 oracle: each sparse row must equal the BM25 output of ITS OWN row's
	// text — alignment is asserted independently of the bucket order.
	schema := s.task.plan.GetSchema()
	expected := expectedBM25SparseRows(t, schema, schema.GetFunctions()[0], texts)
	for i := 0; i < total; i++ {
		pk := cols[100][i].(int64)
		sparse, ok := cols[102][i].([]byte)
		require.True(t, ok, "sparse missing for pk %d", pk)
		require.Equal(t, expected[i], sparse, "sparse content<->row alignment for pk %d", pk)
	}
}
