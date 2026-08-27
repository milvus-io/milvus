// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	benchmarkPKField        = int64(100)
	benchmarkNamespaceField = int64(101)
	benchmarkFloatField     = int64(103)
	benchmarkVectorBase     = int64(200)
	benchmarkBM25TextField  = int64(400)
	benchmarkBM25Field      = int64(401)
	benchmarkNullableField  = int64(402)
	benchmarkTTLField       = int64(403)
)

type mixCompactorBenchmarkLayout int

const (
	benchmarkInterleaved mixCompactorBenchmarkLayout = iota
	benchmarkPartialOverlap
	benchmarkDisjoint
)

type mixCompactorBenchmarkKey int

const (
	benchmarkInt64Key mixCompactorBenchmarkKey = iota
	benchmarkVarcharKey
	benchmarkNamespaceKey
)

type mixCompactorBenchmarkCase struct {
	name          string
	readers       int
	rowsPerReader int
	layout        mixCompactorBenchmarkLayout
	key           mixCompactorBenchmarkKey
	scalars       int
	vectors       int
	dim           int
	filterPercent int
	missingField  bool
	commitTS      bool
	bm25          bool
	nullableValue bool
	ttlField      bool
}

// The retained suite is representative rather than Cartesian. Together these
// cases cover reader counts 1/2/8/30, disjoint/partial/interleaved ranges,
// int64/varchar/namespace keys, PK-only/wide/vector-heavy schemas, and
// 0/10/90-percent filtering. Each merge case is also driven through the real
// V2 and V3 MultiSegmentWriter path by BenchmarkMixCompactorRealWriter.
var mixCompactorBenchmarkCases = []mixCompactorBenchmarkCase{
	{name: "pk_only_one_reader", readers: 1, rowsPerReader: 8192, layout: benchmarkDisjoint, key: benchmarkInt64Key},
	{name: "wide_interleaved_two", readers: 2, rowsPerReader: 8192, layout: benchmarkInterleaved, key: benchmarkInt64Key, scalars: 16, vectors: 2, dim: 32},
	{name: "varchar_partial_eight_filter10", readers: 8, rowsPerReader: 2048, layout: benchmarkPartialOverlap, key: benchmarkVarcharKey, scalars: 4, vectors: 1, dim: 32, filterPercent: 10},
	{name: "namespace_disjoint_thirty", readers: 30, rowsPerReader: 512, layout: benchmarkDisjoint, key: benchmarkNamespaceKey, scalars: 2},
	{name: "vector_heavy_interleaved_eight", readers: 8, rowsPerReader: 1024, layout: benchmarkInterleaved, key: benchmarkInt64Key, scalars: 2, vectors: 6, dim: 64},
	{name: "missing_field_filter90", readers: 2, rowsPerReader: 8192, layout: benchmarkPartialOverlap, key: benchmarkInt64Key, scalars: 6, vectors: 1, dim: 32, filterPercent: 90, missingField: true},
	{name: "commit_timestamp_two", readers: 2, rowsPerReader: 4096, layout: benchmarkDisjoint, key: benchmarkInt64Key, scalars: 2, commitTS: true},
	{name: "bm25_partial_two", readers: 2, rowsPerReader: 2048, layout: benchmarkPartialOverlap, key: benchmarkInt64Key, bm25: true},
}

var mixCompactorProductionBenchmarkCases = append(append([]mixCompactorBenchmarkCase{}, mixCompactorBenchmarkCases...),
	mixCompactorBenchmarkCase{
		name: "ttl_field_mixed_null_two", readers: 2, rowsPerReader: 4096,
		layout: benchmarkPartialOverlap, key: benchmarkInt64Key, nullableValue: true, ttlField: true,
	},
)

var benchmarkCurrentTime = time.Unix(1_800_000_000, 0).UTC()

type benchmarkRecordReader struct {
	record storage.Record
	done   bool
	active bool
}

type benchmarkNonForwardRecord struct {
	storage.Record
}

func (r *benchmarkRecordReader) Next() (storage.Record, error) {
	if r.done {
		return nil, io.EOF
	}
	r.done = true
	r.record.Retain()
	r.active = true
	return r.record, nil
}

func (r *benchmarkRecordReader) Close() error {
	if r.active {
		r.record.Release()
		r.active = false
	}
	return nil
}

type benchmarkMaterializedRecord struct {
	base     storage.Record
	fieldID  storage.FieldID
	computed arrow.Array
}

func (r *benchmarkMaterializedRecord) Column(fieldID storage.FieldID) arrow.Array {
	if fieldID == r.fieldID {
		return r.computed
	}
	return r.base.Column(fieldID)
}

func (r *benchmarkMaterializedRecord) Len() int { return r.base.Len() }

func (r *benchmarkMaterializedRecord) Retain() {
	r.base.Retain()
	r.computed.Retain()
}

func (r *benchmarkMaterializedRecord) Release() {
	r.base.Release()
	r.computed.Release()
}

type benchmarkCountingWriter struct {
	rows int
}

func (w *benchmarkCountingWriter) Write(r storage.Record) error {
	w.rows += r.Len()
	return nil
}

func (w *benchmarkCountingWriter) GetWrittenUncompressed() uint64 { return 0 }

func (w *benchmarkCountingWriter) Close() error { return nil }

type benchmarkLocalBinlogIO struct {
	storage.ChunkManager
	pool *conc.Pool[any]
}

func newBenchmarkLocalBinlogIO(root string) *benchmarkLocalBinlogIO {
	return &benchmarkLocalBinlogIO{
		ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(root)),
		pool:         conc.NewPool[any](4),
	}
}

func (b *benchmarkLocalBinlogIO) Close() {
	b.pool.Release()
}

func (b *benchmarkLocalBinlogIO) Download(ctx context.Context, paths []string) ([][]byte, error) {
	values := make([][]byte, len(paths))
	for i, path := range paths {
		value, err := b.Read(ctx, path)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func (b *benchmarkLocalBinlogIO) AsyncDownload(ctx context.Context, paths []string) []*conc.Future[any] {
	values := make([]*conc.Future[any], 0, len(paths))
	for _, p := range paths {
		path := p
		values = append(values, b.pool.Submit(func() (any, error) { return b.Read(ctx, path) }))
	}
	return values
}

func (b *benchmarkLocalBinlogIO) Upload(ctx context.Context, kvs map[string][]byte) error {
	for path, value := range kvs {
		if err := b.Write(ctx, path, value); err != nil {
			return err
		}
	}
	return nil
}

func (b *benchmarkLocalBinlogIO) AsyncUpload(ctx context.Context, kvs map[string][]byte) []*conc.Future[any] {
	values := make([]*conc.Future[any], 0, len(kvs))
	for p, v := range kvs {
		path, value := p, v
		values = append(values, b.pool.Submit(func() (any, error) {
			return struct{}{}, b.Write(ctx, path, value)
		}))
	}
	return values
}

type benchmarkSelectionMaterializer struct {
	base              storage.RecordReader
	selectionSchema   *schemapb.CollectionSchema
	filterPercent     int
	missingFieldID    int64
	materializeBefore bool
	current           storage.Record
}

func (r *benchmarkSelectionMaterializer) Next() (storage.Record, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	base, err := r.base.Next()
	if err != nil {
		return nil, err
	}
	if r.filterPercent == 0 && r.missingFieldID == 0 {
		return base, nil
	}
	if r.materializeBefore && r.missingFieldID != 0 {
		base = materializeBenchmarkMissingField(base, r.missingFieldID, false)
	}
	if r.filterPercent > 0 {
		builder := storage.NewRecordBuilder(r.selectionSchema)
		for i := 0; i < base.Len(); i++ {
			if benchmarkRowKept(i, r.filterPercent) {
				if err := builder.Append(base, i, i+1); err != nil {
					builder.Release()
					if _, ok := base.(*benchmarkMaterializedRecord); ok {
						base.Release()
					}
					return nil, err
				}
			}
		}
		selected := builder.Build()
		builder.Release()
		if _, ok := base.(*benchmarkMaterializedRecord); ok {
			base.Release()
		}
		base = selected
	}
	if !r.materializeBefore && r.missingFieldID != 0 {
		base = materializeBenchmarkMissingField(base, r.missingFieldID, true)
	}
	r.current = base
	return base, nil
}

func (r *benchmarkSelectionMaterializer) Close() error {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	return r.base.Close()
}

func benchmarkRowKept(row, filterPercent int) bool {
	return row%100 >= filterPercent
}

func materializeBenchmarkMissingField(base storage.Record, fieldID int64, ownBase bool) storage.Record {
	if !ownBase {
		// The source reader owns its current reference. Keep one extra reference
		// for the wrapper so the borrowed source remains valid until cleanup.
		base.Retain()
	}
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Reserve(base.Len())
	for i := 0; i < base.Len(); i++ {
		builder.Append(fmt.Sprintf("computed-%08d", i))
	}
	computed := builder.NewArray()
	builder.Release()
	return &benchmarkMaterializedRecord{base: base, fieldID: fieldID, computed: computed}
}

func benchmarkMergeKeys(tc mixCompactorBenchmarkCase) []int64 {
	switch tc.key {
	case benchmarkNamespaceKey:
		return []int64{benchmarkNamespaceField, benchmarkPKField}
	case benchmarkVarcharKey:
		return []int64{benchmarkPKField}
	default:
		return []int64{benchmarkPKField}
	}
}

func benchmarkKeyValue(tc mixCompactorBenchmarkCase, reader, row int) int64 {
	switch tc.layout {
	case benchmarkDisjoint:
		return int64(reader*tc.rowsPerReader + row)
	case benchmarkPartialOverlap:
		return int64(reader*(tc.rowsPerReader/2) + row)
	default:
		return int64(row*tc.readers + reader)
	}
}

func benchmarkSchema(tc mixCompactorBenchmarkCase) *schemapb.CollectionSchema {
	pkField := &schemapb.FieldSchema{FieldID: benchmarkPKField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	if tc.key == benchmarkVarcharKey {
		pkField.DataType = schemapb.DataType_VarChar
		pkField.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}
	}
	fields := []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
		pkField,
	}
	if tc.key == benchmarkNamespaceKey {
		fields = append(fields, &schemapb.FieldSchema{FieldID: benchmarkNamespaceField, Name: "namespace", DataType: schemapb.DataType_Int64, IsPartitionKey: true})
	}
	for i := 0; i < tc.scalars; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: benchmarkFloatField + int64(i), Name: fmt.Sprintf("scalar_%02d", i), DataType: schemapb.DataType_Double})
	}
	for i := 0; i < tc.vectors; i++ {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: benchmarkVectorBase + int64(i), Name: fmt.Sprintf("vector_%02d", i), DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(tc.dim)}},
		})
	}
	if tc.missingField {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: benchmarkVectorBase + 100, Name: "materialized", DataType: schemapb.DataType_VarChar,
			Nullable:   true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
		})
	}
	var functions []*schemapb.FunctionSchema
	if tc.bm25 {
		fields = append(fields,
			&schemapb.FieldSchema{
				FieldID: benchmarkBM25TextField, Name: "bm25_text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			&schemapb.FieldSchema{FieldID: benchmarkBM25Field, Name: "bm25_sparse", DataType: schemapb.DataType_SparseFloatVector},
		)
		functions = []*schemapb.FunctionSchema{{
			Name: "bm25", Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"bm25_text"}, InputFieldIds: []int64{benchmarkBM25TextField},
			OutputFieldNames: []string{"bm25_sparse"}, OutputFieldIds: []int64{benchmarkBM25Field},
		}}
	}
	if tc.nullableValue {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: benchmarkNullableField, Name: "nullable_payload", DataType: schemapb.DataType_VarChar,
			Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
		})
	}
	properties := []*commonpb.KeyValuePair(nil)
	if tc.ttlField {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: benchmarkTTLField, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true,
		})
		properties = []*commonpb.KeyValuePair{{Key: common.CollectionTTLFieldKey, Value: "expire_at"}}
	}
	return &schemapb.CollectionSchema{
		Name: tc.name, EnableNamespace: tc.key == benchmarkNamespaceKey, Fields: fields, Functions: functions, Properties: properties,
	}
}

func benchmarkRecords(b testing.TB, tc mixCompactorBenchmarkCase, schema *schemapb.CollectionSchema) []storage.Record {
	b.Helper()
	inputSchema := benchmarkInputSchema(tc, schema)
	arrowSchema, err := storage.ConvertToArrowSchema(inputSchema, false)
	if err != nil {
		b.Fatal(err)
	}
	inputFields := inputSchema.GetFields()
	records := make([]storage.Record, tc.readers)
	for reader := 0; reader < tc.readers; reader++ {
		builders := make([]array.Builder, len(inputFields))
		for i, field := range inputFields {
			switch field.GetDataType() {
			case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
				builders[i] = array.NewInt64Builder(memory.DefaultAllocator)
			case schemapb.DataType_VarChar:
				builders[i] = array.NewStringBuilder(memory.DefaultAllocator)
			case schemapb.DataType_Double:
				builders[i] = array.NewFloat64Builder(memory.DefaultAllocator)
			case schemapb.DataType_FloatVector:
				builders[i] = array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: tc.dim * 4})
			case schemapb.DataType_SparseFloatVector:
				builders[i] = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			default:
				b.Fatalf("unsupported benchmark field type %s", field.GetDataType())
			}
			builders[i].Reserve(tc.rowsPerReader)
		}
		vectorValue := make([]byte, tc.dim*4)
		for row := 0; row < tc.rowsPerReader; row++ {
			key := benchmarkKeyValue(tc, reader, row)
			rowTimestamp := uint64(1)
			if tc.filterPercent > 0 {
				rowTime := benchmarkCurrentTime.Add(-30 * time.Minute)
				if !benchmarkRowKept(row, tc.filterPercent) {
					rowTime = benchmarkCurrentTime.Add(-2 * time.Hour)
				}
				rowTimestamp = tsoutil.ComposeTSByTime(rowTime)
			}
			for i, field := range inputFields {
				switch field.GetFieldID() {
				case common.RowIDField:
					builders[i].(*array.Int64Builder).Append(key)
				case common.TimeStampField:
					builders[i].(*array.Int64Builder).Append(int64(rowTimestamp))
				case benchmarkPKField:
					if tc.key == benchmarkVarcharKey {
						builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%032d", key))
					} else {
						builders[i].(*array.Int64Builder).Append(key)
					}
				case benchmarkNamespaceField:
					builders[i].(*array.Int64Builder).Append(int64(reader / 2))
				case benchmarkBM25TextField:
					builders[i].(*array.StringBuilder).Append(fmt.Sprintf("doc-%08d", key))
				case benchmarkBM25Field:
					builders[i].(*array.BinaryBuilder).Append(typeutil.CreateSparseFloatRow(
						[]uint32{uint32(key%1024 + 1)}, []float32{1},
					))
				case benchmarkNullableField:
					builder := builders[i].(*array.StringBuilder)
					if row%5 == 0 {
						builder.AppendNull()
					} else {
						builder.Append(fmt.Sprintf("payload-%016d", key))
					}
				case benchmarkTTLField:
					builder := builders[i].(*array.Int64Builder)
					switch row % 5 {
					case 0:
						builder.AppendNull()
					case 1:
						builder.Append(benchmarkCurrentTime.Add(-time.Minute).UnixMicro())
					case 2:
						builder.Append(-1)
					default:
						builder.Append(benchmarkCurrentTime.Add(time.Hour + time.Duration(key)*time.Microsecond).UnixMicro())
					}
				default:
					switch builder := builders[i].(type) {
					case *array.Float64Builder:
						builder.Append(float64(key) + float64(field.GetFieldID())/1000)
					case *array.FixedSizeBinaryBuilder:
						vectorValue[0] = byte(key)
						vectorValue[len(vectorValue)-1] = byte(key >> 8)
						builder.Append(vectorValue)
					}
				}
			}
		}
		arrays := make([]arrow.Array, len(builders))
		field2Col := make(map[storage.FieldID]int, len(builders))
		for i, builder := range builders {
			arrays[i] = builder.NewArray()
			builder.Release()
			field2Col[inputFields[i].GetFieldID()] = i
		}
		arrowRecord := array.NewRecord(arrowSchema, arrays, int64(tc.rowsPerReader))
		for _, values := range arrays {
			values.Release()
		}
		records[reader] = storage.NewSimpleArrowRecord(arrowRecord, field2Col)
	}
	return records
}

func benchmarkReaders(tc mixCompactorBenchmarkCase, schema *schemapb.CollectionSchema, records []storage.Record, materializeBefore bool) []storage.RecordReader {
	readers := make([]storage.RecordReader, len(records))
	selectionSchema := schema
	missingFieldID := int64(0)
	if tc.missingField {
		missingFieldID = benchmarkVectorBase + 100
		if !materializeBefore {
			selectionSchema = &schemapb.CollectionSchema{Name: schema.GetName(), EnableNamespace: schema.GetEnableNamespace(), Fields: schema.GetFields()[:len(schema.GetFields())-1]}
		}
	}
	for i, record := range records {
		base := storage.RecordReader(&benchmarkRecordReader{record: record})
		if tc.filterPercent > 0 || missingFieldID != 0 {
			base = &benchmarkSelectionMaterializer{
				base: base, selectionSchema: selectionSchema, filterPercent: tc.filterPercent,
				missingFieldID: missingFieldID, materializeBefore: materializeBefore,
			}
		}
		readers[i] = base
	}
	return readers
}

func benchmarkSourceBytes(records []storage.Record, schema *schemapb.CollectionSchema) int64 {
	var total uint64
	for _, record := range records {
		for _, field := range schema.GetFields() {
			col := record.Column(field.GetFieldID())
			if col != nil {
				total += storage.ActualSizeInBytes(col.Data())
			}
		}
	}
	return int64(total)
}

func closeBenchmarkReaders(readers []storage.RecordReader) {
	for _, reader := range readers {
		_ = reader.Close()
	}
}

func benchmarkPhaseRows(tc mixCompactorBenchmarkCase) int {
	keptPerReader := 0
	for row := 0; row < tc.rowsPerReader; row++ {
		if benchmarkRowKept(row, tc.filterPercent) {
			keptPerReader++
		}
	}
	return tc.readers * keptPerReader
}

type benchmarkProductionFixture struct {
	tc            mixCompactorBenchmarkCase
	version       int64
	root          string
	binlogIO      *benchmarkLocalBinlogIO
	schema        *schemapb.CollectionSchema
	params        compaction.Params
	plan          *datapb.CompactionPlan
	inputs        []*datapb.CompactionSegmentBinlogs
	expectedRows  int
	sourceBytes   int64
	collectionTTL int64
	commitTS      uint64
}

type benchmarkExpectedRow struct {
	reader int
	row    int
	key    int64
}

func benchmarkExpectedRows(tc mixCompactorBenchmarkCase) []benchmarkExpectedRow {
	rows := make([]benchmarkExpectedRow, 0, benchmarkPhaseRows(tc))
	for reader := 0; reader < tc.readers; reader++ {
		for row := 0; row < tc.rowsPerReader; row++ {
			if !benchmarkProductionRowKept(tc, row) {
				continue
			}
			rows = append(rows, benchmarkExpectedRow{
				reader: reader, row: row, key: benchmarkKeyValue(tc, reader, row),
			})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if tc.key == benchmarkNamespaceKey {
			left, right := rows[i].reader/2, rows[j].reader/2
			if left != right {
				return left < right
			}
		}
		if rows[i].key != rows[j].key {
			return rows[i].key < rows[j].key
		}
		return rows[i].reader < rows[j].reader
	})
	return rows
}

func benchmarkProductionRowKept(tc mixCompactorBenchmarkCase, row int) bool {
	if !benchmarkRowKept(row, tc.filterPercent) {
		return false
	}
	return !tc.ttlField || row%5 != 1
}

func benchmarkTTLValue(tc mixCompactorBenchmarkCase, row int, key int64) (int64, bool) {
	if !tc.ttlField || row%5 == 0 {
		return 0, false
	}
	switch row % 5 {
	case 1:
		return benchmarkCurrentTime.Add(-time.Minute).UnixMicro(), true
	case 2:
		return -1, true
	default:
		return benchmarkCurrentTime.Add(time.Hour + time.Duration(key)*time.Microsecond).UnixMicro(), true
	}
}

func benchmarkPrimaryKey(tc mixCompactorBenchmarkCase, key int64) storage.PrimaryKey {
	if tc.key == benchmarkVarcharKey {
		return storage.NewVarCharPrimaryKey(fmt.Sprintf("%032d", key))
	}
	return storage.NewInt64PrimaryKey(key)
}

func benchmarkPrimaryKeyBytes(pk storage.PrimaryKey) []byte {
	switch value := pk.GetValue().(type) {
	case string:
		return []byte(value)
	case int64:
		encoded := make([]byte, 8)
		common.Endian.PutUint64(encoded, uint64(value))
		return encoded
	default:
		panic(fmt.Sprintf("unsupported benchmark primary key type %T", value))
	}
}

func benchmarkExpectedTimestamp(tc mixCompactorBenchmarkCase, row int, commitTS uint64) int64 {
	if commitTS != 0 {
		return int64(commitTS)
	}
	if tc.filterPercent == 0 {
		return 1
	}
	return int64(tsoutil.ComposeTSByTime(benchmarkCurrentTime.Add(-30 * time.Minute)))
}

func benchmarkInputSchema(tc mixCompactorBenchmarkCase, schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	if !tc.missingField {
		return schema
	}
	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields())-1)
	for _, field := range schema.GetFields() {
		if field.GetFieldID() != benchmarkVectorBase+100 {
			fields = append(fields, field)
		}
	}
	return &schemapb.CollectionSchema{
		Name:            schema.GetName(),
		EnableNamespace: schema.GetEnableNamespace(),
		Fields:          fields,
		Functions:       schema.GetFunctions(),
		Properties:      schema.GetProperties(),
	}
}

func newBenchmarkProductionFixture(b testing.TB, tc mixCompactorBenchmarkCase, version int64) *benchmarkProductionFixture {
	b.Helper()
	const (
		collectionID = int64(1)
		partitionID  = int64(1)
	)
	root := b.TempDir()
	binlogIO := newBenchmarkLocalBinlogIO(root)
	b.Cleanup(binlogIO.Close)
	schema := benchmarkSchema(tc)
	inputSchema := benchmarkInputSchema(tc, schema)
	records := benchmarkRecords(b, tc, schema)
	b.Cleanup(func() {
		for _, record := range records {
			record.Release()
		}
	})
	params := compaction.Params{
		StorageVersion: version,
		BinLogMaxSize:  64 * 1024 * 1024,
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local", RootPath: root},
	}
	fixture := &benchmarkProductionFixture{
		tc:           tc,
		version:      version,
		root:         root,
		binlogIO:     binlogIO,
		schema:       schema,
		params:       params,
		expectedRows: len(benchmarkExpectedRows(tc)),
		sourceBytes:  benchmarkSourceBytes(records, inputSchema),
	}
	if tc.filterPercent > 0 {
		fixture.collectionTTL = int64(time.Hour)
	}
	if tc.commitTS {
		fixture.commitTS = tsoutil.ComposeTSByTime(benchmarkCurrentTime.Add(-5 * time.Minute))
	}

	fixture.inputs = make([]*datapb.CompactionSegmentBinlogs, 0, len(records))
	for i, record := range records {
		segmentID := int64(10_000 + i)
		logStart := int64(100_000_000 + i*1_000_000)
		writer, err := NewMultiSegmentWriter(context.Background(), binlogIO,
			NewCompactionAllocator(
				allocator.NewLocalAllocator(segmentID, segmentID+1),
				allocator.NewLocalAllocator(logStart, logStart+1_000_000),
			),
			256*1024*1024, inputSchema, params, int64(record.Len()), partitionID, collectionID,
			"benchmark-source", 4096,
			storage.WithStorageConfig(params.StorageConfig), storage.WithVersion(version))
		if err == nil {
			err = writer.Write(record)
		}
		if err == nil {
			err = writer.Close()
		}
		if err != nil {
			b.Fatalf("write source segment %d: %v", segmentID, err)
		}
		segments := writer.GetCompactionSegments()
		if len(segments) != 1 {
			b.Fatalf("source writer produced %d segments, expected 1", len(segments))
		}
		segment := segments[0]
		fixture.inputs = append(fixture.inputs, &datapb.CompactionSegmentBinlogs{
			CollectionID:        collectionID,
			PartitionID:         partitionID,
			SegmentID:           segment.GetSegmentID(),
			FieldBinlogs:        segment.GetInsertLogs(),
			Field2StatslogPaths: segment.GetField2StatslogPaths(),
			Deltalogs:           []*datapb.FieldBinlog{},
			IsSorted:            tc.key != benchmarkNamespaceKey,
			IsSortedByNamespace: tc.key == benchmarkNamespaceKey,
			StorageVersion:      version,
			Manifest:            segment.GetManifest(),
			ExpirQuantiles:      segment.GetExpirQuantiles(),
			CommitTimestamp:     fixture.commitTS,
		})
	}
	fixture.plan = &datapb.CompactionPlan{
		PlanID:         1,
		Type:           datapb.CompactionType_MixCompaction,
		Schema:         schema,
		SegmentBinlogs: fixture.inputs,
		MaxSize:        256 * 1024 * 1024,
		Channel:        "benchmark-output",
	}
	return fixture
}

func (f *benchmarkProductionFixture) run(iteration int64) ([]*datapb.CompactionSegment, error) {
	plan := proto.Clone(f.plan).(*datapb.CompactionPlan)
	segmentStart := int64(1_000_000_000) + iteration*1_000_000
	logStart := int64(2_000_000_000) + iteration*10_000_000
	plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: segmentStart, End: segmentStart + 1_000_000}
	plan.PreAllocatedLogIDs = &datapb.IDRange{Begin: logStart, End: logStart + 10_000_000}
	return mergeSortMultipleSegments(
		context.Background(), plan, 1, 1, int64(f.expectedRows), f.binlogIO, f.inputs,
		timerecord.NewTimeRecorder("mix-compactor-benchmark"), benchmarkCurrentTime, f.collectionTTL,
		f.params,
		[]storage.RwOption{storage.WithStorageConfig(f.params.StorageConfig), storage.WithVersion(f.version)},
		nil, benchmarkMergeKeys(f.tc),
	)
}

func benchmarkResultBytes(segments []*datapb.CompactionSegment) int64 {
	var total int64
	for _, segment := range segments {
		var segmentBytes int64
		for _, fieldBinlogs := range [][]*datapb.FieldBinlog{
			segment.GetInsertLogs(), segment.GetField2StatslogPaths(), segment.GetBm25Logs(),
		} {
			for _, fieldBinlog := range fieldBinlogs {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					segmentBytes += binlog.GetLogSize()
				}
			}
		}
		if segmentBytes == 0 {
			stats := segment.GetStats()
			segmentBytes = stats.GetInsertBinlogSize() + stats.GetStatsBinlogSize()
		}
		total += segmentBytes
	}
	return total
}

func benchmarkExpectedTimestampRange(tc mixCompactorBenchmarkCase, rows []benchmarkExpectedRow, commitTS uint64) (uint64, uint64) {
	from, to := uint64(math.MaxUint64), uint64(0)
	for _, row := range rows {
		ts := uint64(benchmarkExpectedTimestamp(tc, row.row, commitTS))
		from = min(from, ts)
		to = max(to, ts)
	}
	return from, to
}

func benchmarkExpectedNullCount(fieldID int64, rows []benchmarkExpectedRow) int64 {
	var count int64
	for _, row := range rows {
		switch fieldID {
		case benchmarkNullableField, benchmarkTTLField:
			if row.row%5 == 0 {
				count++
			}
		case benchmarkVectorBase + 100:
			count++
		}
	}
	return count
}

func benchmarkExpectedExpirQuantiles(tc mixCompactorBenchmarkCase, rows []benchmarkExpectedRow) []int64 {
	if !tc.ttlField || len(rows) == 0 {
		return nil
	}
	values := make([]int64, 0, len(rows))
	for _, row := range rows {
		value, valid := benchmarkTTLValue(tc, row.row, row.key)
		if valid && value > 0 {
			values = append(values, value)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	quantiles := make([]int64, 5)
	for i, percentile := range []float64{0.2, 0.4, 0.6, 0.8, 1.0} {
		index := int(math.Ceil(percentile*float64(len(rows)))) - 1
		if index >= len(values) {
			quantiles[i] = math.MaxInt64
		} else {
			quantiles[i] = values[index]
		}
	}
	return quantiles
}

func (f *benchmarkProductionFixture) validate(b testing.TB, segments []*datapb.CompactionSegment, readOutput bool) {
	b.Helper()
	expected := benchmarkExpectedRows(f.tc)
	writtenRows := int64(0)
	expectedOffset := 0
	for _, segment := range segments {
		writtenRows += segment.GetNumOfRows()
		segmentEnd := expectedOffset + int(segment.GetNumOfRows())
		if segmentEnd > len(expected) {
			b.Fatalf("segment rows exceed expected output: end=%d expected=%d", segmentEnd, len(expected))
		}
		segmentRows := expected[expectedOffset:segmentEnd]
		expectedOffset = segmentEnd
		if segment.GetStorageVersion() != f.version {
			b.Fatalf("storage version=%d expected=%d", segment.GetStorageVersion(), f.version)
		}
		if len(segment.GetInsertLogs()) == 0 {
			b.Fatal("missing output insert logs")
		}
		if len(segment.GetField2StatslogPaths()) == 0 {
			b.Fatal("missing output stats logs")
		}
		if f.tc.missingField && segment.GetStats().GetNullCounts()[benchmarkVectorBase+100] != segment.GetNumOfRows() {
			b.Fatalf("missing-field null count=%d expected=%d",
				segment.GetStats().GetNullCounts()[benchmarkVectorBase+100], segment.GetNumOfRows())
		}
		for _, fieldID := range []int64{benchmarkNullableField, benchmarkTTLField} {
			enabled := fieldID == benchmarkNullableField && f.tc.nullableValue || fieldID == benchmarkTTLField && f.tc.ttlField
			if enabled {
				want := benchmarkExpectedNullCount(fieldID, segmentRows)
				if got := segment.GetStats().GetNullCounts()[fieldID]; got != want {
					b.Fatalf("field %d null count=%d expected=%d", fieldID, got, want)
				}
			}
		}
		if f.tc.ttlField {
			want := benchmarkExpectedExpirQuantiles(f.tc, segmentRows)
			if got := segment.GetExpirQuantiles(); !slices.Equal(got, want) {
				b.Fatalf("expiry quantiles=%v expected=%v", got, want)
			}
		}
		tsFrom, tsTo := benchmarkExpectedTimestampRange(f.tc, segmentRows, f.commitTS)
		if stats := segment.GetStats(); stats.GetTimestampFrom() != tsFrom || stats.GetTimestampTo() != tsTo {
			b.Fatalf("segment timestamp range=[%d,%d] expected=[%d,%d]",
				stats.GetTimestampFrom(), stats.GetTimestampTo(), tsFrom, tsTo)
		}
		for _, fieldBinlog := range segment.GetInsertLogs() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if binlog.GetTimestampFrom() != tsFrom || binlog.GetTimestampTo() != tsTo {
					b.Fatalf("field %d binlog timestamp range=[%d,%d] expected=[%d,%d]",
						fieldBinlog.GetFieldID(), binlog.GetTimestampFrom(), binlog.GetTimestampTo(), tsFrom, tsTo)
				}
			}
		}
		if f.tc.bm25 && f.version == storage.StorageV2 {
			var bm25Rows int64
			for _, fieldBinlog := range segment.GetBm25Logs() {
				if fieldBinlog.GetFieldID() == benchmarkBM25Field {
					for _, binlog := range fieldBinlog.GetBinlogs() {
						bm25Rows += binlog.GetEntriesNum()
					}
				}
			}
			if bm25Rows != segment.GetNumOfRows() {
				b.Fatalf("BM25 stats rows=%d expected=%d", bm25Rows, segment.GetNumOfRows())
			}
		}
		if f.version == storage.StorageV3 && segment.GetManifest() == "" {
			b.Fatal("missing V3 output manifest")
		}
		if f.tc.key == benchmarkNamespaceKey {
			if segment.GetIsSorted() || !segment.GetIsSortedByNamespace() {
				b.Fatal("namespace output sorted flags are incorrect")
			}
		} else if !segment.GetIsSorted() || segment.GetIsSortedByNamespace() {
			b.Fatal("output sorted flags are incorrect")
		}
	}
	if writtenRows != int64(f.expectedRows) {
		b.Fatalf("written rows=%d expected=%d", writtenRows, f.expectedRows)
	}
	if benchmarkResultBytes(segments) == 0 {
		b.Fatal("output metadata reports zero bytes")
	}
	if !readOutput {
		return
	}

	readRows := 0
	for _, segment := range segments {
		input := &datapb.CompactionSegmentBinlogs{
			CollectionID:        1,
			PartitionID:         1,
			SegmentID:           segment.GetSegmentID(),
			FieldBinlogs:        segment.GetInsertLogs(),
			Field2StatslogPaths: segment.GetField2StatslogPaths(),
			StorageVersion:      segment.GetStorageVersion(),
			Manifest:            segment.GetManifest(),
		}
		reader, _, err := newCompactionSegmentRecordReader(context.Background(), input, f.schema, f.params.StorageConfig,
			storage.WithCollectionID(1),
			storage.WithDownloader(f.binlogIO.Download),
			storage.WithVersion(f.version),
			storage.WithStorageConfig(f.params.StorageConfig))
		if err != nil {
			b.Fatalf("open output segment: %v", err)
		}
		segmentPKs := make([]storage.PrimaryKey, 0, segment.GetNumOfRows())
		for {
			record, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = reader.Close()
				b.Fatalf("read output segment: %v", err)
			}
			rowIDs := record.Column(common.RowIDField).(*array.Int64)
			timestamps := record.Column(common.TimeStampField).(*array.Int64)
			for row := 0; row < record.Len(); row++ {
				if readRows >= len(expected) {
					_ = reader.Close()
					b.Fatalf("read unexpected output row %d", readRows)
				}
				want := expected[readRows]
				if rowIDs.Value(row) != want.key {
					_ = reader.Close()
					b.Fatalf("row %d row ID=%d expected=%d", readRows, rowIDs.Value(row), want.key)
				}
				if f.tc.key == benchmarkVarcharKey {
					got := record.Column(benchmarkPKField).(*array.String).Value(row)
					if got != fmt.Sprintf("%032d", want.key) {
						_ = reader.Close()
						b.Fatalf("row %d varchar PK=%q expected key=%d", readRows, got, want.key)
					}
				} else if got := record.Column(benchmarkPKField).(*array.Int64).Value(row); got != want.key {
					_ = reader.Close()
					b.Fatalf("row %d PK=%d expected=%d", readRows, got, want.key)
				}
				wantTS := benchmarkExpectedTimestamp(f.tc, want.row, f.commitTS)
				if timestamps.Value(row) != wantTS {
					_ = reader.Close()
					b.Fatalf("row %d timestamp=%d expected=%d", readRows, timestamps.Value(row), wantTS)
				}
				if f.tc.key == benchmarkNamespaceKey {
					got := record.Column(benchmarkNamespaceField).(*array.Int64).Value(row)
					if got != int64(want.reader/2) {
						_ = reader.Close()
						b.Fatalf("row %d namespace=%d expected=%d", readRows, got, want.reader/2)
					}
				}
				for scalar := 0; scalar < f.tc.scalars; scalar++ {
					fieldID := benchmarkFloatField + int64(scalar)
					wantValue := float64(want.key) + float64(fieldID)/1000
					if got := record.Column(fieldID).(*array.Float64).Value(row); got != wantValue {
						_ = reader.Close()
						b.Fatalf("row %d scalar field %d=%v expected=%v", readRows, fieldID, got, wantValue)
					}
				}
				for vector := 0; vector < f.tc.vectors; vector++ {
					got := record.Column(benchmarkVectorBase + int64(vector)).(*array.FixedSizeBinary).Value(row)
					wantValue := make([]byte, f.tc.dim*4)
					wantValue[0] = byte(want.key)
					wantValue[len(wantValue)-1] = byte(want.key >> 8)
					if !bytes.Equal(got, wantValue) {
						_ = reader.Close()
						b.Fatalf("row %d vector %d differs", readRows, vector)
					}
				}
				if f.tc.missingField && !record.Column(benchmarkVectorBase+100).IsNull(row) {
					_ = reader.Close()
					b.Fatalf("row %d materialized missing field is not null", readRows)
				}
				if f.tc.bm25 {
					text := record.Column(benchmarkBM25TextField).(*array.String).Value(row)
					if text != fmt.Sprintf("doc-%08d", want.key) {
						_ = reader.Close()
						b.Fatalf("row %d BM25 text=%q", readRows, text)
					}
					wantSparse := typeutil.CreateSparseFloatRow([]uint32{uint32(want.key%1024 + 1)}, []float32{1})
					if got := record.Column(benchmarkBM25Field).(*array.Binary).Value(row); !bytes.Equal(got, wantSparse) {
						_ = reader.Close()
						b.Fatalf("row %d BM25 sparse output differs", readRows)
					}
				}
				segmentPKs = append(segmentPKs, benchmarkPrimaryKey(f.tc, want.key))
				readRows++
			}
		}
		if err := reader.Close(); err != nil {
			b.Fatalf("close output reader: %v", err)
		}

		resolver := packed.NewStatsResolver(segment.GetManifest(), f.params.StorageConfig).
			WithStatslogs(segment.GetField2StatslogPaths())
		if f.tc.bm25 && f.version == storage.StorageV3 {
			bm25Paths, err := resolver.BM25StatsPaths()
			if err != nil || len(bm25Paths[benchmarkBM25Field]) == 0 {
				b.Fatalf("resolve V3 BM25 stats: paths=%v err=%v", bm25Paths, err)
			}
		}
		paths, err := resolver.BloomFilterPaths(benchmarkPKField)
		if err != nil {
			b.Fatalf("resolve output PK stats: %v", err)
		}
		if len(paths) == 0 {
			b.Fatal("output manifest/stats logs contain no PK bloom filter")
		}
		if f.version == storage.StorageV3 {
			memorySize, err := resolver.BloomFilterMemorySize(benchmarkPKField)
			if err != nil || memorySize <= 0 {
				b.Fatalf("V3 PK bloom memory size=%d err=%v", memorySize, err)
			}
			blobs := make([]*storage.Blob, 0, len(paths))
			for _, statsPath := range paths {
				value, err := packed.ReadFile(f.params.StorageConfig, statsPath)
				if err != nil {
					b.Fatalf("read V3 PK stats %s: %v", statsPath, err)
				}
				blobs = append(blobs, &storage.Blob{Value: value})
			}
			v3Stats, err := storage.DeserializeStats(blobs)
			if err != nil || len(v3Stats) == 0 {
				b.Fatalf("deserialize V3 PK stats: stats=%d err=%v", len(v3Stats), err)
			}
			minPK, maxPK := segmentPKs[0], segmentPKs[len(segmentPKs)-1]
			statsMin := v3Stats[0].MinPk
			statsMax := v3Stats[0].MaxPk
			for _, stat := range v3Stats[1:] {
				if stat.MinPk.LT(statsMin) {
					statsMin = stat.MinPk
				}
				if stat.MaxPk.GT(statsMax) {
					statsMax = stat.MaxPk
				}
			}
			if !statsMin.EQ(minPK) || !statsMax.EQ(maxPK) {
				b.Fatalf("V3 PK stats range=[%v,%v] expected=[%v,%v]",
					statsMin.GetValue(), statsMax.GetValue(), minPK.GetValue(), maxPK.GetValue())
			}
			for _, pk := range segmentPKs {
				encoded := benchmarkPrimaryKeyBytes(pk)
				found := false
				for _, stat := range v3Stats {
					if stat.BF.Test(encoded) {
						found = true
						break
					}
				}
				if !found {
					b.Fatalf("V3 PK bloom filter misses %v", pk.GetValue())
				}
			}
			continue
		}
		stats, err := compaction.LoadStatsFromPaths(context.Background(), f.binlogIO.ChunkManager, segment.GetSegmentID(), paths)
		if err != nil || len(stats) == 0 {
			b.Fatalf("load output PK stats: stats=%d err=%v", len(stats), err)
		}
		minPK, maxPK := segmentPKs[0], segmentPKs[len(segmentPKs)-1]
		statsMin, statsMax := stats[0].MinPK, stats[0].MaxPK
		for _, stat := range stats[1:] {
			if stat.MinPK.LT(statsMin) {
				statsMin = stat.MinPK
			}
			if stat.MaxPK.GT(statsMax) {
				statsMax = stat.MaxPK
			}
		}
		if !statsMin.EQ(minPK) || !statsMax.EQ(maxPK) {
			b.Fatalf("PK stats range=[%v,%v] expected=[%v,%v]",
				statsMin.GetValue(), statsMax.GetValue(), minPK.GetValue(), maxPK.GetValue())
		}
		for _, pk := range segmentPKs {
			encoded := benchmarkPrimaryKeyBytes(pk)
			found := false
			for _, stat := range stats {
				if stat.PkFilter.Test(encoded) {
					found = true
					break
				}
			}
			if !found {
				b.Fatalf("PK bloom filter misses %v", pk.GetValue())
			}
		}
	}
	if readRows != f.expectedRows {
		b.Fatalf("read rows=%d expected=%d", readRows, f.expectedRows)
	}
}

func BenchmarkMixCompactorPhases(b *testing.B) {
	decodeCase := mixCompactorBenchmarkCases[1]
	for _, version := range []int64{storage.StorageV2, storage.StorageV3} {
		version := version
		b.Run(fmt.Sprintf("reader_decode/v%d", version), func(b *testing.B) {
			fixture := newBenchmarkProductionFixture(b, decodeCase, version)
			expected := decodeCase.readers * decodeCase.rowsPerReader
			b.ReportAllocs()
			b.SetBytes(fixture.sourceBytes)
			b.ResetTimer()
			for range b.N {
				rows := 0
				for _, input := range fixture.inputs {
					reader, _, err := newCompactionSegmentRecordReader(context.Background(), input, fixture.schema, fixture.params.StorageConfig,
						storage.WithCollectionID(1),
						storage.WithDownloader(fixture.binlogIO.Download),
						storage.WithVersion(version),
						storage.WithStorageConfig(fixture.params.StorageConfig))
					if err != nil {
						b.Fatal(err)
					}
					for {
						record, err := reader.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							_ = reader.Close()
							b.Fatal(err)
						}
						rows += record.Len()
					}
					if err := reader.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if rows != expected {
					b.Fatalf("decoded rows=%d expected=%d", rows, expected)
				}
			}
		})
	}

	b.Run("predicate_selection_materialization", func(b *testing.B) {
		filtered := mixCompactorBenchmarkCases[5]
		schema := benchmarkSchema(filtered)
		inputSchema := benchmarkInputSchema(filtered, schema)
		records := benchmarkRecords(b, filtered, schema)
		defer func() {
			for _, record := range records {
				record.Release()
			}
		}()
		existingFields := collectionSchemaFields(inputSchema)
		pkField, err := typeutil.GetPrimaryFieldSchema(schema)
		if err != nil {
			b.Fatal(err)
		}
		expected := benchmarkPhaseRows(filtered)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows := 0
			for _, record := range records {
				materializer, err := NewRecordMaterializer(schema, schema.GetFunctions(), existingFields)
				if err != nil {
					b.Fatal(err)
				}
				filter := compaction.NewEntityFilter(nil, int64(time.Hour), benchmarkCurrentTime, 0)
				selection, err := selectFullRewriteRecord(record, pkField, filter, -1, false)
				if err != nil {
					materializer.Close()
					b.Fatal(err)
				}
				wrapped, err := materializer.WrapWithSelection(record, selection)
				if err != nil {
					materializer.Close()
					b.Fatal(err)
				}
				rows += wrapped.Len()
				cleanupMaterializedRecord(wrapped)
				materializer.Close()
			}
			if rows != expected {
				b.Fatalf("selected rows=%d expected=%d", rows, expected)
			}
		}
	})

	b.Run("wide_output_construction_only", func(b *testing.B) {
		tc := mixCompactorBenchmarkCases[1]
		tc.readers = 1
		tc.layout = benchmarkDisjoint
		schema := benchmarkSchema(tc)
		records := benchmarkRecords(b, tc, schema)
		defer records[0].Release()
		expected := benchmarkPhaseRows(tc)
		b.ReportAllocs()
		b.SetBytes(benchmarkSourceBytes(records, schema))
		b.ResetTimer()
		for range b.N {
			builder := storage.NewRecordBuilder(schema)
			if err := builder.Append(records[0], 0, records[0].Len()); err != nil {
				builder.Release()
				b.Fatal(err)
			}
			output := builder.Build()
			if output.Len() != expected {
				output.Release()
				builder.Release()
				b.Fatalf("output rows=%d expected=%d", output.Len(), expected)
			}
			output.Release()
			builder.Release()
		}
	})

	for _, version := range []int64{storage.StorageV2, storage.StorageV3} {
		version := version
		b.Run(fmt.Sprintf("writer/v%d", version), func(b *testing.B) {
			tc := mixCompactorBenchmarkCases[1]
			tc.readers = 1
			tc.layout = benchmarkDisjoint
			schema := benchmarkSchema(tc)
			records := benchmarkRecords(b, tc, schema)
			defer records[0].Release()
			root := b.TempDir()
			binlogIO := newBenchmarkLocalBinlogIO(root)
			defer binlogIO.Close()
			params := compaction.Params{
				StorageVersion: version,
				BinLogMaxSize:  64 * 1024 * 1024,
				StorageConfig:  &indexpb.StorageConfig{StorageType: "local", RootPath: root},
			}
			run := func(iteration int64) ([]*datapb.CompactionSegment, error) {
				segmentStart := int64(3_000_000_000) + iteration*1_000_000
				logStart := int64(4_000_000_000) + iteration*10_000_000
				writer, err := NewMultiSegmentWriter(context.Background(), binlogIO,
					NewCompactionAllocator(
						allocator.NewLocalAllocator(segmentStart, segmentStart+1_000_000),
						allocator.NewLocalAllocator(logStart, logStart+10_000_000),
					),
					256*1024*1024, schema, params, int64(records[0].Len()), 1, 1, "benchmark-writer", 4096,
					storage.WithStorageConfig(params.StorageConfig), storage.WithVersion(version))
				if err == nil {
					err = writer.Write(records[0])
				}
				if err == nil {
					err = writer.Close()
				}
				if err != nil {
					return nil, err
				}
				return writer.GetCompactionSegments(), nil
			}
			if _, err := run(0); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(benchmarkSourceBytes(records, schema))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				segments, err := run(int64(i + 1))
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if len(segments) != 1 || segments[0].GetNumOfRows() != int64(records[0].Len()) {
					b.Fatal("writer phase output mismatch")
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkMixCompactorMergeCore(b *testing.B) {
	for _, tc := range mixCompactorBenchmarkCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			schema := benchmarkSchema(tc)
			records := benchmarkRecords(b, tc, schema)
			defer func() {
				for _, record := range records {
					record.Release()
				}
			}()
			b.ReportAllocs()
			b.SetBytes(benchmarkSourceBytes(records, benchmarkInputSchema(tc, schema)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				readers := benchmarkReaders(tc, schema, records, false)
				writer := &benchmarkCountingWriter{}
				rows, err := storage.MergeSort(64*1024*1024, schema, readers, writer,
					func(storage.Record, int, int) bool { return true }, benchmarkMergeKeys(tc))
				closeBenchmarkReaders(readers)
				if err != nil {
					b.Fatal(err)
				}
				expectedRows := benchmarkPhaseRows(tc)
				if rows != expectedRows || writer.rows != expectedRows {
					b.Fatalf("row mismatch: merge=%d writer=%d expected=%d", rows, writer.rows, expectedRows)
				}
			}
		})
	}
}

func BenchmarkMixCompactorSelectionBeforeMaterialization(b *testing.B) {
	tc := mixCompactorBenchmarkCases[5]
	schema := benchmarkSchema(tc)
	records := benchmarkRecords(b, tc, schema)
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()
	for _, materializeBefore := range []bool{true, false} {
		name := "select_first"
		if materializeBefore {
			name = "materialize_first"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				readers := benchmarkReaders(tc, schema, records, materializeBefore)
				writer := &benchmarkCountingWriter{}
				rows, err := storage.MergeSort(64*1024*1024, schema, readers, writer,
					func(storage.Record, int, int) bool { return true }, benchmarkMergeKeys(tc))
				closeBenchmarkReaders(readers)
				if err != nil {
					b.Fatal(err)
				}
				expectedRows := benchmarkPhaseRows(tc)
				if rows != expectedRows || writer.rows != expectedRows {
					b.Fatalf("row mismatch: merge=%d writer=%d expected=%d", rows, writer.rows, expectedRows)
				}
			}
		})
	}
}

func BenchmarkMixCompactorRealWriter(b *testing.B) {
	for _, version := range []int64{storage.StorageV2, storage.StorageV3} {
		versionName := fmt.Sprintf("v%d", version)
		for _, tc := range mixCompactorProductionBenchmarkCases {
			tc := tc
			b.Run(versionName+"/"+tc.name, func(b *testing.B) {
				fixture := newBenchmarkProductionFixture(b, tc, version)
				warm, err := fixture.run(0)
				if err != nil {
					b.Fatal(err)
				}
				fixture.validate(b, warm, true)
				b.ReportAllocs()
				b.SetBytes(fixture.sourceBytes)
				b.ResetTimer()
				var measured time.Duration
				var outputBytes int64
				for i := 0; i < b.N; i++ {
					start := time.Now()
					segments, err := fixture.run(int64(i + 1))
					measured += time.Since(start)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					fixture.validate(b, segments, false)
					outputBytes += benchmarkResultBytes(segments)
					b.StartTimer()
				}
				b.StopTimer()
				if measured > 0 {
					b.ReportMetric(float64(fixture.expectedRows*b.N)/measured.Seconds(), "rows/s")
				}
				if b.N > 0 {
					b.ReportMetric(float64(outputBytes)/float64(b.N), "output_bytes/op")
				}
			})
		}
	}
}

func TestMergeSortMultipleSegmentsRewriteConditions(t *testing.T) {
	cases := []mixCompactorBenchmarkCase{
		{name: "collection_ttl", readers: 2, rowsPerReader: 100, layout: benchmarkPartialOverlap, key: benchmarkInt64Key, filterPercent: 90},
		{name: "ttl_field_mixed_null", readers: 2, rowsPerReader: 100, layout: benchmarkPartialOverlap, key: benchmarkInt64Key, nullableValue: true, ttlField: true},
		{name: "varchar_pk", readers: 2, rowsPerReader: 100, layout: benchmarkPartialOverlap, key: benchmarkVarcharKey, filterPercent: 10},
		{name: "commit_timestamp", readers: 2, rowsPerReader: 100, layout: benchmarkDisjoint, key: benchmarkInt64Key, commitTS: true},
		{name: "bm25_output", readers: 2, rowsPerReader: 100, layout: benchmarkPartialOverlap, key: benchmarkInt64Key, bm25: true, filterPercent: 10},
	}
	for _, version := range []int64{storage.StorageV2, storage.StorageV3} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				fixture := newBenchmarkProductionFixture(t, tc, version)
				segments, err := fixture.run(0)
				if err != nil {
					t.Fatal(err)
				}
				fixture.validate(t, segments, true)
			})
		}
	}
}
