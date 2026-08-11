package fastpb

import (
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	msgpb "github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var insertRequestViewBenchmarkSink []byte

// BenchmarkInsertRequestViewEncoder10Kx768 measures the allocation difference
// this view is designed to remove: materializing a 10k-row destination float
// vector before protobuf marshal versus gathering directly into the final wire
// payload.
func BenchmarkInsertRequestViewEncoder10Kx768(b *testing.B) {
	const (
		rowCount = 10_000
		dim      = 768
	)
	rows := make([]int, rowCount)
	rowIDs := make([]int64, rowCount)
	timestamps := make([]uint64, rowCount)
	vectors := make([]float32, rowCount*dim)
	for row := 0; row < rowCount; row++ {
		rows[row] = row
		rowIDs[row] = int64(row)
		timestamps[row] = uint64(row)
		for column := 0; column < dim; column++ {
			vectors[row*dim+column] = float32(row + column)
		}
	}
	source := &msgpb.InsertRequest{
		NumRows:    rowCount,
		RowIDs:     rowIDs,
		Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{{
			Type:      schemapb.DataType_FloatVector,
			FieldName: "embedding",
			FieldId:   100,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: vectors},
				},
			}},
		}},
	}
	benchmarkInsertRequestViewEncoder(b, source, rows)
}

// BenchmarkInsertRequestViewEncoder10KVarChar1KiB covers a document-style
// trusted string payload.
func BenchmarkInsertRequestViewEncoder10KVarChar1KiB(b *testing.B) {
	const rowCount = 10_000
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	document := strings.Repeat("Milvus 向量数据库 document payload. ", 26)
	texts := make([]string, rowCount)
	for row := range texts {
		texts[row] = document + strconv.Itoa(row)
	}
	source := &msgpb.InsertRequest{
		NumRows:    rowCount,
		RowIDs:     rowIDs,
		Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{
			scalarField(100, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: texts},
			}),
		},
	}
	benchmarkInsertRequestViewEncoder(b, source, rows)
}

// BenchmarkInsertRequestViewEncoder10KArray64Int64 covers nested protobuf rows,
// whose sizes must be measured once and reused while writing the final payload.
func BenchmarkInsertRequestViewEncoder10KArray64Int64(b *testing.B) {
	const (
		rowCount    = 10_000
		elementsPer = 64
	)
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	arrayRows := make([]*schemapb.ScalarField, rowCount)
	for row := range arrayRows {
		values := make([]int64, elementsPer)
		for element := range values {
			values[element] = int64(row*elementsPer + element)
		}
		arrayRows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: values},
		}}
	}
	source := &msgpb.InsertRequest{
		NumRows:    rowCount,
		RowIDs:     rowIDs,
		Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{
			scalarField(100, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{
					Data:        arrayRows,
					ElementType: schemapb.DataType_Int64,
				},
			}),
		},
	}
	benchmarkInsertRequestViewEncoder(b, source, rows)
}

func BenchmarkInsertRequestViewExactSplit10K(b *testing.B) {
	const (
		rowCount    = 10_000
		elementsPer = 64
		bodyBudget  = 64 << 10
	)
	template := insertViewTemplate()
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	document := strings.Repeat("Milvus exact split document payload. ", 26)
	texts := make([]string, rowCount)
	arrayRows := make([]*schemapb.ScalarField, rowCount)
	vectorRows := make([]*schemapb.VectorField, rowCount)
	for row := 0; row < rowCount; row++ {
		texts[row] = document + strconv.Itoa(row)
		values := make([]int64, elementsPer)
		for element := range values {
			values[element] = int64(row*elementsPer + element)
		}
		arrayRows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: values},
		}}
		vectorRows[row] = &schemapb.VectorField{
			Dim: 4,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
				Data: []float32{float32(row), 1, 2, 3},
			}},
		}
	}

	cases := []struct {
		name   string
		source *msgpb.InsertRequest
	}{
		{name: "varchar_1kib", source: &msgpb.InsertRequest{
			NumRows:    rowCount,
			RowIDs:     rowIDs,
			Timestamps: timestamps,
			FieldsData: []*schemapb.FieldData{
				scalarField(100, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: texts},
				}),
			},
		}},
		{name: "array_and_array_of_vector", source: &msgpb.InsertRequest{
			NumRows:    rowCount,
			RowIDs:     rowIDs,
			Timestamps: timestamps,
			FieldsData: []*schemapb.FieldData{
				scalarField(100, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{Data: arrayRows, ElementType: schemapb.DataType_Int64},
				}),
				{
					Type: schemapb.DataType_ArrayOfVector, FieldId: 101, FieldName: "vectors",
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
						Dim: 4,
						Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
							Dim: 4, ElementType: schemapb.DataType_FloatVector, Data: vectorRows,
						}},
					}},
				},
			},
		}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var bytesPerIteration int64
			for i := 0; i < b.N; i++ {
				cursor, err := NewInsertRequestViewCursor(tc.source)
				if err != nil {
					b.Fatal(err)
				}
				var encodedBytes int64
				for start := 0; start < len(rows); {
					encoder, consumed, err := cursor.NextEncoder(template, rows[start:], bodyBudget)
					if err != nil {
						b.Fatal(err)
					}
					size, err := encoder.EncodedSize()
					if err != nil {
						b.Fatal(err)
					}
					payload := make([]byte, size)
					if _, err := encoder.MarshalTo(payload); err != nil {
						b.Fatal(err)
					}
					encodedBytes += int64(size)
					insertRequestViewBenchmarkSink = payload
					start += consumed
				}
				if i == 0 {
					bytesPerIteration = encodedBytes
				}
			}
			b.SetBytes(bytesPerIteration)
		})
	}
}

func benchmarkInsertRows(rowCount int) ([]int, []int64, []uint64) {
	rows := make([]int, rowCount)
	rowIDs := make([]int64, rowCount)
	timestamps := make([]uint64, rowCount)
	for row := range rows {
		rows[row] = row
		rowIDs[row] = int64(row)
		timestamps[row] = uint64(row)
	}
	return rows, rowIDs, timestamps
}

func benchmarkInsertRequestViewEncoder(b *testing.B, source *msgpb.InsertRequest, rows []int) {
	b.Helper()
	template := insertViewTemplate()
	encoder, err := NewInsertRequestViewEncoder(template, source, rows)
	if err != nil {
		b.Fatal(err)
	}
	payloadSize, err := encoder.EncodedSize()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("materialized_append_then_proto_marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(payloadSize))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			materialized := materializeWithAppendFieldData(template, source, rows)
			payload, err := proto.Marshal(materialized)
			if err != nil {
				b.Fatal(err)
			}
			insertRequestViewBenchmarkSink = payload
		}
	})

	b.Run("preallocated_materialize_then_proto_marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(payloadSize))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			materialized := materializePreallocatedForBenchmark(template, source, rows)
			payload, err := proto.Marshal(materialized)
			if err != nil {
				b.Fatal(err)
			}
			insertRequestViewBenchmarkSink = payload
		}
	})

	b.Run("view_encode_to_final_payload", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(payloadSize))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			view, err := NewInsertRequestViewEncoder(template, source, rows)
			if err != nil {
				b.Fatal(err)
			}
			size, err := view.EncodedSize()
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, size)
			if _, err := view.MarshalTo(payload); err != nil {
				b.Fatal(err)
			}
			insertRequestViewBenchmarkSink = payload
		}
	})
}

func materializePreallocatedForBenchmark(template, source *msgpb.InsertRequest, rows []int) *msgpb.InsertRequest {
	request := proto.Clone(template).(*msgpb.InsertRequest)
	request.Timestamps = make([]uint64, 0, len(rows))
	request.RowIDs = make([]int64, 0, len(rows))
	request.RowData = nil
	request.FieldsData = typeutil.PrepareResultFieldData(source.GetFieldsData(), int64(len(rows)))
	request.NumRows = uint64(len(rows))

	idxComputer := typeutil.NewFieldDataIdxComputer(source.GetFieldsData())
	for _, row := range rows {
		fieldIndices := idxComputer.Compute(int64(row))
		typeutil.AppendFieldData(request.FieldsData, source.GetFieldsData(), int64(row), fieldIndices...)
		request.Timestamps = append(request.Timestamps, source.GetTimestamps()[row])
		request.RowIDs = append(request.RowIDs, source.GetRowIDs()[row])
	}
	return request
}

// BenchmarkInsertRequestViewWideTable stresses the sizing loop rather than
// payload volume. previewSize walks every field for every row, so its
// O(rows x fields) cost is invisible in the single-field benchmarks above --
// there, row-major and field-major traversal touch memory in the same order.
// A wide table separates the two: 128 narrow columns mean each row's sizing
// pass jumps across 128 slices.
//
// The array columns are the only ones whose per-row size is not O(1), so this
// is also where an estimated (rather than measured) array size would have to
// pay off if it pays off anywhere.
func BenchmarkInsertRequestViewWideTable(b *testing.B) {
	const (
		rowCount    = 2_000
		scalarCount = 96
		arrayCount  = 32
		elementsPer = 8
		bodyBudget  = 256 << 10
	)
	template := insertViewTemplate()
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)

	fields := make([]*schemapb.FieldData, 0, scalarCount+arrayCount)
	for field := 0; field < scalarCount; field++ {
		values := make([]int64, rowCount)
		for row := range values {
			values[row] = int64(row + field)
		}
		fields = append(fields, scalarField(int64(100+field), schemapb.DataType_Int64,
			&schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}))
	}
	for field := 0; field < arrayCount; field++ {
		cells := make([]*schemapb.ScalarField, rowCount)
		for row := range cells {
			values := make([]int64, elementsPer)
			for element := range values {
				values[element] = int64(row*elementsPer + element)
			}
			cells[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: values},
			}}
		}
		fields = append(fields, scalarField(int64(1000+field), schemapb.DataType_Array,
			&schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data: cells, ElementType: schemapb.DataType_Int64,
			}}))
	}

	source := &msgpb.InsertRequest{
		NumRows:    rowCount,
		RowIDs:     rowIDs,
		Timestamps: timestamps,
		FieldsData: fields,
	}

	b.ReportAllocs()
	var bytesPerIteration int64
	for i := 0; i < b.N; i++ {
		cursor, err := NewInsertRequestViewCursor(source)
		if err != nil {
			b.Fatal(err)
		}
		var encodedBytes int64
		for start := 0; start < len(rows); {
			encoder, consumed, err := cursor.NextEncoder(template, rows[start:], bodyBudget)
			if err != nil {
				b.Fatal(err)
			}
			size, err := encoder.EncodedSize()
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, size)
			if _, err := encoder.MarshalTo(payload); err != nil {
				b.Fatal(err)
			}
			encodedBytes += int64(size)
			insertRequestViewBenchmarkSink = payload
			start += consumed
		}
		if i == 0 {
			bytesPerIteration = encodedBytes
		}
	}
	b.SetBytes(bytesPerIteration)
}
