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

// BenchmarkInsertRequestViewEncoder10KVarChar1KiB covers the document-style
// payload that exposes repeated UTF-8 validation in the view encoder.
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
