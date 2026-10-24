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

package fastpb

import (
	"encoding/binary"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	msgpb "github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var insertRequestViewBenchmarkSink []byte

// BenchmarkInsertRequestViewDataTypes10K compares the master materialize +
// proto.Marshal path with the borrowed view encoder over representative scalar,
// document, nested, dense, nullable, sparse, and ArrayOfVector payloads.
func BenchmarkInsertRequestViewDataTypes10K(b *testing.B) {
	const (
		rowCount  = 10_000
		vectorDim = 768
	)
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)

	bools := make([]bool, rowCount)
	int32s := make([]int32, rowCount)
	int64s := make([]int64, rowCount)
	doubles := make([]float64, rowCount)
	varchars := make([]string, rowCount)
	jsonRows := make([][]byte, rowCount)
	arrayRows := make([]*schemapb.ScalarField, rowCount)
	sparseRows := make([][]byte, rowCount)
	arrayOfVectorRows := make([]*schemapb.VectorField, rowCount)
	valid := make([]bool, rowCount)

	document := strings.Repeat("Milvus vector database payload. ", 32)
	floatVectors := make([]float32, rowCount*vectorDim)
	nullableFloatVectors := make([]float32, 0, rowCount*vectorDim/2)
	binaryVectors := make([]byte, rowCount*vectorDim/8)
	float16Vectors := make([]byte, rowCount*vectorDim*2)
	bfloat16Vectors := make([]byte, rowCount*vectorDim*2)
	int8Vectors := make([]byte, rowCount*vectorDim)

	for row := 0; row < rowCount; row++ {
		bools[row] = row%2 == 0
		int32s[row] = int32(row * 17)
		int64s[row] = int64(row * 17)
		doubles[row] = float64(row) + 0.25
		varchars[row] = document + strconv.Itoa(row)
		jsonRows[row] = []byte(`{"row":` + strconv.Itoa(row) + `,"payload":"` + document + `"}`)

		arrayValues := make([]int64, 64)
		for element := range arrayValues {
			arrayValues[element] = int64(row*64 + element)
		}
		arrayRows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: arrayValues},
		}}

		sparseRows[row] = benchmarkSparseRow(32)
		arrayOfVectorValues := make([]float32, 128)
		for column := range arrayOfVectorValues {
			arrayOfVectorValues[column] = float32(row + column)
		}
		arrayOfVectorRows[row] = &schemapb.VectorField{
			Dim: 128,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
				Data: arrayOfVectorValues,
			}},
		}

		valid[row] = row%2 == 0
		for column := 0; column < vectorDim; column++ {
			value := byte(row + column)
			floatVectors[row*vectorDim+column] = float32(row + column)
			int8Vectors[row*vectorDim+column] = value
			float16Vectors[(row*vectorDim+column)*2] = value
			bfloat16Vectors[(row*vectorDim+column)*2] = value
			if valid[row] {
				nullableFloatVectors = append(nullableFloatVectors, float32(row+column))
			}
		}
		for column := 0; column < vectorDim/8; column++ {
			binaryVectors[row*vectorDim/8+column] = byte(row + column)
		}
	}

	request := func(field *schemapb.FieldData) *msgpb.InsertRequest {
		return &msgpb.InsertRequest{
			NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps,
			FieldsData: []*schemapb.FieldData{field},
		}
	}
	cases := []struct {
		name   string
		source *msgpb.InsertRequest
	}{
		{"rowid_timestamp_only", &msgpb.InsertRequest{NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps}},
		{"bool", request(scalarField(100, schemapb.DataType_Bool, &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: bools}}))},
		{"int32", request(scalarField(100, schemapb.DataType_Int32, &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: int32s}}))},
		{"int64", request(scalarField(100, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: int64s}}))},
		{"double", request(scalarField(100, schemapb.DataType_Double, &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: doubles}}))},
		{"varchar_1kib", request(scalarField(100, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: varchars}}))},
		{"json_1kib", request(scalarField(100, schemapb.DataType_JSON, &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsonRows}}))},
		{"array_64_int64", request(scalarField(100, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: arrayRows, ElementType: schemapb.DataType_Int64}}))},
		{"binary_vector_768", request(vectorField(100, schemapb.DataType_BinaryVector, vectorDim, &schemapb.VectorField_BinaryVector{BinaryVector: binaryVectors}, nil))},
		{"float_vector_768", request(vectorField(100, schemapb.DataType_FloatVector, vectorDim, &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatVectors}}, nil))},
		{"nullable_float_vector_768_50pct", request(vectorField(100, schemapb.DataType_FloatVector, vectorDim, &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: nullableFloatVectors}}, valid))},
		{"float16_vector_768", request(vectorField(100, schemapb.DataType_Float16Vector, vectorDim, &schemapb.VectorField_Float16Vector{Float16Vector: float16Vectors}, nil))},
		{"bfloat16_vector_768", request(vectorField(100, schemapb.DataType_BFloat16Vector, vectorDim, &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: bfloat16Vectors}, nil))},
		{"int8_vector_768", request(vectorField(100, schemapb.DataType_Int8Vector, vectorDim, &schemapb.VectorField_Int8Vector{Int8Vector: int8Vectors}, nil))},
		{"sparse_vector_32nnz", request(vectorField(100, schemapb.DataType_SparseFloatVector, 4096, &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 4096, Contents: sparseRows}}, nil))},
		{"array_of_vector_float_128", request(vectorField(100, schemapb.DataType_ArrayOfVector, 128, &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 128, ElementType: schemapb.DataType_FloatVector, Data: arrayOfVectorRows}}, nil))},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkInsertRequestMasterVsView(b, tc.source, rows)
		})
	}
}

func vectorField(id int64, dataType schemapb.DataType, dim int64, data any, valid []bool) *schemapb.FieldData {
	vector := &schemapb.VectorField{Dim: dim}
	switch value := data.(type) {
	case *schemapb.VectorField_BinaryVector:
		vector.Data = value
	case *schemapb.VectorField_FloatVector:
		vector.Data = value
	case *schemapb.VectorField_Float16Vector:
		vector.Data = value
	case *schemapb.VectorField_Bfloat16Vector:
		vector.Data = value
	case *schemapb.VectorField_SparseFloatVector:
		vector.Data = value
	case *schemapb.VectorField_Int8Vector:
		vector.Data = value
	case *schemapb.VectorField_VectorArray:
		vector.Data = value
	default:
		panic("unsupported vector benchmark data")
	}
	return &schemapb.FieldData{
		Type: dataType, FieldName: "vector", FieldId: id, ValidData: valid,
		Field: &schemapb.FieldData_Vectors{Vectors: vector},
	}
}

func benchmarkSparseRow(nonZero int) []byte {
	row := make([]byte, nonZero*8)
	for i := 0; i < nonZero; i++ {
		binary.LittleEndian.PutUint32(row[i*8:], uint32(i*128))
		binary.LittleEndian.PutUint32(row[i*8+4:], 0x3f800000)
	}
	return row
}

func benchmarkInsertRequestMasterVsView(b *testing.B, source *msgpb.InsertRequest, rows []int) {
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

	b.Run("master_materialize_then_proto_marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(payloadSize))
		for i := 0; i < b.N; i++ {
			materialized := materializeWithAppendFieldData(template, source, rows)
			payload, err := proto.Marshal(materialized)
			if err != nil {
				b.Fatal(err)
			}
			insertRequestViewBenchmarkSink = payload
		}
	})

	b.Run("local_view_to_final_payload", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(payloadSize))
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

	b.Run("master_estimate_split_materialize_marshal", func(b *testing.B) {
		b.ReportAllocs()
		var bytesPerIteration int64
		var messagesPerIteration int
		for i := 0; i < b.N; i++ {
			encodedBytes, messages, err := benchmarkMasterSplitAndMarshal(template, source, rows, 2<<20)
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				bytesPerIteration = encodedBytes
				messagesPerIteration = messages
			}
		}
		b.SetBytes(bytesPerIteration)
		b.ReportMetric(float64(messagesPerIteration), "messages/op")
	})

	b.Run("local_exact_split_view", func(b *testing.B) {
		b.ReportAllocs()
		var bytesPerIteration int64
		var messagesPerIteration int
		for i := 0; i < b.N; i++ {
			encodedBytes, messages, err := benchmarkViewSplitAndMarshal(template, source, rows, (2<<20)-(4<<10))
			if err != nil {
				b.Fatal(err)
			}
			if i == 0 {
				bytesPerIteration = encodedBytes
				messagesPerIteration = messages
			}
		}
		b.SetBytes(bytesPerIteration)
		b.ReportMetric(float64(messagesPerIteration), "messages/op")
	})
}

func benchmarkMasterSplitAndMarshal(template, source *msgpb.InsertRequest, rows []int, bodyBudget int) (int64, int, error) {
	fields := source.GetFieldsData()
	idxComputer := typeutil.NewFieldDataIdxComputer(fields)
	requestSize := 0
	var encodedBytes int64
	messages := 0
	newRequest := func() *msgpb.InsertRequest {
		request := proto.Clone(template).(*msgpb.InsertRequest)
		request.Timestamps = nil
		request.RowIDs = nil
		request.RowData = nil
		request.FieldsData = make([]*schemapb.FieldData, len(fields))
		request.NumRows = 0
		return request
	}
	request := newRequest()
	flush := func() error {
		payload, err := proto.Marshal(request)
		if err != nil {
			return err
		}
		insertRequestViewBenchmarkSink = payload
		encodedBytes += int64(len(payload))
		messages++
		return nil
	}

	for _, row := range rows {
		fieldIndices := idxComputer.Compute(int64(row))
		rowSize, err := typeutil.EstimateEntitySize(fields, row, fieldIndices...)
		if err != nil {
			return 0, 0, err
		}
		if request.GetNumRows() > 0 && requestSize+rowSize >= bodyBudget {
			if err := flush(); err != nil {
				return 0, 0, err
			}
			request = newRequest()
			requestSize = 0
		}
		typeutil.AppendFieldData(request.FieldsData, fields, int64(row), fieldIndices...)
		request.Timestamps = append(request.Timestamps, source.GetTimestamps()[row])
		request.RowIDs = append(request.RowIDs, source.GetRowIDs()[row])
		request.NumRows++
		requestSize += rowSize
	}
	if request.GetNumRows() > 0 {
		if err := flush(); err != nil {
			return 0, 0, err
		}
	}
	return encodedBytes, messages, nil
}

func benchmarkViewSplitAndMarshal(template, source *msgpb.InsertRequest, rows []int, bodyBudget int) (int64, int, error) {
	cursor, err := NewInsertRequestViewCursor(source)
	if err != nil {
		return 0, 0, err
	}
	var encodedBytes int64
	messages := 0
	for start := 0; start < len(rows); {
		encoder, consumed, err := cursor.NextEncoder(template, rows[start:], bodyBudget)
		if err != nil {
			return 0, 0, err
		}
		size, err := encoder.EncodedSize()
		if err != nil {
			return 0, 0, err
		}
		payload := make([]byte, size)
		if _, err := encoder.MarshalTo(payload); err != nil {
			return 0, 0, err
		}
		insertRequestViewBenchmarkSink = payload
		encodedBytes += int64(size)
		messages++
		start += consumed
	}
	return encodedBytes, messages, nil
}

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
	sparseRows := make([][]byte, rowCount)
	arrayRows := make([]*schemapb.ScalarField, rowCount)
	vectorRows := make([]*schemapb.VectorField, rowCount)
	for row := 0; row < rowCount; row++ {
		texts[row] = document + strconv.Itoa(row)
		sparseRows[row] = make([]byte, 128*8)
		for element := 0; element < 128; element++ {
			offset := element * 8
			binary.LittleEndian.PutUint32(sparseRows[row][offset:], uint32(element*2+row%2))
			binary.LittleEndian.PutUint32(sparseRows[row][offset+4:], 0x3f800000)
		}
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
		{name: "sparse_1kib", source: &msgpb.InsertRequest{
			NumRows:    rowCount,
			RowIDs:     rowIDs,
			Timestamps: timestamps,
			FieldsData: []*schemapb.FieldData{{
				Type: schemapb.DataType_SparseFloatVector, FieldId: 100, FieldName: "sparse",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: sparseRows}},
				}},
			}},
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
