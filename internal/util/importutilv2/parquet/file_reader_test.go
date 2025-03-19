package parquet

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// TestParseSparseFloatRowVector tests the parseSparseFloatRowVector function
func TestParseSparseFloatRowVector(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantMaxIdx uint32
		wantErrMsg string
	}{
		{
			name:       "empty sparse vector",
			input:      "{}",
			wantMaxIdx: 0,
		},
		{
			name:       "key-value format",
			input:      "{\"275574541\":1.5383775}",
			wantMaxIdx: 275574542, // max index 275574541 + 1
		},
		{
			name:       "multiple key-value pairs",
			input:      "{\"1\":0.5,\"10\":1.5,\"100\":2.5}",
			wantMaxIdx: 101, // max index 100 + 1
		},
		{
			name:       "invalid format - missing braces",
			input:      "\"275574541\":1.5383775",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "invalid JSON format",
			input:      "{275574541:1.5383775}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "malformed JSON",
			input:      "{\"key\": value}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "non-numeric index",
			input:      "{\"abc\":1.5}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "non-numeric value",
			input:      "{\"123\":\"abc\"}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "negative index",
			input:      "{\"-1\":1.5}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowVec, maxIdx, err := parseSparseFloatRowVector(tt.input)

			if tt.wantErrMsg != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantMaxIdx, maxIdx)

			// Verify the rowVec is properly formatted
			if maxIdx > 0 {
				elemCount := len(rowVec) / 8
				assert.Greater(t, elemCount, 0)

				// Check the last index matches our expectation
				lastIdx := typeutil.SparseFloatRowIndexAt(rowVec, elemCount-1)
				assert.Equal(t, tt.wantMaxIdx-1, lastIdx)
			} else {
				assert.Empty(t, rowVec)
			}
		})
	}
}

func TestParseSparseFloatVectorStructs(t *testing.T) {
	mem := memory.NewGoAllocator()

	checkFunc := func(indices arrow.Array, values arrow.Array, expectSucceed bool) ([][]byte, uint32) {
		st := make(map[string]arrow.Array)
		if indices != nil {
			st[sparseVectorIndice] = indices
		}
		if values != nil {
			st[sparseVectorValues] = values
		}

		structs := make([]map[string]arrow.Array, 0)
		structs = append(structs, st)

		byteArr, maxDim, err := parseSparseFloatVectorStructs(structs)
		if expectSucceed {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
		return byteArr, maxDim
	}

	genInt32Arr := func(len int) *array.Int32 {
		builder := array.NewInt32Builder(mem)
		data := make([]int32, 0)
		validData := make([]bool, 0)
		for i := 0; i < len; i++ {
			data = append(data, (int32)(i))
			validData = append(validData, i%2 == 0)
		}

		builder.AppendValues(data, validData)
		return builder.NewInt32Array()
	}

	// values field missed
	checkFunc(genInt32Arr(1), nil, false)

	genFloat32Arr := func(len int) *array.Float32 {
		builder := array.NewFloat32Builder(mem)
		data := make([]float32, 0)
		validData := make([]bool, 0)
		for i := 0; i < len; i++ {
			data = append(data, (float32)(i))
			validData = append(validData, i%2 == 0)
		}
		builder.AppendValues(data, validData)
		return builder.NewFloat32Array()
	}

	// length of indices and values are different
	checkFunc(genInt32Arr(10), genFloat32Arr(20), false)

	// indices or values is not array.List
	checkFunc(genInt32Arr(10), genFloat32Arr(10), false)

	genInt32ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Int32Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Int32Builder).Append((int32)(v))
		}
		return builder.NewListArray()
	}

	genUint32ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Uint32Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Uint32Builder).Append(v)
		}
		return builder.NewListArray()
	}

	genInt64ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Int64Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Int64Builder).Append((int64)(v))
		}
		return builder.NewListArray()
	}

	genUint64ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Uint64Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Uint64Builder).Append((uint64)(v))
		}
		return builder.NewListArray()
	}

	genFloat32ArrList := func(arr []float32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Float32Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Float32Builder).Append(v)
		}
		return builder.NewListArray()
	}

	genFloat64ArrList := func(arr []float32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Float64Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Float64Builder).Append((float64)(v))
		}
		return builder.NewListArray()
	}

	// indices is not list of int32/uint32/int64/uint64 array
	checkFunc(genFloat32ArrList([]float32{0.1, 0.2, 0.3}), genFloat32ArrList([]float32{0.1, 0.2, 0.3}), false)

	// values is not list of float32/float64 array
	checkFunc(genUint32ArrList([]uint32{1, 2, 3}), genUint32ArrList([]uint32{1, 2, 3}), false)

	// duplicated indices
	indices := []uint32{4, 5, 4}
	values := []float32{0.11, 0.22, 0.23}
	checkFunc(genUint32ArrList(indices), genFloat32ArrList(values), false)

	// can handle empty indices/values
	byteArr, maxDim := checkFunc(genUint32ArrList([]uint32{}), genFloat32ArrList([]float32{}), true)
	assert.Equal(t, uint32(0), maxDim)
	assert.Equal(t, 1, len(byteArr))
	assert.Equal(t, 0, len(byteArr[0]))

	// check result is correct
	// note that the input indices is not sorted, the parseSparseFloatVectorStructs
	// returns correct maxDim and byteArr
	indices = []uint32{25, 78, 56}
	values = []float32{0.11, 0.22, 0.23}
	sortedIndices, sortedValues := typeutil.SortSparseFloatRow(indices, values)
	rowBytes := typeutil.CreateSparseFloatRow(sortedIndices, sortedValues)

	isValidFunc := func(indices arrow.Array, values arrow.Array) {
		byteArr, maxDim := checkFunc(indices, values, true)
		assert.Equal(t, uint32(78), maxDim)
		assert.Equal(t, 1, len(byteArr))
		assert.Equal(t, rowBytes, byteArr[0])
	}

	isValidFunc(genUint32ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genUint32ArrList(indices), genFloat64ArrList(values))
	isValidFunc(genInt32ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genInt32ArrList(indices), genFloat64ArrList(values))
	isValidFunc(genUint64ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genUint64ArrList(indices), genFloat64ArrList(values))
	isValidFunc(genInt64ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genInt64ArrList(indices), genFloat64ArrList(values))
}
