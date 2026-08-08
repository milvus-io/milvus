// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package pyudf

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/memory/mallocator"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPyUDFIdentityRoundTrip(t *testing.T) {
	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)

	intInput := newPyUDFTestInt64Chunked(t, allocator)
	stringInput := newPyUDFTestStringChunked(t, allocator)
	inputs := []*arrow.Chunked{intInput, stringInput}
	inputTypes := []arrow.DataType{intInput.DataType(), stringInput.DataType()}
	outputs, err := runPyUDFIdentity(inputs)
	require.NoError(t, err)
	require.Len(t, outputs, len(inputs))

	intInput.Release()
	stringInput.Release()

	for outputIdx, output := range outputs {
		require.Len(t, output.Chunks(), 2)
		assert.True(t, arrow.TypeEqual(inputTypes[outputIdx], output.DataType()))
	}

	assert.Equal(t, 1, outputs[0].Chunk(0).Data().Offset())
	assert.Equal(t, 1, outputs[1].Chunk(0).Data().Offset())
	assert.Equal(t, 2, outputs[0].Len())
	assert.Equal(t, 2, outputs[1].Len())
	assert.Equal(t, 1, outputs[0].NullN())
	assert.Equal(t, 1, outputs[1].NullN())
	assert.Equal(t, "20", outputs[0].Chunk(0).ValueStr(0))
	assert.True(t, outputs[0].Chunk(0).IsNull(1))
	assert.Equal(t, "beta", outputs[1].Chunk(0).ValueStr(0))
	assert.True(t, outputs[1].Chunk(0).IsNull(1))
	assert.Zero(t, outputs[0].Chunk(1).Len())
	assert.Zero(t, outputs[1].Chunk(1).Len())

	releasePyUDFChunked(outputs)
}

func TestPyUDFIdentityValidation(t *testing.T) {
	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)

	oneChunk := newPyUDFTestInt64Values(allocator, []int64{1}, nil)
	twoChunksFirst := newPyUDFTestInt64Values(allocator, []int64{1}, nil)
	twoChunksSecond := newPyUDFTestInt64Values(allocator, []int64{2}, nil)
	mismatchedRows := newPyUDFTestInt64Values(allocator, []int64{1, 2}, nil)
	defer oneChunk.Release()
	defer twoChunksFirst.Release()
	defer twoChunksSecond.Release()
	defer mismatchedRows.Release()

	columnOneChunk := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, oneChunk)
	columnTwoChunks := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, twoChunksFirst, twoChunksSecond)
	columnMismatchedRows := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, mismatchedRows)
	defer columnOneChunk.Release()
	defer columnTwoChunks.Release()
	defer columnMismatchedRows.Release()

	_, err := runPyUDFIdentity([]*arrow.Chunked{columnOneChunk, columnTwoChunks})
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = runPyUDFIdentity([]*arrow.Chunked{columnOneChunk, columnMismatchedRows})
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = runPyUDFIdentity([]*arrow.Chunked{nil})
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestPyUDFIdentityPartialImportFailureReleasesDescriptors(t *testing.T) {
	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)

	input := newPyUDFTestInt64Chunked(t, allocator)
	importCount := 0
	outputs, err := runPyUDFIdentityWithImporter([]*arrow.Chunked{input}, func(arr *cdata.CArrowArray, schema *cdata.CArrowSchema) (arrow.Field, arrow.Array, error) {
		importCount++
		if importCount == 2 {
			return arrow.Field{}, nil, errors.New("injected import failure")
		}
		return cdata.ImportCArray(arr, schema)
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Nil(t, outputs)
	input.Release()
}

func TestPyUDFIdentityEmptyInputs(t *testing.T) {
	outputs, err := runPyUDFIdentity(nil)
	require.NoError(t, err)
	assert.Empty(t, outputs)
}

func TestNativeResourceIdentityRoundTrip(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	wheel := writeNativeRuntimeTestWheel(t, "native_resource_identity", `
class UDF:
    def transform_query(self, params, columns):
        return [columns[0]]

def create_udf(context):
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_resource_identity", wheel, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, resource.Close()) }()

	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)
	input := newPyUDFTestInt64Chunked(t, allocator)
	params, err := NewRunParams("native_resource_identity", "L2_rerank", nil)
	require.NoError(t, err)
	serializedParams, err := MarshalRunParams(params)
	require.NoError(t, err)
	outputs, err := resource.run([]*arrow.Chunked{input}, serializedParams)
	require.NoError(t, err)
	input.Release()
	require.Len(t, outputs, 1)
	assert.Equal(t, 2, outputs[0].Len())
	assert.Equal(t, "20", outputs[0].Chunk(0).ValueStr(0))
	releasePyUDFChunked(outputs)
}

func TestNativeResourceTransformQueryUsesParams(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	wheel := writeNativeRuntimeTestWheel(t, "native_transform_query", `
import pyarrow as pa

class UDF:
    def transform_query(self, params, columns):
        values = [value.as_py() * params["factor"] for value in columns[0]]
        return [pa.array(values, type=pa.int64()), columns[1]]

def create_udf(context):
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_transform_query", wheel, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, resource.Close()) }()

	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)
	left := newPyUDFTestInt64Values(allocator, []int64{2, 3}, nil)
	leftChunked := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, left)
	left.Release()
	rightArray := newPyUDFTestStringValues(allocator, []string{"a", "b"})
	right := newPyUDFTestChunked(arrow.BinaryTypes.String, rightArray)
	rightArray.Release()
	params, err := NewRunParams("native_transform_query", "L2_rerank", &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"factor": intParamValue(4),
	}})
	require.NoError(t, err)
	serializedParams, err := MarshalRunParams(params)
	require.NoError(t, err)
	outputs, err := resource.run([]*arrow.Chunked{leftChunked, right}, serializedParams)
	require.NoError(t, err)
	leftChunked.Release()
	right.Release()
	require.Len(t, outputs, 2)
	assert.Equal(t, "8", outputs[0].Chunk(0).ValueStr(0))
	assert.Equal(t, "12", outputs[0].Chunk(0).ValueStr(1))
	assert.Equal(t, "a", outputs[1].Chunk(0).ValueStr(0))
	releasePyUDFChunked(outputs)
}

func TestNativeResourceSupportsDynamicOutputCounts(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	t.Run("one input to two outputs", func(t *testing.T) {
		wheel := writeNativeRuntimeTestWheel(t, "native_one_to_two_outputs", `
class UDF:
    def transform_query(self, params, columns):
        return [columns[0], columns[0]]

def create_udf(context):
    return UDF()
`)
		resource, err := loadNativeRuntimeTestResource("native_one_to_two_outputs", wheel, 1)
		require.NoError(t, err)
		defer func() { require.NoError(t, resource.Close()) }()

		allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
		defer allocator.AssertSize(t, 0)
		input := newPyUDFTestInt64Chunked(t, allocator)
		params, err := NewRunParams("native_one_to_two_outputs", "L2_rerank", nil)
		require.NoError(t, err)
		serializedParams, err := MarshalRunParams(params)
		require.NoError(t, err)
		outputs, err := resource.run([]*arrow.Chunked{input}, serializedParams)
		require.NoError(t, err)
		input.Release()
		require.Len(t, outputs, 2)
		assert.Equal(t, "20", outputs[0].Chunk(0).ValueStr(0))
		assert.Equal(t, "20", outputs[1].Chunk(0).ValueStr(0))
		releasePyUDFChunked(outputs)
	})

	t.Run("two inputs to one output", func(t *testing.T) {
		wheel := writeNativeRuntimeTestWheel(t, "native_two_to_one_output", `
class UDF:
    def transform_query(self, params, columns):
        return [columns[1]]

def create_udf(context):
    return UDF()
`)
		resource, err := loadNativeRuntimeTestResource("native_two_to_one_output", wheel, 1)
		require.NoError(t, err)
		defer func() { require.NoError(t, resource.Close()) }()

		allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
		defer allocator.AssertSize(t, 0)
		left := newPyUDFTestInt64Chunked(t, allocator)
		right := newPyUDFTestStringChunked(t, allocator)
		params, err := NewRunParams("native_two_to_one_output", "L2_rerank", nil)
		require.NoError(t, err)
		serializedParams, err := MarshalRunParams(params)
		require.NoError(t, err)
		outputs, err := resource.run([]*arrow.Chunked{left, right}, serializedParams)
		require.NoError(t, err)
		left.Release()
		right.Release()
		require.Len(t, outputs, 1)
		assert.Equal(t, "beta", outputs[0].Chunk(0).ValueStr(0))
		releasePyUDFChunked(outputs)
	})
}

func TestNativeResourceValidatesOutputConsistency(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	tests := []struct {
		name        string
		packageName string
		module      string
		errorText   string
	}{
		{
			name:        "output count",
			packageName: "native_output_count_change",
			module: `
class UDF:
    def transform_query(self, params, columns):
        if len(columns[0]) == 0:
            return [columns[0], columns[0]]
        return [columns[0]]

def create_udf(context):
    return UDF()
`,
			errorText: "output count changed",
		},
		{
			name:        "output type",
			packageName: "native_output_type_change",
			module: `
import pyarrow as pa

class UDF:
    def transform_query(self, params, columns):
        if len(columns[0]) == 0:
            return [pa.array([], type=pa.string())]
        return [columns[0]]

def create_udf(context):
    return UDF()
`,
			errorText: "type changed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wheel := writeNativeRuntimeTestWheel(t, test.packageName, test.module)
			resource, err := loadNativeRuntimeTestResource(test.packageName, wheel, 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, resource.Close()) }()

			allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
			defer allocator.AssertSize(t, 0)
			input := newPyUDFTestInt64Chunked(t, allocator)
			defer input.Release()
			params, err := NewRunParams(test.packageName, "L2_rerank", nil)
			require.NoError(t, err)
			serializedParams, err := MarshalRunParams(params)
			require.NoError(t, err)
			outputs, err := resource.run([]*arrow.Chunked{input}, serializedParams)
			require.ErrorContains(t, err, test.errorText)
			assert.Nil(t, outputs)
		})
	}
}

func TestNativeResourceHandlesZeroRowChunk(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	wheel := writeNativeRuntimeTestWheel(t, "native_zero_row_chunk", `
class UDF:
    def transform_query(self, params, columns):
        return [columns[0]]

def create_udf(context):
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_zero_row_chunk", wheel, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, resource.Close()) }()

	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)
	empty := newPyUDFTestInt64Values(allocator, nil, nil)
	input := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, empty)
	empty.Release()
	params, err := NewRunParams("native_zero_row_chunk", "L2_rerank", nil)
	require.NoError(t, err)
	serializedParams, err := MarshalRunParams(params)
	require.NoError(t, err)
	outputs, err := resource.run([]*arrow.Chunked{input}, serializedParams)
	require.NoError(t, err)
	input.Release()
	require.Len(t, outputs, 1)
	require.Len(t, outputs[0].Chunks(), 1)
	assert.Zero(t, outputs[0].Len())
	assert.True(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int64, outputs[0].DataType()))
	releasePyUDFChunked(outputs)
}

func TestNativeResourcePartialImportFailureReleasesDynamicOutputs(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	wheel := writeNativeRuntimeTestWheel(t, "native_dynamic_import_failure", `
class UDF:
    def transform_query(self, params, columns):
        return [columns[0], columns[0]]

def create_udf(context):
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_dynamic_import_failure", wheel, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, resource.Close()) }()

	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)
	inputArray := newPyUDFTestInt64Values(allocator, []int64{1}, nil)
	input := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, inputArray)
	inputArray.Release()
	params, err := NewRunParams("native_dynamic_import_failure", "L2_rerank", nil)
	require.NoError(t, err)
	serializedParams, err := MarshalRunParams(params)
	require.NoError(t, err)
	importCount := 0
	outputs, err := resource.runWithImporter([]*arrow.Chunked{input}, serializedParams, func(arr *cdata.CArrowArray, schema *cdata.CArrowSchema) (arrow.Field, arrow.Array, error) {
		importCount++
		if importCount == 2 {
			return arrow.Field{}, nil, errors.New("injected import failure")
		}
		return cdata.ImportCArray(arr, schema)
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Nil(t, outputs)
	input.Release()
}

func TestNativeResourceFunctionFailuresAreTyped(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	tests := []struct {
		name        string
		packageName string
		module      string
		match       string
	}{
		{
			name:        "user exception",
			packageName: "native_transform_exception",
			module: `
class UDF:
    def transform_query(self, params, columns):
        raise ValueError("transform boom")

def create_udf(context):
    return UDF()
`,
			match: "transform boom",
		},
		{
			name:        "invalid output",
			packageName: "native_invalid_output",
			module: `
class UDF:
    def transform_query(self, params, columns):
        return [[1]]

def create_udf(context):
    return UDF()
`,
			match: "must be pyarrow.Array",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wheel := writeNativeRuntimeTestWheel(t, test.packageName, test.module)
			resource, err := loadNativeRuntimeTestResource(test.packageName, wheel, 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, resource.Close()) }()

			allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
			defer allocator.AssertSize(t, 0)
			inputArray := newPyUDFTestInt64Values(allocator, []int64{1}, nil)
			input := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, inputArray)
			inputArray.Release()
			defer input.Release()
			params, err := NewRunParams(test.packageName, "L2_rerank", nil)
			require.NoError(t, err)
			serializedParams, err := MarshalRunParams(params)
			require.NoError(t, err)
			outputs, err := resource.run([]*arrow.Chunked{input}, serializedParams)
			require.Error(t, err)
			assert.Nil(t, outputs)
			assert.ErrorIs(t, err, merr.ErrFunctionFailed)
			assert.ErrorContains(t, err, test.match)
		})
	}
}

func TestNativeResourceRejectsUnsupportedTransform(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}
	require.NoError(t, initializeNativeRuntime())

	wheel := writeNativeRuntimeTestWheel(t, "native_transform_unsupported", `
class UDF:
    def transform(self, params, columns):
        return columns

def create_udf(context):
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_transform_unsupported", wheel, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, resource.Close()) }()

	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)
	inputArray := newPyUDFTestInt64Values(allocator, []int64{1}, nil)
	input := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, inputArray)
	inputArray.Release()
	defer input.Release()
	params, err := NewRunParams("native_transform_unsupported", "L2_rerank", nil)
	require.NoError(t, err)
	serializedParams, err := MarshalRunParams(params)
	require.NoError(t, err)
	outputs, err := resource.run([]*arrow.Chunked{input}, serializedParams)
	require.ErrorContains(t, err, "does not implement transform_query")
	assert.ErrorIs(t, err, merr.ErrFunctionFailed)
	assert.Nil(t, outputs)
}

func TestNativeResourceIdentityRejectsClosedResource(t *testing.T) {
	var resource *nativeResource
	_, err := resource.run(nil, nil)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	resource = &nativeResource{}
	_, err = resource.run(nil, nil)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func newPyUDFTestInt64Chunked(t *testing.T, allocator memory.Allocator) *arrow.Chunked {
	t.Helper()
	base := newPyUDFTestInt64Values(allocator, []int64{10, 20, 30}, []bool{true, true, false})
	slice := array.NewSlice(base, 1, 3)
	base.Release()
	empty := newPyUDFTestInt64Values(allocator, nil, nil)
	chunked := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, slice, empty)
	slice.Release()
	empty.Release()
	return chunked
}

func newPyUDFTestStringChunked(t *testing.T, allocator memory.Allocator) *arrow.Chunked {
	t.Helper()
	builder := array.NewStringBuilder(allocator)
	builder.Append("alpha")
	builder.Append("beta")
	builder.AppendNull()
	base := builder.NewArray()
	builder.Release()
	slice := array.NewSlice(base, 1, 3)
	base.Release()

	emptyBuilder := array.NewStringBuilder(allocator)
	empty := emptyBuilder.NewArray()
	emptyBuilder.Release()
	chunked := newPyUDFTestChunked(arrow.BinaryTypes.String, slice, empty)
	slice.Release()
	empty.Release()
	return chunked
}

func newPyUDFTestInt64Values(allocator memory.Allocator, values []int64, valid []bool) arrow.Array {
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues(values, valid)
	result := builder.NewArray()
	builder.Release()
	return result
}

func newPyUDFTestStringValues(allocator memory.Allocator, values []string) arrow.Array {
	builder := array.NewStringBuilder(allocator)
	builder.AppendValues(values, nil)
	result := builder.NewArray()
	builder.Release()
	return result
}

func newPyUDFTestChunked(dataType arrow.DataType, chunks ...arrow.Array) *arrow.Chunked {
	return arrow.NewChunked(dataType, chunks)
}
