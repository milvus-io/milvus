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

package pyudf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadRequestCodec(t *testing.T) {
	resource := fileresource.ResolvedFileResource{
		ID:        101,
		Name:      "rank_udf",
		Path:      "/remote/rank_udf.whl",
		LocalPath: "/local/101/rank_udf.whl",
	}
	request, err := NewLoadRequest(resource, " L2_rerank ", 3)
	require.NoError(t, err)
	assert.Equal(t, "rank_udf", request.GetResourceName())
	assert.Equal(t, int64(101), request.GetResourceId())
	assert.Equal(t, resource.Path, request.GetResourcePath())
	assert.Equal(t, resource.LocalPath, request.GetLocalPath())
	assert.Equal(t, "L2_rerank", request.GetStage())
	assert.Equal(t, int32(3), request.GetInstanceCount())

	data, err := MarshalLoadRequest(request)
	require.NoError(t, err)
	decoded := &cgopb.PyUDFLoadRequest{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	assert.True(t, proto.Equal(request, decoded))
}

func TestRunParamsCodec(t *testing.T) {
	bytesValue := []byte{0, 1, 2, 255}
	input := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"bool":   boolParamValue(true),
		"int":    intParamValue(42),
		"double": doubleParamValue(1.25),
		"string": stringParamValue("value"),
		"bytes":  bytesParamValue(bytesValue),
		"array": arrayParamValue(
			stringParamValue("nested"),
			objectParamValue(map[string]*schemapb.FunctionParamValue{
				"empty_array":  arrayParamValue(),
				"empty_object": objectParamValue(nil),
			}),
		),
	}}
	original := proto.Clone(input).(*schemapb.FunctionParamObject)

	params, err := NewRunParams(" rank_udf ", " L2_rerank ", input)
	require.NoError(t, err)
	assert.Equal(t, "rank_udf", params.GetResourceName())
	assert.Equal(t, "L2_rerank", params.GetStage())
	assert.True(t, proto.Equal(input, params.GetUdfParams()))
	assert.NotSame(t, input, params.GetUdfParams())

	params.GetUdfParams().GetFields()["bytes"].GetBytesValue()[0] = 99
	params.GetUdfParams().GetFields()["string"] = stringParamValue("changed")
	assert.True(t, proto.Equal(original, input))
	assert.Equal(t, byte(0), input.GetFields()["bytes"].GetBytesValue()[0])

	data, err := MarshalRunParams(params)
	require.NoError(t, err)
	decoded := &cgopb.PyUDFRunParams{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	assert.True(t, proto.Equal(params, decoded))
}

func TestRunParamsDefaultsToEmptyObject(t *testing.T) {
	params, err := NewRunParams("rank_udf", "L2_rerank", nil)
	require.NoError(t, err)
	require.NotNil(t, params.GetUdfParams())
	assert.Empty(t, params.GetUdfParams().GetFields())
}

func TestRunParamsDeterministicMarshal(t *testing.T) {
	first, err := NewRunParams("rank_udf", "L2_rerank", &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"z": intParamValue(1),
		"a": stringParamValue("first"),
	}})
	require.NoError(t, err)
	second, err := NewRunParams("rank_udf", "L2_rerank", &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"a": stringParamValue("first"),
		"z": intParamValue(1),
	}})
	require.NoError(t, err)

	firstData, err := MarshalRunParams(first)
	require.NoError(t, err)
	secondData, err := MarshalRunParams(second)
	require.NoError(t, err)
	assert.Equal(t, firstData, secondData)
}

func TestRunParamsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		params *schemapb.FunctionParamObject
		match  string
	}{
		{
			name:   "nil map value",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"bad": nil}},
			match:  `udf_params["bad"] is unset`,
		},
		{
			name:   "unset oneof",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"bad": {}}},
			match:  `udf_params["bad"] is unset`,
		},
		{
			name: "nil array element",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
				"bad": arrayParamValue(nil),
			}},
			match: `udf_params["bad"][0] is unset`,
		},
		{
			name: "nil array",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
				"bad": {Value: &schemapb.FunctionParamValue_ArrayValue{}},
			}},
			match: `udf_params["bad"] contains a nil array`,
		},
		{
			name: "nil object",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
				"bad": {Value: &schemapb.FunctionParamValue_ObjectValue{}},
			}},
			match: `udf_params["bad"] contains a nil object`,
		},
		{
			name: "invalid UTF-8 string",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
				"bad": stringParamValue(string([]byte{0xff})),
			}},
			match: `udf_params["bad"] is not valid UTF-8`,
		},
		{
			name: "invalid UTF-8 object key",
			params: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
				string([]byte{0xff}): stringParamValue("value"),
			}},
			match: "contains an object key that is not valid UTF-8",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewRunParams("rank_udf", "L2_rerank", test.params)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.ErrorContains(t, err, test.match)
		})
	}
}

func TestRunParamsNestingDepth(t *testing.T) {
	value := stringParamValue("leaf")
	for range maxParamNestingDepth + 1 {
		value = arrayParamValue(value)
	}
	_, err := NewRunParams("rank_udf", "L2_rerank", &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"nested": value,
	}})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.ErrorContains(t, err, "exceeds the maximum nesting depth")
}

func TestControlRequestValidation(t *testing.T) {
	validResource := fileresource.ResolvedFileResource{
		ID:        1,
		Name:      "rank_udf",
		Path:      "/remote/rank_udf.whl",
		LocalPath: "/local/rank_udf.whl",
	}

	_, err := NewRunParams("", "L2_rerank", nil)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = NewRunParams("rank_udf", "", nil)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = NewRunParams(string([]byte{0xff}), "L2_rerank", nil)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	_, err = NewLoadRequest(fileresource.ResolvedFileResource{}, "L2_rerank", 1)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = NewLoadRequest(validResource, "", 1)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = NewLoadRequest(validResource, "L2_rerank", 0)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	_, err = MarshalLoadRequest(nil)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = MarshalRunParams(nil)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestRunParamsBytesAllowInvalidUTF8(t *testing.T) {
	params, err := NewRunParams("rank_udf", "L2_rerank", &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"binary": bytesParamValue([]byte{0xff, 0xfe}),
	}})
	require.NoError(t, err)
	assert.Equal(t, []byte{0xff, 0xfe}, params.GetUdfParams().GetFields()["binary"].GetBytesValue())
}

func boolParamValue(value bool) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: value}}
}

func intParamValue(value int64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: value}}
}

func doubleParamValue(value float64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: value}}
}

func stringParamValue(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: value}}
}

func bytesParamValue(value []byte) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BytesValue{BytesValue: value}}
}

func arrayParamValue(values ...*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: values}}}
}

func objectParamValue(fields map[string]*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: fields}}}
}
