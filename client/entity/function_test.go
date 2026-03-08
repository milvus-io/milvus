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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionSchema(t *testing.T) {
	functions := []*Function{
		NewFunction().WithName("text_bm25_emb").WithType(FunctionTypeBM25).WithInputFields("a", "b").WithOutputFields("c").WithParam("key", "value"),
		NewFunction().WithName("other_emb").WithType(FunctionTypeTextEmbedding).WithInputFields("c").WithOutputFields("b", "a"),
	}

	for _, function := range functions {
		funcSchema := function.ProtoMessage()
		assert.Equal(t, function.Name, funcSchema.Name)
		assert.Equal(t, function.Type, funcSchema.Type)
		assert.Equal(t, function.InputFieldNames, funcSchema.InputFieldNames)
		assert.Equal(t, function.OutputFieldNames, funcSchema.OutputFieldNames)
		assert.Equal(t, function.Params, KvPairsMap(funcSchema.GetParams()))

		nf := NewFunction()
		nf.ReadProto(funcSchema)

		assert.Equal(t, function.Name, nf.Name)
		assert.Equal(t, function.Type, nf.Type)
		assert.Equal(t, function.InputFieldNames, nf.InputFieldNames)
		assert.Equal(t, function.OutputFieldNames, nf.OutputFieldNames)
		assert.Equal(t, function.Params, nf.Params)
	}
}

func TestFunctionWithParamSliceHandling(t *testing.T) {
	// Test slice value handling - the main change in this PR
	f := NewFunction().WithParam("slice_key", []string{"a", "b", "c"})
	assert.Equal(t, `["a","b","c"]`, f.Params["slice_key"])

	// Test int slice
	f = NewFunction().WithParam("int_slice_key", []int{1, 2, 3})
	assert.Equal(t, "[1,2,3]", f.Params["int_slice_key"])

	// Test float slice
	f = NewFunction().WithParam("float_slice_key", []float64{1.1, 2.2, 3.3})
	assert.Equal(t, "[1.1,2.2,3.3]", f.Params["float_slice_key"])

	// Test empty slice
	f = NewFunction().WithParam("empty_slice_key", []string{})
	assert.Equal(t, "[]", f.Params["empty_slice_key"])

	// Test the JSON marshal error fallback path
	type unmarshalableType struct {
		Channel chan int
	}
	f = NewFunction().WithParam("complex_slice_key", []unmarshalableType{{Channel: make(chan int)}})
	// This should fallback to fmt.Sprintf since channels can't be JSON marshaled
	assert.Contains(t, f.Params["complex_slice_key"], "0x")

	// Test non-slice value (existing behavior should remain unchanged)
	f = NewFunction().WithParam("string_key", "string_value")
	assert.Equal(t, "string_value", f.Params["string_key"])
}
