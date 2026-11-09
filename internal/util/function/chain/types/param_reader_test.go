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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestParamReaderObject(t *testing.T) {
	object := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"mode": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "add"}},
	}}
	reader := NewParamReader("py_udf", map[string]*schemapb.FunctionParamValue{
		"udf_params": {Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: object}},
	})

	actual, err := reader.Object("udf_params", true)
	require.NoError(t, err)
	assert.Same(t, object, actual)
}

func TestParamReaderObjectMissing(t *testing.T) {
	reader := NewParamReader("py_udf", nil)

	actual, err := reader.Object("udf_params", false)
	require.NoError(t, err)
	assert.Nil(t, actual)

	_, err = reader.Object("udf_params", true)
	require.Error(t, err)
	assert.ErrorContains(t, err, `missing required parameter "udf_params"`)
}

func TestParamReaderObjectInvalid(t *testing.T) {
	t.Run("wrong type", func(t *testing.T) {
		reader := NewParamReader("py_udf", map[string]*schemapb.FunctionParamValue{
			"udf_params": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "invalid"}},
		})

		_, err := reader.Object("udf_params", false)
		require.Error(t, err)
		assert.ErrorContains(t, err, `parameter "udf_params" must be an object`)
	})

	t.Run("nil object", func(t *testing.T) {
		reader := NewParamReader("py_udf", map[string]*schemapb.FunctionParamValue{
			"udf_params": {Value: &schemapb.FunctionParamValue_ObjectValue{}},
		})

		_, err := reader.Object("udf_params", false)
		require.Error(t, err)
		assert.ErrorContains(t, err, `parameter "udf_params" must be an object`)
	})
}
