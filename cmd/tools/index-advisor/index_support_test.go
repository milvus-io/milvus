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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestKindOf(t *testing.T) {
	for _, c := range []struct {
		col  column
		want columnKind
	}{
		{column{dataType: schemapb.DataType_Bool}, kindBool},
		{column{dataType: schemapb.DataType_Int8}, kindInt},
		{column{dataType: schemapb.DataType_Float}, kindFloat},
		{column{dataType: schemapb.DataType_VarChar}, kindString},
		{column{dataType: schemapb.DataType_Timestamptz}, kindTimestamptz},
		{column{dataType: schemapb.DataType_Array, elementType: schemapb.DataType_Int64}, kindArray},
		{column{dataType: schemapb.DataType_JSON, jsonPath: []string{"a"}}, kindJSONPath},
		{column{dataType: schemapb.DataType_JSON}, kindOther},
		{column{dataType: schemapb.DataType_Geometry}, kindGeometry},
		{column{dataType: schemapb.DataType_FloatVector}, kindOther},
	} {
		assert.Equal(t, c.want, kindOf(c.col), c.col.dataType.String())
	}
}

func TestJSONCastType(t *testing.T) {
	for kind, want := range map[string]string{
		"bool": "BOOL", "int": "DOUBLE", "float": "DOUBLE", "string": "VARCHAR", "array": "", "": "",
		"array_bool": "ARRAY_BOOL", "array_int": "ARRAY_DOUBLE", "array_float": "ARRAY_DOUBLE", "array_string": "ARRAY_VARCHAR",
	} {
		assert.Equal(t, want, jsonCastType(kind), kind)
	}
}

func TestIsArrayPosition(t *testing.T) {
	for key, want := range map[string]bool{"0": true, "12": true, "-1": true, "+3": true, "": false, "+": false, "a1": false, "1a": false, "1.5": false} {
		assert.Equal(t, want, isArrayPosition(key), key)
	}
}

// TestIndexAccepts pins indexAccepts to the CheckValidDataType and JSON cast
// rules in internal/util/indexparamcheck, which this tool cannot import. The
// expected values are copied from those checkers and must be updated by hand
// when Milvus changes them.
func TestIndexAccepts(t *testing.T) {
	var (
		boolCol  = column{dataType: schemapb.DataType_Bool}
		intCol   = column{dataType: schemapb.DataType_Int64}
		pkCol    = column{dataType: schemapb.DataType_Int64, isPrimaryKey: true}
		floatCol = column{dataType: schemapb.DataType_Double}
		strCol   = column{dataType: schemapb.DataType_VarChar}
		strArray = column{dataType: schemapb.DataType_Array, elementType: schemapb.DataType_VarChar}
		dblArray = column{dataType: schemapb.DataType_Array, elementType: schemapb.DataType_Double}
		jsonPath = column{dataType: schemapb.DataType_JSON, jsonPath: []string{"a"}}
	)
	cases := []struct {
		index string
		col   column
		cast  string
		want  bool
	}{
		{indexBitmap, boolCol, "", true},
		{indexBitmap, intCol, "", true},
		{indexBitmap, pkCol, "", false},
		{indexBitmap, floatCol, "", false},
		{indexBitmap, strArray, "", true},
		{indexBitmap, dblArray, "", false},
		{indexBitmap, jsonPath, "VARCHAR", true},
		{indexBitmap, jsonPath, "DOUBLE", false},

		{indexNgram, strCol, "", true},
		{indexNgram, jsonPath, "VARCHAR", true},
		{indexNgram, jsonPath, "DOUBLE", false},
		{indexNgram, intCol, "", false},

		{indexInverted, boolCol, "", true},
		{indexInverted, floatCol, "", true},
		{indexInverted, dblArray, "", true},
		{indexInverted, jsonPath, "ARRAY_VARCHAR", true},
		{indexInverted, jsonPath, "ARRAY_DOUBLE", true},
		{indexInverted, jsonPath, "", false},
		{indexInverted, column{dataType: schemapb.DataType_Geometry}, "", false},

		{indexHybrid, intCol, "", false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, indexAccepts(c.index, c.col, c.cast), "%s on %s cast=%q", c.index, c.col.dataType, c.cast)
	}
}

// TestAutoIndexResolution pins the default scalarAutoIndex.params.build
// mapping (pkg/util/paramtable/autoindex_param.go). The expected values are
// copied from there and must be updated by hand when Milvus changes them.
func TestAutoIndexResolution(t *testing.T) {
	jsonPath := column{dataType: schemapb.DataType_JSON, jsonPath: []string{"a"}}
	for _, c := range []struct {
		col  column
		cast string
		want string
	}{
		{column{dataType: schemapb.DataType_Bool}, "", indexBitmap},
		{column{dataType: schemapb.DataType_Int32}, "", indexHybrid},
		{column{dataType: schemapb.DataType_Float}, "", indexHybrid},
		{column{dataType: schemapb.DataType_VarChar}, "", indexHybrid},
		{column{dataType: schemapb.DataType_Timestamptz}, "", indexSort},
		{column{dataType: schemapb.DataType_Geometry}, "", indexRTree},
		{column{dataType: schemapb.DataType_Array, elementType: schemapb.DataType_Bool}, "", indexBitmap},
		{column{dataType: schemapb.DataType_Array, elementType: schemapb.DataType_Int64}, "", indexHybrid},
		{column{dataType: schemapb.DataType_Array, elementType: schemapb.DataType_Geometry}, "", ""},
		{jsonPath, "VARCHAR", indexHybrid},
		{jsonPath, "ARRAY_DOUBLE", ""},
		{column{dataType: schemapb.DataType_JSON}, "", ""},
		{column{dataType: schemapb.DataType_FloatVector}, "", ""},
	} {
		assert.Equal(t, c.want, autoIndexResolution(c.col, c.cast), "%s cast=%q", c.col.dataType, c.cast)
	}
}
