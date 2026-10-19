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

// This file holds the Milvus facts the rules rely on: which index types Milvus
// accepts on a column, and what AUTOINDEX builds. Each fact cites the Milvus
// code it mirrors. The values are copied by hand, and tests pin them, so a
// change in Milvus must be mirrored here and in the tests.

import (
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	indexAuto     = "AUTOINDEX"
	indexBitmap   = "BITMAP"
	indexNgram    = "NGRAM"
	indexInverted = "INVERTED"
	indexHybrid   = "HYBRID"
	indexSort     = "STL_SORT"
	indexRTree    = "RTREE"
)

// Documentation each candidate cites as its source.
const (
	docScalarIndex = "https://milvus.io/docs/index-scalar-fields.md"
	docBitmap      = "https://milvus.io/docs/bitmap.md"
	docNgram       = "https://milvus.io/docs/ngram.md"
	docJSONIndex   = "https://milvus.io/docs/json-indexing.md"
)

// bitmapMaxDistinct is the cardinality below which BITMAP is a candidate:
// "bitmap indexes are most effective when the cardinality of a field is less
// than 500" (docBitmap).
const bitmapMaxDistinct = 500

// hybridBitmapLimit is the default bitmap_cardinality_limit of the HYBRID index
// that AUTOINDEX builds for most types (DefaultBitmapCardinalityLimit in
// pkg/util/paramtable/autoindex_param.go).
const hybridBitmapLimit = 100

// Suggested NGRAM parameters. A LIKE filter with a fixed part shorter than
// min_gram cannot use the index and falls back to a full scan (docNgram).
const (
	ngramMinGram = 2
	ngramMaxGram = 3
)

// columnKind is the type class rules are written against. Every path inside a
// JSON field is kindJSONPath; its json_cast_type says how it is indexed.
type columnKind string

const (
	kindBool        columnKind = "bool"
	kindInt         columnKind = "int"
	kindFloat       columnKind = "float"
	kindString      columnKind = "string"
	kindTimestamptz columnKind = "timestamptz"
	kindArray       columnKind = "array"
	kindJSONPath    columnKind = "json_path"
	kindGeometry    columnKind = "geometry"
	kindOther       columnKind = "other"
)

func kindOf(col column) columnKind {
	t := col.dataType
	switch {
	case typeutil.IsJSONType(t):
		if len(col.jsonPath) == 0 {
			return kindOther // a whole JSON document, not a path
		}
		return kindJSONPath
	case typeutil.IsBoolType(t):
		return kindBool
	case typeutil.IsIntegerType(t):
		return kindInt
	case typeutil.IsFloatingType(t):
		return kindFloat
	case typeutil.IsStringType(t):
		return kindString
	case typeutil.IsTimestamptzType(t):
		return kindTimestamptz
	case typeutil.IsArrayType(t):
		return kindArray
	case typeutil.IsGeometryType(t):
		return kindGeometry
	default:
		return kindOther
	}
}

// jsonCastType maps the value kind of a predicate on a JSON path to the
// json_cast_type an index on that path needs; "" when it cannot be inferred.
func jsonCastType(valueKind string) string {
	switch valueKind {
	case "bool":
		return "BOOL"
	case "int", "float":
		return "DOUBLE"
	case "string":
		return "VARCHAR"
	case "array_bool":
		return "ARRAY_BOOL"
	case "array_int", "array_float":
		return "ARRAY_DOUBLE"
	case "array_string":
		return "ARRAY_VARCHAR"
	default:
		return ""
	}
}

// isArrayCast reports whether castType indexes the elements of a JSON array.
func isArrayCast(castType string) bool {
	return strings.HasPrefix(castType, "ARRAY_")
}

// autoIndexResolution returns the index type Milvus builds when AUTOINDEX is
// requested on col, under the default scalarAutoIndex.params.build
// (pkg/util/paramtable/autoindex_param.go, resolved in
// internal/proxy/ddl/task_index.go); "" when AUTOINDEX is not accepted. An
// array resolves by its element type. A JSON path needs a json_cast_type that
// HYBRID accepts (validHYBRIDJSONCastTypes in
// internal/util/indexparamcheck/hybrid_index_checker.go).
func autoIndexResolution(col column, castType string) string {
	byType := func(t schemapb.DataType) string {
		switch {
		case typeutil.IsBoolType(t):
			return indexBitmap
		case typeutil.IsIntegerType(t), typeutil.IsFloatingType(t), typeutil.IsStringType(t):
			return indexHybrid
		default:
			return ""
		}
	}
	switch kindOf(col) {
	case kindBool, kindInt, kindFloat, kindString:
		return byType(col.dataType)
	case kindArray:
		return byType(col.elementType)
	case kindTimestamptz:
		return indexSort
	case kindGeometry:
		return indexRTree
	case kindJSONPath:
		if lo.Contains([]string{"BOOL", "DOUBLE", "VARCHAR"}, castType) {
			return indexHybrid
		}
	}
	return ""
}

// indexAccepts reports whether Milvus accepts an index of type indexType on col
// with the given json_cast_type (for JSON paths), for the specific types this
// tool can suggest. It mirrors CheckValidDataType and the JSON cast checks in
// internal/util/indexparamcheck (that package is not imported because it links
// the C++ core through internal/util/cgo):
//   - bitmap_index_checker.go: not a primary key; bool, integer, string, and
//     arrays of those; JSON casts BOOL, VARCHAR
//   - ngram_index_checker.go: string; JSON cast VARCHAR
//   - inverted_checker.go: bool, numeric, string, array; JSON casts BOOL,
//     DOUBLE, VARCHAR, ARRAY_BOOL, ARRAY_DOUBLE, ARRAY_VARCHAR, JSON
func indexAccepts(indexType string, col column, castType string) bool {
	t, elem := col.dataType, col.elementType
	isJSON := typeutil.IsJSONType(t)
	switch indexType {
	case indexBitmap:
		if col.isPrimaryKey {
			return false
		}
		if isJSON {
			return lo.Contains([]string{"BOOL", "VARCHAR"}, castType)
		}
		scalar := func(dt schemapb.DataType) bool {
			return typeutil.IsBoolType(dt) || typeutil.IsIntegerType(dt) || typeutil.IsStringType(dt)
		}
		return scalar(t) || (typeutil.IsArrayType(t) && scalar(elem))
	case indexNgram:
		if isJSON {
			return castType == "VARCHAR"
		}
		return typeutil.IsStringType(t)
	case indexInverted:
		if isJSON {
			return lo.Contains([]string{"BOOL", "DOUBLE", "VARCHAR", "ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR", "JSON"}, castType)
		}
		return typeutil.IsBoolType(t) || typeutil.IsArithmetic(t) || typeutil.IsStringType(t) || typeutil.IsArrayType(t)
	default:
		return false
	}
}

// jsonPathVersionGated reports whether an index of type indexType on a JSON
// path needs scalar index engine version
// common.MinScalarIndexVersionForJsonPathMultiType. On an older cluster,
// DataCoord rejects an explicit request for it, and builds INVERTED instead
// when AUTOINDEX resolves to it (CreateIndex in
// internal/datacoord/index_service.go).
func jsonPathVersionGated(indexType string) bool {
	return lo.Contains([]string{indexBitmap, indexHybrid, indexSort}, indexType)
}

// isArrayPosition reports whether a JSON path key is read as an array
// position: an optional sign followed by digits (milvus::IsInteger in
// internal/core/src/common/Utils.h).
func isArrayPosition(key string) bool {
	digits := key
	if strings.HasPrefix(key, "+") || strings.HasPrefix(key, "-") {
		digits = key[1:]
	}
	return digits != "" && strings.Trim(digits, "0123456789") == ""
}

// unservedOps are op classes no single-column scalar index serves, with the
// reason given when a column is filtered only by these.
var unservedOps = map[opClass]string{
	opNotIndexable: "field-to-field comparisons and arithmetic filters",
	opArrayElement: "comparisons on one element of an array, which Milvus evaluates on the raw data",
	opTextMatch:    "text_match, which uses the field's analyzer and match index (enable_match), not a scalar index",
	opOther:        "filters this tool does not classify",
}
