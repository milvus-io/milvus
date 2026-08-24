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

package taskresource

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// FixedWidthFieldBytes returns the exact uncompressed bytes a field occupies
// for numRows rows, or 0 when its type has no closed form.
//
// It matters far beyond a convenience: this is the ONLY field-sizing route
// that survives storage V3. V3 persists SegmentInfo.Stats but not the
// per-FieldBinlog arrays, so for a V3 segment loaded after a restart there is
// no per-field byte figure anywhere in the metadata -- on either side of the
// RPC. A type whose size is a function of the row count alone does not need
// one.
//
// Returning 0 for the variable-width types (VarChar, JSON, Array, and the
// vector types whose per-row width is not fixed) is the caller's signal to
// fall back, not a claim that the field is empty.
func FixedWidthFieldBytes(field *schemapb.FieldSchema, numRows int64) int64 {
	if numRows <= 0 {
		return 0
	}

	dataType := field.GetDataType()
	if typeutil.IsFixDimVectorType(dataType) {
		dim, err := typeutil.GetDim(field)
		if err != nil || dim <= 0 {
			return 0
		}
		return vectorFieldByteSize(dataType, dim, numRows)
	}

	if width := fixedScalarWidth(dataType); width > 0 {
		return width * numRows
	}
	return 0
}

// fixedScalarWidth is the per-row byte width of a scalar type, or 0 for the
// variable-width ones. Nullable fields also carry a validity bit per row; it
// is under a thousandth of any of these widths and is left out rather than
// pretending to a precision the rest of the estimate does not have.
func fixedScalarWidth(dataType schemapb.DataType) int64 {
	switch dataType {
	case schemapb.DataType_Bool, schemapb.DataType_Int8:
		return 1
	case schemapb.DataType_Int16:
		return 2
	case schemapb.DataType_Int32, schemapb.DataType_Float:
		return 4
	case schemapb.DataType_Int64, schemapb.DataType_Double, schemapb.DataType_Timestamptz:
		return 8
	default:
		// VarChar, String, JSON, Array, ArrayOfVector, SparseFloatVector, Text:
		// no closed form.
		return 0
	}
}

// StatsTouchedFieldIDs returns the fields a stats sub-job actually reads, and
// whether the sub-job was recognized at all.
//
// The selection mirrors internal/datanode/index/task_stats.go's Execute: a
// text-index job touches the fields with EnableMatch, a json-key job those
// with EnableJSONKeyStatsIndex, and a BM25 job the sparse-vector output field
// of a BM25 function. Sort is deliberately NOT here -- it is priced as
// CompactionType_SortCompaction through EstimateCompaction, not as a stats job.
//
// recognized=false means "no known field subset", which the caller must answer
// by charging the whole segment. It is not the same as a recognized sub-job
// that matched no field, and collapsing the two would turn an unknown future
// sub-job into a free one.
func StatsTouchedFieldIDs(subJob indexpb.StatsSubJob, schema *schemapb.CollectionSchema) (map[int64]bool, bool) {
	var matches func(*schemapb.FieldSchema) bool
	switch subJob {
	case indexpb.StatsSubJob_TextIndexJob:
		matches = func(f *schemapb.FieldSchema) bool {
			return typeutil.CreateFieldSchemaHelper(f).EnableMatch()
		}
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		matches = func(f *schemapb.FieldSchema) bool {
			return typeutil.CreateFieldSchemaHelper(f).EnableJSONKeyStatsIndex()
		}
	case indexpb.StatsSubJob_BM25Job:
		matches = func(f *schemapb.FieldSchema) bool {
			return typeutil.IsBM25FunctionOutputField(f, schema)
		}
	default:
		return nil, false
	}

	ids := make(map[int64]bool)
	for _, f := range typeutil.GetAllFieldSchemas(schema) {
		if matches(f) {
			ids[f.GetFieldID()] = true
		}
	}
	return ids, true
}

// SumFieldBinlogMemoryForFieldSet sums Binlog.MemorySize across every entry in
// logs carrying at least one field in ids -- by its own FieldID on storage V1,
// or through ChildFields on the packed layouts.
//
// An entry is counted ONCE however many of its members are in the set: a
// column group is one set of binlogs, and adding it per matching child would
// multiply the same bytes by the number of fields sharing the group.
//
// Returns 0 when the arrays carry nothing, which on V3 is the normal case
// rather than an error; the caller falls back from there.
func SumFieldBinlogMemoryForFieldSet(logs []*datapb.FieldBinlog, ids map[int64]bool) int64 {
	return sumFieldBinlogMemoryForFieldSet(logs, ids)
}

// SumFieldBinlogMemoryForField sums Binlog.MemorySize for the first FieldBinlog
// carrying fieldID. Returns 0 when the field is not present in the arrays.
func SumFieldBinlogMemoryForField(logs []*datapb.FieldBinlog, fieldID int64) int64 {
	return sumFieldBinlogMemoryForField(logs, fieldID)
}

// VectorFieldByteSize is the closed form for a fixed-width vector type:
// Dim x rows x the per-element width the build path itself applies. It returns
// 0 for a type with no closed form, which the caller must read as "fall back",
// not as "empty".
func VectorFieldByteSize(dataType schemapb.DataType, dim, numRows int64) int64 {
	return vectorFieldByteSize(dataType, dim, numRows)
}
