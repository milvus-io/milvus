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

package queryutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// GetRowCount returns the number of rows using the shared Query helper.
func GetRowCount(result *internalpb.RetrieveResults) int {
	return getRowCount(result)
}

// RowRef lets ReduceStream reuse the existing row selection without conversion.
type RowRef = rowRef

// NewRowRef identifies one row in the input result slice.
func NewRowRef(resultIdx int, rowIdx int64) RowRef {
	return rowRef{resultIdx: resultIdx, rowIdx: rowIdx}
}

// BuildMergedRetrieveResults copies selected rows using the shared Query helpers.
func BuildMergedRetrieveResults(results []*internalpb.RetrieveResults, rows []RowRef, schema *schemapb.CollectionSchema) (*internalpb.RetrieveResults, error) {
	return buildMergedRetrieveResults(results, rows, schema)
}

// ComparePK uses the existing Query primary-key ordering.
func ComparePK(a, b any) int {
	return comparePK(a, b)
}

// CompareFieldDataRows uses the existing Query scalar and NULL ordering.
func CompareFieldDataRows(left *schemapb.FieldData, leftIndex int, right *schemapb.FieldData, rightIndex int, dataType schemapb.DataType, ascending, nullsFirst bool) int {
	return compareFieldDataRows(left, leftIndex, right, rightIndex, dataType, ascending, nullsFirst)
}

// ExtractTimestamps returns the existing Query timestamp column, if present.
func ExtractTimestamps(result *internalpb.RetrieveResults) []int64 {
	return extractTimestamps(result)
}

// ValidateElementLevelConsistency applies the shared Query element checks.
func ValidateElementLevelConsistency(results []*internalpb.RetrieveResults, rows []RowRef) error {
	return validateElementLevelConsistency(results, rows)
}

// NewRowSizeCalculator prepares reusable Query output-size accounting.
func NewRowSizeCalculator(result *internalpb.RetrieveResults) *rowSizeCalculator {
	return newRowSizeCalculator(result)
}

// RowSize uses the existing Query output-size calculation for one row.
func (c *rowSizeCalculator) RowSize(rowIdx int64) int64 {
	return c.rowSize(rowIdx)
}
