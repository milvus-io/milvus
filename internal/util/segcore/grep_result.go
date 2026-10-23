// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// newGrepScanResult validates the native boundary once. Callers may release
// record after return: positions are copied and details retain their own reference.
func newGrepScanResult(record arrow.Record, inputRows int, limit int64) (*native.ScanResult, error) {
	if record.NumCols() != 2 || record.NumRows() > limit {
		return nil, merr.WrapErrServiceInternalMsg("invalid native grep result schema or row count")
	}
	positions, positionsOK := record.Column(0).(*array.Int64)
	details, detailsOK := record.Column(1).(*array.Struct)
	if !positionsOK || !detailsOK || !arrow.TypeEqual(details.DataType(), native.GrepDetailsType()) {
		return nil, merr.WrapErrServiceInternalMsg("invalid native grep result column types")
	}
	if positions.NullN() != 0 || details.NullN() != 0 ||
		int64(positions.Len()) != record.NumRows() || details.Len() != positions.Len() {
		return nil, merr.WrapErrServiceInternalMsg("native grep result columns have inconsistent rows or nulls")
	}
	selected := make([]int, positions.Len())
	previous := int64(-1)
	for i, position := range positions.Int64Values() {
		if position <= previous || position >= int64(inputRows) {
			return nil, merr.WrapErrServiceInternalMsg("grep runner positions must be increasing input positions")
		}
		// inputRows is an int; the validated range guarantees conversion fits.
		selected[i] = int(position)
		previous = position
	}
	if err := validateGrepDetails(details); err != nil {
		return nil, err
	}
	details.Retain()
	return &native.ScanResult{Positions: selected, Details: details}, nil
}

// validateGrepDetails checks the one native result layout before it enters chain.
// The exact schema was checked by newGrepScanResult, so typed child access is safe.
func validateGrepDetails(details *array.Struct) error {
	contentRows := details.Field(0).(*array.List)
	startRows := details.Field(1).(*array.List)
	endRows := details.Field(2).(*array.List)
	content := contentRows.ListValues().(*array.String)
	starts := startRows.ListValues().(*array.List)
	ends := endRows.ListValues().(*array.List)
	a, b := starts.ListValues().(*array.Int64), ends.ListValues().(*array.Int64)
	for row := 0; row < details.Len(); row++ {
		if contentRows.IsNull(row) || startRows.IsNull(row) || endRows.IsNull(row) {
			return merr.WrapErrServiceInternalMsg("null native grep document child")
		}
		contentBegin, contentEnd := contentRows.ValueOffsets(row)
		startBegin, startEnd := startRows.ValueOffsets(row)
		endBegin, endEnd := endRows.ValueOffsets(row)
		count := contentEnd - contentBegin
		if count == 0 {
			return merr.WrapErrServiceInternalMsg("grep runner selected a row without matching lines")
		}
		if count != startEnd-startBegin || count != endEnd-endBegin {
			return merr.WrapErrServiceInternalMsg("native grep document child counts differ")
		}
		for line := int64(0); line < count; line++ {
			ci, si, ei := int(contentBegin+line), int(startBegin+line), int(endBegin+line)
			if content.IsNull(ci) || starts.IsNull(si) || ends.IsNull(ei) {
				return merr.WrapErrServiceInternalMsg("null native grep detail value")
			}
			first, last := starts.ValueOffsets(si)
			endFirst, endLast := ends.ValueOffsets(ei)
			if last-first != endLast-endFirst {
				return merr.WrapErrServiceInternalMsg("native grep span boundary counts differ")
			}
			for j := int64(0); j < last-first; j++ {
				ai, bi := int(first+j), int(endFirst+j)
				if a.IsNull(ai) || b.IsNull(bi) {
					return merr.WrapErrServiceInternalMsg("null native grep span")
				}
				if a.Value(ai) < 0 || a.Value(ai) > b.Value(bi) {
					return merr.WrapErrServiceInternalMsg("invalid native grep span boundary")
				}
			}
		}
	}
	return nil
}
