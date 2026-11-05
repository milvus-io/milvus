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
	"encoding/json"
	"unicode/utf8"

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
	details, detailsOK := record.Column(1).(*array.Binary)
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

// JSON is validated only at the native boundary. Later reducers treat each
// document as opaque bytes and never decode/re-encode its matching details.
func validateGrepDetails(details *array.Binary) error {
	for row := 0; row < details.Len(); row++ {
		var lines []*struct {
			Content *string  `json:"content"`
			Starts  []*int64 `json:"starts"`
			Ends    []*int64 `json:"ends"`
		}
		payload := details.Value(row)
		if !utf8.Valid(payload) {
			return merr.WrapErrServiceInternalMsg("invalid UTF-8 in native grep JSON")
		}
		if err := json.Unmarshal(payload, &lines); err != nil {
			return merr.WrapErrServiceInternalErr(err, "invalid native grep JSON")
		}
		if len(lines) == 0 {
			return merr.WrapErrServiceInternalMsg("grep runner selected a row without matching lines")
		}
		for _, line := range lines {
			if line == nil || line.Content == nil || line.Starts == nil || line.Ends == nil {
				return merr.WrapErrServiceInternalMsg("missing or null native grep detail value")
			}
			if len(line.Starts) != len(line.Ends) {
				return merr.WrapErrServiceInternalMsg("native grep span boundary counts differ")
			}
			for i, start := range line.Starts {
				if start == nil || line.Ends[i] == nil {
					return merr.WrapErrServiceInternalMsg("null native grep span")
				}
				if *start < 0 || *start > *line.Ends[i] {
					return merr.WrapErrServiceInternalMsg("invalid native grep span boundary")
				}
			}
		}
	}
	return nil
}
