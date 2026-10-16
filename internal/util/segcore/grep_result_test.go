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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const grepResultHit = `[{"content":"timeout","spans":[{"start":0,"end":7}]}]`

func grepResultRecord(t *testing.T, pool memory.Allocator, positions, details string,
	positionType, detailType arrow.DataType, oneColumn, truncated bool,
) arrow.Record {
	t.Helper()
	pos, _, err := array.FromJSON(pool, positionType, strings.NewReader(positions))
	require.NoError(t, err)
	defer pos.Release()
	hits, _, err := array.FromJSON(pool, detailType, strings.NewReader(details))
	require.NoError(t, err)
	defer hits.Release()
	fields := []arrow.Field{{Name: "positions", Type: positionType, Nullable: true}, {Name: "details", Type: detailType, Nullable: true}}
	columns := []arrow.Array{pos, hits}
	if oneColumn {
		fields, columns = fields[:1], columns[:1]
	}
	rows := int64(pos.Len())
	if truncated {
		rows = 0
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), columns, rows)
}

func TestGrepScanResultRejectsInvalidNativeResults(t *testing.T) {
	for name, tc := range map[string]struct {
		positions, details                                 string
		wrongPositions, wrongDetails, oneColumn, truncated bool
	}{
		"negative":        {positions: "[-1]", details: "[" + grepResultHit + "]"},
		"out of range":    {positions: "[4]", details: "[" + grepResultHit + "]"},
		"duplicate":       {positions: "[1,1]", details: "[" + grepResultHit + "," + grepResultHit + "]"},
		"unordered":       {positions: "[1,0]", details: "[" + grepResultHit + "," + grepResultHit + "]"},
		"too many":        {positions: "[0,1,2]", details: "[" + grepResultHit + "," + grepResultHit + "," + grepResultHit + "]"},
		"length":          {positions: "[1]", details: "[" + grepResultHit + "," + grepResultHit + "]"},
		"record length":   {positions: "[1]", details: "[" + grepResultHit + "]", truncated: true},
		"empty hit":       {positions: "[1]", details: "[[]]"},
		"null hit":        {positions: "[1]", details: "[null]"},
		"null position":   {positions: "[null]", details: "[" + grepResultHit + "]"},
		"wrong positions": {positions: `["1"]`, details: "[" + grepResultHit + "]", wrongPositions: true},
		"wrong details":   {positions: "[1]", details: "[1]", wrongDetails: true},
		"column count":    {positions: "[1]", details: "[" + grepResultHit + "]", oneColumn: true},
	} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			var positionType arrow.DataType = arrow.PrimitiveTypes.Int64
			var detailType arrow.DataType = native.GrepDetailsType()
			if tc.wrongPositions {
				positionType = arrow.BinaryTypes.String
			}
			if tc.wrongDetails {
				detailType = arrow.PrimitiveTypes.Int64
			}
			record := grepResultRecord(t, pool, tc.positions, tc.details, positionType, detailType, tc.oneColumn, tc.truncated)
			defer record.Release()
			result, err := newGrepScanResult(record, 4, 2)
			require.Nil(t, result)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestGrepScanResultOwnsBuffers(t *testing.T) {
	for _, empty := range []bool{false, true} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		positions, details := "[0,2]", "["+grepResultHit+","+grepResultHit+"]"
		if empty {
			positions, details = "[]", "[]"
		}
		record := grepResultRecord(t, pool, positions, details, arrow.PrimitiveTypes.Int64, native.GrepDetailsType(), false, false)
		result, err := newGrepScanResult(record, 4, 2)
		record.Release()
		require.NoError(t, err)
		if empty {
			require.Empty(t, result.Positions)
			require.Zero(t, result.Details.Len())
		} else {
			require.Equal(t, []int64{0, 2}, result.Positions)
			lines := result.Details.(*array.List).ListValues().(*array.Struct)
			require.Equal(t, "timeout", lines.Field(0).(*array.String).Value(0))
		}
		result.Release()
		pool.AssertSize(t, 0)
	}
}
