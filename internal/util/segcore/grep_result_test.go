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

	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const grepResultHit = `{"content":["timeout"],"starts":[[0]],"ends":[[7]]}`

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
		positions, details                                                  string
		wrongPositions, wrongDetails, unsignedDetails, oneColumn, truncated bool
	}{
		"negative start":          {positions: "[0]", details: `[{"content":["x"],"starts":[[-1]],"ends":[[1]]}]`},
		"negative end":            {positions: "[0]", details: `[{"content":["x"],"starts":[[0]],"ends":[[-1]]}]`},
		"reversed span":           {positions: "[0]", details: `[{"content":["x"],"starts":[[2]],"ends":[[1]]}]`},
		"span count":              {positions: "[0]", details: `[{"content":["x"],"starts":[[0]],"ends":[[]]}]`},
		"null span":               {positions: "[0]", details: `[{"content":["x"],"starts":[[null]],"ends":[[1]]}]`},
		"null line":               {positions: "[0]", details: `[{"content":[null],"starts":[[]],"ends":[[]]}]`},
		"null content array":      {positions: "[0]", details: `[{"content":null,"starts":[[0]],"ends":[[1]]}]`},
		"null starts array":       {positions: "[0]", details: `[{"content":["x"],"starts":null,"ends":[[1]]}]`},
		"null ends array":         {positions: "[0]", details: `[{"content":["x"],"starts":[[0]],"ends":null}]`},
		"null line starts":        {positions: "[0]", details: `[{"content":["x"],"starts":[null],"ends":[[1]]}]`},
		"null end":                {positions: "[0]", details: `[{"content":["x"],"starts":[[0]],"ends":[[null]]}]`},
		"document child count":    {positions: "[0]", details: `[{"content":["x","y"],"starts":[[0]],"ends":[[1],[1]]}]`},
		"shifted document counts": {positions: "[0,1]", details: `[{"content":["x"],"starts":[[0],[0]],"ends":[[1]]},{"content":["y","z"],"starts":[[0]],"ends":[[1],[1]]}]`},
		"huge position":           {positions: "[9223372036854775807]", details: "[" + grepResultHit + "]"},
		"negative":                {positions: "[-1]", details: "[" + grepResultHit + "]"},
		"out of range":            {positions: "[4]", details: "[" + grepResultHit + "]"},
		"duplicate":               {positions: "[1,1]", details: "[" + grepResultHit + "," + grepResultHit + "]"},
		"unordered":               {positions: "[1,0]", details: "[" + grepResultHit + "," + grepResultHit + "]"},
		"too many":                {positions: "[0,1,2]", details: "[" + grepResultHit + "," + grepResultHit + "," + grepResultHit + "]"},
		"length":                  {positions: "[1]", details: "[" + grepResultHit + "," + grepResultHit + "]"},
		"record length":           {positions: "[1]", details: "[" + grepResultHit + "]", truncated: true},
		"empty hit":               {positions: "[1]", details: `[{"content":[],"starts":[],"ends":[]}]`},
		"null hit":                {positions: "[1]", details: "[null]"},
		"null position":           {positions: "[null]", details: "[" + grepResultHit + "]"},
		"wrong positions":         {positions: `["1"]`, details: "[" + grepResultHit + "]", wrongPositions: true},
		"unsigned details":        {positions: "[1]", details: "[" + grepResultHit + "]", unsignedDetails: true},
		"wrong details":           {positions: "[1]", details: "[1]", wrongDetails: true},
		"column count":            {positions: "[1]", details: "[" + grepResultHit + "]", oneColumn: true},
	} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			var positionType arrow.DataType = arrow.PrimitiveTypes.Int64
			var detailType arrow.DataType = native.GrepDetailsType()
			if tc.wrongPositions {
				positionType = arrow.BinaryTypes.String
			}
			if tc.unsignedDetails {
				bounds := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Uint64})
				detailType = arrow.StructOf(
					arrow.Field{Name: "content", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String})},
					arrow.Field{Name: "starts", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: bounds})},
					arrow.Field{Name: "ends", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: bounds})},
				)
			}
			if tc.wrongDetails {
				detailType = arrow.PrimitiveTypes.Int64
			}
			record := grepResultRecord(t, pool, tc.positions, tc.details, positionType, detailType, tc.oneColumn, tc.truncated)
			defer record.Release()
			result, err := newGrepScanResult(record, 4, 2)
			require.Nil(t, result)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
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
			require.Equal(t, []int{0, 2}, result.Positions)
			lines := result.Details.(*array.Struct)
			require.Equal(t, "timeout", lines.Field(0).(*array.List).ListValues().(*array.String).Value(0))
		}
		result.Release()
		pool.AssertSize(t, 0)
	}
}

func TestGrepScanResultSlicedStructArrayExport(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	record := grepResultRecord(t, pool, `[3,0,2]`, `[
 {"content":["skip"],"starts":[[0]],"ends":[[4]]},
 {"content":["foo bar foo","hello foo"],"starts":[[0,8],[6]],"ends":[[3,11],[9]]},
 {"content":["foo"],"starts":[[0]],"ends":[[3]]}]`, arrow.PrimitiveTypes.Int64, native.GrepDetailsType(), false, false)
	sliced := record.NewSlice(1, 3)
	record.Release()
	result, err := newGrepScanResult(sliced, 4, 2)
	sliced.Release()
	require.NoError(t, err)
	column := arrow.NewChunked(result.Details.DataType(), []arrow.Array{result.Details})
	result.Release()
	exported, err := arrowconv.ArrowChunkedStructColumnToFieldData("matches", column, nil)
	column.Release()
	require.NoError(t, err)
	fields := exported.GetStructArrays().Fields
	require.Equal(t, []string{"foo bar foo", "hello foo"}, fields[0].GetScalars().GetArrayData().Data[0].GetStringData().Data)
	require.Equal(t, []int64{0, 8}, fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().Data[0].GetLongData().Data)
	require.Equal(t, []int64{9}, fields[2].GetScalars().GetArrayData().Data[0].GetArrayData().Data[1].GetLongData().Data)
	require.Equal(t, []string{"foo"}, fields[0].GetScalars().GetArrayData().Data[1].GetStringData().Data)
}
