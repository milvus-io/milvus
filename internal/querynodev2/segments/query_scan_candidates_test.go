// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestImportQueryScanCandidates(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mode           bool
		listType       arrow.DataType
		offsets, lists string
		wantError      bool
	}{
		{"document", false, nil, `[100,200]`, "", false},
		{"elements", true, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[100,200]`, `[[1,3],[0,2,5]]`, false},
		{"empty elements", true, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[]`, `[]`, false},
		{"missing list", true, nil, `[100]`, "", true},
		{"wrong list type", true, arrow.PrimitiveTypes.Int32, `[100]`, `[1]`, true},
		{"wrong child type", true, arrow.ListOf(arrow.PrimitiveTypes.Int64), `[100]`, `[[1]]`, true},
		{"null list", true, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[100]`, `[null]`, true},
		{"null offset", true, arrow.ListOf(arrow.PrimitiveTypes.Int32), `[null]`, `[[1]]`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			offsets, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int64, strings.NewReader(tc.offsets))
			require.NoError(t, err)
			fields := []arrow.Field{{Name: types.SegOffsetFieldName, Type: offsets.DataType()}}
			cols := []arrow.Array{offsets}
			if tc.listType != nil {
				lists, _, err := array.FromJSON(pool, tc.listType, strings.NewReader(tc.lists))
				require.NoError(t, err)
				fields = append(fields, arrow.Field{Name: types.QueryElementIndicesFieldName, Type: tc.listType})
				cols = append(cols, lists)
			}
			record := array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(offsets.Len()))
			for _, col := range cols {
				col.Release()
			}
			input := RetrieveSegmentResult{Result: &segcorepb.RetrieveResults{ElementLevel: tc.mode}, Record: record}
			rows := record.NumRows()
			frame, err := importQueryScanCandidates(input)
			record.Release()
			if tc.wantError {
				require.Nil(t, frame)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				return
			}
			require.NoError(t, err)
			defer frame.Release()
			require.Equal(t, tc.mode, frame.HasColumn(types.QueryElementIndicesFieldName))
			// The imported arrays outlive the source record.
			require.Equal(t, rows, frame.NumRows())
			if tc.mode {
				want, _, err := array.FromJSON(pool, tc.listType, strings.NewReader(tc.lists))
				require.NoError(t, err)
				defer want.Release()
				require.True(t, array.Equal(want, frame.Column(types.QueryElementIndicesFieldName).Chunk(0)))
			}
		})
	}
}
