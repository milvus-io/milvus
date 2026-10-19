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

package arrowconv

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAlignComputedStructArrayByName(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	content := arrow.Field{Name: "content", Type: arrow.ListOf(arrow.BinaryTypes.String)}
	starts := arrow.Field{Name: "starts", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))}
	ends := starts
	ends.Name = "ends"
	target := arrow.StructOf(content, starts, ends)
	source := arrow.StructOf(ends, content, starts)
	rows, _, err := array.FromJSON(pool, source, strings.NewReader(`[
 {"content":["skip"],"starts":[[0]],"ends":[[1]]},
 {"content":["hit"],"starts":[[3,3]],"ends":[[7,9]]},null]`))
	require.NoError(t, err)
	sliced := array.NewSlice(rows, 1, 3)
	rows.Release()
	got, err := AlignComputedStructArray(sliced, target)
	require.NoError(t, err)
	// All payload buffers and the sliced parent bitmap remain shared.
	require.Same(t, sliced.Data().Buffers()[0], got.Data().Buffers()[0])
	require.Same(t, sliced.Data().Children()[1], got.Data().Children()[0])
	require.True(t, arrow.TypeEqual(source, sliced.DataType()))
	sliced.Release()
	defer got.Release()
	want, _, err := array.FromJSON(pool, target, strings.NewReader(`[{"content":["hit"],"starts":[[3,3]],"ends":[[7,9]]},null]`))
	require.NoError(t, err)
	defer want.Release()
	require.True(t, array.Equal(want, got))
	empty := array.NewSlice(got, 0, 0)
	defer empty.Release()
	alignedEmpty, err := AlignComputedStructArray(empty, target)
	require.NoError(t, err)
	defer alignedEmpty.Release()
	require.Zero(t, alignedEmpty.Len())
	require.True(t, arrow.TypeEqual(target, alignedEmpty.DataType()))

	for _, bad := range []*arrow.StructType{
		arrow.StructOf(content, starts),
		arrow.StructOf(content, starts, starts),
		arrow.StructOf(content, starts, arrow.Field{Name: "other", Type: ends.Type}),
		arrow.StructOf(content, starts, arrow.Field{Name: "ends", Type: arrow.ListOf(arrow.BinaryTypes.String)}),
	} {
		result, err := AlignComputedStructArray(got, bad)
		require.Nil(t, result)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}
