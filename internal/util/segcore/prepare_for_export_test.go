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

package segcore_test

import (
	"context"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Functional correctness of PrepareSearchResultsForExport (filter + fill_pk +
// optional truncate/refine) is covered by the C++ unittest
// SearchResultExport.PrepareSearchResultsForExport_* which exercises the full
// path via real segments. The Go-level tests below focus on the wrapper's
// Go-side validation — nil / empty / unaligned inputs that are rejected
// before reaching the CGO boundary.

func TestPrepareSearchResultsForExport_NilPlan(t *testing.T) {
	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		nil,
		segcore.NewDummyPlaceholderGroupForTest(),
		[]*segcore.SearchResult{{}},
		[]int64{1},
		[]int64{10},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "nil search plan")
}

func TestPrepareSearchResultsForExport_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := segcore.PrepareSearchResultsForExport(
		ctx,
		nil,
		unsafe.Pointer(nil),
		nil,
		nil,
		nil,
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPrepareSearchResultsForExport_NilPlaceholderGroup(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		plan,
		unsafe.Pointer(nil),
		[]*segcore.SearchResult{{}},
		[]int64{1},
		[]int64{10},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "nil placeholder group")
}

func TestPrepareSearchResultsForExport_EmptyResults(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		plan,
		segcore.NewDummyPlaceholderGroupForTest(),
		nil,
		[]int64{1},
		[]int64{10},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "empty search results")
}

func TestPrepareSearchResultsForExport_UnalignedSlices(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		plan,
		segcore.NewDummyPlaceholderGroupForTest(),
		[]*segcore.SearchResult{{}},
		[]int64{1, 2},
		[]int64{10},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "unaligned slice")
}

func TestPrepareSearchResultsForExport_EmptySlices(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		plan,
		segcore.NewDummyPlaceholderGroupForTest(),
		[]*segcore.SearchResult{{}},
		nil,
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "unaligned slice")
}

func TestPrepareSearchResultsForExport_NilResultInSlice(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		plan,
		segcore.NewDummyPlaceholderGroupForTest(),
		[]*segcore.SearchResult{nil},
		[]int64{1},
		[]int64{10},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "nil search result at index 0")
}

func TestFillOutputFieldsOrdered_EmptyResults(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.FillOutputFieldsOrdered(nil, plan, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty search results")
}

func TestFillOutputFieldsOrdered_UnalignedSources(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.FillOutputFieldsOrdered(
		[]*segcore.SearchResult{{}},
		plan,
		[]int32{0},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unaligned segment indices")
}

func TestFillOutputFieldsOrdered_NilResultInSlice(t *testing.T) {
	plan := segcore.NewDummySearchPlanForTest(t)
	_, err := segcore.FillOutputFieldsOrdered([]*segcore.SearchResult{nil}, plan, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil search result at index 0")
}
