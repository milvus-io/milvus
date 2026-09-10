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

package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
)

// TestTemplateParamSlicesAreAliasedNotCopied pins the lifetime contract that
// Request() documents. The typed fast paths hand the caller's backing array
// straight to the protobuf message instead of rebuilding it, which is what
// keeps a large `in {list}` from being duplicated on the heap — but it means a
// returned request is not a snapshot.
//
// This is deliberately asserted rather than merely documented: the previous
// reflect-based path DID copy, so a future change that reintroduces a copy
// would silently restore snapshot semantics and quietly undo the memory win.
// If that copy is ever wanted back, this test is the place that says so.
func TestTemplateParamSlicesAreAliasedNotCopied(t *testing.T) {
	vectors := []entity.Vector{entity.FloatVector(make([]float32, 8))}

	t.Run("int64", func(t *testing.T) {
		ids := []int64{1, 2, 3}
		req, err := NewSearchOption("c", 10, vectors).
			WithANNSField("vec").
			WithFilter("id in {ids}").
			WithTemplateParam("ids", ids).
			Request()
		require.NoError(t, err)

		got := req.GetExprTemplateValues()["ids"].GetArrayVal().GetLongData().GetData()
		require.Equal(t, ids, got)
		require.Same(t, &ids[0], &got[0],
			"the request must alias the caller's array, not copy it")

		// Mutating through the caller's slice is visible in the already-built
		// request. That is the hazard the doc comments warn about.
		ids[0] = 100
		require.Equal(t, int64(100), got[0])
	})

	t.Run("string", func(t *testing.T) {
		names := []string{"a", "b"}
		req, err := NewQueryOption("c").
			WithFilter("name in {names}").
			WithTemplateParam("names", names).
			Request()
		require.NoError(t, err)

		got := req.GetExprTemplateValues()["names"].GetArrayVal().GetStringData().GetData()
		require.Same(t, &names[0], &got[0], "string slices are aliased too")
	})

	t.Run("float64 and bool", func(t *testing.T) {
		scores := []float64{1.5, 2.5}
		flags := []bool{true, false}
		req, err := NewQueryOption("c").
			WithFilter("s in {s} and f in {f}").
			WithTemplateParam("s", scores).
			WithTemplateParam("f", flags).
			Request()
		require.NoError(t, err)

		gotS := req.GetExprTemplateValues()["s"].GetArrayVal().GetDoubleData().GetData()
		gotF := req.GetExprTemplateValues()["f"].GetArrayVal().GetBoolData().GetData()
		require.Same(t, &scores[0], &gotS[0])
		require.Same(t, &flags[0], &gotF[0])
	})

	t.Run("non-fast-path slice still copies", func(t *testing.T) {
		// []int32 has no typed fast path and falls through to the reflect
		// rebuild, so it keeps snapshot semantics. Pinned so the two behaviors
		// stay distinguishable rather than drifting into one.
		ids := []int32{7, 8}
		req, err := NewQueryOption("c").
			WithFilter("id in {ids}").
			WithTemplateParam("ids", ids).
			Request()
		require.NoError(t, err)

		got := req.GetExprTemplateValues()["ids"].GetArrayVal().GetLongData().GetData()
		require.Equal(t, []int64{7, 8}, got)
		ids[0] = 70
		require.Equal(t, int64(7), got[0],
			"a widened slice is a fresh allocation and must stay a snapshot")
	})
}
