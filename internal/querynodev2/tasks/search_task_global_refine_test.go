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

package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mock_segcore "github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
)

// Global Refine (master PR #48895) is configured via QueryInfo.search_topk_ratio
// and refine_topk_ratio (both >= 1.0) set by the delegator's queryHook. When
// reduce runs, PlanProto.cpp parses the ratios and CanUseGlobalRefine checks
// whether any segment's index supports refine (HNSW-SQ / DISKANN / etc.).
//
// The brute-force segments used in these tests do NOT have refine-capable
// indexes, so CanUseGlobalRefine returns false and PreReduce falls back to
// the standard filter + fill_pk path — the proto fields are still parsed
// end-to-end, which is what we verify here (no crash, no assertion
// failure, results correct). Actual truncate/refine semantics are covered
// by the C++ tests (test_search_group_by.cpp) and, in follow-up work,
// integration tests against real HNSW indexes.

// TestSearchTaskGlobalRefineRatiosPassThrough runs SearchTask.Execute with
// global_refine ratios set (both > 1.0 so PlanProto treats the flag as
// enabled). Segments are brute-force so CanUseGlobalRefine returns false
// and the non-refine path runs — the test verifies the ratios propagate
// through PlanProto.cpp parsing without erroring.
func TestSearchTaskGlobalRefineRatiosPassThrough(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		nq          int64 = 1
		topK        int64 = 10
	)

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	queryReq, err := mock_segcore.GenQueryRequestWithGlobalRefine(
		ts.collection.GetCCollection(), ts.segIDs,
		nq, topK, testCollectionID,
		4.0, // searchTopkRatio
		2.0, // refineTopkRatio
	)
	require.NoError(t, err)

	task := NewSearchTask(context.Background(), ts.collection, ts.manager, queryReq, 1)
	require.NoError(t, task.PreExecute())
	require.NoError(t, task.Execute(),
		"Execute should succeed end-to-end with refine ratios in QueryInfo")

	result := task.SearchResult()
	require.NotNil(t, result)
	assert.Equal(t, nq, result.NumQueries)
	assert.Equal(t, topK, result.TopK)
	assert.NotEmpty(t, result.SlicedBlob, "should have a non-empty sliced blob")

	// Unmarshal the blob and sanity-check it is a valid SearchResultData with
	// the expected shape (topK ids and scores per NQ).
	var sliced schemapb.SearchResultData
	require.NoError(t, proto.Unmarshal(result.SlicedBlob, &sliced))
	assert.EqualValues(t, topK, sliced.TopK)
	assert.EqualValues(t, nq, sliced.NumQueries)
	require.Len(t, sliced.Topks, int(nq))
	assert.LessOrEqual(t, sliced.Topks[0], topK,
		"per-NQ result count must not exceed requested topK")
	assert.Greater(t, sliced.Topks[0], int64(0),
		"should return at least one row per NQ on non-empty segments")
}

// TestSearchTaskGlobalRefineDisabled runs the same path with ratios == 0
// (feature disabled). Guards against a regression where unset ratios might
// accidentally flip global_refine_enable on the C++ side.
func TestSearchTaskGlobalRefineDisabled(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		nq          int64 = 1
		topK        int64 = 10
	)

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	queryReq, err := mock_segcore.GenQueryRequestWithGlobalRefine(
		ts.collection.GetCCollection(), ts.segIDs,
		nq, topK, testCollectionID,
		0, // both 0 → disabled
		0,
	)
	require.NoError(t, err)

	task := NewSearchTask(context.Background(), ts.collection, ts.manager, queryReq, 1)
	require.NoError(t, task.PreExecute())
	require.NoError(t, task.Execute())

	result := task.SearchResult()
	require.NotNil(t, result)
	assert.Equal(t, topK, result.TopK)
	assert.NotEmpty(t, result.SlicedBlob)
}

// TestSearchTaskGlobalRefineInvalidRatio ensures the C++ PlanProto parser
// rejects ratios in (0, 1) — both must be >= 1.0 when enabled. This
// prevents configuration typos from silently producing wrong-shape pools.
func TestSearchTaskGlobalRefineInvalidRatio(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		nq          int64 = 1
		topK        int64 = 10
	)

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	// search_topk_ratio = 0.5 → invalid (must be >= 1.0 when > 0).
	queryReq, err := mock_segcore.GenQueryRequestWithGlobalRefine(
		ts.collection.GetCCollection(), ts.segIDs,
		nq, topK, testCollectionID,
		0.5,
		2.0,
	)
	require.NoError(t, err)

	task := NewSearchTask(context.Background(), ts.collection, ts.manager, queryReq, 1)
	require.NoError(t, task.PreExecute())
	err = task.Execute()
	require.Error(t, err, "invalid search_topk_ratio must be rejected")
	assert.Contains(t, err.Error(), "search_topk_ratio")
}

// TestSearchTaskGlobalRefineInvalidRefineRatio is the symmetric case to
// TestSearchTaskGlobalRefineInvalidRatio: refine_topk_ratio in (0, 1) must
// also be rejected by PlanProto. Without this test, a typo on the refine
// side would be silently accepted if the search side already passes.
func TestSearchTaskGlobalRefineInvalidRefineRatio(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		nq          int64 = 1
		topK        int64 = 10
	)

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	// search_topk_ratio passes validation; refine_topk_ratio = 0.5 must be rejected.
	queryReq, err := mock_segcore.GenQueryRequestWithGlobalRefine(
		ts.collection.GetCCollection(), ts.segIDs,
		nq, topK, testCollectionID,
		2.0,
		0.5,
	)
	require.NoError(t, err)

	task := NewSearchTask(context.Background(), ts.collection, ts.manager, queryReq, 1)
	require.NoError(t, task.PreExecute())
	err = task.Execute()
	require.Error(t, err, "invalid refine_topk_ratio must be rejected")
	assert.Contains(t, err.Error(), "refine_topk_ratio")
}
