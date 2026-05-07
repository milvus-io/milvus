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
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mock_segcore "github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// testSegments holds pre-created segments and search results for testing.
type testSegments struct {
	manager       *segments.Manager
	collection    *segments.Collection
	segs          []segments.Segment
	segIDs        []int64
	insertData    []*storage.InsertData // one per segment, aligned with segIDs
	searchResults []*segcore.SearchResult
	searchReq     *segcore.SearchRequest
	chunkManager  storage.ChunkManager
	rootPath      string
}

func (ts *testSegments) cleanup() {
	for _, r := range ts.searchResults {
		r.Release()
	}
	if ts.searchReq != nil {
		ts.searchReq.Delete()
	}
	for _, seg := range ts.segs {
		seg.Release(context.Background())
	}
	ts.manager.Collection.Unref(ts.collection.ID(), 1)
	ts.chunkManager.RemoveWithPrefix(context.Background(), ts.rootPath)
}

// testCollectionID / testPartitionID are shared across every helper and test
// in this file. They must stay aligned with the CollectionID callers pass into
// mock_segcore.GenQueryRequest / GenFilterOnlySearchRequests.
const (
	testCollectionID int64 = 100
	testPartitionID  int64 = 10
)

// setupOpts varies the search request shape across test cases.
// At most one of OutputFieldIDs / GroupByFieldID should be set.
// When SkipSearchReq is true NQ/TopK are ignored and no SearchRequest is built.
// NullableVec marks the float_vector field as Nullable so the insert data
// generator stamps ValidData using mock_segcore.NullablePatternValidData —
// required to cover the nullable-vector output path.
type setupOpts struct {
	NQ             int64
	TopK           int64
	OutputFieldIDs []int64
	GroupByFieldID int64
	GroupSize      int64
	FilterOnly     bool
	SkipSearchReq  bool
	NullableVec    bool
}

func setupTestSegments(t *testing.T, numSegments int, msgLength int, opts setupOpts) *testSegments {
	t.Helper()
	require.False(t, opts.GroupByFieldID > 0 && len(opts.OutputFieldIDs) > 0,
		"setupOpts: OutputFieldIDs and GroupByFieldID are mutually exclusive")

	paramtable.Init()

	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	if err := initcore.InitMmapManager(paramtable.Get(), 1); err != nil {
		t.Fatal(err)
	}
	if err := initcore.InitTieredStorage(paramtable.Get()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	rootPath := t.Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), rootPath)
	chunkManager, _ := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	collectionID := testCollectionID
	partitionID := testPartitionID

	var schema *schemapb.CollectionSchema
	if opts.NullableVec {
		schema = mock_segcore.GenTestCollectionSchemaWithNullableVec("test-late-mat", schemapb.DataType_Int64)
	} else {
		schema = mock_segcore.GenTestCollectionSchema("test-late-mat", schemapb.DataType_Int64, true)
	}
	indexMeta := mock_segcore.GenTestIndexMeta(collectionID, schema)

	manager := segments.NewManager()
	manager.Collection.PutOrRef(collectionID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: collectionID,
		PartitionIDs: []int64{partitionID},
	})
	collection := manager.Collection.Get(collectionID)

	ts := &testSegments{
		manager:      manager,
		collection:   collection,
		chunkManager: chunkManager,
		rootPath:     rootPath,
		segIDs:       make([]int64, numSegments),
		insertData:   make([]*storage.InsertData, numSegments),
	}

	for i := 0; i < numSegments; i++ {
		segmentID := int64(i + 1)
		ts.segIDs[i] = segmentID

		seg, err := segments.NewSegment(ctx, collection, manager.Segment,
			segments.SegmentTypeSealed, 0,
			&querypb.SegmentLoadInfo{
				SegmentID:     segmentID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     int64(msgLength),
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID),
				Level:         datapb.SegmentLevel_Legacy,
			})
		if err != nil {
			t.Fatal(err)
		}

		// Generate + save via the explicit helper so tests that need to
		// verify per-row data (e.g., nullable vector output) can inspect
		// ts.insertData[i] as ground truth.
		insertData, err := mock_segcore.GenInsertData(msgLength, schema)
		if err != nil {
			t.Fatal(err)
		}
		ts.insertData[i] = insertData
		binlogs, _, err := mock_segcore.SaveBinLogWithData(ctx,
			collectionID, partitionID, segmentID, insertData, schema, chunkManager)
		if err != nil {
			t.Fatal(err)
		}

		// Transition segment state from OnlyMeta → DataLoaded so subsequent
		// operations see it as "loaded". LoadFieldData itself does not drive
		// this transition; production code goes through Loader.loadSegment,
		// which wraps the loads in StartLoadData() / guard.Done().
		guard, err := seg.(*segments.LocalSegment).StartLoadData()
		if err != nil {
			t.Fatal(err)
		}
		for _, binlog := range binlogs {
			if err := seg.(*segments.LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog); err != nil {
				t.Fatal(err)
			}
		}
		guard.Done(nil)

		manager.Segment.Put(ctx, segments.SegmentTypeSealed, seg)
		ts.segs = append(ts.segs, seg)
	}

	if opts.SkipSearchReq {
		return ts
	}

	var searchReq *segcore.SearchRequest
	var err error
	switch {
	case opts.FilterOnly:
		searchReq, err = mock_segcore.GenSearchPlanAndRequestsFilterOnly(
			collection.GetCCollection(), ts.segIDs, opts.NQ, opts.TopK)
	case opts.GroupByFieldID > 0:
		searchReq, err = mock_segcore.GenSearchPlanAndRequestsWithGroupBy(
			collection.GetCCollection(), ts.segIDs, opts.NQ, opts.TopK, opts.GroupByFieldID, opts.GroupSize)
	case len(opts.OutputFieldIDs) > 0:
		searchReq, err = mock_segcore.GenSearchPlanAndRequestsWithOutputFields(
			collection.GetCCollection(), ts.segIDs, opts.NQ, opts.TopK, opts.OutputFieldIDs)
	default:
		searchReq, err = mock_segcore.GenSearchPlanAndRequestsWithTopK(
			collection.GetCCollection(), ts.segIDs, opts.NQ, opts.TopK)
	}
	if err != nil {
		t.Fatal(err)
	}
	ts.searchReq = searchReq

	for _, seg := range ts.segs {
		result, err := seg.Search(ctx, searchReq)
		if err != nil {
			t.Fatal(err)
		}
		ts.searchResults = append(ts.searchResults, result)
	}

	return ts
}

// runGoReducePipeline runs the same pipeline as executeGoReduce
// and returns the merged mergeResult and DataFrames (caller must release).
// Group-by is auto-detected from the first SearchResult, mirroring production.
func runGoReducePipeline(t *testing.T, ts *testSegments) (*mergeResult, []*chain.DataFrame) {
	t.Helper()

	pool := memory.NewGoAllocator()
	plan := ts.searchReq.Plan()
	topK := plan.GetTopK()

	// Mirror SearchTask.Execute: prep must run before export so PKs are
	// filled and invalid rows compacted. Also exercises the full path
	// including FilterInvalidSearchResults + FillPrimaryKey.
	allSearchCount, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		plan,
		ts.searchReq.PlaceholderGroup(),
		ts.searchResults,
		[]int64{ts.searchReq.GetNumOfQuery()},
		[]int64{topK},
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, allSearchCount, int64(0))

	segDFs := make([]*chain.DataFrame, 0, len(ts.searchResults))
	for _, res := range ts.searchResults {
		reader, err := segcore.ExportSearchResultAsArrowStream(res, plan, nil)
		require.NoError(t, err)

		df, err := dataFrameFromArrowReader(reader)
		require.NoError(t, err)
		segDFs = append(segDFs, df)
	}

	var groupByOpts *groupByOptions
	if len(segDFs) > 0 && len(ts.searchResults) > 0 {
		groupByColumns := groupByColumnNames(segDFs[0])
		if len(groupByColumns) > 0 {
			groupByOpts = &groupByOptions{
				GroupSize: ts.searchResults[0].GetMetadata().GroupSize,
				Columns:   groupByColumns,
			}
		}
	}

	reduceResult, err := heapMergeReduce(pool, segDFs, topK, groupByOpts)
	require.NoError(t, err)

	return reduceResult, segDFs
}

func TestPrepareSearchResultsForExportAllSearchCountUsesSearchResultCount(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		nq          int64 = 1
		topK        int64 = 5
	)

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	ctx := context.Background()
	searchReq, err := mock_segcore.GenSearchPlanAndRequestsWithNoMatchPKPredicate(
		ts.collection.GetCCollection(), ts.segIDs, nq, topK)
	require.NoError(t, err)
	ts.searchReq = searchReq

	for _, seg := range ts.segs {
		result, err := seg.Search(ctx, searchReq)
		require.NoError(t, err)
		ts.searchResults = append(ts.searchResults, result)
	}

	allSearchCount, err := segcore.PrepareSearchResultsForExport(
		ctx,
		searchReq.Plan(),
		searchReq.PlaceholderGroup(),
		ts.searchResults,
		[]int64{nq},
		[]int64{topK},
	)
	require.NoError(t, err)
	require.Zero(t, allSearchCount)
	require.NotEqual(t, int64(numSegments*msgLength), allSearchCount,
		"all_search_count must come from SearchResult.total_data_cnt_, not static LoadInfo.NumOfRows")
}

func TestLateMaterializeOutputFields(t *testing.T) {
	// Field IDs from GenTestCollectionSchema (100+i):
	// 103=Int32, 104=Float (scalar fields easy to verify)
	outputFieldIDs := []int64{103, 104}

	ts := setupTestSegments(t, 2, 2000, setupOpts{NQ: 2, TopK: 10, OutputFieldIDs: outputFieldIDs})
	defer ts.cleanup()

	reduceResult, segDFs := runGoReducePipeline(t, ts)
	defer func() {
		reduceResult.DF.Release()
		for _, df := range segDFs {
			df.Release()
		}
	}()

	// Marshal base result (ids + scores only)
	searchResultData, err := marshalReduceResult(reduceResult)
	require.NoError(t, err)

	// Before Late Mat: no field data
	assert.Empty(t, searchResultData.FieldsData)

	// Run Late Materialization
	plan := ts.searchReq.Plan()
	err = lateMaterializeOutputFields(ts.searchResults, plan, reduceResult.Sources, searchResultData)
	require.NoError(t, err)

	// After Late Mat: should have field data for each output field
	require.Len(t, searchResultData.FieldsData, len(outputFieldIDs),
		"should have one FieldData entry per output field")

	totalRows := int(reduceResult.DF.NumRows())
	require.Greater(t, totalRows, 0, "should have some results")

	// Verify each field has correct number of rows
	for _, fd := range searchResultData.FieldsData {
		switch f := fd.Field.(type) {
		case *schemapb.FieldData_Scalars:
			switch d := f.Scalars.Data.(type) {
			case *schemapb.ScalarField_IntData:
				assert.Len(t, d.IntData.Data, totalRows,
					"int field %s row count mismatch", fd.FieldName)
			case *schemapb.ScalarField_FloatData:
				assert.Len(t, d.FloatData.Data, totalRows,
					"float field %s row count mismatch", fd.FieldName)
			default:
				t.Errorf("unexpected scalar type for field %s", fd.FieldName)
			}
		default:
			t.Errorf("expected scalar field, got %T for field %s", fd.Field, fd.FieldName)
		}
	}

	// Verify IDs and scores also match total rows
	assert.Len(t, searchResultData.Scores, totalRows)

	t.Logf("Late Mat OK: %d output fields, %d total rows", len(searchResultData.FieldsData), totalRows)
}

func TestLateMaterializeOutputFields_NoOutputFields(t *testing.T) {
	// No output fields in the plan
	ts := setupTestSegments(t, 2, 2000, setupOpts{NQ: 1, TopK: 5})
	defer ts.cleanup()

	reduceResult, segDFs := runGoReducePipeline(t, ts)
	defer func() {
		reduceResult.DF.Release()
		for _, df := range segDFs {
			df.Release()
		}
	}()

	searchResultData, err := marshalReduceResult(reduceResult)
	require.NoError(t, err)

	plan := ts.searchReq.Plan()
	err = lateMaterializeOutputFields(ts.searchResults, plan, reduceResult.Sources, searchResultData)
	require.NoError(t, err)

	// No output fields → FieldsData remains empty
	assert.Empty(t, searchResultData.FieldsData)
}

func TestLateMaterializeOutputFields_EmptySources(t *testing.T) {
	outputFieldIDs := []int64{103, 104}
	ts := setupTestSegments(t, 1, 100, setupOpts{NQ: 1, TopK: 5, OutputFieldIDs: outputFieldIDs})
	defer ts.cleanup()

	plan := ts.searchReq.Plan()
	searchResultData := &schemapb.SearchResultData{}

	err := lateMaterializeOutputFields(ts.searchResults, plan, nil, searchResultData)
	require.NoError(t, err)
	require.Len(t, searchResultData.FieldsData, len(outputFieldIDs),
		"empty sources must still produce typed FieldData entries")

	searchResultData2 := &schemapb.SearchResultData{}
	err = lateMaterializeOutputFields(ts.searchResults, plan, [][]segmentSource{}, searchResultData2)
	require.NoError(t, err)
	require.Len(t, searchResultData2.FieldsData, len(outputFieldIDs),
		"empty sources must still produce typed FieldData entries")
}

// TestGoReduceGroupBy verifies the end-to-end group-by reduce path: each NQ's
// results respect group_size across two segments.
//
// Uses the bool field (id 100) because mock data alternates true/false, giving
// exactly two well-populated buckets. With topK >= 2*group_size every bucket
// fills exactly to group_size, so the test asserts that strict invariant.
func TestGoReduceGroupBy(t *testing.T) {
	const (
		groupByFieldID int64 = 100 // Bool: 2 buckets, each well-populated
		groupSize      int64 = 3
		nq             int64 = 2
		topK           int64 = 10
	)

	ts := setupTestSegments(t, 2, 100,
		setupOpts{NQ: nq, TopK: topK, GroupByFieldID: groupByFieldID, GroupSize: groupSize})
	defer ts.cleanup()

	// Sanity: the C++ side should report group-by enabled on every result.
	for i, r := range ts.searchResults {
		md := r.GetMetadata()
		require.True(t, md.HasGroupBy, "result[%d] missing group_by_values_", i)
		require.Equal(t, groupSize, md.GroupSize, "result[%d] group_size mismatch", i)
	}

	reduceResult, segDFs := runGoReducePipeline(t, ts)
	defer func() {
		reduceResult.DF.Release()
		for _, df := range segDFs {
			df.Release()
		}
	}()

	// The merged DataFrame should carry the $group_by_<fieldID> column propagated by
	// pickGroupByValues, with one chunk per NQ.
	groupByColName := groupByColumnName(groupByFieldID)
	gbCol := reduceResult.DF.Column(groupByColName)
	require.NotNil(t, gbCol, "merged DataFrame missing %s column", groupByColName)
	require.Equal(t, int(nq), reduceResult.DF.NumChunks(),
		"expected one chunk per NQ")

	for nqIdx := 0; nqIdx < int(nq); nqIdx++ {
		chunk := gbCol.Chunk(nqIdx)
		boolArr, ok := chunk.(*array.Boolean)
		require.True(t, ok, "expected %s chunk[%d] to be Boolean, got %T", groupByColName, nqIdx, chunk)

		counts := make(map[bool]int)
		for i := 0; i < boolArr.Len(); i++ {
			require.False(t, boolArr.IsNull(i),
				"NQ %d row %d: bool group_by should not be null", nqIdx, i)
			v := boolArr.Value(i)
			counts[v]++
			require.LessOrEqual(t, int64(counts[v]), groupSize,
				"NQ %d: group %v exceeded group_size (%d > %d)",
				nqIdx, v, counts[v], groupSize)
		}

		// Both buckets are well-populated and group_size×buckets (6) ≤ topK (10),
		// so each bucket should fill to exactly group_size after merge.
		require.Equal(t, 2, len(counts),
			"NQ %d: expected both true and false buckets", nqIdx)
		require.Equal(t, int(groupSize), counts[true],
			"NQ %d: true bucket should fill to group_size", nqIdx)
		require.Equal(t, int(groupSize), counts[false],
			"NQ %d: false bucket should fill to group_size", nqIdx)
		t.Logf("NQ %d: %d rows, true=%d false=%d",
			nqIdx, boolArr.Len(), counts[true], counts[false])
	}
}

// TestFillOutputFieldsOrdered_NoCMemoryLeak verifies that calling
// FillOutputFieldsOrdered in a loop does not leak C heap memory.
// The C++ side allocates via malloc; the Go side must C.free it after use.
// Uses jemalloc stats to precisely measure C heap growth.
func TestFillOutputFieldsOrdered_NoCMemoryLeak(t *testing.T) {
	outputFieldIDs := []int64{103, 104} // Int32, Float

	ts := setupTestSegments(t, 2, 2000, setupOpts{NQ: 2, TopK: 10, OutputFieldIDs: outputFieldIDs})
	defer ts.cleanup()

	reduceResult, segDFs := runGoReducePipeline(t, ts)
	defer func() {
		reduceResult.DF.Release()
		for _, df := range segDFs {
			df.Release()
		}
	}()

	// Build the segIndices/segOffsets arrays from Sources (same as lateMaterializeOutputFields).
	totalRows := 0
	for _, chunk := range reduceResult.Sources {
		totalRows += len(chunk)
	}
	require.Greater(t, totalRows, 0)

	segIndices := make([]int32, totalRows)
	segOffsets := make([]int64, totalRows)
	pos := 0
	for _, chunk := range reduceResult.Sources {
		for _, src := range chunk {
			segIndices[pos] = int32(src.InputIdx)
			segOffsets[pos] = src.SegOffset
			pos++
		}
	}

	plan := ts.searchReq.Plan()
	const iterations = 500

	before := segcore.GetJemallocStats()
	if !before.Success {
		t.Skip("jemalloc stats not available on this platform")
	}

	// Warmup: let jemalloc steady-state settle.
	for i := 0; i < 50; i++ {
		b, err := segcore.FillOutputFieldsOrdered(ts.searchResults, plan, segIndices, segOffsets)
		require.NoError(t, err)
		_ = b
	}
	runtime.GC()

	baseline := segcore.GetJemallocStats()

	for i := 0; i < iterations; i++ {
		b, err := segcore.FillOutputFieldsOrdered(ts.searchResults, plan, segIndices, segOffsets)
		require.NoError(t, err)
		_ = b
	}
	runtime.GC()

	after := segcore.GetJemallocStats()
	growth := int64(after.Allocated) - int64(baseline.Allocated)

	// Without the C.free fix, each call leaks the serialized proto buffer
	// (~150 bytes per call with 2 output fields × 20 rows). Over 500
	// iterations that accumulates to ~75 KB.
	// With the fix, growth is jemalloc pool noise only (~6 KB).
	const maxAllowedGrowth = 20 * 1024 // 20 KB
	assert.Less(t, growth, int64(maxAllowedGrowth),
		"jemalloc allocated grew %d bytes over %d iterations — likely C memory leak", growth, iterations)

	t.Logf("jemalloc C heap growth: %d bytes over %d iterations (limit %d)", growth, iterations, maxAllowedGrowth)
}

// TestExecuteFilterOnly verifies that the Execute() method correctly handles
// FilterOnly requests: it should return early with FilterValidCounts (one per
// segment) and no SlicedBlob, supporting the two-stage search protocol.
func TestExecuteFilterOnly(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		nq          int64 = 1
		topK        int64 = 10
	)

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	ctx := context.Background()

	queryReq, searchReqFilterOnly, err := mock_segcore.GenFilterOnlySearchRequests(
		ts.collection.GetCCollection(), ts.segIDs, nq, topK, testCollectionID)
	require.NoError(t, err)
	defer searchReqFilterOnly.Delete()

	task := NewSearchTask(ctx, ts.collection, ts.manager, queryReq, 1)
	require.NoError(t, task.PreExecute())
	require.NoError(t, task.Execute())

	result := task.SearchResult()
	require.NotNil(t, result)

	assert.Len(t, result.FilterValidCounts, numSegments)
	assert.Len(t, result.SealedSegmentIDsSearched, numSegments)

	// brute-force plan has no predicate filter, so every row is valid.
	for i, count := range result.FilterValidCounts {
		assert.Equal(t, int64(msgLength), count,
			"segment %d: expected all %d rows to pass filter",
			result.SealedSegmentIDsSearched[i], msgLength)
	}

	assert.Empty(t, result.SlicedBlob)
	assert.Zero(t, result.NumQueries)
	assert.Zero(t, result.TopK)
	assert.NotNil(t, result.CostAggregation)
}

// TestExecuteMergedSubTasks exercises the multi-sub-task slicing path: after
// two SearchTasks with different NQ but identical topK are merged, Execute()
// must run reduce once on the combined group and write a correctly-sliced
// SearchResult into each sub-task.
func TestExecuteMergedSubTasks(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 100
		topK        int64 = 5
	)
	subTaskNqs := []int64{1, 2}

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()

	ctx := context.Background()

	tasks := make([]*SearchTask, len(subTaskNqs))
	for i, nq := range subTaskNqs {
		queryReq, err := mock_segcore.GenQueryRequest(
			ts.collection.GetCCollection(), ts.segIDs, nq, topK, testCollectionID)
		require.NoError(t, err)
		tasks[i] = NewSearchTask(ctx, ts.collection, ts.manager, queryReq, 1)
	}

	require.True(t, tasks[0].Merge(tasks[1]),
		"two tasks with identical plan / topK / collection must be mergeable")
	require.Equal(t, []int64{subTaskNqs[0], subTaskNqs[1]}, tasks[0].originNqs)
	require.Equal(t, []int64{topK, topK}, tasks[0].originTopks)
	require.Len(t, tasks[0].others, 1)

	require.NoError(t, tasks[0].PreExecute())
	require.NoError(t, tasks[0].Execute())

	for i, nq := range subTaskNqs {
		sub := tasks[0].subTaskAt(i)
		res := sub.SearchResult()
		mock_segcore.CheckSearchResult(t, res, nq, topK)

		slice := &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(res.SlicedBlob, slice))
		assert.Equal(t, int64(numSegments*msgLength), slice.GetAllSearchCount(),
			"sub %d AllSearchCount must match legacy C++ reduce all_search_count", i)
	}

	assert.NotEqual(t,
		tasks[0].subTaskAt(0).SearchResult().NumQueries,
		tasks[0].subTaskAt(1).SearchResult().NumQueries,
		"distinct sub-tasks must get distinct slices")

	t.Logf("merged slicing OK: sub-task NQs=%v, topK=%d", subTaskNqs, topK)
}

// TestExecuteMergedSubTasks_MixedTopK exercises mixed-topK slicing: when two
// sub-tasks with DIFFERENT originTopks[i] share one reduced result, each slice
// must carry at most originTopks[i] rows per NQ. The old C++ reduce enforced
// this via slice_topKs_[slice_index]; the Go path must match that contract.
//
// The mock request bakes topK into SerializedExprPlan, so Merge() rejects
// different-topK tasks via the bytes.Equal guard — we cannot reach this state
// through Merge(). Instead we drive executeGoReduce directly, which is where
// the per-slice truncation contract lives. Receiver is constructed with
// plan.topK=maxTopK (so segment search + heapMergeReduce emit up to maxTopK
// rows per NQ), and originTopks[0] is set smaller than maxTopK — the missing
// per-slice truncation leaves sub 0 with > smallTopK rows per NQ.
func TestExecuteMergedSubTasks_MixedTopK(t *testing.T) {
	const (
		numSegments       = 2
		msgLength         = 2000
		nqPerSub    int64 = 2
		smallTopK   int64 = 3
		maxTopK     int64 = 8
	)
	subTaskNqs := []int64{nqPerSub, nqPerSub}
	subTaskTopks := []int64{smallTopK, maxTopK}
	totalNq := subTaskNqs[0] + subTaskNqs[1]

	ts := setupTestSegments(t, numSegments, int(msgLength), setupOpts{
		NQ:   totalNq,
		TopK: maxTopK,
	})
	defer ts.cleanup()

	ctx := context.Background()

	// Receiver holds all totalNq vectors and plan.topK=maxTopK. We drive
	// executeGoReduce rather than Execute to bypass combinePlaceHolderGroups.
	receiverReq, err := mock_segcore.GenQueryRequest(
		ts.collection.GetCCollection(), ts.segIDs, totalNq, maxTopK, testCollectionID)
	require.NoError(t, err)
	receiver := NewSearchTask(ctx, ts.collection, ts.manager, receiverReq, 1)

	// Second sub-task exists only so subTaskAt(1) can write its result.
	otherReq, err := mock_segcore.GenQueryRequest(
		ts.collection.GetCCollection(), ts.segIDs, subTaskNqs[1], subTaskTopks[1], testCollectionID)
	require.NoError(t, err)
	other := NewSearchTask(ctx, ts.collection, ts.manager, otherReq, 1)
	other.merged = true

	// Post-merge state with mixed topKs.
	receiver.nq = totalNq
	receiver.topk = maxTopK
	receiver.originNqs = subTaskNqs
	receiver.originTopks = subTaskTopks
	receiver.others = []*SearchTask{other}

	allSearchCount, err := segcore.PrepareSearchResultsForExport(
		ctx,
		ts.searchReq.Plan(),
		ts.searchReq.PlaceholderGroup(),
		ts.searchResults,
		subTaskNqs,
		subTaskTopks,
	)
	require.NoError(t, err)

	// Build Arrow DataFrames from the per-segment SearchResults.
	segDFs, err := receiver.exportSearchResultsAsArrow(ts.searchResults, ts.searchReq.Plan(), nil)
	require.NoError(t, err)
	defer func() {
		for _, df := range segDFs {
			df.Release()
		}
	}()

	tr := timerecord.NewTimeRecorder("mixed-topk-test")
	require.NoError(t, receiver.executeGoReduce(segDFs, ts.searchResults, ts.searchReq, "IP", tr, 0, allSearchCount))

	for i, wantTopK := range subTaskTopks {
		sub := receiver.subTaskAt(i)
		res := sub.SearchResult()
		require.NotNil(t, res, "sub %d: result must be populated", i)

		assert.Equal(t, subTaskNqs[i], res.NumQueries, "sub %d NumQueries", i)
		assert.Equal(t, wantTopK, res.TopK, "sub %d outer TopK", i)

		slice := &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(res.SlicedBlob, slice), "sub %d unmarshal", i)

		assert.Equal(t, subTaskNqs[i], slice.NumQueries, "sub %d slice NumQueries", i)
		assert.Equal(t, wantTopK, slice.TopK, "sub %d slice TopK", i)
		assert.Equal(t, allSearchCount, slice.GetAllSearchCount(), "sub %d AllSearchCount", i)

		require.Len(t, slice.Topks, int(subTaskNqs[i]), "sub %d Topks length", i)
		for j, perNq := range slice.Topks {
			assert.LessOrEqual(t, perNq, wantTopK,
				"sub %d NQ %d: got %d rows, must be <= originTopks[%d]=%d",
				i, j, perNq, i, wantTopK)
		}

		// Proxy's checkSearchResultData requires len(Scores) == sum(Topks).
		var want int64
		for _, k := range slice.Topks {
			want += k
		}
		assert.Equal(t, int(want), len(slice.Scores), "sub %d Scores length", i)

		// Ids length must equal Scores length; both come from the same slice.
		intIDs, ok := slice.Ids.IdField.(*schemapb.IDs_IntId)
		require.True(t, ok, "sub %d: expected int64 PKs", i)
		assert.Len(t, intIDs.IntId.Data, int(want), "sub %d Ids length", i)
	}

	t.Logf("mixed-topK slicing OK: originTopks=%v, originNqs=%v, maxTopK=%d",
		subTaskTopks, subTaskNqs, maxTopK)
}

// TestExecuteNullableVectorOutput regression-tests the MergeBase physical
// offset handling in FillOutputFieldsOrdered. When a nullable vector field is
// in the output set, FillTargetEntry compacts the vector buffer (null rows
// dropped) while valid_data keeps its logical length. Without
// setValidDataOffset, MergeDataArray would read at wrong physical positions
// and return the wrong row's vector for any selected row that has null rows
// before it in the same segment's batch.
func TestExecuteNullableVectorOutput(t *testing.T) {
	const (
		numSegments       = 1
		msgLength         = 30 // 10 null, 20 valid with NullablePatternValidData
		nq          int64 = 1
		topK        int64 = 30 // return everything so we exercise null-before-valid ordering
	)

	// Build the schema up front to resolve the float_vector field id/dim:
	// GenTestCollectionSchema assigns sequential FieldIDs so the hard-coded
	// SimpleFloatVecField.ID is not what ends up on the schema.
	schema := mock_segcore.GenTestCollectionSchemaWithNullableVec("probe", schemapb.DataType_Int64)
	var floatVecFieldID int64
	var dim int
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			floatVecFieldID = f.FieldID
			for _, tp := range f.TypeParams {
				if tp.Key == common.DimKey {
					d, _ := strconv.Atoi(tp.Value)
					dim = d
				}
			}
			break
		}
	}
	require.NotZero(t, floatVecFieldID, "float_vector field should exist")
	require.NotZero(t, dim, "float_vector dim should be set")

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{
		NQ:             nq,
		TopK:           topK,
		OutputFieldIDs: []int64{floatVecFieldID},
		NullableVec:    true,
	})
	defer ts.cleanup()

	// Bypass the search result — segcore's ANN already filters null rows,
	// which hides the MergeBase offset bug. Construct a sources list that
	// explicitly interleaves null and valid seg_offsets within the same
	// segment, the exact shape FillOutputFieldsOrdered must handle
	// correctly for L0 rerank / future callers that don't pre-filter.
	interleavedOffsets := make([]int64, msgLength)
	for i := range interleavedOffsets {
		interleavedOffsets[i] = int64(i)
	}
	sources := [][]segmentSource{make([]segmentSource, 0, msgLength)}
	for _, off := range interleavedOffsets {
		sources[0] = append(sources[0], segmentSource{
			InputIdx:    0,
			SegOffset:   off,
			OriginalIdx: int(off),
		})
	}
	searchResultData := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       int64(msgLength),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: interleavedOffsets},
			},
		},
	}

	plan := ts.searchReq.Plan()
	require.NoError(t, lateMaterializeOutputFields(ts.searchResults, plan, sources, searchResultData))

	// Locate the float_vector FieldData and its raw payload.
	var vecFD *schemapb.FieldData
	for _, fd := range searchResultData.FieldsData {
		if fd.FieldId == floatVecFieldID {
			vecFD = fd
			break
		}
	}
	require.NotNil(t, vecFD, "float_vector must be in output FieldsData")
	returnedVecs := vecFD.GetVectors().GetFloatVector().GetData()
	validBits := vecFD.GetValidData()

	// Ground truth: the inserted FloatVectorFieldData.Data is already compacted
	// to valid_count * dim (binlog convention for nullable vectors). Map each
	// logical row index to its physical index into that compacted buffer.
	insertedVecs := ts.insertData[0].Data[floatVecFieldID].(*storage.FloatVectorFieldData).Data
	validPattern := mock_segcore.NullablePatternValidData(msgLength)
	logicalToPhys := make([]int, msgLength)
	v := 0
	for i, ok := range validPattern {
		if ok {
			logicalToPhys[i] = v
			v++
		} else {
			logicalToPhys[i] = -1
		}
	}

	ids := searchResultData.GetIds().GetIntId().GetData()
	require.Equal(t, len(ids), len(validBits),
		"valid_data must have one bit per returned row")

	// Assert per-row: valid bit matches pattern, and for valid rows the vector
	// matches the originally inserted row exactly. Mismatches here indicate
	// MergeDataArray read at the wrong physical offset.
	physOut := 0
	validCount := 0
	for i, id := range ids {
		require.GreaterOrEqual(t, id, int64(0))
		require.Less(t, id, int64(msgLength))

		wantValid := validPattern[id]
		assert.Equal(t, wantValid, validBits[i],
			"row %d (pk=%d): valid_data mismatch", i, id)
		if !wantValid {
			continue
		}
		validCount++

		require.LessOrEqual(t, (physOut+1)*dim, len(returnedVecs),
			"returned vector buffer too short at physical index %d", physOut)
		got := returnedVecs[physOut*dim : (physOut+1)*dim]
		physIn := logicalToPhys[id]
		want := insertedVecs[physIn*dim : (physIn+1)*dim]
		assert.Equal(t, want, got,
			"row %d (pk=%d, phys_out=%d, phys_in=%d): vector payload mismatch — MergeBase.setValidDataOffset regression",
			i, id, physOut, physIn)
		physOut++
	}

	assert.Equal(t, validCount*dim, len(returnedVecs),
		"compacted vector buffer size must equal valid_count * dim")
	t.Logf("nullable vector OK: %d total rows, %d valid, dim=%d", len(ids), validCount, dim)
}
