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
	"math"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mock_segcore "github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// testSegments holds pre-created segments and search results for testing.
type fixedTopKQueryHook struct {
	topK        int64
	searchParam string
}

func (h fixedTopKQueryHook) Run(params map[string]any) error {
	params[common.TopKKey] = h.topK
	params[common.SearchParamKey] = h.searchParam
	return nil
}

func (fixedTopKQueryHook) Init(string) error                        { return nil }
func (fixedTopKQueryHook) InitTuningConfig(map[string]string) error { return nil }
func (fixedTopKQueryHook) DeleteTuningConfig(string) error          { return nil }
func (fixedTopKQueryHook) CalculateEffectiveSegmentNum([]int64, int64) int {
	return 1
}

var _ optimizers.QueryHook = fixedTopKQueryHook{}

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

func TestExportSearchResultsAsArrowReleasesCompletedDataFramesOnMultiSegmentError(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	successResult := new(segments.SearchResult)
	failResult := new(segments.SearchResult)
	results := []*segments.SearchResult{successResult, failResult}
	injectedErr := errors.New("injected export failure")

	makeRecord := func() arrow.Record {
		idBuilder := array.NewInt64Builder(pool)
		idBuilder.AppendValues([]int64{1, 2}, nil)
		ids := idBuilder.NewArray()
		idBuilder.Release()

		scoreBuilder := array.NewFloat32Builder(pool)
		scoreBuilder.AppendValues([]float32{0.9, 0.8}, nil)
		scores := scoreBuilder.NewArray()
		scoreBuilder.Release()

		offsetBuilder := array.NewInt64Builder(pool)
		offsetBuilder.AppendValues([]int64{10, 20}, nil)
		offsets := offsetBuilder.NewArray()
		offsetBuilder.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "$id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "$score", Type: arrow.PrimitiveTypes.Float32},
			{Name: "$seg_offset", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		record := array.NewRecord(schema, []arrow.Array{ids, scores, offsets}, int64(ids.Len()))
		ids.Release()
		scores.Release()
		offsets.Release()
		return record
	}

	mocker := mockey.Mock(segcore.ExportSearchResultAsArrowRecordBatch).To(
		func(ctx context.Context, result *segcore.SearchResult, plan *segcore.SearchPlan, extraFieldIDs []int64) (arrow.Record, []int64, error) {
			if result == failResult {
				return nil, nil, injectedErr
			}
			return makeRecord(), []int64{2}, nil
		},
	).Build()
	defer mocker.UnPatch()

	task := &SearchTask{ctx: context.Background()}
	_, err := task.exportSearchResultsAsArrow(results, nil, nil)
	require.ErrorIs(t, err, injectedErr)
}

// setupOpts varies the search request shape across test cases.
// At most one of OutputFieldIDs / GroupByFieldID should be set.
// When SkipSearchReq is true NQ/TopK are ignored and no SearchRequest is built.
// NullableVec marks the float_vector field as Nullable so the insert data
// generator stamps ValidData using mock_segcore.NullablePatternValidData —
// required to cover the nullable-vector output path. MutateInsertData lets a
// test make generated segment rows deterministic before binlog serialization.
type setupOpts struct {
	NQ               int64
	TopK             int64
	OutputFieldIDs   []int64
	GroupByFieldID   int64
	GroupSize        int64
	FilterOnly       bool
	SkipSearchReq    bool
	NullableVec      bool
	MutateInsertData func(segmentIdx int, data *storage.InsertData)
}

func setupTestSegments(t *testing.T, numSegments int, msgLength int, opts setupOpts) *testSegments {
	t.Helper()
	require.False(t, opts.GroupByFieldID > 0 && len(opts.OutputFieldIDs) > 0,
		"setupOpts: OutputFieldIDs and GroupByFieldID are mutually exclusive")

	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

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
		if opts.MutateInsertData != nil {
			opts.MutateInsertData(i, insertData)
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

// runGoReducePipeline runs the reduce stage and returns the merged
// mergeResult and DataFrames (caller must release).
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
		record, chunkSizes, err := segcore.ExportSearchResultAsArrowRecordBatch(context.Background(), res, plan, nil)
		require.NoError(t, err)
		defer record.Release()

		df, err := dataFrameFromArrowRecordBatch(record, chunkSizes)
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

func TestResolveGroupSizeFromMetadata(t *testing.T) {
	t.Run("uses first positive group size", func(t *testing.T) {
		groupSize := resolveGroupSizeFromMetadata([]segcore.SearchResultMetadata{
			{HasGroupBy: true, GroupSize: 0},
			{HasGroupBy: true, GroupSize: 3},
		})

		assert.Equal(t, int64(3), groupSize)
	})

	t.Run("defaults empty group-by results to one", func(t *testing.T) {
		groupSize := resolveGroupSizeFromMetadata([]segcore.SearchResultMetadata{
			{HasGroupBy: true, GroupSize: 0},
			{HasGroupBy: true, GroupSize: 0},
		})

		assert.Equal(t, int64(1), groupSize)
	})
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
	err = lateMaterializeOutputFields(context.Background(), ts.searchResults, plan, reduceResult.Sources, searchResultData)
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

func TestL1RerankLateMaterializationKeepsOutputFieldsAligned(t *testing.T) {
	const (
		int32FieldID int64 = 103
		floatFieldID int64 = 104
		pkFieldID    int64 = 109
		topK         int64 = 6
	)

	ts := setupTestSegments(t, 2, 20, setupOpts{
		NQ:             1,
		TopK:           topK,
		OutputFieldIDs: []int64{floatFieldID},
		MutateInsertData: func(segmentIdx int, data *storage.InsertData) {
			pks := data.Data[pkFieldID].(*storage.Int64FieldData)
			output := data.Data[floatFieldID].(*storage.FloatFieldData)
			for row := range pks.Data {
				pks.Data[row] = int64(row*2 + segmentIdx)
				output.Data[row] = float32((segmentIdx+1)*100000 + row)
			}
		},
	})
	defer ts.cleanup()

	reduced, segDFs := runGoReducePipeline(t, ts)
	defer func() {
		for _, df := range segDFs {
			df.Release()
		}
	}()

	originalSources := make([]segmentSource, len(reduced.Sources[0]))
	copy(originalSources, reduced.Sources[0])
	selected := []int{3, 0, 4, 1}
	l1Values := make([]int64, len(originalSources))
	for rank, row := range selected {
		l1Values[row] = int64((len(selected) - rank) * 1000000)
	}

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(
		mapOpWithParamsForTest(
			"l1_sort_key",
			chainexpr.NumCombineFuncName,
			map[string]*schemapb.FunctionParamValue{
				chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum),
			},
			columnArgForTest(chaintypes.ScoreFieldName),
			columnArgForTest("int32Field"),
		),
		&schemapb.FunctionChainOp{
			Op:     chaintypes.OpTypeSort,
			Inputs: []string{"l1_sort_key", chaintypes.IDFieldName},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
			},
		},
		&schemapb.FunctionChainOp{
			Op: chaintypes.OpTypeLimit,
			Params: map[string]*schemapb.FunctionParamValue{
				"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: int64(len(selected))}},
			},
		},
		mapOpWithParamsForTest(
			chaintypes.ScoreFieldName,
			chainexpr.NumCombineFuncName,
			map[string]*schemapb.FunctionParamValue{
				chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum),
			},
			columnArgForTest("l1_sort_key"),
			columnArgForTest(chaintypes.IDFieldName),
		),
	))
	require.NoError(t, err)

	oldReader := fillL1FieldsOrdered
	fillL1FieldsOrdered = func(
		ctx context.Context,
		results []*segcore.SearchResult,
		plan *segcore.SearchPlan,
		fieldIDs []int64,
		segIndices []int32,
		segOffsets []int64,
	) (arrow.Record, error) {
		require.NoError(t, ctx.Err())
		require.Equal(t, []int64{int32FieldID}, fieldIDs)
		require.Len(t, segIndices, len(originalSources))
		require.Len(t, segOffsets, len(originalSources))
		builder := array.NewInt32Builder(defaultAllocator)
		for row := range originalSources {
			builder.Append(int32(l1Values[row]))
		}
		values := builder.NewArray()
		builder.Release()
		field := arrow.Field{
			Name: "int32Field",
			Type: arrow.PrimitiveTypes.Int32,
			Metadata: arrow.NewMetadata(
				[]string{arrowMetadataFieldIDKey, arrowMetadataDataTypeKey},
				[]string{strconv.FormatInt(int32FieldID, 10), strconv.Itoa(int(schemapb.DataType_Int32))},
			),
		}
		record := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, int64(values.Len()))
		values.Release()
		return record, nil
	}
	t.Cleanup(func() { fillL1FieldsOrdered = oldReader })

	task := &SearchTask{ctx: t.Context()}
	reranked, err := task.applyL1RerankResult(reduced, ts.searchResults, ts.searchReq.Plan(), &preparedL1FunctionChain{
		chain:         repr,
		inputFieldIDs: []int64{int32FieldID},
	})
	require.NoError(t, err)
	defer reranked.DF.Release()

	searchResultData, err := marshalReduceResult(reranked)
	require.NoError(t, err)
	require.NoError(t, lateMaterializeOutputFields(t.Context(), ts.searchResults, ts.searchReq.Plan(), reranked.Sources, searchResultData))

	require.Len(t, searchResultData.GetFieldsData(), 1)
	assert.Equal(t, floatFieldID, searchResultData.GetFieldsData()[0].GetFieldId())
	ids := searchResultData.GetIds().GetIntId().GetData()
	outputs := searchResultData.GetFieldsData()[0].GetScalars().GetFloatData().GetData()
	require.Len(t, ids, len(selected))
	require.Len(t, outputs, len(selected))
	for row, source := range reranked.Sources[0] {
		assert.Equal(t, ts.insertData[source.InputIdx].Data[pkFieldID].(*storage.Int64FieldData).Data[source.SegOffset], ids[row])
		assert.Equal(t, ts.insertData[source.InputIdx].Data[floatFieldID].(*storage.FloatFieldData).Data[source.SegOffset], outputs[row])
	}
	assert.False(t, reranked.DF.HasColumn("int32Field"))
	assert.False(t, reranked.DF.HasColumn("l1_sort_key"))
	assert.False(t, reranked.DF.HasColumn(l1SourceIndexColumn))
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
	err = lateMaterializeOutputFields(context.Background(), ts.searchResults, plan, reduceResult.Sources, searchResultData)
	require.NoError(t, err)

	// No output fields → FieldsData remains empty
	assert.Empty(t, searchResultData.FieldsData)
}

func TestLateMaterializeOutputFields_NoOutputFieldsCanceled(t *testing.T) {
	ts := setupTestSegments(t, 1, 100, setupOpts{NQ: 1, TopK: 5})
	defer ts.cleanup()

	plan := ts.searchReq.Plan()
	require.False(t, plan.HasTargetEntries())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := lateMaterializeOutputFields(
		ctx,
		ts.searchResults,
		plan,
		nil,
		&schemapb.SearchResultData{},
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLateMaterializeOutputFields_OnlyPrimaryKey(t *testing.T) {
	const primaryKeyFieldID int64 = 109

	ts := setupTestSegments(t, 1, 100, setupOpts{
		NQ:             1,
		TopK:           5,
		OutputFieldIDs: []int64{primaryKeyFieldID},
	})
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
	require.True(t, plan.HasTargetEntries())
	require.NoError(t, lateMaterializeOutputFields(
		context.Background(),
		ts.searchResults,
		plan,
		reduceResult.Sources,
		searchResultData,
	))

	require.Len(t, searchResultData.FieldsData, 1)
	assert.Equal(t, primaryKeyFieldID, searchResultData.FieldsData[0].GetFieldId())
	pkData := searchResultData.FieldsData[0].GetScalars().GetLongData().GetData()
	require.NotNil(t, searchResultData.GetIds().GetIntId())
	assert.Equal(t, searchResultData.GetIds().GetIntId().GetData(), pkData)
}

func TestLateMaterializeOutputFields_PreservesPrimaryKeyWithOtherFields(t *testing.T) {
	const (
		primaryKeyFieldID int64 = 109
		scalarFieldID     int64 = 103
	)

	outputFieldIDs := []int64{primaryKeyFieldID, scalarFieldID}
	ts := setupTestSegments(t, 1, 100, setupOpts{
		NQ:             1,
		TopK:           5,
		OutputFieldIDs: outputFieldIDs,
	})
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
	require.True(t, plan.HasTargetEntries())
	require.NoError(t, lateMaterializeOutputFields(
		context.Background(),
		ts.searchResults,
		plan,
		reduceResult.Sources,
		searchResultData,
	))

	require.Len(t, searchResultData.FieldsData, 2)
	assert.Equal(t, primaryKeyFieldID, searchResultData.FieldsData[0].GetFieldId())
	assert.Equal(t, scalarFieldID, searchResultData.FieldsData[1].GetFieldId())

	pkData := searchResultData.FieldsData[0].GetScalars().GetLongData().GetData()
	require.NotNil(t, searchResultData.GetIds().GetIntId())
	assert.Equal(t, searchResultData.GetIds().GetIntId().GetData(), pkData)
}

func TestLateMaterializeOutputFields_EmptySources(t *testing.T) {
	outputFieldIDs := []int64{109, 103, 104}
	ts := setupTestSegments(t, 1, 100, setupOpts{NQ: 1, TopK: 5, OutputFieldIDs: outputFieldIDs})
	defer ts.cleanup()

	plan := ts.searchReq.Plan()
	searchResultData := &schemapb.SearchResultData{}

	err := lateMaterializeOutputFields(context.Background(), ts.searchResults, plan, nil, searchResultData)
	require.NoError(t, err)
	require.Len(t, searchResultData.FieldsData, len(outputFieldIDs),
		"empty sources must still produce typed FieldData entries")

	searchResultData2 := &schemapb.SearchResultData{}
	err = lateMaterializeOutputFields(context.Background(), ts.searchResults, plan, [][]segmentSource{}, searchResultData2)
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

func TestHeapMergeReduceRangeGroupByMixedTopKMatchesPerSliceReduce(t *testing.T) {
	const (
		groupSize int64 = 2
		smallTopK int64 = 2
		maxTopK   int64 = 3
	)

	pool := memory.NewGoAllocator()
	groupByFieldName := groupByColumnName(100)
	df := buildTestDFWithGroupBy(pool,
		[][]int64{{1, 2, 3, 4, 5, 6}},
		[][]float32{{0.99, 0.98, 0.97, 0.96, 0.95, 0.94}},
		[][]int64{{10, 20, 30, 10, 20, 30}},
		groupByFieldName)
	defer df.Release()

	rangeReduce, err := heapMergeReduceRange(pool, []*chain.DataFrame{df}, smallTopK, &groupByOptions{
		GroupSize: groupSize,
		Columns:   []string{groupByFieldName},
	}, 0, 1)
	require.NoError(t, err)
	defer rangeReduce.DF.Release()

	maxReduce, err := heapMergeReduce(pool, []*chain.DataFrame{df}, maxTopK, &groupByOptions{
		GroupSize: groupSize,
		Columns:   []string{groupByFieldName},
	})
	require.NoError(t, err)
	defer maxReduce.DF.Release()

	gotIDs := collectInt64Chunks(t, rangeReduce.DF.Column(idFieldName))
	gotGroups := collectInt64Chunks(t, rangeReduce.DF.Column(groupByFieldName))
	maxGroups := collectInt64Chunks(t, maxReduce.DF.Column(groupByFieldName))

	require.Equal(t, [][]int64{{1, 2, 4, 5}}, gotIDs,
		"mixed-topK group-by range reduce must keep second rows from accepted groups")
	require.Equal(t, [][]int64{{10, 20, 10, 20}}, gotGroups,
		"mixed-topK group-by range reduce must not admit groups rejected by the smaller topK")
	require.NotEqual(t, maxGroups[0][:int(smallTopK*groupSize)], gotGroups[0],
		"row truncating a max-topK group reduce is not equivalent to per-slice group reduce")
}

func TestRequiresPerRequestReduce(t *testing.T) {
	require.False(t, requiresPerRequestReduce(nil, []int64{10, 20}, false))
	require.True(t, requiresPerRequestReduce(nil, []int64{10, 20}, true))
	require.True(t, requiresPerRequestReduce(&groupByOptions{GroupSize: 2}, []int64{10, 20}, false))
	require.True(t, requiresPerRequestReduce(&groupByOptions{GroupSize: 2}, []int64{10, 20}, true))
	require.False(t, requiresPerRequestReduce(&groupByOptions{GroupSize: 1}, []int64{10, 20}, false))
	require.False(t, requiresPerRequestReduce(&groupByOptions{GroupSize: 2}, []int64{10, 10}, true))
}

func TestHasMixedTopK(t *testing.T) {
	require.False(t, hasMixedTopK(nil))
	require.False(t, hasMixedTopK([]int64{10}))
	require.False(t, hasMixedTopK([]int64{10, 10}))
	require.True(t, hasMixedTopK([]int64{10, 20}))
}

type reduceLayoutTestCase struct {
	name        string
	groupByOpts *groupByOptions
	hasL1       bool
	wantPerReq  bool
}

func TestBuildReduceLayoutPerRequestL1(t *testing.T) {
	for _, tc := range []reduceLayoutTestCase{
		{name: "ordinary mixed topk without l1", hasL1: false, wantPerReq: false},
		{name: "ordinary mixed topk with l1", hasL1: true, wantPerReq: true},
		{name: "group mixed topk without l1", groupByOpts: &groupByOptions{GroupSize: 2}, hasL1: false, wantPerReq: true},
		{name: "group size one mixed topk without l1", groupByOpts: &groupByOptions{GroupSize: 1}, hasL1: false, wantPerReq: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &SearchTask{
				nq:          3,
				topk:        20,
				originNqs:   []int64{1, 2},
				originTopks: []int64{10, 20},
			}
			layout, err := task.buildReduceLayout(tc.groupByOpts, tc.hasL1)
			require.NoError(t, err)
			require.Equal(t, tc.wantPerReq, layout.PerRequestReduce)
		})
	}
}

// TestBuildReduceLayout retains detailed range assertions for the group-by
// mixed TopK path.
func TestBuildReduceLayout(t *testing.T) {
	t.Run("shared standard reduce keeps merged candidate topk", func(t *testing.T) {
		task := &SearchTask{
			nq:          3,
			topk:        20,
			originNqs:   []int64{1, 2},
			originTopks: []int64{10, 20},
		}

		layout, err := task.buildReduceLayout(nil, false)
		require.NoError(t, err)
		require.False(t, layout.PerRequestReduce)
		require.Equal(t, 3, layout.NQ)
		require.Equal(t, int64(1), layout.GroupSize)
		require.Equal(t, []reduceRange{
			{NQOffset: 0, NQCount: 1, ReduceTopK: 20, OutputTopK: 10},
			{NQOffset: 1, NQCount: 2, ReduceTopK: 20, OutputTopK: 20},
		}, layout.Ranges)
	})

	t.Run("mixed group topk reduces each request range independently", func(t *testing.T) {
		task := &SearchTask{
			nq:          5,
			topk:        20,
			originNqs:   []int64{2, 3},
			originTopks: []int64{10, 20},
		}

		layout, err := task.buildReduceLayout(&groupByOptions{GroupSize: 2, Columns: []string{"$group_by_100"}}, false)
		require.NoError(t, err)
		require.True(t, layout.PerRequestReduce)
		require.Equal(t, int64(2), layout.GroupSize)
		require.Equal(t, []reduceRange{
			{NQOffset: 0, NQCount: 2, ReduceTopK: 10, OutputTopK: 10},
			{NQOffset: 2, NQCount: 3, ReduceTopK: 20, OutputTopK: 20},
		}, layout.Ranges)
	})

	t.Run("equal group topk shares reduce", func(t *testing.T) {
		task := &SearchTask{
			nq:          2,
			topk:        10,
			originNqs:   []int64{1, 1},
			originTopks: []int64{10, 10},
		}

		layout, err := task.buildReduceLayout(&groupByOptions{GroupSize: 3}, false)
		require.NoError(t, err)
		require.False(t, layout.PerRequestReduce)
		require.Equal(t, int64(3), layout.GroupSize)
	})

	t.Run("rejects inconsistent task metadata", func(t *testing.T) {
		_, err := (&SearchTask{
			nq:          2,
			topk:        10,
			originNqs:   []int64{1, 1},
			originTopks: []int64{10},
		}).buildReduceLayout(nil, false)
		require.Error(t, err)

		_, err = (&SearchTask{
			nq:          3,
			topk:        10,
			originNqs:   []int64{1, 1},
			originTopks: []int64{10, 10},
		}).buildReduceLayout(nil, false)
		require.Error(t, err)

		_, err = (&SearchTask{
			nq:          1,
			topk:        10,
			originNqs:   []int64{1},
			originTopks: []int64{-1},
		}).buildReduceLayout(nil, false)
		require.Error(t, err)
	})
}

func TestAttributeStorageCostZeroNQAndEmptyResults(t *testing.T) {
	task := &SearchTask{
		originNqs: []int64{0, 0},
		result: &internalpb.SearchResults{
			ScannedRemoteBytes: 7,
			ScannedTotalBytes:  11,
		},
		others: []*SearchTask{
			{
				result: &internalpb.SearchResults{
					ScannedRemoteBytes: 13,
					ScannedTotalBytes:  17,
				},
			},
		},
	}

	task.attributeStorageCost(nil)
	assert.Equal(t, int64(7), task.result.ScannedRemoteBytes)
	assert.Equal(t, int64(11), task.result.ScannedTotalBytes)
	assert.Equal(t, int64(13), task.others[0].result.ScannedRemoteBytes)
	assert.Equal(t, int64(17), task.others[0].result.ScannedTotalBytes)

	task.originNqs = []int64{1, 3}
	task.attributeStorageCost(nil)
	assert.Zero(t, task.result.ScannedRemoteBytes)
	assert.Zero(t, task.result.ScannedTotalBytes)
	assert.Zero(t, task.others[0].result.ScannedRemoteBytes)
	assert.Zero(t, task.others[0].result.ScannedTotalBytes)
}

func collectInt64Chunks(t *testing.T, col *arrow.Chunked) [][]int64 {
	t.Helper()

	require.NotNil(t, col)
	out := make([][]int64, 0, len(col.Chunks()))
	for _, chunk := range col.Chunks() {
		arr, ok := chunk.(*array.Int64)
		require.True(t, ok, "expected *array.Int64, got %T", chunk)

		values := make([]int64, 0, arr.Len())
		for i := 0; i < arr.Len(); i++ {
			values = append(values, arr.Value(i))
		}
		out = append(out, values)
	}
	return out
}

func assertNoSustainedJemallocGrowth(t *testing.T, runOnce func()) {
	t.Helper()

	const (
		warmupIterations       = 300
		windowIterations       = 1000
		measurementWindows     = 5
		positiveWindowNoiseMax = 96 * 1024
		maxPositiveWindows     = 2
	)

	before := segcore.GetJemallocStats()
	if !before.Success {
		t.Skip("jemalloc stats not available on this platform")
	}

	// Let allocator caches reach steady state before sampling.
	for i := 0; i < warmupIterations; i++ {
		runOnce()
	}
	runtime.GC()

	windowBaseline := segcore.GetJemallocStats()
	positiveWindows := 0
	windowGrowths := make([]int64, 0, measurementWindows)

	for window := 0; window < measurementWindows; window++ {
		for i := 0; i < windowIterations; i++ {
			runOnce()
		}
		runtime.GC()

		afterWindow := segcore.GetJemallocStats()
		growth := int64(afterWindow.Allocated) - int64(windowBaseline.Allocated)
		windowGrowths = append(windowGrowths, growth)
		if growth > positiveWindowNoiseMax {
			positiveWindows++
		}
		windowBaseline = afterWindow
	}

	// Assert sustained positive growth instead of a single noisy jemalloc delta.
	assert.LessOrEqual(t, positiveWindows, maxPositiveWindows,
		"jemalloc allocated had sustained positive growth over %d/%d windows (growths=%v, threshold=%d)",
		positiveWindows, measurementWindows, windowGrowths, positiveWindowNoiseMax)

	t.Logf("jemalloc C heap growth windows=%v, positiveWindows=%d/%d",
		windowGrowths, positiveWindows, measurementWindows)
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
	assertNoSustainedJemallocGrowth(t, func() {
		b, err := segcore.FillOutputFieldsOrdered(context.Background(), ts.searchResults, plan, segIndices, segOffsets)
		require.NoError(t, err)
		_ = b
	})
}

func TestExportSearchResultAsArrowRecordBatch_NoCMemoryLeak(t *testing.T) {
	extraFieldIDs := []int64{103} // Int32

	ts := setupTestSegments(t, 1, 2000, setupOpts{NQ: 2, TopK: 10, OutputFieldIDs: extraFieldIDs})
	defer ts.cleanup()

	_, err := segcore.PrepareSearchResultsForExport(
		context.Background(),
		ts.searchReq.Plan(),
		ts.searchReq.PlaceholderGroup(),
		ts.searchResults,
		[]int64{ts.searchReq.GetNumOfQuery()},
		[]int64{10},
	)
	require.NoError(t, err)

	before := segcore.GetJemallocStats()
	if !before.Success {
		t.Skip("jemalloc stats not available on this platform")
	}

	exportOnce := func() {
		for _, res := range ts.searchResults {
			record, _, err := segcore.ExportSearchResultAsArrowRecordBatch(context.Background(), res, ts.searchReq.Plan(), extraFieldIDs)
			require.NoError(t, err)
			record.Release()
		}
	}

	assertNoSustainedJemallocGrowth(t, exportOnce)
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

func TestExecuteEmptySearchReturnsNQEmptyResult(t *testing.T) {
	const (
		nq   int64 = 1
		topK int64 = 10
	)

	paramtable.Init()
	schema := mock_segcore.GenTestCollectionSchema("test-empty-search", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(testCollectionID, schema)
	manager := segments.NewManager()
	manager.Collection.PutOrRef(testCollectionID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: testCollectionID,
		PartitionIDs: []int64{testPartitionID},
	})
	collection := manager.Collection.Get(testCollectionID)
	defer manager.Collection.Unref(collection.ID(), 1)

	ctx := context.Background()
	queryReq, err := mock_segcore.GenQueryRequest(
		collection.GetCCollection(), nil, nq, topK, testCollectionID)
	require.NoError(t, err)

	task := NewSearchTask(ctx, collection, manager, queryReq, 1)
	require.NoError(t, task.PreExecute())
	require.NoError(t, task.Execute())

	result := task.SearchResult()
	require.NotNil(t, result)
	assert.Equal(t, nq, result.NumQueries)
	assert.Equal(t, topK, result.TopK)

	var resultData *schemapb.SearchResultData
	if result.ResultData != nil {
		resultData = result.ResultData
	} else {
		require.NotEmpty(t, result.SlicedBlob)
		resultData = &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(result.SlicedBlob, resultData))
	}

	assert.Equal(t, nq, resultData.NumQueries)
	assert.Equal(t, topK, resultData.TopK)
	assert.Equal(t, []int64{0}, resultData.Topks)
	assert.Empty(t, resultData.Scores)
	assert.Zero(t, typeutil.GetSizeOfIDs(resultData.GetIds()))
	assert.Empty(t, resultData.FieldsData)
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

func TestExecuteMergedSubTasks_MixedTopKWithL1Rerank(t *testing.T) {
	const (
		numSegments = 2
		msgLength   = 200
		maxTopK     = int64(8)
	)
	originNqs := []int64{1, 2}
	originTopks := []int64{3, maxTopK}

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()
	ctx := context.Background()

	l1Chain := l1FunctionChainForTest(mapOpWithParamsForTest(
		chaintypes.ScoreFieldName,
		chainexpr.RoundDecimalFuncName,
		map[string]*schemapb.FunctionParamValue{
			"decimal": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 0}},
		},
		columnArgForTest(chaintypes.ScoreFieldName),
	))

	rawRequests := make([]*querypb.SearchRequest, len(originNqs))
	for i := range originNqs {
		req, err := mock_segcore.GenQueryRequest(
			ts.collection.GetCCollection(), ts.segIDs, originNqs[i], originTopks[i], testCollectionID)
		require.NoError(t, err)
		rawRequests[i] = req
	}

	buildRequests := func(withL1 bool) []*querypb.SearchRequest {
		t.Helper()
		requests := make([]*querypb.SearchRequest, len(originNqs))
		for i := range originNqs {
			req := proto.Clone(rawRequests[i]).(*querypb.SearchRequest)
			if withL1 {
				plan := &planpb.PlanNode{}
				require.NoError(t, proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), plan))
				plan.QuerynodeFunctionChains = []*schemapb.FunctionChain{l1Chain}
				var err error
				req.Req.SerializedExprPlan, err = proto.Marshal(plan)
				require.NoError(t, err)
			}
			requests[i] = req
		}
		require.NotEqual(t, requests[0].GetReq().GetSerializedExprPlan(), requests[1].GetReq().GetSerializedExprPlan())

		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key) })
		hook := fixedTopKQueryHook{topK: maxTopK, searchParam: `{}`}
		for i, req := range requests {
			optimized, err := optimizers.OptimizeSearchParams(ctx, req, hook, numSegments, false, func(int64) int64 { return 128 })
			require.NoError(t, err)
			requests[i] = optimized
		}

		require.Equal(t, requests[0].GetReq().GetSerializedExprPlan(), requests[1].GetReq().GetSerializedExprPlan())
		require.Equal(t, originTopks[0], requests[0].GetReq().GetTopk())
		require.Equal(t, originTopks[1], requests[1].GetReq().GetTopk())
		plan := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(requests[0].GetReq().GetSerializedExprPlan(), plan))
		require.Equal(t, maxTopK, plan.GetVectorAnns().GetQueryInfo().GetTopk())
		if withL1 {
			require.Len(t, plan.GetQuerynodeFunctionChains(), 1)
			require.True(t, proto.Equal(l1Chain, plan.GetQuerynodeFunctionChains()[0]))
		} else {
			require.Empty(t, plan.GetQuerynodeFunctionChains())
		}
		return requests
	}

	execute := func(requests []*querypb.SearchRequest) []*schemapb.SearchResultData {
		t.Helper()
		receiver := NewSearchTask(ctx, ts.collection, ts.manager, requests[0], 1)
		other := NewSearchTask(ctx, ts.collection, ts.manager, requests[1], 1)
		require.True(t, receiver.Merge(other))
		require.Equal(t, int64(3), receiver.nq)
		require.Equal(t, maxTopK, receiver.topk)
		require.Equal(t, originNqs, receiver.originNqs)
		require.Equal(t, originTopks, receiver.originTopks)
		require.Len(t, receiver.others, 1)
		require.True(t, other.merged)
		require.NoError(t, receiver.PreExecute())
		require.NoError(t, receiver.Execute())

		results := make([]*schemapb.SearchResultData, len(originNqs))
		for i := range originNqs {
			res := receiver.subTaskAt(i).SearchResult()
			require.NotNil(t, res)
			require.Equal(t, originNqs[i], res.GetNumQueries())
			require.Equal(t, originTopks[i], res.GetTopK())
			data := &schemapb.SearchResultData{}
			require.NoError(t, proto.Unmarshal(res.GetSlicedBlob(), data))
			require.Equal(t, originNqs[i], data.GetNumQueries())
			require.Equal(t, originTopks[i], data.GetTopK())
			require.Len(t, data.GetTopks(), int(originNqs[i]))
			var rowCount int64
			for _, topK := range data.GetTopks() {
				require.LessOrEqual(t, topK, originTopks[i])
				rowCount += topK
			}
			require.Len(t, data.GetScores(), int(rowCount))
			require.Equal(t, int(rowCount), typeutil.GetSizeOfIDs(data.GetIds()))
			require.Equal(t, int64(numSegments*msgLength), data.GetAllSearchCount())
			results[i] = data
		}
		return results
	}

	baseline := execute(buildRequests(false))
	reranked := execute(buildRequests(true))
	for i := range baseline {
		require.Equal(t, baseline[i].GetTopks(), reranked[i].GetTopks())
		require.Len(t, reranked[i].GetScores(), len(baseline[i].GetScores()))
		for _, score := range reranked[i].GetScores() {
			assert.Equal(t, float32(math.Trunc(float64(score))), score)
		}
		var changed bool
		for _, score := range baseline[i].GetScores() {
			if score != float32(math.Trunc(float64(score))) {
				changed = true
				break
			}
		}
		require.True(t, changed, "baseline scores must contain a fractional value to prove L1 rounding executed")
	}
}

// TestExecuteMergedSubTasks_MixedTopK exercises mixed-topK slicing: when two
// sub-tasks with DIFFERENT originTopks[i] share one reduced result, each slice
// must carry at most originTopks[i] rows per NQ. The old C++ reduce enforced
// this via slice_topKs_[slice_index]; the Go path must match that contract.
//
// The mock request bakes topK into SerializedExprPlan, so Merge() rejects
// different-topK tasks via the bytes.Equal guard — we cannot reach this state
// through Merge(). Instead we drive the explicit reduce, rerank, and finalize
// stages directly. Receiver is constructed with
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
	// the explicit stages rather than Execute to bypass combinePlaceHolderGroups.
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
	groupByOpts := resolveGroupByOptions(segDFs, ts.searchResults)
	nqOffset := 0
	for i, nq := range receiver.originNqs {
		reduced, err := receiver.executeGoReduce(segDFs, receiver.originTopks[i], groupByOpts, nqOffset, int(nq))
		require.NoError(t, err)

		searchResultData, err := receiver.marshalReducedResult(i, reduced, allSearchCount)
		require.NoError(t, err)
		require.NoError(t, lateMaterializeOutputFields(ctx, ts.searchResults, ts.searchReq.Plan(), reduced.Sources, searchResultData))
		require.NoError(t, receiver.encodeAndAssignReducedResult(i, searchResultData, "IP", tr, 0))
		reduced.DF.Release()
		nqOffset += int(nq)
	}
	receiver.attributeStorageCost(ts.searchResults)

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

func TestExecuteMergedSubTasks_MixedTopKWithL1RerankMatchesIndependentRequests(t *testing.T) {
	const (
		maxTopK      int64 = 8
		nqSmall      int64 = 1
		nqLarge      int64 = 2
		msgLength          = 200
		int32FieldID int64 = 103
		pkFieldID    int64 = 109
	)

	// Keep the inserted vectors generated by the mock so the ANN search returns
	// a stable candidate ranking; the scalar field below makes the L1 stage
	// sensitive to whether the reduce window is 3 or 8.
	ts := setupTestSegments(t, 2, msgLength, setupOpts{
		SkipSearchReq: true,
		MutateInsertData: func(segmentIdx int, data *storage.InsertData) {
			pks := data.Data[pkFieldID].(*storage.Int64FieldData)
			values := data.Data[int32FieldID].(*storage.Int32FieldData)
			for row := range pks.Data {
				pks.Data[row] = int64(segmentIdx*msgLength + row)
				values.Data[row] = int32(segmentIdx*msgLength + row)
			}
		},
	})
	defer ts.cleanup()

	ctx := context.Background()
	l1Chain := l1FunctionChainForTest(mapOpWithParamsForTest(
		chaintypes.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{
			chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum),
		},
		columnArgForTest(chaintypes.ScoreFieldName),
		columnArgForTest("int32Field"),
	))

	setPlanTopK := func(req *querypb.SearchRequest, topK int64) {
		t.Helper()
		plan := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), plan))
		plan.GetVectorAnns().GetQueryInfo().Topk = topK
		plan.QuerynodeFunctionChains = []*schemapb.FunctionChain{l1Chain}
		serialized, err := proto.Marshal(plan)
		require.NoError(t, err)
		req.Req.SerializedExprPlan = serialized
	}

	makeRequest := func(nq, requestTopK int64) *querypb.SearchRequest {
		req, err := mock_segcore.GenQueryRequest(
			ts.collection.GetCCollection(), ts.segIDs, nq, requestTopK, testCollectionID)
		require.NoError(t, err)
		setPlanTopK(req, maxTopK)
		return req
	}

	baseRequests := []*querypb.SearchRequest{
		makeRequest(nqSmall, 3),
		makeRequest(nqLarge, maxTopK),
	}
	// Merge() compares serialized plans, while the request-level TopK remains
	// in internalpb and is retained in originTopks for the reduce layout.
	baseRequests[1].Req.SerializedExprPlan = append([]byte(nil), baseRequests[0].Req.SerializedExprPlan...)

	execute := func(requests ...*querypb.SearchRequest) []*schemapb.SearchResultData {
		t.Helper()
		receiver := NewSearchTask(ctx, ts.collection, ts.manager, requests[0], 1)
		for _, req := range requests[1:] {
			other := NewSearchTask(ctx, ts.collection, ts.manager, req, 1)
			require.True(t, receiver.Merge(other))
		}
		require.NoError(t, receiver.PreExecute())
		require.NoError(t, receiver.Execute())

		results := make([]*schemapb.SearchResultData, len(receiver.originNqs))
		for i := range receiver.originNqs {
			searchResult := receiver.subTaskAt(i).SearchResult()
			require.NotNil(t, searchResult)
			data := &schemapb.SearchResultData{}
			require.NoError(t, proto.Unmarshal(searchResult.GetSlicedBlob(), data))
			results[i] = data
		}
		return results
	}

	cloneRequest := func(req *querypb.SearchRequest, requestTopK, planTopK int64) *querypb.SearchRequest {
		cloned := proto.Clone(req).(*querypb.SearchRequest)
		cloned.Req.Topk = requestTopK
		setPlanTopK(cloned, planTopK)
		return cloned
	}

	merged := execute(
		cloneRequest(baseRequests[0], 3, maxTopK),
		cloneRequest(baseRequests[1], maxTopK, maxTopK),
	)
	independentSmall := execute(cloneRequest(baseRequests[0], 3, 3))[0]
	independentLarge := execute(cloneRequest(baseRequests[1], maxTopK, maxTopK))[0]

	ids := func(data *schemapb.SearchResultData) []int64 {
		t.Helper()
		intIDs, ok := data.GetIds().GetIdField().(*schemapb.IDs_IntId)
		require.True(t, ok)
		return intIDs.IntId.GetData()
	}
	assertEquivalent := func(name string, got, want *schemapb.SearchResultData) {
		t.Helper()
		require.Equal(t, want.GetTopks(), got.GetTopks(), "%s topks", name)
		require.Equal(t, ids(want), ids(got), "%s ids and ordering", name)
		require.Len(t, got.GetScores(), len(want.GetScores()), "%s score count", name)
		for i := range want.GetScores() {
			assert.InDelta(t, want.GetScores()[i], got.GetScores()[i], 1e-5, "%s score[%d]", name, i)
		}
		require.Equal(t, want.GetAllSearchCount(), got.GetAllSearchCount(), "%s all search count", name)
	}

	// The independent TopK=3 result is the reference candidate-window
	// behavior: a merged task must not rerank it using the merged TopK=8 window.
	assertEquivalent("small merged request", merged[0], independentSmall)
	assertEquivalent("large merged request", merged[1], independentLarge)
}

func TestExecuteMergedSubTasks_MixedGroupByTopKWithL1Rerank(t *testing.T) {
	const (
		maxTopK        int64 = 8
		groupByFieldID int64 = 103 // Int32, provides more than two groups
		groupSize      int64 = 2
	)
	originNqs := []int64{1, 2}
	originTopks := []int64{3, maxTopK}

	ts := setupTestSegments(t, 2, 200, setupOpts{SkipSearchReq: true})
	defer ts.cleanup()
	ctx := context.Background()

	l1Chain := l1FunctionChainForTest(mapOpWithParamsForTest(
		chaintypes.ScoreFieldName,
		chainexpr.RoundDecimalFuncName,
		map[string]*schemapb.FunctionParamValue{
			"decimal": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 0}},
		},
		columnArgForTest(chaintypes.ScoreFieldName),
	))

	buildRequest := func(nq, requestTopK int64) *querypb.SearchRequest {
		req, err := mock_segcore.GenQueryRequest(
			ts.collection.GetCCollection(), ts.segIDs, nq, requestTopK, testCollectionID)
		require.NoError(t, err)
		plan := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), plan))
		plan.GetVectorAnns().QueryInfo.GroupByFieldId = groupByFieldID
		plan.GetVectorAnns().QueryInfo.GroupSize = groupSize
		plan.QuerynodeFunctionChains = []*schemapb.FunctionChain{l1Chain}
		req.Req.SerializedExprPlan, err = proto.Marshal(plan)
		require.NoError(t, err)
		return req
	}

	// The receiver contributes one query vector and the second sub-task
	// contributes two. Execute combines their placeholder groups into the
	// receiver's three-query search while preserving request-level TopKs below.
	receiverReq := buildRequest(originNqs[0], originTopks[0])
	otherReq := buildRequest(originNqs[1], originTopks[1])
	// Make the plan bytes identical so the normal Merge guard accepts the
	// requests, while retaining their request-level TopKs in internalpb.
	var receiverPlan planpb.PlanNode
	require.NoError(t, proto.Unmarshal(receiverReq.GetReq().GetSerializedExprPlan(), &receiverPlan))
	receiverPlan.GetVectorAnns().QueryInfo.Topk = maxTopK
	serializedPlan, err := proto.Marshal(&receiverPlan)
	require.NoError(t, err)
	receiverReq.Req.SerializedExprPlan = serializedPlan
	otherReq.Req.SerializedExprPlan = append([]byte(nil), serializedPlan...)
	receiver := NewSearchTask(ctx, ts.collection, ts.manager, receiverReq, 1)
	other := NewSearchTask(ctx, ts.collection, ts.manager, otherReq, 1)
	require.True(t, receiver.Merge(other))
	require.Equal(t, originNqs, receiver.originNqs)
	require.Equal(t, originTopks, receiver.originTopks)
	require.Equal(t, maxTopK, receiver.topk)

	require.NoError(t, receiver.PreExecute())
	require.NoError(t, receiver.Execute())

	for i, expectedNQ := range originNqs {
		result := receiver.subTaskAt(i).SearchResult()
		require.NotNil(t, result)
		require.Equal(t, expectedNQ, result.GetNumQueries())
		require.Equal(t, originTopks[i], result.GetTopK())

		data := &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(result.GetSlicedBlob(), data))
		require.Equal(t, expectedNQ, data.GetNumQueries())
		require.Equal(t, originTopks[i], data.GetTopK())
		require.Len(t, data.GetTopks(), int(expectedNQ))
		require.Len(t, data.GetGroupByFieldValues(), 1)
		require.Equal(t, groupByFieldID, data.GetGroupByFieldValues()[0].GetFieldId())

		var rowCount int
		for _, topK := range data.GetTopks() {
			require.LessOrEqual(t, topK, originTopks[i]*groupSize)
			rowCount += int(topK)
		}
		require.Len(t, data.GetScores(), rowCount)
		require.Equal(t, rowCount, typeutil.GetSizeOfIDs(data.GetIds()))

		groups := data.GetGroupByFieldValues()[0].GetScalars().GetIntData().GetData()
		require.Len(t, groups, rowCount)
		counts := make(map[int32]int)
		for _, group := range groups {
			counts[group]++
			require.LessOrEqual(t, counts[group], int(groupSize))
		}
		require.GreaterOrEqual(t, len(counts), 2,
			"group-by reduce should retain multiple groups for sub-task %d", i)
		for _, score := range data.GetScores() {
			assert.Equal(t, float32(math.Trunc(float64(score))), score,
				"L1 normalization should round scores for sub-task %d", i)
		}
	}
}

func TestExecuteGoReduceFastPathUsesOriginTopKWhenPlanTopKReduced(t *testing.T) {
	paramtable.Init()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	// Simulate three per-segment result sets produced after the delegator reduced
	// plan topK to 2. The worker can still assemble 5 rows from all segment
	// candidates, matching the original request topK.
	segDFs := []*chain.DataFrame{
		buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.99, 0.98}}),
		buildTestDF(pool, [][]int64{{3, 4}}, [][]float32{{0.97, 0.96}}),
		buildTestDF(pool, [][]int64{{5, 6}}, [][]float32{{0.95, 0.94}}),
	}
	defer func() {
		for _, df := range segDFs {
			df.Release()
		}
	}()

	schema := mock_segcore.GenTestCollectionSchema("test-reduced-plan-topk", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(testCollectionID, schema)
	manager := segments.NewManager()
	require.NoError(t, manager.Collection.PutOrRef(testCollectionID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: testCollectionID,
		PartitionIDs: []int64{testPartitionID},
	}))
	collection := manager.Collection.Get(testCollectionID)
	require.NotNil(t, collection)
	defer manager.Collection.Unref(collection.ID(), 1)

	reducedPlanReq, err := mock_segcore.GenSearchPlanAndRequestsWithTopK(
		collection.GetCCollection(), []int64{1, 2, 3}, 1, 2)
	require.NoError(t, err)
	defer reducedPlanReq.Delete()
	reducedPlan := reducedPlanReq.Plan()
	require.Equal(t, int64(2), reducedPlan.GetTopK())

	mocker := mockey.Mock(lateMaterializeOutputFields).To(
		func(ctx context.Context, results []*segments.SearchResult, plan *segcore.SearchPlan, sources [][]segmentSource, searchResultData *schemapb.SearchResultData) error {
			return nil
		},
	).Build()
	defer mocker.UnPatch()

	task := &SearchTask{
		ctx:         context.Background(),
		topk:        5,
		originNqs:   []int64{1},
		originTopks: []int64{5},
		serverID:    1,
	}

	tr := timerecord.NewTimeRecorder("reduced-plan-topk-test")
	reduced, err := task.executeGoReduce(segDFs, task.topk, nil, 0, 1)
	require.NoError(t, err)
	defer reduced.DF.Release()
	searchResultData, err := task.marshalReducedResult(0, reduced, 6)
	require.NoError(t, err)
	require.NoError(t, lateMaterializeOutputFields(task.ctx, nil, reducedPlan, reduced.Sources, searchResultData))
	require.NoError(t, task.encodeAndAssignReducedResult(0, searchResultData, "IP", tr, 0))

	require.NotNil(t, task.result)
	assert.Equal(t, int64(1), task.result.NumQueries)
	assert.Equal(t, int64(5), task.result.TopK)

	data := task.result.ResultData
	if data == nil {
		require.NotEmpty(t, task.result.SlicedBlob)
		data = &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(task.result.SlicedBlob, data))
	}

	assert.Equal(t, int64(1), data.NumQueries)
	assert.Equal(t, int64(5), data.TopK)
	assert.Equal(t, []int64{5}, data.Topks)
	assert.Len(t, data.Scores, 5)

	intIDs, ok := data.Ids.IdField.(*schemapb.IDs_IntId)
	require.True(t, ok)
	assert.Len(t, intIDs.IntId.Data, 5)
}

func TestExecuteGoReduceHonorsEnableResultZeroCopy(t *testing.T) {
	const (
		numSegments = 1
		msgLength   = 200
		nq          = 2
		topK        = 3
	)

	key := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.Key
	original := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.GetValue()
	defer paramtable.Get().Save(key, original)
	paramtable.Get().Save(key, "true")

	ts := setupTestSegments(t, numSegments, msgLength, setupOpts{
		NQ:   nq,
		TopK: topK,
	})
	defer ts.cleanup()

	ctx := context.Background()
	queryReq, err := mock_segcore.GenQueryRequest(
		ts.collection.GetCCollection(), ts.segIDs, nq, topK, testCollectionID)
	require.NoError(t, err)
	task := NewSearchTask(ctx, ts.collection, ts.manager, queryReq, 1)

	allSearchCount, err := segcore.PrepareSearchResultsForExport(
		ctx,
		ts.searchReq.Plan(),
		ts.searchReq.PlaceholderGroup(),
		ts.searchResults,
		[]int64{nq},
		[]int64{topK},
	)
	require.NoError(t, err)

	segDFs, err := task.exportSearchResultsAsArrow(ts.searchResults, ts.searchReq.Plan(), nil)
	require.NoError(t, err)
	defer func() {
		for _, df := range segDFs {
			df.Release()
		}
	}()

	tr := timerecord.NewTimeRecorder("zero-copy-test")
	reduced, err := task.executeGoReduce(segDFs, task.topk, resolveGroupByOptions(segDFs, ts.searchResults), 0, int(nq))
	require.NoError(t, err)
	defer reduced.DF.Release()
	searchResultData, err := task.marshalReducedResult(0, reduced, allSearchCount)
	require.NoError(t, err)
	require.NoError(t, lateMaterializeOutputFields(ctx, ts.searchResults, ts.searchReq.Plan(), reduced.Sources, searchResultData))
	require.NoError(t, task.encodeAndAssignReducedResult(0, searchResultData, "IP", tr, 0))
	task.attributeStorageCost(ts.searchResults)

	res := task.SearchResult()
	require.NotNil(t, res)
	require.NotNil(t, res.ResultData)
	assert.Empty(t, res.SlicedBlob)
	assert.Equal(t, int64(nq), res.NumQueries)
	assert.Equal(t, int64(topK), res.TopK)
	assert.Equal(t, int64(nq), res.ResultData.NumQueries)
	assert.Equal(t, int64(topK), res.ResultData.TopK)
	assert.Equal(t, allSearchCount, res.ResultData.GetAllSearchCount())
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
	require.NoError(t, lateMaterializeOutputFields(context.Background(), ts.searchResults, plan, sources, searchResultData))

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
	validBits := typeutil.GetFieldDataValidData(vecFD)

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
