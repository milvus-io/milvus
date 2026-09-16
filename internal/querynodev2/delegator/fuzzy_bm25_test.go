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

package delegator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/textindex"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeFuzzyGrowingSegment struct {
	segments.Segment
	id            int64
	collectionID  int64
	matches       [][]textindex.FuzzyMatch
	generation    uint64
	err           error
	fieldID       int64
	preparedCount int
	prepared      []*textindex.PreparedFuzzySearch
	maxExpansions uint32
	rowCount      int64
}

func (s *fakeFuzzyGrowingSegment) ID() int64 {
	return s.id
}

func (s *fakeFuzzyGrowingSegment) Collection() int64 {
	return s.collectionID
}

func (s *fakeFuzzyGrowingSegment) InsertCount() int64 {
	return s.rowCount
}

func (s *fakeFuzzyGrowingSegment) ExpandTextTerms(
	fieldID int64,
	prepared []*textindex.PreparedFuzzySearch,
	maxExpansions uint32,
) ([][]textindex.FuzzyMatch, uint64, error) {
	s.fieldID = fieldID
	s.preparedCount = len(prepared)
	s.prepared = prepared
	s.maxExpansions = maxExpansions
	return s.matches, s.generation, s.err
}

func withFuzzyTestAdmission(sd *shardDelegator) *shardDelegator {
	sd.fuzzyExpansionRPCSemaphore = syncutil.NewSemaphore(16)
	sd.fuzzyExpansionNativeSemaphore = syncutil.NewSemaphore(16)
	return sd
}

func setFuzzyTestNodeID(t *testing.T, nodeID int64) {
	oldNodeID := paramtable.GetNodeID()
	paramtable.SetNodeID(nodeID)
	t.Cleanup(func() {
		paramtable.SetNodeID(oldNodeID)
	})
}

func TestBuildFuzzyBM25QueryTF(t *testing.T) {
	expanded := map[uint32][]*querypb.ExpandedTextTerm{
		0: {
			{Term: []byte("book"), EditDistance: 1},
			{Term: []byte("boon"), EditDistance: 0},
			{Term: []byte("ghost"), EditDistance: 0},
		},
		1: {
			{Term: []byte("book"), EditDistance: 0},
			{Term: []byte("back"), EditDistance: 0},
		},
	}

	rows, err := buildFuzzyBM25QueryTF([]map[uint32]float32{{0: 2, 1: 3}}, expanded)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, map[uint32]float32{
		typeutil.HashString2LessUint32("back"):  3,
		typeutil.HashString2LessUint32("book"):  5,
		typeutil.HashString2LessUint32("boon"):  2,
		typeutil.HashString2LessUint32("ghost"): 2,
	}, typeutil.SparseFloatBytesToMap(rows[0]))
}

func TestBuildFuzzyBM25QueryTFRejectsProjectedOversize(t *testing.T) {
	params := paramtable.Get()
	oldMaxOutputSize := params.QuotaConfig.MaxOutputSize.GetValue()
	params.QuotaConfig.MaxOutputSize.SwapTempValue("15")
	t.Cleanup(func() {
		params.QuotaConfig.MaxOutputSize.SwapTempValue(oldMaxOutputSize)
	})

	_, err := buildFuzzyBM25QueryTF(
		[]map[uint32]float32{{0: 1}, {0: 1}},
		map[uint32][]*querypb.ExpandedTextTerm{0: {{Term: []byte("book")}}},
	)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func TestValidateFuzzyBM25ExpansionOutputSize(t *testing.T) {
	response := &querypb.ExpandTextTermsResponse{
		Status: merr.Success(),
		Terms: []*querypb.ExpandedTextTerm{{
			SourceIndex:  0,
			Term:         []byte("book"),
			EditDistance: 1,
		}},
	}
	params := paramtable.Get()
	outputSize := proto.Size(response)
	oldMaxOutputSize := params.QuotaConfig.MaxOutputSize.GetValue()
	params.QuotaConfig.MaxOutputSize.SwapTempValue(fmt.Sprint(outputSize))
	t.Cleanup(func() {
		params.QuotaConfig.MaxOutputSize.SwapTempValue(oldMaxOutputSize)
	})

	require.NoError(t, ValidateFuzzyBM25ExpansionOutputSize(response))
	params.QuotaConfig.MaxOutputSize.SwapTempValue(fmt.Sprint(outputSize - 1))
	err := ValidateFuzzyBM25ExpansionOutputSize(response)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	assert.Contains(t, err.Error(), "fuzzy BM25 term expansion result size")
}

func TestExpandGrowingFuzzyBM25TermsRejectsOversizedOutput(t *testing.T) {
	params := paramtable.Get()
	oldMaxOutputSize := params.QuotaConfig.MaxOutputSize.GetValue()
	params.QuotaConfig.MaxOutputSize.SwapTempValue("1")
	t.Cleanup(func() {
		params.QuotaConfig.MaxOutputSize.SwapTempValue(oldMaxOutputSize)
	})

	segment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		matches: [][]textindex.FuzzyMatch{{{
			Term: []byte("book"), EditDistance: 1,
		}}},
		generation: 1,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(segment).Once()
	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:   1000,
		segmentManager: segmentManager,
	})

	_, err := sd.expandLocalFuzzyBM25Terms(context.Background(), &querypb.ExpandTextTermsRequest{
		CollectionID:    1000,
		FieldID:         101,
		SourceTerms:     [][]byte{[]byte("bok")},
		MaxEditDistance: 1,
		MaxExpansions:   50,
	}, []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}, nil)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func TestExpandGrowingFuzzyBM25TermsHonorsNativeConcurrencyCancellation(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	nativeSemaphore := syncutil.NewSemaphore(1)
	require.True(t, nativeSemaphore.TryAcquire())
	defer nativeSemaphore.Release()
	sd := &shardDelegator{
		collectionID:                  1000,
		segmentManager:                segmentManager,
		fuzzyExpansionNativeSemaphore: nativeSemaphore,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := sd.expandLocalFuzzyBM25Terms(ctx, &querypb.ExpandTextTermsRequest{
		CollectionID:    1000,
		FieldID:         101,
		SourceTerms:     [][]byte{[]byte("bok")},
		MaxEditDistance: 1,
		MaxExpansions:   50,
	}, []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestExpandGrowingFuzzyBM25TermsRejectsUnavailableSegmentBeforePreparation(t *testing.T) {
	first := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		generation:   1,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(first).Once()
	segmentManager.EXPECT().GetGrowing(int64(21)).Return(nil).Once()
	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:   1000,
		segmentManager: segmentManager,
	})

	response, err := sd.expandLocalFuzzyBM25Terms(context.Background(), &querypb.ExpandTextTermsRequest{
		CollectionID:    1000,
		FieldID:         101,
		SourceTerms:     [][]byte{[]byte("bok")},
		MaxEditDistance: 1,
		MaxExpansions:   50,
	}, []SegmentEntry{
		{SegmentID: 20, Level: datapb.SegmentLevel_L1},
		{SegmentID: 21, Level: datapb.SegmentLevel_L1},
	}, nil)
	require.ErrorIs(t, err, merr.ErrSegmentNotLoaded)
	require.NotNil(t, response)
	assert.Zero(t, first.preparedCount)
}

func TestBuildFuzzyBM25IDFUsesGlobalStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
				},
			},
			{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
			Params: []*commonpb.KeyValuePair{
				{Key: common.EnableFuzzyKey, Value: "true"},
			},
		}},
	}
	runner, err := function.NewBM25FunctionRunner(schema, schema.GetFunctions()[0])
	require.NoError(t, err)
	defer runner.Close()

	oracle := NewIDFOracle(t.Name(), schema.GetFunctions()).(*idfOracle)
	defer oracle.Close()
	globalStats, err := oracle.current.GetStats(103)
	require.NoError(t, err)
	bookHash := typeutil.HashString2LessUint32("book")
	globalStats.Append(map[uint32]float32{bookHash: 1})
	globalStats.Append(map[uint32]float32{typeutil.HashString2LessUint32("global-only"): 3})

	growingSegment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		matches: [][]textindex.FuzzyMatch{{{
			Term: []byte("book"), EditDistance: 1,
		}}},
		generation: 200,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(growingSegment).Once()
	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		segmentManager: segmentManager,
	})
	sd.publishIDFOracle(oracle)

	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Type:   commonpb.PlaceholderType_VarChar,
			Values: [][]byte{[]byte("bok")},
		}},
	})
	require.NoError(t, err)
	plan, err := proto.Marshal(&planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{QueryInfo: &planpb.QueryInfo{}},
		},
	})
	require.NoError(t, err)
	req := &querypb.SearchRequest{Req: &internalpb.SearchRequest{
		CollectionID:       1000,
		FieldId:            103,
		PlaceholderGroup:   placeholder,
		SerializedExprPlan: plan,
		FuzzyBm25Options: &internalpb.FuzzyBM25SearchOptions{
			MaxEditDistance: 1,
			MaxExpansions:   50,
		},
	}}

	avgdl, err := sd.buildFuzzyBM25IDF(
		context.Background(), req, runner, nil,
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}, nil)
	require.NoError(t, err)
	assert.Equal(t, globalStats.GetAvgdl(), avgdl)
	require.Len(t, req.GetTextTermGenerations(), 1)
	assert.EqualValues(t, 20, req.GetTextTermGenerations()[0].GetSegmentID())

	rewritten := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(req.GetReq().GetPlaceholderGroup(), rewritten))
	require.Len(t, rewritten.GetPlaceholders(), 1)
	require.Len(t, rewritten.GetPlaceholders()[0].GetValues(), 1)
	expectedTF := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{bookHash: 1})
	assert.Equal(t, globalStats.BuildIDF(expectedTF), rewritten.GetPlaceholders()[0].GetValues()[0])

	invalidPlaceholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Type:   commonpb.PlaceholderType_VarChar,
			Values: [][]byte{{0xff}},
		}},
	})
	require.NoError(t, err)
	req.Req.PlaceholderGroup = invalidPlaceholder
	_, err = sd.buildFuzzyBM25IDF(context.Background(), req, runner, nil, nil, nil)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestMultiAnalyzerBranchesShareFuzzyVocabulary(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
					{Key: "multi_analyzer_params", Value: `{"by_field":"language","analyzers":{"default":{"type":"standard"},"english":{"type":"english"}}}`},
				},
			},
			{FieldID: 102, Name: "language", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
		}},
	}
	runner, err := function.NewBM25FunctionRunner(schema, schema.GetFunctions()[0])
	require.NoError(t, err)
	defer runner.Close()

	materializer := runner.(function.TextTermMaterializer)
	_, batches, err := materializer.BatchRunWithTextTerms(
		[]string{"book", "books"},
		[]string{"english", "default"},
	)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.ElementsMatch(t, [][]byte{[]byte("book"), []byte("books")}, batches[0].Terms)

	expanded := []*querypb.ExpandedTextTerm{
		{Term: []byte("book"), EditDistance: 0},
		{Term: []byte("books"), EditDistance: 1},
	}
	rows, err := buildFuzzyBM25QueryTF(
		[]map[uint32]float32{{0: 1}},
		map[uint32][]*querypb.ExpandedTextTerm{0: expanded},
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, map[uint32]float32{
		typeutil.HashString2LessUint32("book"):  1,
		typeutil.HashString2LessUint32("books"): 1,
	}, typeutil.SparseFloatBytesToMap(rows[0]))
}

func TestFuzzySearchTargetsExcludeL0(t *testing.T) {
	sealed, growing := fuzzySearchTargets(
		[]SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
			{SegmentID: 10, Level: datapb.SegmentLevel_L1},
			{SegmentID: 11, Level: datapb.SegmentLevel_L0},
		}}},
		[]SegmentEntry{
			{SegmentID: 20, Level: datapb.SegmentLevel_L1},
			{SegmentID: 21, Level: datapb.SegmentLevel_L0},
		},
	)
	require.Len(t, sealed, 1)
	require.Len(t, sealed[0].Segments, 1)
	assert.EqualValues(t, 10, sealed[0].Segments[0].SegmentID)
	require.Len(t, growing, 1)
	assert.EqualValues(t, 20, growing[0].SegmentID)
}

func TestModifySearchRequestFiltersTextTermGenerations(t *testing.T) {
	sd := &shardDelegator{vchannelName: "channel"}
	modified := sd.modifySearchRequest(&querypb.SearchRequest{
		Req: &internalpb.SearchRequest{},
		TextTermGenerations: []*querypb.SegmentTextTermGeneration{
			{SegmentID: 10, Generation: 1},
			{SegmentID: 20, Generation: 2},
		},
	}, querypb.DataScope_Historical, []int64{20}, 100)

	require.Len(t, modified.GetTextTermGenerations(), 1)
	assert.EqualValues(t, 20, modified.GetTextTermGenerations()[0].GetSegmentID())
	assert.EqualValues(t, 2, modified.GetTextTermGenerations()[0].GetGeneration())
}

func TestExpandFuzzyBM25TermsPreservesPartialResultPolicy(t *testing.T) {
	setFuzzyTestNodeID(t, 100)

	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	successWorker := cluster.NewMockWorker(t)
	successWorker.EXPECT().ExpandTextTerms(mock.Anything, mock.MatchedBy(func(req *querypb.ExpandTextTermsRequest) bool {
		segmentIDs := req.GetSegmentIDs()
		return req.GetMaxExpansions() == 50 &&
			len(segmentIDs) == 1 && segmentIDs[0] == 10
	})).Return(&querypb.ExpandTextTermsResponse{
		Status: merr.Success(),
		Terms: []*querypb.ExpandedTextTerm{{
			SourceIndex:  0,
			Term:         []byte("book"),
			EditDistance: 1,
		}},
		Generations: []*querypb.SegmentTextTermGeneration{{
			SegmentID:  10,
			Generation: 100,
		}},
	}, nil).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()
	workerManager.EXPECT().GetWorker(mock.Anything, int64(-1)).Return(nil, merr.ErrNodeNotFound).Once()

	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:  1000,
		vchannelName:  "channel",
		workerManager: workerManager,
	})
	sealed := []SnapshotItem{
		{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
		{NodeID: -1, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
	}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 1, 50, 0,
		sealed, nil, map[int64]int64{10: 100, 20: 100})
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 1)
	assert.EqualValues(t, 10, generations[0].GetSegmentID())
}

func TestExpandFuzzyBM25TermsGroupsSegmentsPerWorkerAndForwardsLimit(t *testing.T) {
	setFuzzyTestNodeID(t, 100)

	worker := cluster.NewMockWorker(t)
	worker.EXPECT().ExpandTextTerms(mock.Anything, mock.MatchedBy(func(req *querypb.ExpandTextTermsRequest) bool {
		return req.GetMaxEditDistance() == 2 &&
			req.GetMaxExpansions() == 7 &&
			req.GetPrefixLength() == 3 &&
			assert.ElementsMatch(t, []int64{10, 11}, req.GetSegmentIDs())
	})).Return(&querypb.ExpandTextTermsResponse{
		Status: merr.Success(),
		Terms: []*querypb.ExpandedTextTerm{{
			SourceIndex:  0,
			Term:         []byte("book"),
			EditDistance: 1,
		}},
		Generations: []*querypb.SegmentTextTermGeneration{
			{SegmentID: 10, Generation: 100},
			{SegmentID: 11, Generation: 101},
		},
	}, nil).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(worker, nil).Once()

	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:  1000,
		vchannelName:  "channel",
		workerManager: workerManager,
	})
	sealed := []SnapshotItem{{
		NodeID: 1,
		Segments: []SegmentEntry{
			{SegmentID: 10, Level: datapb.SegmentLevel_L1},
			{SegmentID: 11, Level: datapb.SegmentLevel_L1},
		},
	}}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 2, 7, 3,
		sealed, nil, map[int64]int64{10: 100, 11: 100})
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 2)
}

func TestExpandFuzzyBM25TermsCollectsGenerationWithoutSourceTerms(t *testing.T) {
	segment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		generation:   200,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(segment).Once()
	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		segmentManager: segmentManager,
	})

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, nil, 1, 50, 0, nil,
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}, nil)
	require.NoError(t, err)
	assert.Empty(t, expanded)
	require.Len(t, generations, 1)
	assert.EqualValues(t, 20, generations[0].GetSegmentID())
	assert.EqualValues(t, 200, generations[0].GetGeneration())
	assert.Zero(t, segment.preparedCount)
}

func TestExpandFuzzyBM25TermsReusesDFAAcrossColocatedGrowingAndSealed(t *testing.T) {
	setFuzzyTestNodeID(t, 1)

	growingSegment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		matches: [][]textindex.FuzzyMatch{{{
			Term: []byte("books"), EditDistance: 2,
		}}},
		generation: 200,
	}
	sealedSegment := &fakeFuzzyGrowingSegment{
		id:           10,
		collectionID: 1000,
		matches: [][]textindex.FuzzyMatch{{{
			Term: []byte("book"), EditDistance: 1,
		}}},
		generation: 100,
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(growingSegment).Once()
	segmentManager.EXPECT().GetSealed(int64(10)).Return(sealedSegment).Once()

	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		segmentManager: segmentManager,
	})
	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(),
		101,
		[][]byte{[]byte("bok")},
		2,
		7,
		3,
		[]SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}},
		}},
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}},
		map[int64]int64{10: 100},
	)
	require.NoError(t, err)
	require.Len(t, expanded[0], 2)
	assert.ElementsMatch(t, [][]byte{[]byte("book"), []byte("books")}, [][]byte{
		expanded[0][0].GetTerm(), expanded[0][1].GetTerm(),
	})
	require.Len(t, generations, 2)
	assert.Equal(t, int64(101), growingSegment.fieldID)
	assert.Equal(t, 1, growingSegment.preparedCount)
	assert.Equal(t, 1, sealedSegment.preparedCount)
	require.Len(t, growingSegment.prepared, 1)
	require.Len(t, sealedSegment.prepared, 1)
	assert.Same(t, growingSegment.prepared[0], sealedSegment.prepared[0])
	assert.EqualValues(t, 7, growingSegment.maxExpansions)
}

func TestExpandFuzzyBM25TermsRejectsAggregateOutput(t *testing.T) {
	setFuzzyTestNodeID(t, 100)

	params := paramtable.Get()
	oldMaxOutputSize := params.QuotaConfig.MaxOutputSize.GetValue()
	oneTermSize := FuzzyBM25ExpandedTermWireSize(&querypb.ExpandedTextTerm{
		Term: []byte("book"), EditDistance: 1,
	})
	oneGenerationSize := fuzzyBM25GenerationWireSize(&querypb.SegmentTextTermGeneration{
		SegmentID: 10, Generation: 1,
	})
	params.QuotaConfig.MaxOutputSize.SwapTempValue(fmt.Sprint(oneTermSize + oneGenerationSize))
	t.Cleanup(func() {
		params.QuotaConfig.MaxOutputSize.SwapTempValue(oldMaxOutputSize)
	})

	newWorker := func(segmentID int64, term string) *cluster.MockWorker {
		worker := cluster.NewMockWorker(t)
		worker.EXPECT().ExpandTextTerms(mock.Anything, mock.Anything).
			Return(&querypb.ExpandTextTermsResponse{
				Status: merr.Success(),
				Terms: []*querypb.ExpandedTextTerm{{
					SourceIndex: 0, Term: []byte(term), EditDistance: 1,
				}},
				Generations: []*querypb.SegmentTextTermGeneration{{
					SegmentID: segmentID, Generation: 1,
				}},
			}, nil).Once()
		return worker
	}
	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(newWorker(10, "book"), nil).Once()
	workerManager.EXPECT().GetWorker(mock.Anything, int64(2)).Return(newWorker(20, "back"), nil).Once()
	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID: 1000, vchannelName: "channel", workerManager: workerManager,
	})

	_, _, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 1, 1, 0,
		[]SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
			{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
		}, nil, map[int64]int64{10: 100, 20: 100})
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func TestExpandFuzzyBM25TermsAppliesPartialPolicyToLocalGrowingFailure(t *testing.T) {
	setFuzzyTestNodeID(t, 100)

	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	worker := cluster.NewMockWorker(t)
	worker.EXPECT().ExpandTextTerms(mock.Anything, mock.AnythingOfType("*querypb.ExpandTextTermsRequest")).
		Return(&querypb.ExpandTextTermsResponse{
			Status: merr.Success(),
			Terms: []*querypb.ExpandedTextTerm{{
				SourceIndex: 0, Term: []byte("book"), EditDistance: 1,
			}},
			Generations: []*querypb.SegmentTextTermGeneration{{
				SegmentID: 10, Generation: 100,
			}},
		}, nil).Once()
	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(worker, nil).Once()

	growingSegment := &fakeFuzzyGrowingSegment{
		id:           20,
		collectionID: 1000,
		rowCount:     101,
		err:          errors.New("local growing expansion failed"),
	}
	segmentManager := segments.NewMockSegmentManager(t)
	segmentManager.EXPECT().GetGrowing(int64(20)).Return(growingSegment).Once()

	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:   1000,
		vchannelName:   "channel",
		workerManager:  workerManager,
		segmentManager: segmentManager,
	})
	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(),
		101,
		[][]byte{[]byte("bok")},
		1,
		50,
		0,
		[]SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}},
		}},
		[]SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1, Candidate: growingSegment}},
		map[int64]int64{10: 100},
	)
	require.Error(t, err)
	assert.Nil(t, expanded)
	assert.Nil(t, generations)
}

func TestExpandFuzzyBM25TermsPreservesPartialResultAfterWorkerFailure(t *testing.T) {
	setFuzzyTestNodeID(t, 100)

	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	successWorker := cluster.NewMockWorker(t)
	successWorker.EXPECT().ExpandTextTerms(mock.Anything, mock.AnythingOfType("*querypb.ExpandTextTermsRequest")).
		Return(&querypb.ExpandTextTermsResponse{
			Status: merr.Success(),
			Terms: []*querypb.ExpandedTextTerm{{
				SourceIndex:  0,
				Term:         []byte("book"),
				EditDistance: 1,
			}},
			Generations: []*querypb.SegmentTextTermGeneration{{
				SegmentID:  10,
				Generation: 100,
			}},
		}, nil).Once()
	failedWorker := cluster.NewMockWorker(t)
	failedWorker.EXPECT().ExpandTextTerms(mock.Anything, mock.AnythingOfType("*querypb.ExpandTextTermsRequest")).
		Return(nil, merr.WrapErrNodeNotFound(2)).Once()

	workerManager := cluster.NewMockManager(t)
	workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()
	workerManager.EXPECT().GetWorker(mock.Anything, int64(2)).Return(failedWorker, nil).Once()

	distribution := NewDistribution("channel", NewChannelQueryView(nil, map[int64]int64{10: 100, 20: 100}, nil, 1))
	distribution.AddDistributions(
		SegmentEntry{NodeID: 1, SegmentID: 10, Version: 1},
		SegmentEntry{NodeID: 2, SegmentID: 20, Version: 1},
	)
	t.Cleanup(distribution.Close)
	sd := withFuzzyTestAdmission(&shardDelegator{
		collectionID:  1000,
		vchannelName:  "channel",
		workerManager: workerManager,
		distribution:  distribution,
	})
	sealed := []SnapshotItem{
		{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
		{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
	}

	expanded, generations, err := sd.expandFuzzyBM25Terms(
		context.Background(), 101, [][]byte{[]byte("bok")}, 1, 50, 0,
		sealed, nil, map[int64]int64{10: 100, 20: 100})
	require.NoError(t, err)
	require.Len(t, expanded[0], 1)
	assert.Equal(t, []byte("book"), expanded[0][0].GetTerm())
	require.Len(t, generations, 1)
	assert.EqualValues(t, 10, generations[0].GetSegmentID())
	distribution.mut.RLock()
	failedEntry := distribution.sealedSegments[20]
	distribution.mut.RUnlock()
	assert.True(t, failedEntry.Offline)
	assert.EqualValues(t, -1, failedEntry.NodeID)
	assert.EqualValues(t, unreadableTargetVersion, failedEntry.Version)
}

func TestSearchFuzzyBM25RetriesFromExpansionAfterGenerationMismatch(t *testing.T) {
	mockey.PatchConvey("retry the complete lexical preparation after search generation mismatch", t, func() {
		prepareCalls := 0
		executeCalls := 0
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			_ *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			prepareCalls++
			return 1, false, nil
		}).Build()
		mockey.Mock((*shardDelegator).executeSearchSubTasks).To(func(
			_ *shardDelegator,
			_ context.Context,
			_ *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) ([]*internalpb.SearchResults, error) {
			executeCalls++
			if executeCalls == 1 {
				return nil, merr.WrapErrServiceUnavailableMsg("text term generation changed")
			}
			return []*internalpb.SearchResults{{Status: merr.Success()}}, nil
		}).Build()

		sd := &shardDelegator{}
		results, err := sd.searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{}},
			nil,
			nil,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, 2, prepareCalls)
		assert.Equal(t, 2, executeCalls)
	})
}

func TestSearchFuzzyBM25RetriesOtherPreparationResourceFailure(t *testing.T) {
	mockey.PatchConvey("retry non-deterministic preparation resource failure", t, func() {
		prepareCalls := 0
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			_ *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			prepareCalls++
			if prepareCalls == 1 {
				return 0, false, merr.Wrap(merr.ErrServiceResourceInsufficient, "temporary allocation failure")
			}
			return 0, true, nil
		}).Build()

		results, err := (&shardDelegator{}).searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{}},
			nil,
			nil,
			nil,
		)
		require.NoError(t, err)
		assert.Empty(t, results)
		assert.Equal(t, 2, prepareCalls)
	})
}

func TestSearchFuzzyBM25PreservesPartialResultPolicy(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key, "0.5"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryNodeCfg.PartialResultRequiredDataRatio.Key))
	})

	mockey.PatchConvey("fuzzy search uses the ordinary partial-result policy", t, func() {
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			req *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			req.TextTermGenerations = []*querypb.SegmentTextTermGeneration{
				{SegmentID: 10, Generation: 1},
				{SegmentID: 20, Generation: 1},
			}
			return 1, false, nil
		}).Build()

		successWorker := cluster.NewMockWorker(t)
		successWorker.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(&internalpb.SearchResults{Status: merr.Success()}, nil).Once()
		failedWorker := cluster.NewMockWorker(t)
		failedWorker.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(nil, errors.New("worker unavailable")).Once()

		workerManager := cluster.NewMockManager(t)
		workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()
		workerManager.EXPECT().GetWorker(mock.Anything, int64(2)).Return(failedWorker, nil).Once()

		sd := &shardDelegator{
			vchannelName:  "channel",
			workerManager: workerManager,
		}
		sealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
			{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
		}
		results, err := sd.searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}},
			sealed,
			nil,
			map[int64]int64{10: 100, 20: 100},
		)

		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

func TestSearchFuzzyBM25UsesExpansionPartialTargets(t *testing.T) {
	mockey.PatchConvey("fuzzy search uses only segments served by term expansion", t, func() {
		mockey.Mock((*shardDelegator).prepareSearchFunction).To(func(
			_ *shardDelegator,
			_ context.Context,
			req *querypb.SearchRequest,
			_ []SnapshotItem,
			_ []SegmentEntry,
			_ map[int64]int64,
		) (float64, bool, error) {
			req.TextTermGenerations = []*querypb.SegmentTextTermGeneration{{SegmentID: 10, Generation: 1}}
			return 1, false, nil
		}).Build()

		successWorker := cluster.NewMockWorker(t)
		successWorker.EXPECT().SearchSegments(mock.Anything, mock.MatchedBy(func(req *querypb.SearchRequest) bool {
			return len(req.GetSegmentIDs()) == 1 && req.GetSegmentIDs()[0] == 10
		})).Return(&internalpb.SearchResults{Status: merr.Success()}, nil).Once()

		workerManager := cluster.NewMockManager(t)
		workerManager.EXPECT().GetWorker(mock.Anything, int64(1)).Return(successWorker, nil).Once()

		sd := &shardDelegator{
			vchannelName:  "channel",
			workerManager: workerManager,
		}
		sealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 10, Level: datapb.SegmentLevel_L1}}},
			{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 20, Level: datapb.SegmentLevel_L1}}},
		}
		results, err := sd.searchFuzzyBM25(
			context.Background(),
			&querypb.SearchRequest{Req: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}},
			sealed,
			nil,
			map[int64]int64{10: 100, 20: 100},
		)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}
