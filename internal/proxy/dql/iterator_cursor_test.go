// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"math"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/iteratorutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/stretchr/testify/require"
)

func cursorTestSchema(kind schemapb.DataType) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "pk", DataType: kind, IsPrimaryKey: true}}}
}
func cursorTestParams(pairs ...string) []*commonpb.KeyValuePair {
	result := make([]*commonpb.KeyValuePair, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		result = append(result, &commonpb.KeyValuePair{Key: pairs[i], Value: pairs[i+1]})
	}
	return result
}

func TestIteratorPKScoringRejectsLiveBM25Statistics(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_SparseFloatVector}
	schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{
		{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{101}},
	}}
	for _, continuation := range []bool{false, true} {
		cursor := &planpb.SearchIteratorV2Info{CursorVersion: 2}
		if continuation {
			cursor.LastPk = &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}}
		}
		info := &planpb.QueryInfo{SearchIteratorV2Info: cursor}
		// Resolve the function even when the caller omitted its metric.
		require.ErrorIs(t, validateIteratorPKScoring(info, field, schema), merr.ErrParameterInvalid)
		require.Equal(t, uint32(2), cursor.CursorVersion)
		info.MetricType = "bm25"
		require.ErrorIs(t, validateIteratorPKScoring(info, field, nil), merr.ErrParameterInvalid)
		info.MetricType = "IP"
		require.NoError(t, validateIteratorPKScoring(info, field, &schemapb.CollectionSchema{}))
		require.NoError(t, validateIteratorPKScoring(info, &schemapb.FieldSchema{FieldID: 102}, schema))
		cursor.CursorVersion = 0
		require.NoError(t, validateIteratorPKScoring(info, field, schema))
		info.MetricType = "BM25"
		require.NoError(t, validateIteratorPKScoring(info, field, schema))
	}
	require.NoError(t, validateIteratorPKScoring(nil, field, schema))
}
func TestConfigureIteratorPKCursorLegacyAndInitialRequests(t *testing.T) {
	bound := float32(0)
	legacy := &planpb.SearchIteratorV2Info{Token: "old-token", LastBound: &bound}
	require.NoError(t, configureIteratorPKCursor(nil, legacy, cursorTestSchema(schemapb.DataType_Int64)))
	require.Zero(t, legacy.CursorVersion)
	require.Same(t, &bound, legacy.LastBound)
	require.Equal(t, "old-token", legacy.Token)
	initial := &planpb.SearchIteratorV2Info{}
	require.NoError(t, configureIteratorPKCursor(cursorTestParams(iteratorutil.CursorVersionKey, "2"), initial, cursorTestSchema(schemapb.DataType_Int64)))
	require.Equal(t, uint32(2), initial.CursorVersion)
	require.Nil(t, initial.LastPk)
	require.Nil(t, initial.LastBound)
}
func TestConfigureIteratorPKCursorLiteralKeysAndPrecision(t *testing.T) {
	for _, value := range []string{"-9223372036854775808", "9007199254740993", "9223372036854775807"} {
		t.Run(value, func(t *testing.T) {
			bound := float32(0.25)
			info := &planpb.SearchIteratorV2Info{LastBound: &bound}
			require.NoError(t, configureIteratorPKCursor(cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "int64", iteratorutil.LastPKKey, value), info, cursorTestSchema(schemapb.DataType_Int64)))
			require.NotNil(t, info.LastPk)
			require.IsType(t, &planpb.GenericValue_Int64Val{}, info.LastPk.Val)
		})
	}
	for _, value := range []string{"", "quoted\"\\雪\n", "nul\x00尾"} {
		bound := float32(-0.25)
		info := &planpb.SearchIteratorV2Info{LastBound: &bound}
		require.NoError(t, configureIteratorPKCursor(cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "varchar", iteratorutil.LastPKKey, value), info, cursorTestSchema(schemapb.DataType_VarChar)))
		require.IsType(t, &planpb.GenericValue_StringVal{}, info.LastPk.Val)
		require.Equal(t, value, info.LastPk.GetStringVal())
	}
}
func TestConfigureIteratorPKCursorRejectsPartialOrMalformedClientState(t *testing.T) {
	bound := float32(0.5)
	cases := []struct {
		name   string
		params []*commonpb.KeyValuePair
		info   *planpb.SearchIteratorV2Info
		schema *schemapb.CollectionSchema
	}{
		{"no version", cursorTestParams(iteratorutil.LastPKTypeKey, "int64", iteratorutil.LastPKKey, "1"), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_Int64)},
		{"unknown version", cursorTestParams(iteratorutil.CursorVersionKey, "3"), &planpb.SearchIteratorV2Info{}, cursorTestSchema(schemapb.DataType_Int64)},
		{"requires V2", cursorTestParams(iteratorutil.CursorVersionKey, "2"), nil, cursorTestSchema(schemapb.DataType_Int64)},
		{"missing value", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "int64"), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_Int64)},
		{"missing kind", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKKey, "1"), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_Int64)},
		{"old bound alone", cursorTestParams(iteratorutil.CursorVersionKey, "2"), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_Int64)},
		{"key without bound", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "int64", iteratorutil.LastPKKey, "1"), &planpb.SearchIteratorV2Info{}, cursorTestSchema(schemapb.DataType_Int64)},
		{"schema mismatch", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "varchar", iteratorutil.LastPKKey, "1"), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_Int64)},
		{"overflow", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "int64", iteratorutil.LastPKKey, "9223372036854775808"), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_Int64)},
		{"duplicate version", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.CursorVersionKey, "2"), &planpb.SearchIteratorV2Info{}, cursorTestSchema(schemapb.DataType_Int64)},
		{"duplicate empty key", cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "varchar", iteratorutil.LastPKKey, "", iteratorutil.LastPKKey, ""), &planpb.SearchIteratorV2Info{LastBound: &bound}, cursorTestSchema(schemapb.DataType_VarChar)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { require.Error(t, configureIteratorPKCursor(tc.params, tc.info, tc.schema)) })
	}
	for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
		require.Error(t, configureIteratorPKCursor(cursorTestParams(iteratorutil.CursorVersionKey, "2", iteratorutil.LastPKTypeKey, "int64", iteratorutil.LastPKKey, "1"), &planpb.SearchIteratorV2Info{LastBound: &value}, cursorTestSchema(schemapb.DataType_Int64)))
	}
	require.ErrorIs(t, configureIteratorPKCursor(cursorTestParams(iteratorutil.CursorVersionKey, "2"), &planpb.SearchIteratorV2Info{}, &schemapb.CollectionSchema{}), merr.ErrServiceInternal)
}
func markedCursorWorker() *internalpb.SearchResults {
	status := merr.Success()
	iteratorutil.MarkPKCursor(status)
	return &internalpb.SearchResults{Status: status}
}
func TestAttachIteratorPKCursorRequiresEveryWorkerAndRejectsContinuationDowngrade(t *testing.T) {
	worker := markedCursorWorker()
	old := &internalpb.SearchResults{Status: merr.Success()}
	result := &milvuspb.SearchResults{Status: &commonpb.Status{ExtraInfo: map[string]string{"report_value": "7"}}}
	info := &planpb.SearchIteratorV2Info{CursorVersion: 2}
	require.NoError(t, attachIteratorPKCursor(result, info, []*internalpb.SearchResults{worker, old}))
	require.NotContains(t, result.Status.ExtraInfo, iteratorutil.CursorVersionKey)
	require.Equal(t, "7", result.Status.ExtraInfo["report_value"])
	info.LastPk = &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}
	require.ErrorIs(t, attachIteratorPKCursor(result, info, []*internalpb.SearchResults{worker, old}), merr.ErrServiceUnimplemented)
	require.NotContains(t, result.Status.ExtraInfo, iteratorutil.CursorVersionKey)
	for _, workers := range [][]*internalpb.SearchResults{nil, {worker, nil}} {
		info.LastPk = nil
		require.NoError(t, attachIteratorPKCursor(result, info, workers))
		require.NotContains(t, result.Status.ExtraInfo, iteratorutil.CursorVersionKey)
	}
}
func TestAttachIteratorPKCursorUsesLastRawIDAndPreservesOtherMetadata(t *testing.T) {
	result := &milvuspb.SearchResults{Status: &commonpb.Status{ExtraInfo: map[string]string{"report_value": "7"}}, Results: &schemapb.SearchResultData{
		NumQueries: 1, Topks: []int64{2},
		Scores: []float32{0.1, 0.2}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 9007199254740993}}}},
	}}
	require.NoError(t, attachIteratorPKCursor(result, &planpb.SearchIteratorV2Info{CursorVersion: 2}, []*internalpb.SearchResults{markedCursorWorker()}))
	require.Equal(t, "2", result.Status.ExtraInfo[iteratorutil.CursorVersionKey])
	require.Equal(t, "int64", result.Status.ExtraInfo[iteratorutil.LastPKTypeKey])
	require.Equal(t, "9007199254740993", result.Status.ExtraInfo[iteratorutil.LastPKKey])
	require.Equal(t, "7", result.Status.ExtraInfo["report_value"])
	for _, key := range []string{"", "quote\"\\雪\n", "nul\x00尾"} {
		result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{NumQueries: 1, Topks: []int64{1}, Scores: []float32{0.2}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{key}}}}}}
		require.NoError(t, attachIteratorPKCursor(result, &planpb.SearchIteratorV2Info{CursorVersion: 2}, []*internalpb.SearchResults{markedCursorWorker()}))
		require.Equal(t, "varchar", result.Status.ExtraInfo[iteratorutil.LastPKTypeKey])
		value, present := result.Status.ExtraInfo[iteratorutil.LastPKKey]
		require.True(t, present)
		require.Equal(t, key, value)
	}
}
func TestAttachIteratorPKCursorEmptyResultStillAcknowledgesExecutionAndMalformedIDsFail(t *testing.T) {
	result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{NumQueries: 1, Topks: []int64{0}}}
	require.NoError(t, attachIteratorPKCursor(result, &planpb.SearchIteratorV2Info{CursorVersion: 2}, []*internalpb.SearchResults{markedCursorWorker()}))
	require.Equal(t, "2", result.Status.ExtraInfo[iteratorutil.CursorVersionKey])
	require.NotContains(t, result.Status.ExtraInfo, iteratorutil.LastPKKey)
	for _, ids := range []*schemapb.IDs{nil, {IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}} {
		result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{NumQueries: 1, Topks: []int64{1}, Scores: []float32{0.2}, Ids: ids}}
		require.ErrorIs(t, attachIteratorPKCursor(result, &planpb.SearchIteratorV2Info{CursorVersion: 2}, []*internalpb.SearchResults{markedCursorWorker()}), merr.ErrServiceInternal)
	}
}

func TestAttachIteratorPKCursorRejectsMalformedNqOrTopks(t *testing.T) {
	for _, data := range []*schemapb.SearchResultData{
		{NumQueries: 2, Topks: []int64{1}, Scores: []float32{0.2}},
		{NumQueries: 1, Topks: nil, Scores: []float32{0.2}},
		{NumQueries: 1, Topks: []int64{2}, Scores: []float32{0.2}},
	} {
		data.Ids = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
		result := &milvuspb.SearchResults{Results: data}
		require.ErrorIs(t, attachIteratorPKCursor(result, &planpb.SearchIteratorV2Info{CursorVersion: 2}, []*internalpb.SearchResults{markedCursorWorker()}), merr.ErrServiceInternal)
	}
}

func TestDeclineIteratorPKCursorRejectsUnsupportedStrictOptions(t *testing.T) {
	require.NoError(t, declineIteratorPKCursor(nil, "unsupported search feature"))
	initial := &planpb.SearchIteratorV2Info{CursorVersion: 2, Token: "token"}
	require.ErrorIs(t, declineIteratorPKCursor(initial, "embedding lists"), merr.ErrParameterInvalid)
	require.Equal(t, uint32(2), initial.CursorVersion)
	require.Equal(t, "token", initial.Token)
	bound := float32(0.25)
	legacy := &planpb.SearchIteratorV2Info{Token: "legacy-token", LastBound: &bound}
	require.NoError(t, declineIteratorPKCursor(legacy, "embedding lists"))
	require.Same(t, &bound, legacy.LastBound)
	continuation := &planpb.SearchIteratorV2Info{
		CursorVersion: 2, LastBound: &bound,
		LastPk: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
	}
	require.ErrorIs(t, declineIteratorPKCursor(continuation, "embedding lists"), merr.ErrParameterInvalid)
	require.Equal(t, uint32(2), continuation.CursorVersion)
	require.Same(t, &bound, continuation.LastBound)
}

func TestAttachIteratorPKCursorRejectsEmptyIDsAndNonfiniteScoresBeforeMarking(t *testing.T) {
	invalid := []*schemapb.SearchResultData{
		{NumQueries: 1, Topks: []int64{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}},
	}
	for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
		invalid = append(invalid, &schemapb.SearchResultData{
			NumQueries: 1, Topks: []int64{2}, Scores: []float32{value, 0.5},
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		})
	}
	for _, data := range invalid {
		result := &milvuspb.SearchResults{
			Status: &commonpb.Status{ExtraInfo: map[string]string{"report_value": "7"}}, Results: data,
		}
		require.ErrorIs(t, attachIteratorPKCursor(result, &planpb.SearchIteratorV2Info{CursorVersion: 2}, []*internalpb.SearchResults{markedCursorWorker()}), merr.ErrServiceInternal)
		require.Equal(t, map[string]string{"report_value": "7"}, result.Status.ExtraInfo)
	}
}
