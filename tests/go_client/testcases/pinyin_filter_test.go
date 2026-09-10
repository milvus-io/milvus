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

// L0 coverage for Pinyin Text Match through the public Go SDK.
package testcases

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	pinyinIndexedSealedCount   = 3000
	pinyinUnindexedSealedCount = 500
	pinyinGrowingCount         = 500
	pinyinTotalCount           = pinyinIndexedSealedCount + pinyinUnindexedSealedCount + pinyinGrowingCount
	pinyinVectorDim            = 2
)

var pinyinTargetIDs = []int64{
	0,
	pinyinIndexedSealedCount,
	pinyinIndexedSealedCount + pinyinUnindexedSealedCount,
}

func pinyinAnalyzerParams(keepOriginal bool) map[string]any {
	return map[string]any{
		"tokenizer": "jieba",
		"filter": []any{
			map[string]any{
				"type":                       "pinyin",
				"keep_original":              keepOriginal,
				"keep_full_pinyin":           false,
				"keep_joined_full_pinyin":    true,
				"keep_separate_first_letter": false,
			},
		},
	}
}

func pinyinCollectionSchema(collectionName string, analyzerParams map[string]any) *entity.Schema {
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(analyzerParams)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(pinyinVectorDim))
}

func createPinyinCollection(
	t *testing.T,
	ctx CtxT,
	mc MC,
	collectionName string,
	analyzerParams map[string]any,
) {
	t.Helper()

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(
		collectionName,
		pinyinCollectionSchema(collectionName, analyzerParams),
	).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})
}

func indexAndLoadPinyinCollection(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	t.Helper()

	indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, "vector",
		index.NewIvfFlatIndex(entity.L2, 64)))
	require.NoError(t, err)
	require.NoError(t, indexTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
}

func requirePinyinFieldAnalyzerTokens(
	t *testing.T,
	ctx CtxT,
	mc MC,
	collectionName string,
	text string,
	expected []string,
) {
	t.Helper()

	results, err := mc.RunAnalyzer(ctx, client.NewRunAnalyzerOption(text).
		WithField(collectionName, "text"))
	require.NoError(t, err)
	require.Len(t, results, 1)

	tokens := make([]string, len(results[0].Tokens))
	for i, token := range results[0].Tokens {
		tokens[i] = token.Text
	}
	require.Equal(t, expected, tokens)
}

func insertPinyinRows(t *testing.T, ctx CtxT, mc MC, collectionName string, start, count int) {
	ids := make([]int64, count)
	texts := make([]string, count)
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		rowID := int64(start + i)
		ids[i] = rowID
		texts[i] = "向量数据库样本"
		if rowID == pinyinTargetIDs[0] || rowID == pinyinTargetIDs[1] || rowID == pinyinTargetIDs[2] {
			texts[i] = "中文测试"
		}
		vectors[i] = []float32{float32(rowID % 2), float32((rowID / 2) % 2)}
	}

	result, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn("text", texts).
		WithFloatVectorColumn("vector", pinyinVectorDim, vectors))
	require.NoError(t, err)
	require.EqualValues(t, count, result.InsertCount)
}

func flushPinyinRows(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
		if err == nil {
			require.NoError(t, flushTask.Await(ctx))
			return
		}
		lastErr = err
		require.True(t, strings.Contains(err.Error(), "rate limit exceeded"), err)
		time.Sleep(2 * time.Second)
	}
	require.NoError(t, lastErr)
}

func searchPinyinIDs(
	t *testing.T,
	ctx CtxT,
	mc MC,
	collectionName string,
	queryText string,
	minimumShouldMatch int,
) []int64 {
	filter := fmt.Sprintf("text_match(text, %q)", queryText)
	if minimumShouldMatch > 0 {
		filter = fmt.Sprintf("text_match(text, %q, minimum_should_match=%d)", queryText, minimumShouldMatch)
	}
	result, err := mc.Search(ctx, client.NewSearchOption(collectionName, pinyinTotalCount,
		[]entity.Vector{entity.FloatVector{0, 0}}).
		WithANNSField("vector").
		WithSearchParam("nprobe", "64").
		WithFilter(filter).
		WithOutputFields("id", "text").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Len(t, result, 1)

	idColumn := result[0].GetColumn("id")
	require.NotNil(t, idColumn)
	ids := make([]int64, result[0].ResultCount)
	for i := 0; i < result[0].ResultCount; i++ {
		value, err := idColumn.Get(i)
		require.NoError(t, err)
		ids[i] = value.(int64)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func searchPinyinIDsUntilExpected(
	t *testing.T,
	ctx CtxT,
	mc MC,
	collectionName string,
	queryText string,
	minimumShouldMatch int,
	expectedIDs []int64,
) []int64 {
	deadline := time.Now().Add(30 * time.Second)
	var ids []int64
	for time.Now().Before(deadline) {
		ids = searchPinyinIDs(t, ctx, mc, collectionName, queryText, minimumShouldMatch)
		if len(ids) == len(expectedIDs) {
			matched := true
			for i := range ids {
				if ids[i] != expectedIDs[i] {
					matched = false
					break
				}
			}
			if matched {
				return ids
			}
		}
		time.Sleep(time.Second)
	}
	return ids
}

func TestPinyinFilterTextMatchAcrossDataPaths(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("pinyin_filter", 6)

	analyzerParams := pinyinAnalyzerParams(true)
	createPinyinCollection(t, ctx, mc, collectionName, analyzerParams)

	withoutOriginalCollectionName := common.GenRandomString("pinyin_filter_no_original", 6)
	createPinyinCollection(t, ctx, mc, withoutOriginalCollectionName, pinyinAnalyzerParams(false))
	indexAndLoadPinyinCollection(t, ctx, mc, withoutOriginalCollectionName)
	requirePinyinFieldAnalyzerTokens(
		t,
		ctx,
		mc,
		withoutOriginalCollectionName,
		"中文测试",
		[]string{"zhongwen", "ceshi"},
	)

	require.NoError(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(collectionName).
		WithProperty("collection.autocompaction.enabled", false)))

	insertPinyinRows(t, ctx, mc, collectionName, 0, pinyinIndexedSealedCount)
	flushPinyinRows(t, ctx, mc, collectionName)

	insertPinyinRows(t, ctx, mc, collectionName, pinyinIndexedSealedCount, pinyinUnindexedSealedCount)
	flushPinyinRows(t, ctx, mc, collectionName)

	indexAndLoadPinyinCollection(t, ctx, mc, collectionName)
	requirePinyinFieldAnalyzerTokens(
		t,
		ctx,
		mc,
		collectionName,
		"中文测试",
		[]string{"中文", "zhongwen", "测试", "ceshi"},
	)
	requirePinyinFieldAnalyzerTokens(
		t,
		ctx,
		mc,
		collectionName,
		"中文",
		[]string{"中文", "zhongwen"},
	)

	insertPinyinRows(t, ctx, mc, collectionName,
		pinyinIndexedSealedCount+pinyinUnindexedSealedCount, pinyinGrowingCount)

	searchCases := []struct {
		name               string
		queryText          string
		minimumShouldMatch int
		expectedIDs        []int64
	}{
		{name: "joined_pinyin", queryText: "zhongwen", expectedIDs: pinyinTargetIDs},
		{name: "original", queryText: "中文", minimumShouldMatch: 2, expectedIDs: pinyinTargetIDs},
		{name: "disabled_full_pinyin", queryText: "zhong", expectedIDs: []int64{}},
		{name: "disabled_first_letters", queryText: "zw", expectedIDs: []int64{}},
	}
	for _, searchCase := range searchCases {
		t.Run(searchCase.name, func(t *testing.T) {
			ids := searchPinyinIDsUntilExpected(
				t,
				ctx,
				mc,
				collectionName,
				searchCase.queryText,
				searchCase.minimumShouldMatch,
				searchCase.expectedIDs,
			)
			require.Equal(t, searchCase.expectedIDs, ids)
		})
	}
}
