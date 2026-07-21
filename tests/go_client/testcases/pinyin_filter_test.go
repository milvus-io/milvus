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

func queryPinyinIDs(t *testing.T, ctx CtxT, mc MC, collectionName, queryText string) []int64 {
	result, err := mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(`text_match(text, "`+queryText+`")`).
		WithOutputFields("id", "text").
		WithLimit(pinyinTotalCount).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)

	idColumn := result.GetColumn("id")
	require.NotNil(t, idColumn)
	ids := make([]int64, result.ResultCount)
	for i := 0; i < result.ResultCount; i++ {
		value, err := idColumn.Get(i)
		require.NoError(t, err)
		ids[i] = value.(int64)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func queryPinyinIDsUntilExpected(t *testing.T, ctx CtxT, mc MC, collectionName, queryText string) []int64 {
	deadline := time.Now().Add(30 * time.Second)
	var ids []int64
	for time.Now().Before(deadline) {
		ids = queryPinyinIDs(t, ctx, mc, collectionName, queryText)
		if len(ids) == len(pinyinTargetIDs) {
			matched := true
			for i := range ids {
				if ids[i] != pinyinTargetIDs[i] {
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

	analyzerParams := map[string]any{
		"tokenizer": "jieba",
		"filter": []any{
			map[string]any{
				"type":                       "pinyin",
				"keep_original":              true,
				"keep_full_pinyin":           false,
				"keep_joined_full_pinyin":    true,
				"keep_separate_first_letter": false,
			},
		},
	}
	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(analyzerParams)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(pinyinVectorDim))

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	require.NoError(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(collectionName).
		WithProperty("collection.autocompaction.enabled", false)))

	insertPinyinRows(t, ctx, mc, collectionName, 0, pinyinIndexedSealedCount)
	flushPinyinRows(t, ctx, mc, collectionName)

	insertPinyinRows(t, ctx, mc, collectionName, pinyinIndexedSealedCount, pinyinUnindexedSealedCount)
	flushPinyinRows(t, ctx, mc, collectionName)

	indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, "vector",
		index.NewIvfFlatIndex(entity.L2, 64)))
	require.NoError(t, err)
	require.NoError(t, indexTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))

	insertPinyinRows(t, ctx, mc, collectionName,
		pinyinIndexedSealedCount+pinyinUnindexedSealedCount, pinyinGrowingCount)

	for _, queryText := range []string{"zhongwen", "中文"} {
		ids := queryPinyinIDsUntilExpected(t, ctx, mc, collectionName, queryText)
		require.Equal(t, pinyinTargetIDs, ids)
	}
}
