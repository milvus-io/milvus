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

package testcases

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	textLOBIDField             = "id"
	textLOBVectorField         = "vector"
	textLOBContentField        = "content"
	textLOBZHField             = "content_zh"
	textLOBAltField            = "content_alt"
	textLOBSentinelField       = "content_sentinel"
	textLOBSparseField         = "content_sparse"
	textLOBZHSparseField       = "content_zh_sparse"
	textLOBSentinelSparseField = "content_sentinel_sparse"
	textLOBVectorIndex         = "text_lob_vector_idx"
	textLOBSparseIndex         = "text_lob_sparse_idx"
	textLOBZHSparseIndex       = "text_lob_zh_sparse_idx"
	textLOBSentinelSparseIndex = "text_lob_sentinel_sparse_idx"
	textLOBVectorDim           = 16

	textLOBIndexedSealedRows   = 3000
	textLOBUnindexedSealedRows = 500
	textLOBGrowingRows         = 500
	textLOBTotalRows           = textLOBIndexedSealedRows + textLOBUnindexedSealedRows + textLOBGrowingRows

	textLOBIndexedMarkerID   = int64(7)
	textLOBUnindexedMarkerID = int64(textLOBIndexedSealedRows + textLOBUnindexedSealedRows/2)
	textLOBGrowingMarkerID   = int64(textLOBIndexedSealedRows + textLOBUnindexedSealedRows + textLOBGrowingRows/2)
)

const (
	textLOBStorageV3Config      = "common.storage.useLoonFFI"
	textLOBMinRowsConfig        = "indexCoord.segment.minSegmentNumRowsToEnableIndex"
	textLOBMinRowsToEnableIndex = 1024
)

type textLOBRow struct {
	id        int64
	vector    []float32
	content   *string
	contentZH *string
	alt       *string
	sentinel  string
}

type textLOBFixture struct {
	collectionName string
	rows           []textLOBRow
	rowsByID       map[int64]textLOBRow
	markerIDs      []int64
	sealedIDs      []int64
}

func textLOBValue(value string) *string {
	return &value
}

func makeTextLOB(size int, seed string) string {
	if size == 0 {
		return ""
	}
	base := fmt.Sprintf("seed %s vector database milvus text lob storage bm25 payload boundary checksum %s ", seed, seed)
	return strings.Repeat(base, size/len(base)+1)[:size]
}

func textLOBVector(id int64, marker bool) []float32 {
	vector := make([]float32, textLOBVectorDim)
	if marker {
		vector[0] = 1
		return vector
	}

	state := uint64(19530) + uint64(id)
	for i := range vector {
		state = state*6364136223846793005 + 1442695040888963407
		vector[i] = float32(state>>40) / float32(1<<24)
	}
	return vector
}

func newTextLOBRows() []textLOBRow {
	rows := make([]textLOBRow, textLOBTotalRows)
	for i := range rows {
		id := int64(i)
		content := textLOBValue(fmt.Sprintf("text lob fixture row %d vector database", id))
		contentZH := textLOBValue(fmt.Sprintf("向量数据库 中文检索 文本行 %d", id))
		alt := textLOBValue(fmt.Sprintf("alternate text lob fixture row %d", id))
		marker := id == textLOBIndexedMarkerID || id == textLOBUnindexedMarkerID || id == textLOBGrowingMarkerID

		switch id {
		case 0:
			content = textLOBValue("vector database milvus text lob smoke")
		case 1:
			content = textLOBValue("")
			contentZH = textLOBValue("")
			alt = textLOBValue("")
		case 2:
			content = nil
			contentZH = nil
			alt = nil
		case 3:
			content = textLOBValue("Milvus stores multilingual text: English 中文 日本語 Русский العربية emoji 😀🚀 데이터베이스")
			contentZH = textLOBValue("向量数据库 支持 中文检索 和 混合搜索")
			alt = textLOBValue("alternate multilingual 中文 payload 😀")
		case 4:
			content = textLOBValue(makeTextLOB(64*1024-17, "below-64k"))
			contentZH = nil
		case 5:
			content = textLOBValue(makeTextLOB(64*1024, "at-64k"))
			contentZH = nil
		case 6:
			content = textLOBValue(makeTextLOB(64*1024+4096, "above-64k"))
			contentZH = nil
		case textLOBIndexedMarkerID:
			content = textLOBValue(makeTextLOB(1024*1024, "indexed-sealed-one-mib"))
			contentZH = nil
			alt = textLOBValue(makeTextLOB(128*1024, "indexed-sealed-alt"))
		case 8:
			content = textLOBValue("vector database")
		case 9:
			content = textLOBValue(strings.Repeat("vector database ", 4) + "milvus retrieval")
		case 10:
			content = textLOBValue(strings.Repeat("vector database ", 12) + "milvus bm25 ranking ranking")
		case 11:
			content = textLOBValue("english sidecar text for chinese bm25")
			contentZH = textLOBValue("向量数据库 支持 中文检索。Milvus 提供 混合搜索 和 稀疏向量 检索。")
		case textLOBUnindexedMarkerID:
			content = textLOBValue(makeTextLOB(256*1024, "unindexed-sealed"))
			contentZH = nil
			alt = textLOBValue(makeTextLOB(128*1024, "unindexed-sealed-alt"))
		case textLOBGrowingMarkerID:
			content = textLOBValue(makeTextLOB(256*1024, "growing"))
			contentZH = nil
			alt = textLOBValue(makeTextLOB(128*1024, "growing-alt"))
		}

		rows[i] = textLOBRow{
			id:        id,
			vector:    textLOBVector(id, marker),
			content:   content,
			contentZH: contentZH,
			alt:       alt,
			sentinel:  fmt.Sprintf("sentinel text output %d", id),
		}
	}
	return rows
}

func textLOBCollectionSchema(collectionName string) *entity.Schema {
	standardAnalyzer := map[string]any{"tokenizer": "standard"}
	jiebaAnalyzer := map[string]any{
		"tokenizer": map[string]any{
			"type": "jieba",
			"dict": []string{"向量数据库", "混合搜索", "稀疏向量"},
			"mode": "exact",
			"hmm":  false,
		},
	}
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName(textLOBIDField).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(textLOBVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(textLOBVectorDim)).
		WithField(entity.NewField().WithName(textLOBContentField).WithDataType(entity.FieldTypeText).WithNullable(true).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(standardAnalyzer)).
		WithField(entity.NewField().WithName(textLOBZHField).WithDataType(entity.FieldTypeText).WithNullable(true).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(jiebaAnalyzer)).
		WithField(entity.NewField().WithName(textLOBAltField).WithDataType(entity.FieldTypeText).WithNullable(true).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(standardAnalyzer)).
		WithField(entity.NewField().WithName(textLOBSentinelField).WithDataType(entity.FieldTypeText).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(standardAnalyzer)).
		WithField(entity.NewField().WithName(textLOBSparseField).WithDataType(entity.FieldTypeSparseVector)).
		WithField(entity.NewField().WithName(textLOBZHSparseField).WithDataType(entity.FieldTypeSparseVector)).
		WithField(entity.NewField().WithName(textLOBSentinelSparseField).WithDataType(entity.FieldTypeSparseVector)).
		WithFunction(entity.NewFunction().WithName("content_bm25").WithType(entity.FunctionTypeBM25).
			WithInputFields(textLOBContentField).WithOutputFields(textLOBSparseField)).
		WithFunction(entity.NewFunction().WithName("content_zh_bm25").WithType(entity.FunctionTypeBM25).
			WithInputFields(textLOBZHField).WithOutputFields(textLOBZHSparseField)).
		WithFunction(entity.NewFunction().WithName("content_sentinel_bm25").WithType(entity.FunctionTypeBM25).
			WithInputFields(textLOBSentinelField).WithOutputFields(textLOBSentinelSparseField))
}

func nullableTextLOBColumn(t *testing.T, fieldName string, rows []textLOBRow, value func(textLOBRow) *string) *column.ColumnText {
	t.Helper()

	values := make([]string, 0, len(rows))
	validData := make([]bool, len(rows))
	for i, row := range rows {
		if text := value(row); text != nil {
			values = append(values, *text)
			validData[i] = true
		}
	}
	result, err := column.NewNullableColumnText(fieldName, values, validData)
	require.NoError(t, err)
	return result
}

func insertTextLOBRows(
	t *testing.T,
	ctx context.Context,
	mc *base.MilvusClient,
	collectionName string,
	rows []textLOBRow,
) {
	t.Helper()

	ids := make([]int64, len(rows))
	vectors := make([][]float32, len(rows))
	sentinels := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.id
		vectors[i] = row.vector
		sentinels[i] = row.sentinel
	}

	contentColumn := nullableTextLOBColumn(t, textLOBContentField, rows, func(row textLOBRow) *string {
		return row.content
	})
	contentZHColumn := nullableTextLOBColumn(t, textLOBZHField, rows, func(row textLOBRow) *string {
		return row.contentZH
	})
	altColumn := nullableTextLOBColumn(t, textLOBAltField, rows, func(row textLOBRow) *string {
		return row.alt
	})
	insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column(textLOBIDField, ids).
		WithFloatVectorColumn(textLOBVectorField, textLOBVectorDim, vectors).
		WithColumns(contentColumn, contentZHColumn, altColumn).
		WithTextColumn(textLOBSentinelField, sentinels))
	common.CheckErr(t, err, true)
	require.EqualValues(t, len(rows), insertResult.InsertCount)
}

func waitForTextLOBSealedLayout(
	t *testing.T,
	ctx context.Context,
	mc *base.MilvusClient,
	collectionName string,
) []int64 {
	t.Helper()

	waitCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastSegments []*entity.Segment
	for {
		segments, err := mc.GetPersistentSegmentInfo(waitCtx, client.NewGetPersistentSegmentInfoOption(collectionName))
		if err == nil {
			lastSegments = segments
			if len(segments) == 2 {
				first, second := segments[0], segments[1]
				hasExpectedRows := first.NumRows == textLOBIndexedSealedRows && second.NumRows == textLOBUnindexedSealedRows ||
					first.NumRows == textLOBUnindexedSealedRows && second.NumRows == textLOBIndexedSealedRows
				if hasExpectedRows && first.Flushed() && second.Flushed() {
					return []int64{first.ID, second.ID}
				}
			}
		}

		select {
		case <-waitCtx.Done():
			t.Fatalf("expected 3,000-row and 500-row flushed segments, last segments: %+v", lastSegments)
		case <-ticker.C:
		}
	}
}

func prepareTextLOBFixture(t *testing.T, ctx context.Context, mc *base.MilvusClient) textLOBFixture {
	t.Helper()

	collectionName := common.GenRandomString("text_lob", 6)
	schema := textLOBCollectionSchema(collectionName)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		common.CheckErr(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)), true)
	})

	rows := newTextLOBRows()
	rowsByID := make(map[int64]textLOBRow, len(rows))
	for _, row := range rows {
		rowsByID[row.id] = row
	}

	insertTextLOBRows(t, ctx, mc, collectionName, rows[:textLOBIndexedSealedRows])
	common.CheckErr(t, flushTextLOBCollectionWithRetry(ctx, mc, collectionName), true)
	insertTextLOBRows(t, ctx, mc, collectionName,
		rows[textLOBIndexedSealedRows:textLOBIndexedSealedRows+textLOBUnindexedSealedRows])
	common.CheckErr(t, flushTextLOBCollectionWithRetry(ctx, mc, collectionName), true)

	vectorIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
		collectionName,
		textLOBVectorField,
		index.NewHNSWIndex(entity.COSINE, 16, 200),
	).WithIndexName(textLOBVectorIndex))
	common.CheckErr(t, err, true)
	common.CheckErr(t, vectorIndexTask.Await(ctx), true)

	for _, sparse := range []struct {
		fieldName string
		indexName string
	}{
		{fieldName: textLOBSparseField, indexName: textLOBSparseIndex},
		{fieldName: textLOBZHSparseField, indexName: textLOBZHSparseIndex},
		{fieldName: textLOBSentinelSparseField, indexName: textLOBSentinelSparseIndex},
	} {
		sparseIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
			collectionName,
			sparse.fieldName,
			index.NewSparseInvertedIndex(entity.BM25, 0.1),
		).WithIndexName(sparse.indexName))
		common.CheckErr(t, err, true)
		common.CheckErr(t, sparseIndexTask.Await(ctx), true)
	}

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
	sealedIDs := waitForTextLOBSealedLayout(t, ctx, mc, collectionName)

	insertTextLOBRows(t, ctx, mc, collectionName,
		rows[textLOBIndexedSealedRows+textLOBUnindexedSealedRows:])
	countResult, err := mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("%s >= 0", textLOBIDField)).
		WithOutputFields("count(*)").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, err := countResult.GetColumn("count(*)").GetAsInt64(0)
	common.CheckErr(t, err, true)
	require.EqualValues(t, textLOBTotalRows, count)

	segmentsAfterGrowing, err := mc.GetPersistentSegmentInfo(ctx, client.NewGetPersistentSegmentInfoOption(collectionName))
	common.CheckErr(t, err, true)
	require.Len(t, segmentsAfterGrowing, 2)
	remainingSealedIDs := []int64{segmentsAfterGrowing[0].ID, segmentsAfterGrowing[1].ID}
	require.ElementsMatch(t, sealedIDs, remainingSealedIDs)
	require.EqualValues(t, textLOBIndexedSealedRows+textLOBUnindexedSealedRows,
		segmentsAfterGrowing[0].NumRows+segmentsAfterGrowing[1].NumRows)

	return textLOBFixture{
		collectionName: collectionName,
		rows:           rows,
		rowsByID:       rowsByID,
		markerIDs:      []int64{textLOBIndexedMarkerID, textLOBUnindexedMarkerID, textLOBGrowingMarkerID},
		sealedIDs:      sealedIDs,
	}
}

func flushAndReloadTextLOBCollection(t *testing.T, ctx context.Context, mc *base.MilvusClient, collectionName string) {
	t.Helper()

	common.CheckErr(t, flushTextLOBCollectionWithRetry(ctx, mc, collectionName), true)

	common.CheckErr(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collectionName)), true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
}

func flushTextLOBCollectionWithRetry(ctx context.Context, mc *base.MilvusClient, collectionName string) error {
	retryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		flushTask, err := mc.Flush(retryCtx, client.NewFlushOption(collectionName))
		if err == nil {
			return flushTask.Await(retryCtx)
		}
		if !client.IsRetryableError(err) {
			return err
		}

		select {
		case <-retryCtx.Done():
			return merr.Wrapf(err, "flush collection %q retry timed out", collectionName)
		case <-ticker.C:
		}
	}
}

func requireTextFieldConfig(t *testing.T) {
	t.Helper()

	storageV3, err := hp.GetServerConfig(textLOBStorageV3Config)
	require.NoError(t, err)
	require.Equal(t, "true", storageV3)

	configuredMinRows, err := hp.GetServerConfig(textLOBMinRowsConfig)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(textLOBMinRowsToEnableIndex), configuredMinRows)
	require.Less(t, textLOBUnindexedSealedRows, textLOBMinRowsToEnableIndex)
	require.GreaterOrEqual(t, textLOBIndexedSealedRows, textLOBMinRowsToEnableIndex)
}

func requireExactTextLOB(t *testing.T, expected, actual string) {
	t.Helper()
	require.Equal(t, len(expected), len(actual), "TEXT byte length mismatch")
	require.Equal(t, utf8.RuneCountInString(expected), utf8.RuneCountInString(actual), "TEXT character length mismatch")
	require.Equal(t, sha256.Sum256([]byte(expected)), sha256.Sum256([]byte(actual)), "TEXT checksum mismatch")
}

func requireTextLOBResultRows(
	t *testing.T,
	result client.ResultSet,
	idColumn column.Column,
	expected map[int64]textLOBRow,
) []int64 {
	t.Helper()

	require.NotNil(t, idColumn)
	contentColumn := result.GetColumn(textLOBContentField)
	contentZHColumn := result.GetColumn(textLOBZHField)
	altColumn := result.GetColumn(textLOBAltField)
	sentinelColumn := result.GetColumn(textLOBSentinelField)
	require.IsType(t, &column.ColumnText{}, contentColumn)
	require.IsType(t, &column.ColumnText{}, contentZHColumn)
	require.IsType(t, &column.ColumnText{}, altColumn)
	require.IsType(t, &column.ColumnText{}, sentinelColumn)
	require.Equal(t, entity.FieldTypeText, contentColumn.Type())
	require.Equal(t, entity.FieldTypeText, contentZHColumn.Type())
	require.Equal(t, entity.FieldTypeText, altColumn.Type())
	require.Equal(t, entity.FieldTypeText, sentinelColumn.Type())

	ids := make([]int64, result.Len())
	for i := 0; i < result.Len(); i++ {
		id, err := idColumn.GetAsInt64(i)
		require.NoError(t, err)
		row, ok := expected[id]
		require.True(t, ok, "unexpected primary key %d", id)
		ids[i] = id

		for _, field := range []struct {
			actual   column.Column
			expected *string
		}{
			{actual: contentColumn, expected: row.content},
			{actual: contentZHColumn, expected: row.contentZH},
			{actual: altColumn, expected: row.alt},
		} {
			isNull, err := field.actual.IsNull(i)
			require.NoError(t, err)
			if field.expected == nil {
				require.True(t, isNull)
				continue
			}
			require.False(t, isNull)
			actual, err := field.actual.GetAsString(i)
			require.NoError(t, err)
			requireExactTextLOB(t, *field.expected, actual)
		}

		actualSentinel, err := sentinelColumn.GetAsString(i)
		require.NoError(t, err)
		requireExactTextLOB(t, row.sentinel, actualSentinel)
	}
	return ids
}

// TestTextFieldCRUD verifies TEXT fields across indexed, sealed, and growing segments.
func TestTextFieldCRUD(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	requireTextFieldConfig(t)

	fixture := prepareTextLOBFixture(t, ctx, mc)

	t.Run("schema_and_query", func(t *testing.T) {
		description, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(fixture.collectionName))
		common.CheckErr(t, err, true)

		fields := make(map[string]*entity.Field, len(description.Schema.Fields))
		for _, field := range description.Schema.Fields {
			fields[field.Name] = field
		}
		for _, fieldName := range []string{textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField} {
			require.Contains(t, fields, fieldName)
			require.Equal(t, entity.FieldTypeText, fields[fieldName].DataType)
		}
		require.Len(t, description.Schema.Functions, 3)
		functions := make(map[string]*entity.Function, len(description.Schema.Functions))
		for _, function := range description.Schema.Functions {
			functions[function.Name] = function
		}
		for _, expected := range []struct {
			name        string
			inputField  string
			outputField string
		}{
			{name: "content_bm25", inputField: textLOBContentField, outputField: textLOBSparseField},
			{name: "content_zh_bm25", inputField: textLOBZHField, outputField: textLOBZHSparseField},
			{name: "content_sentinel_bm25", inputField: textLOBSentinelField, outputField: textLOBSentinelSparseField},
		} {
			function := functions[expected.name]
			require.NotNil(t, function)
			require.Equal(t, entity.FunctionTypeBM25, function.Type)
			require.Equal(t, []string{expected.inputField}, function.InputFieldNames)
			require.Equal(t, []string{expected.outputField}, function.OutputFieldNames)
		}

		vectorIndex, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(fixture.collectionName, textLOBVectorIndex))
		common.CheckErr(t, err, true)
		require.Equal(t, index.HNSW, vectorIndex.IndexType())
		require.Equal(t, string(entity.COSINE), vectorIndex.Params()[index.MetricTypeKey])
		for _, indexName := range []string{textLOBSparseIndex, textLOBZHSparseIndex, textLOBSentinelSparseIndex} {
			sparseIndex, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(fixture.collectionName, indexName))
			common.CheckErr(t, err, true)
			require.Equal(t, index.SparseInverted, sparseIndex.IndexType())
			require.Equal(t, string(entity.BM25), sparseIndex.Params()[index.MetricTypeKey])
		}
		require.Len(t, fixture.sealedIDs, 2)

		result, err := mc.Query(ctx, client.NewQueryOption(fixture.collectionName).
			WithFilter(fmt.Sprintf("%s >= 0", textLOBIDField)).
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong).
			WithLimit(len(fixture.rows)))
		common.CheckErr(t, err, true)
		require.Equal(t, len(fixture.rows), result.Len())
		ids := requireTextLOBResultRows(t, result, result.GetColumn(textLOBIDField), fixture.rowsByID)
		seen := make(map[int64]struct{}, len(ids))
		pathCounts := [3]int{}
		for _, id := range ids {
			_, duplicated := seen[id]
			require.False(t, duplicated, "duplicate primary key %d", id)
			seen[id] = struct{}{}
			switch {
			case id < textLOBIndexedSealedRows:
				pathCounts[0]++
			case id < textLOBIndexedSealedRows+textLOBUnindexedSealedRows:
				pathCounts[1]++
			default:
				pathCounts[2]++
			}
		}
		require.Equal(t, [3]int{textLOBIndexedSealedRows, textLOBUnindexedSealedRows, textLOBGrowingRows}, pathCounts)
		for _, markerID := range fixture.markerIDs {
			require.Greater(t, len(*fixture.rowsByID[markerID].content), 64*1024)
			require.Greater(t, len(*fixture.rowsByID[markerID].alt), 64*1024)
		}
	})

	t.Run("dense_search", func(t *testing.T) {
		results, err := mc.Search(ctx, client.NewSearchOption(
			fixture.collectionName,
			3,
			[]entity.Vector{entity.FloatVector(textLOBVector(textLOBIndexedMarkerID, true))},
		).WithANNSField(textLOBVectorField).
			WithOutputFields(textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField).
			WithSearchParam("ef", "64").
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.Len(t, results, 1)
		require.Equal(t, 3, results[0].Len())
		ids := requireTextLOBResultRows(t, results[0], results[0].IDs, fixture.rowsByID)
		require.ElementsMatch(t, fixture.markerIDs, ids)
	})

	t.Run("bm25_search", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			annField string
			query    string
		}{
			{name: "standard_analyzer", annField: textLOBSparseField, query: "vector database"},
			{name: "jieba_analyzer", annField: textLOBZHSparseField, query: "向量数据库 中文检索"},
		} {
			t.Run(test.name, func(t *testing.T) {
				results, err := mc.Search(ctx, client.NewSearchOption(
					fixture.collectionName,
					3,
					[]entity.Vector{entity.Text(test.query)},
				).WithANNSField(test.annField).
					WithOutputFields(textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField).
					WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)
				require.Len(t, results, 1)
				require.NotZero(t, results[0].Len())
				require.LessOrEqual(t, results[0].Len(), 3)
				requireTextLOBResultRows(t, results[0], results[0].IDs, fixture.rowsByID)
			})
		}
	})

	t.Run("query_iterator", func(t *testing.T) {
		iterator, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(fixture.collectionName).
			WithBatchSize(512).
			WithFilter(fmt.Sprintf("%s >= 0", textLOBIDField)).
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)

		seen := make(map[int64]struct{}, len(fixture.rows))
		for {
			batch, err := iterator.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			common.CheckErr(t, err, true)
			require.NotZero(t, batch.Len())
			require.LessOrEqual(t, batch.Len(), 512)
			ids := requireTextLOBResultRows(t, batch, batch.GetColumn(textLOBIDField), fixture.rowsByID)
			for _, id := range ids {
				_, duplicated := seen[id]
				require.False(t, duplicated, "duplicate primary key %d", id)
				seen[id] = struct{}{}
			}
		}
		require.Len(t, seen, len(fixture.rows))
	})

	t.Run("upsert", func(t *testing.T) {
		upsertRows := []textLOBRow{
			{
				id:        2,
				vector:    fixture.rows[2].vector,
				content:   textLOBValue(makeTextLOB(128*1024, "upsert-null-to-large")),
				contentZH: textLOBValue("更新后的向量数据库 中文检索 文本"),
				alt:       textLOBValue("upserted multilingual alternate 中文 payload 😀"),
				sentinel:  "sentinel text upsert 2",
			},
			{
				id:        6,
				vector:    fixture.rows[6].vector,
				content:   textLOBValue(makeTextLOB(256*1024, "upsert-large")),
				contentZH: nil,
				alt:       nil,
				sentinel:  "sentinel text upsert 6",
			},
		}
		ids := []int64{upsertRows[0].id, upsertRows[1].id}
		vectors := [][]float32{upsertRows[0].vector, upsertRows[1].vector}
		sentinels := []string{upsertRows[0].sentinel, upsertRows[1].sentinel}
		contentColumn := nullableTextLOBColumn(t, textLOBContentField, upsertRows, func(row textLOBRow) *string {
			return row.content
		})
		contentZHColumn := nullableTextLOBColumn(t, textLOBZHField, upsertRows, func(row textLOBRow) *string {
			return row.contentZH
		})
		altColumn := nullableTextLOBColumn(t, textLOBAltField, upsertRows, func(row textLOBRow) *string {
			return row.alt
		})

		upsertResult, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(fixture.collectionName).
			WithInt64Column(textLOBIDField, ids).
			WithFloatVectorColumn(textLOBVectorField, textLOBVectorDim, vectors).
			WithColumns(contentColumn, contentZHColumn, altColumn).
			WithTextColumn(textLOBSentinelField, sentinels))
		common.CheckErr(t, err, true)
		require.EqualValues(t, len(upsertRows), upsertResult.UpsertCount)
		flushAndReloadTextLOBCollection(t, ctx, mc, fixture.collectionName)

		expected := make(map[int64]textLOBRow, len(upsertRows))
		for _, row := range upsertRows {
			expected[row.id] = row
		}
		result, err := mc.Query(ctx, client.NewQueryOption(fixture.collectionName).
			WithFilter(fmt.Sprintf("%s in [2, 6]", textLOBIDField)).
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong).
			WithLimit(len(upsertRows)))
		common.CheckErr(t, err, true)
		require.Equal(t, len(upsertRows), result.Len())
		resultIDs := requireTextLOBResultRows(t, result, result.GetColumn(textLOBIDField), expected)
		require.ElementsMatch(t, ids, resultIDs)

		for id, row := range expected {
			fixture.rowsByID[id] = row
		}
	})

	t.Run("delete", func(t *testing.T) {
		deletedID := int64(7)
		deleteResult, err := mc.Delete(ctx, client.NewDeleteOption(fixture.collectionName).
			WithInt64IDs(textLOBIDField, []int64{deletedID}))
		common.CheckErr(t, err, true)
		require.EqualValues(t, 1, deleteResult.DeleteCount)
		flushAndReloadTextLOBCollection(t, ctx, mc, fixture.collectionName)

		deleted, err := mc.Query(ctx, client.NewQueryOption(fixture.collectionName).
			WithFilter(fmt.Sprintf("%s == %d", textLOBIDField, deletedID)).
			WithOutputFields(textLOBIDField, textLOBContentField).
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.Zero(t, deleted.ResultCount)

		survivors := make(map[int64]textLOBRow, len(fixture.rowsByID)-1)
		expectedIDs := make([]int64, 0, len(fixture.rowsByID)-1)
		for id, row := range fixture.rowsByID {
			if id == deletedID {
				continue
			}
			survivors[id] = row
			expectedIDs = append(expectedIDs, id)
		}
		result, err := mc.Query(ctx, client.NewQueryOption(fixture.collectionName).
			WithFilter(fmt.Sprintf("%s >= 0", textLOBIDField)).
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBZHField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong).
			WithLimit(len(survivors)))
		common.CheckErr(t, err, true)
		require.Equal(t, len(survivors), result.Len())
		resultIDs := requireTextLOBResultRows(t, result, result.GetColumn(textLOBIDField), survivors)
		require.ElementsMatch(t, expectedIDs, resultIDs)
	})
}
