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

// L0 CRUD coverage for TEXT LOB through the public Go SDK.
package testcases

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	textLOBIDField       = "id"
	textLOBVectorField   = "vector"
	textLOBContentField  = "content"
	textLOBAltField      = "content_alt"
	textLOBSentinelField = "content_sentinel"
	textLOBSparseField   = "content_sparse"
	textLOBVectorIndex   = "text_lob_vector_idx"
	textLOBSparseIndex   = "text_lob_sparse_idx"
	textLOBVectorDim     = 16
)

type textLOBRow struct {
	id       int64
	vector   []float32
	content  *string
	alt      *string
	sentinel string
}

type textLOBFixture struct {
	collectionName string
	rows           []textLOBRow
	rowsByID       map[int64]textLOBRow
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

func newTextLOBRows() []textLOBRow {
	contents := []*string{
		textLOBValue("vector database milvus text lob smoke"),
		textLOBValue(""),
		nil,
		textLOBValue("Milvus stores multilingual text: English 中文 日本語 Русский العربية emoji 😀🚀 데이터베이스"),
		textLOBValue(makeTextLOB(64*1024-17, "below-64k")),
		textLOBValue(makeTextLOB(64*1024, "at-64k")),
		textLOBValue(makeTextLOB(64*1024+4096, "above-64k")),
		textLOBValue(makeTextLOB(1024*1024, "one-mib")),
	}
	alternates := []*string{
		textLOBValue("alternate vector database payload"),
		textLOBValue(""),
		nil,
		textLOBValue("alternate multilingual 中文 payload 😀"),
		textLOBValue("alternate below boundary payload"),
		textLOBValue("alternate at boundary payload"),
		textLOBValue("alternate above boundary payload"),
		textLOBValue(makeTextLOB(128*1024, "alternate-one-mib")),
	}

	rows := make([]textLOBRow, len(contents))
	for i := range contents {
		vector := make([]float32, textLOBVectorDim)
		vector[i] = 1
		rows[i] = textLOBRow{
			id:       int64(i),
			vector:   vector,
			content:  contents[i],
			alt:      alternates[i],
			sentinel: fmt.Sprintf("sentinel text output %d", i),
		}
	}
	return rows
}

func textLOBCollectionSchema(collectionName string) *entity.Schema {
	analyzerParams := map[string]any{"tokenizer": "standard"}
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName(textLOBIDField).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(textLOBVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(textLOBVectorDim)).
		WithField(entity.NewField().WithName(textLOBContentField).WithDataType(entity.FieldTypeText).WithNullable(true).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(analyzerParams)).
		WithField(entity.NewField().WithName(textLOBAltField).WithDataType(entity.FieldTypeText).WithNullable(true).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(analyzerParams)).
		WithField(entity.NewField().WithName(textLOBSentinelField).WithDataType(entity.FieldTypeText).
			WithEnableAnalyzer(true).WithEnableMatch(true).WithAnalyzerParams(analyzerParams)).
		WithField(entity.NewField().WithName(textLOBSparseField).WithDataType(entity.FieldTypeSparseVector)).
		WithFunction(entity.NewFunction().WithName("content_bm25").WithType(entity.FunctionTypeBM25).
			WithInputFields(textLOBContentField).WithOutputFields(textLOBSparseField))
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

func prepareTextLOBFixture(t *testing.T, ctx context.Context, mc *base.MilvusClient) textLOBFixture {
	t.Helper()

	collectionName := common.GenRandomString("text_lob", 6)
	schema := textLOBCollectionSchema(collectionName)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		common.CheckErr(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)), true)
	})

	rows := newTextLOBRows()
	ids := make([]int64, len(rows))
	vectors := make([][]float32, len(rows))
	sentinels := make([]string, len(rows))
	rowsByID := make(map[int64]textLOBRow, len(rows))
	for i, row := range rows {
		ids[i] = row.id
		vectors[i] = row.vector
		sentinels[i] = row.sentinel
		rowsByID[row.id] = row
	}

	contentColumn := nullableTextLOBColumn(t, textLOBContentField, rows, func(row textLOBRow) *string {
		return row.content
	})
	altColumn := nullableTextLOBColumn(t, textLOBAltField, rows, func(row textLOBRow) *string {
		return row.alt
	})
	insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column(textLOBIDField, ids).
		WithFloatVectorColumn(textLOBVectorField, textLOBVectorDim, vectors).
		WithColumns(contentColumn, altColumn).
		WithTextColumn(textLOBSentinelField, sentinels))
	common.CheckErr(t, err, true)
	require.EqualValues(t, len(rows), insertResult.InsertCount)

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, flushTask.Await(ctx), true)

	vectorIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
		collectionName,
		textLOBVectorField,
		index.NewFlatIndex(entity.COSINE),
	).WithIndexName(textLOBVectorIndex))
	common.CheckErr(t, err, true)
	common.CheckErr(t, vectorIndexTask.Await(ctx), true)

	sparseIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
		collectionName,
		textLOBSparseField,
		index.NewSparseInvertedIndex(entity.BM25, 0.1),
	).WithIndexName(textLOBSparseIndex))
	common.CheckErr(t, err, true)
	common.CheckErr(t, sparseIndexTask.Await(ctx), true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	return textLOBFixture{
		collectionName: collectionName,
		rows:           rows,
		rowsByID:       rowsByID,
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
			return fmt.Errorf("flush collection %q retry timed out: %w", collectionName, err)
		case <-ticker.C:
		}
	}
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
	altColumn := result.GetColumn(textLOBAltField)
	sentinelColumn := result.GetColumn(textLOBSentinelField)
	require.IsType(t, &column.ColumnText{}, contentColumn)
	require.IsType(t, &column.ColumnText{}, altColumn)
	require.IsType(t, &column.ColumnText{}, sentinelColumn)
	require.Equal(t, entity.FieldTypeText, contentColumn.Type())
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
			column   column.Column
			expected *string
		}{
			{column: contentColumn, expected: row.content},
			{column: altColumn, expected: row.alt},
		} {
			isNull, err := field.column.IsNull(i)
			require.NoError(t, err)
			if field.expected == nil {
				require.True(t, isNull)
				continue
			}
			require.False(t, isNull)
			actual, err := field.column.GetAsString(i)
			require.NoError(t, err)
			requireExactTextLOB(t, *field.expected, actual)
		}

		actualSentinel, err := sentinelColumn.GetAsString(i)
		require.NoError(t, err)
		requireExactTextLOB(t, row.sentinel, actualSentinel)
	}
	return ids
}

func TestTextLOBPublicSDKL0(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	previousStorageV3, err := hp.AlterServerConfig("common.storage.useLoonFFI", "true")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = hp.AlterServerConfig("common.storage.useLoonFFI", previousStorageV3)
	})
	fixture := prepareTextLOBFixture(t, ctx, mc)

	t.Run("schema_and_payloads", func(t *testing.T) {
		description, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(fixture.collectionName))
		common.CheckErr(t, err, true)

		fields := make(map[string]*entity.Field, len(description.Schema.Fields))
		for _, field := range description.Schema.Fields {
			fields[field.Name] = field
		}
		for _, fieldName := range []string{textLOBContentField, textLOBAltField, textLOBSentinelField} {
			require.Contains(t, fields, fieldName)
			require.Equal(t, entity.FieldTypeText, fields[fieldName].DataType)
		}
		require.Len(t, description.Schema.Functions, 1)
		function := description.Schema.Functions[0]
		require.Equal(t, entity.FunctionTypeBM25, function.Type)
		require.Equal(t, []string{textLOBContentField}, function.InputFieldNames)
		require.Equal(t, []string{textLOBSparseField}, function.OutputFieldNames)

		sparseIndex, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(fixture.collectionName, textLOBSparseIndex))
		common.CheckErr(t, err, true)
		require.Equal(t, index.SparseInverted, sparseIndex.IndexType())
		require.Equal(t, string(entity.BM25), sparseIndex.Params()[index.MetricTypeKey])

		result, err := mc.Query(ctx, client.NewQueryOption(fixture.collectionName).
			WithFilter(fmt.Sprintf("%s >= 0", textLOBIDField)).
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong).
			WithLimit(len(fixture.rows)))
		common.CheckErr(t, err, true)
		require.Equal(t, len(fixture.rows), result.Len())
		ids := requireTextLOBResultRows(t, result, result.GetColumn(textLOBIDField), fixture.rowsByID)
		require.ElementsMatch(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, ids)
	})

	t.Run("dense_search_output_fields", func(t *testing.T) {
		results, err := mc.Search(ctx, client.NewSearchOption(
			fixture.collectionName,
			3,
			[]entity.Vector{entity.FloatVector(fixture.rows[0].vector)},
		).WithANNSField(textLOBVectorField).
			WithOutputFields(textLOBContentField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.Len(t, results, 1)
		require.Equal(t, 3, results[0].Len())
		ids := requireTextLOBResultRows(t, results[0], results[0].IDs, fixture.rowsByID)
		require.Equal(t, int64(0), ids[0])
	})

	t.Run("query_iterator_payloads", func(t *testing.T) {
		iterator, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(fixture.collectionName).
			WithBatchSize(3).
			WithFilter(fmt.Sprintf("%s >= 0", textLOBIDField)).
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)

		seen := make(map[int64]struct{}, len(fixture.rows))
		for {
			batch, err := iterator.Next(ctx)
			if err == io.EOF {
				break
			}
			common.CheckErr(t, err, true)
			require.NotZero(t, batch.Len())
			require.LessOrEqual(t, batch.Len(), 3)
			ids := requireTextLOBResultRows(t, batch, batch.GetColumn(textLOBIDField), fixture.rowsByID)
			for _, id := range ids {
				_, duplicated := seen[id]
				require.False(t, duplicated, "duplicate primary key %d", id)
				seen[id] = struct{}{}
			}
		}
		require.Len(t, seen, len(fixture.rows))
	})

	t.Run("upsert_payloads", func(t *testing.T) {
		upsertRows := []textLOBRow{
			{
				id:       2,
				vector:   fixture.rows[2].vector,
				content:  textLOBValue(makeTextLOB(128*1024, "upsert-null-to-large")),
				alt:      textLOBValue("upserted multilingual alternate 中文 payload 😀"),
				sentinel: "sentinel text upsert 2",
			},
			{
				id:       6,
				vector:   fixture.rows[6].vector,
				content:  textLOBValue(makeTextLOB(256*1024, "upsert-large")),
				alt:      nil,
				sentinel: "sentinel text upsert 6",
			},
		}
		ids := []int64{upsertRows[0].id, upsertRows[1].id}
		vectors := [][]float32{upsertRows[0].vector, upsertRows[1].vector}
		sentinels := []string{upsertRows[0].sentinel, upsertRows[1].sentinel}
		contentColumn := nullableTextLOBColumn(t, textLOBContentField, upsertRows, func(row textLOBRow) *string {
			return row.content
		})
		altColumn := nullableTextLOBColumn(t, textLOBAltField, upsertRows, func(row textLOBRow) *string {
			return row.alt
		})

		upsertResult, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(fixture.collectionName).
			WithInt64Column(textLOBIDField, ids).
			WithFloatVectorColumn(textLOBVectorField, textLOBVectorDim, vectors).
			WithColumns(contentColumn, altColumn).
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
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBAltField, textLOBSentinelField).
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

	t.Run("delete_payloads", func(t *testing.T) {
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
			WithOutputFields(textLOBIDField, textLOBContentField, textLOBAltField, textLOBSentinelField).
			WithConsistencyLevel(entity.ClStrong).
			WithLimit(len(survivors)))
		common.CheckErr(t, err, true)
		require.Equal(t, len(survivors), result.Len())
		resultIDs := requireTextLOBResultRows(t, result, result.GetColumn(textLOBIDField), survivors)
		require.ElementsMatch(t, expectedIDs, resultIDs)
	})
}
