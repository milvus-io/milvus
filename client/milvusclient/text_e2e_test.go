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

package milvusclient_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
)

type textE2ERow struct {
	ID           int64     `milvus:"name:id"`
	Text         string    `milvus:"name:text"`
	OptionalText *string   `milvus:"name:optional_text"`
	Vector       []float32 `milvus:"name:vector"`
}

func TestTextCRUDAndSearchE2E(t *testing.T) {
	if os.Getenv("MILVUS_TEXT_E2E") != "1" {
		t.Skip("set MILVUS_TEXT_E2E=1 to run against a TEXT-enabled Milvus server")
	}

	address := os.Getenv("MILVUS_ADDR")
	if address == "" {
		host := os.Getenv("MILVUS_HOST")
		if host == "" {
			host = "127.0.0.1"
		}
		address = host + ":19530"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{Address: address})
	require.NoError(t, err)
	defer client.Close(context.Background())

	collectionName := fmt.Sprintf("go_text_e2e_%d", time.Now().UnixNano())
	dropped := false
	defer func() {
		if !dropped {
			_ = client.DropCollection(context.Background(), milvusclient.NewDropCollectionOption(collectionName))
		}
	}()

	schema := entity.NewSchema().
		WithDynamicFieldEnabled(false).
		WithField(entity.NewField().
			WithName("id").
			WithDataType(entity.FieldTypeInt64).
			WithIsPrimaryKey(true)).
		WithField(entity.NewField().
			WithName("text").
			WithDataType(entity.FieldTypeText).
			WithEnableAnalyzer(true).
			WithAnalyzerParams(map[string]any{"tokenizer": "standard"}).
			WithEnableMatch(true)).
		WithField(entity.NewField().
			WithName("optional_text").
			WithDataType(entity.FieldTypeText).
			WithNullable(true)).
		WithField(entity.NewField().
			WithName("vector").
			WithDataType(entity.FieldTypeFloatVector).
			WithDim(4))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)

	description, err := client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collectionName))
	require.NoError(t, err)
	require.Len(t, description.Schema.Fields, 4)
	require.Equal(t, entity.FieldTypeText, description.Schema.Fields[1].DataType)
	require.Empty(t, description.Schema.Fields[1].TypeParams[entity.TypeParamMaxLength])
	require.Equal(t, entity.FieldTypeText, description.Schema.Fields[2].DataType)
	require.True(t, description.Schema.Fields[2].Nullable)

	indexTask, err := client.CreateIndex(ctx, milvusclient.NewCreateIndexOption(
		collectionName,
		"vector",
		index.NewFlatIndex(entity.L2),
	))
	require.NoError(t, err)
	require.NoError(t, indexTask.Await(ctx))

	largeText := "milvus lob marker " + strings.Repeat("large text payload ", 5*1024)
	require.Greater(t, len(largeText), 64*1024)
	require.LessOrEqual(t, len(largeText), 96*1024)
	initialTexts := map[int64]string{
		1: "milvus native text column insert",
		2: largeText,
		3: "milvus native text row insert",
	}
	columnOptionalText := "nullable text column value"
	rowOptionalText := "nullable text row value"
	expectedOptionalTexts := map[int64]*string{
		1: &columnOptionalText,
		2: nil,
		3: &rowOptionalText,
	}
	optionalTextColumn, err := column.NewNullableColumnText(
		"optional_text",
		[]string{columnOptionalText},
		[]bool{true, false},
	)
	require.NoError(t, err)

	insertResult, err := client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", []int64{1, 2}).
		WithTextColumn("text", []string{initialTexts[1], initialTexts[2]}).
		WithColumns(optionalTextColumn).
		WithFloatVectorColumn("vector", 4, [][]float32{
			{0.1, 0.2, 0.3, 0.4},
			{0.2, 0.3, 0.4, 0.5},
		}))
	require.NoError(t, err)
	require.EqualValues(t, 2, insertResult.InsertCount)

	rowInsertResult, err := client.Insert(ctx, milvusclient.NewRowBasedInsertOption(collectionName, &textE2ERow{
		ID:           3,
		Text:         initialTexts[3],
		OptionalText: &rowOptionalText,
		Vector:       []float32{0.3, 0.4, 0.5, 0.6},
	}))
	require.NoError(t, err)
	require.EqualValues(t, 1, rowInsertResult.InsertCount)

	flushTask, err := client.Flush(ctx, milvusclient.NewFlushOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))

	queryResult, err := client.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter("id in [1, 2, 3]").
		WithOutputFields("id", "text", "optional_text").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 3, queryResult.Len())
	assertTextRows(t, queryResult, initialTexts, expectedOptionalTexts)

	matchResult, err := client.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter(`TEXT_MATCH(text, "native")`).
		WithOutputFields("id", "text").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 2, matchResult.Len())
	_, ok := matchResult.GetColumn("text").(*column.ColumnText)
	require.True(t, ok, "TEXT_MATCH output must remain ColumnText")

	searchResults, err := client.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		3,
		[]entity.Vector{entity.FloatVector([]float32{0.1, 0.2, 0.3, 0.4})},
	).
		WithANNSField("vector").
		WithOutputFields("text").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Len(t, searchResults, 1)
	require.Equal(t, 3, searchResults[0].Len())
	searchText, ok := searchResults[0].GetColumn("text").(*column.ColumnText)
	require.True(t, ok, "search output must remain ColumnText")
	require.Equal(t, 3, searchText.Len())

	updatedText := "milvus text updated by upsert"
	updatedOptionalText := "nullable text updated by upsert"
	upsertResult, err := client.Upsert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", []int64{2}).
		WithTextColumn("text", []string{updatedText}).
		WithTextColumn("optional_text", []string{updatedOptionalText}).
		WithFloatVectorColumn("vector", 4, [][]float32{{0.9, 0.8, 0.7, 0.6}}))
	require.NoError(t, err)
	require.EqualValues(t, 1, upsertResult.UpsertCount)

	updatedResult, err := client.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter("id == 2").
		WithOutputFields("text", "optional_text").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 1, updatedResult.Len())
	updatedColumn, ok := updatedResult.GetColumn("text").(*column.ColumnText)
	require.True(t, ok)
	actualUpdatedText, err := updatedColumn.GetAsString(0)
	require.NoError(t, err)
	require.Equal(t, updatedText, actualUpdatedText)
	updatedOptionalColumn, ok := updatedResult.GetColumn("optional_text").(*column.ColumnText)
	require.True(t, ok)
	isNull, err := updatedOptionalColumn.IsNull(0)
	require.NoError(t, err)
	require.False(t, isNull)
	actualUpdatedOptionalText, err := updatedOptionalColumn.GetAsString(0)
	require.NoError(t, err)
	require.Equal(t, updatedOptionalText, actualUpdatedOptionalText)

	deleteResult, err := client.Delete(ctx, milvusclient.NewDeleteOption(collectionName).WithInt64IDs("id", []int64{1}))
	require.NoError(t, err)
	require.EqualValues(t, 1, deleteResult.DeleteCount)

	deletedResult, err := client.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter("id == 1").
		WithOutputFields("text").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Zero(t, deletedResult.Len())

	require.NoError(t, client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName)))
	dropped = true
	exists, err := client.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	require.NoError(t, err)
	require.False(t, exists)
}

func assertTextRows(
	t *testing.T,
	result milvusclient.ResultSet,
	expected map[int64]string,
	expectedOptional map[int64]*string,
) {
	t.Helper()

	idColumn, ok := result.GetColumn("id").(*column.ColumnInt64)
	require.True(t, ok)
	textColumn, ok := result.GetColumn("text").(*column.ColumnText)
	require.True(t, ok, "query output must remain ColumnText")
	require.Equal(t, idColumn.Len(), textColumn.Len())
	optionalTextColumn, ok := result.GetColumn("optional_text").(*column.ColumnText)
	require.True(t, ok, "nullable query output must remain ColumnText")
	require.True(t, optionalTextColumn.Nullable())
	require.Equal(t, idColumn.Len(), optionalTextColumn.Len())

	for i := 0; i < idColumn.Len(); i++ {
		id, err := idColumn.GetAsInt64(i)
		require.NoError(t, err)
		text, err := textColumn.GetAsString(i)
		require.NoError(t, err)
		require.Equal(t, expected[id], text)

		isNull, err := optionalTextColumn.IsNull(i)
		require.NoError(t, err)
		expectedValue := expectedOptional[id]
		if expectedValue == nil {
			require.True(t, isNull)
			continue
		}
		require.False(t, isNull)
		optionalText, err := optionalTextColumn.GetAsString(i)
		require.NoError(t, err)
		require.Equal(t, *expectedValue, optionalText)
	}
}
