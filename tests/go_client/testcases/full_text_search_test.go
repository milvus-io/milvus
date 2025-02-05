package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestSearchFullTextBase tests basic full text search functionality with different languages
func TestSearchFullTextBase(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// Test cases for different languages and analyzers
	testCases := []struct {
		name     string
		language string
		analyzer string
		query    string
		numRows  int
		topK     int
	}{
		{
			name:     "English_Standard",
			language: "english",
			analyzer: "standard",
			query:    "what is information retrieval and its applications?",
			numRows:  3000,
			topK:     5,
		},
		{
			name:     "Chinese_Jieba",
			language: "chinese",
			analyzer: "jieba",
			query:    "信息检索的应用",
			numRows:  2000,
			topK:     10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params := hp.NewFullTextParams().
				TWithCollectionName("full_text_search_" + tc.name).
				TWithTextFields([]string{"text"}).
				TWithMaxLength(1000).
				TWithAnalyzerType(tc.analyzer)

			var schema *entity.Schema
			schema, err := hp.DefaultFullTextPrepare.CreateCollection(ctx, t, mc, params)
			common.CheckErr(t, err, true)

			err = hp.DefaultFullTextPrepare.CreateIndex(ctx, t, mc, schema, "0.2")
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(schema.CollectionName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			option := hp.NewFullTextDataOption().
				TWithLanguage(tc.language).
				TWithEmptyPercent(0)

			err = hp.DefaultFullTextPrepare.InsertDataWithOption(ctx, t, mc, schema, tc.numRows, option)
			common.CheckErr(t, err, true)

			flushTask, err := mc.Flush(ctx, milvusclient.NewFlushOption(schema.CollectionName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			results, err := hp.DefaultFullTextPrepare.Search(ctx, t, mc, schema,
				tc.query, tc.topK, []string{"id", "text"})
			common.CheckErr(t, err, true)
			require.Equal(t, 1, len(results))
			require.Equal(t, tc.topK, results[0].IDs.Len())
		})
	}
}

// TestSearchFullTextWithDynamicField tests full text search with dynamic field enabled
func TestSearchFullTextWithDynamicField(t *testing.T) {
	// Test cases for different languages and analyzers
	testCases := []struct {
		name     string
		language string
		analyzer string
		query    string
		numRows  int
		topK     int
	}{
		{
			name:     "English_Standard",
			language: "english",
			analyzer: "standard",
			query:    "what is information retrieval and its applications?",
			numRows:  1000,
			topK:     5,
		},
		{
			name:     "Chinese_Jieba",
			language: "chinese",
			analyzer: "jieba",
			query:    "信息检索的应用",
			numRows:  1000,
			topK:     5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := createDefaultMilvusClient(ctx, t)

			params := hp.NewFullTextParams().
				TWithCollectionName("full_text_search_dynamic_" + tc.name).
				TWithTextFields([]string{"text"}).
				TWithMaxLength(1000).
				TWithAnalyzerType(tc.analyzer).
				TWithEnableDynamicField(true)

			var schema *entity.Schema
			schema, err := hp.DefaultFullTextPrepare.CreateCollection(ctx, t, mc, params)
			common.CheckErr(t, err, true)

			err = hp.DefaultFullTextPrepare.CreateIndex(ctx, t, mc, schema, "0.2")
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(schema.CollectionName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// Insert data with the specified language
			option := hp.NewFullTextDataOption().TWithLanguage(tc.language)
			err = hp.DefaultFullTextPrepare.InsertDataWithOption(ctx, t, mc, schema, tc.numRows, option)
			common.CheckErr(t, err, true)

			flushTask, err := mc.Flush(ctx, milvusclient.NewFlushOption(schema.CollectionName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// Test search with the language-specific query
			results, err := hp.DefaultFullTextPrepare.Search(ctx, t, mc, schema,
				tc.query, tc.topK, []string{"id", "text"})
			common.CheckErr(t, err, true)
			require.Equal(t, 1, len(results))
			require.Greater(t, results[0].IDs.Len(), 0)
		})
	}
}

// TestSearchFullTextWithPartitionKey tests full text search with partition key
func TestSearchFullTextWithPartitionKey(t *testing.T) {
	// Test cases for different languages and analyzers
	testCases := []struct {
		name     string
		language string
		analyzer string
		query    string
		numRows  int
		topK     int
	}{
		{
			name:     "English_Standard",
			language: "english",
			analyzer: "standard",
			query:    "what is information retrieval and its applications?",
			numRows:  1000,
			topK:     5,
		},
		{
			name:     "Chinese_Jieba",
			language: "chinese",
			analyzer: "jieba",
			query:    "信息检索的应用",
			numRows:  1000,
			topK:     5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := createDefaultMilvusClient(ctx, t)

			params := hp.NewFullTextParams().
				TWithCollectionName("full_text_search_partition_" + tc.name).
				TWithTextFields([]string{"text"}).
				TWithMaxLength(1000).
				TWithAnalyzerType(tc.analyzer).
				TWithIsPartitionKey(true)

			var schema *entity.Schema
			schema, err := hp.DefaultFullTextPrepare.CreateCollection(ctx, t, mc, params)
			common.CheckErr(t, err, true)

			err = hp.DefaultFullTextPrepare.CreateIndex(ctx, t, mc, schema, "0.2")
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(schema.CollectionName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// Insert data with the specified language
			option := hp.NewFullTextDataOption().TWithLanguage(tc.language)
			err = hp.DefaultFullTextPrepare.InsertDataWithOption(ctx, t, mc, schema, tc.numRows, option)
			common.CheckErr(t, err, true)

			flushTask, err := mc.Flush(ctx, milvusclient.NewFlushOption(schema.CollectionName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// Test search with the language-specific query
			results, err := hp.DefaultFullTextPrepare.Search(ctx, t, mc, schema,
				tc.query, tc.topK, []string{"id", "text"})
			common.CheckErr(t, err, true)
			require.Equal(t, 1, len(results))
			require.Greater(t, results[0].IDs.Len(), 0)
		})
	}
}

// TestSearchFullTextWithEmptyData tests full text search with empty data
func TestSearchFullTextWithEmptyData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	params := hp.NewFullTextParams().
		TWithCollectionName("full_text_search_empty").
		TWithTextFields([]string{"text"})

	var schema *entity.Schema
	schema, err := hp.DefaultFullTextPrepare.CreateCollection(ctx, t, mc, params)
	common.CheckErr(t, err, true)

	err = hp.DefaultFullTextPrepare.CreateIndex(ctx, t, mc, schema, "0.2")
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Insert data with high empty percentage
	option := hp.NewFullTextDataOption().
		TWithLanguage("english").
		TWithEmptyPercent(50)

	err = hp.DefaultFullTextPrepare.InsertDataWithOption(ctx, t, mc, schema, 1000, option)
	common.CheckErr(t, err, true)

	flushTask, err := mc.Flush(ctx, milvusclient.NewFlushOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	err = flushTask.Await(ctx)
	common.CheckErr(t, err, true)

	results, err := hp.DefaultFullTextPrepare.Search(ctx, t, mc, schema,
		"test query", 5, []string{"id", "text"})
	common.CheckErr(t, err, true)
	require.Equal(t, 1, len(results))
}
