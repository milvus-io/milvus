package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestFullTextSearchDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	analyzerParams := map[string]any{"tokenizer": "standard"}
	fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
	function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
	insertOption := hp.TNewDataOption().TWithTextLang(common.DefaultTextLang)
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
	prepare.CreateIndex(ctx, t, mc, indexparams)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search
	queries := hp.GenFullTextQuery(common.DefaultNq, common.DefaultTextLang)
	vectors := make([]entity.Vector, 0, len(queries))
	for _, query := range queries {
		vectors = append(vectors, entity.Text(query))
	}
	resSearch, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
}

// TestSearchFullTextBase tests basic full text search functionality with different languages
func TestSearchFullTextWithDiffLang(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
			topK:     10,
		},
		{
			name:     "Chinese_Jieba",
			language: "chinese",
			analyzer: "jieba",
			query:    "信息检索的应用",
			numRows:  3000,
			topK:     10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			analyzerParams := map[string]any{"tokenizer": tc.analyzer}
			fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
			function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
			insertOption := hp.TNewDataOption().TWithTextLang(tc.language).TWithNb(tc.numRows)
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
			prepare.FlushData(ctx, t, mc, schema.CollectionName)

			indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
			prepare.CreateIndex(ctx, t, mc, indexparams)
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			// search
			queries := []string{tc.query}
			vectors := make([]entity.Vector, 0, len(queries))
			for _, query := range queries {
				vectors = append(vectors, entity.Text(query))
			}
			resSearch, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, tc.topK, vectors).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)
			common.CheckSearchResult(t, resSearch, len(queries), tc.topK)
		})
	}
}

// TestSearchFullTextWithDynamicField tests full text search with dynamic field enabled
func TestSearchFullTextWithDynamicField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
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
			analyzerParams := map[string]any{"tokenizer": tc.analyzer}
			fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
			function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
			schemaOption := hp.TNewSchemaOption().TWithFunction(function).TWithEnableDynamicField(true)
			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
			insertOption := hp.TNewDataOption().TWithTextLang(tc.language).TWithNb(tc.numRows)
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
			prepare.FlushData(ctx, t, mc, schema.CollectionName)

			indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
			prepare.CreateIndex(ctx, t, mc, indexparams)
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			// search
			queries := []string{tc.query}
			vectors := make([]entity.Vector, 0, len(queries))
			for _, query := range queries {
				vectors = append(vectors, entity.Text(query))
			}
			resSearch, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, tc.topK, vectors).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)
			common.CheckSearchResult(t, resSearch, len(queries), tc.topK)
		})
	}
}

// TestSearchFullTextWithPartitionKey tests full text search with partition key
func TestSearchFullTextWithPartitionKey(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

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
			analyzerParams := map[string]any{"tokenizer": tc.analyzer}
			fieldsOption := hp.TNewFieldOptions().WithFieldOption(common.DefaultTextFieldName, hp.TNewFieldsOption().TWithIsPartitionKey(true).TWithAnalyzerParams(analyzerParams))
			function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
			insertOption := hp.TNewDataOption().TWithTextLang(tc.language).TWithNb(tc.numRows)
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
			prepare.FlushData(ctx, t, mc, schema.CollectionName)

			indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
			prepare.CreateIndex(ctx, t, mc, indexparams)
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			// add field
			newField := entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithDefaultValueLong(100)
			err := mc.AddCollectionField(ctx, milvusclient.NewAddCollectionFieldOption(schema.CollectionName, newField))
			common.CheckErr(t, err, true)

			// search
			queries := []string{tc.query}
			vectors := make([]entity.Vector, 0, len(queries))
			for _, query := range queries {
				vectors = append(vectors, entity.Text(query))
			}
			resSearch, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, tc.topK, vectors).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)
			common.CheckSearchResult(t, resSearch, len(queries), tc.topK)

			// search with new field filter
			resSearch, err = mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, tc.topK, vectors).WithConsistencyLevel(entity.ClStrong).
				WithFilter(fmt.Sprintf("%s == 100", common.DefaultNewField)))
			common.CheckErr(t, err, true)
			common.CheckSearchResult(t, resSearch, len(queries), tc.topK)
		})
	}
}

// TestSearchFullTextWithEmptyData tests full text search with empty data
func TestSearchFullTextWithEmptyData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Test cases for different empty percent
	testCases := []struct {
		name         string
		language     string
		analyzer     string
		query        string
		numRows      int
		topK         int
		emptyPercent int
	}{
		{
			name:         "English_Standard",
			language:     "english",
			analyzer:     "standard",
			query:        "what is information retrieval and its applications?",
			numRows:      3000,
			topK:         5,
			emptyPercent: 90,
		},
		{
			name:         "Chinese_Jieba",
			language:     "chinese",
			analyzer:     "jieba",
			query:        "信息检索的应用",
			numRows:      3000,
			topK:         5,
			emptyPercent: 90,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			analyzerParams := map[string]any{"tokenizer": tc.analyzer}
			fieldsOption := hp.TNewFieldOptions().WithFieldOption(common.DefaultTextFieldName, hp.TNewFieldsOption().TWithIsPartitionKey(true).TWithAnalyzerParams(analyzerParams))
			function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
			insertOption := hp.TNewDataOption().TWithTextLang(tc.language).TWithNb(tc.numRows).TWithTextEmptyPercent(tc.emptyPercent)
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
			prepare.FlushData(ctx, t, mc, schema.CollectionName)

			indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
			prepare.CreateIndex(ctx, t, mc, indexparams)
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			// search
			queries := []string{tc.query}
			vectors := make([]entity.Vector, 0, len(queries))
			for _, query := range queries {
				vectors = append(vectors, entity.Text(query))
			}
			resSearch, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, tc.topK, vectors).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)
			common.CheckSearchResult(t, resSearch, len(queries), tc.topK)
		})
	}
}

// test full-text-search with default text, output text
func TestFullTextSearchDefaultValue(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	defaultText := "data mining supports query expansion"
	analyzerParams := map[string]any{"tokenizer": "standard"}
	fieldsOption := hp.TNewFieldOptions().WithFieldOption(common.DefaultTextFieldName, hp.TNewFieldsOption().TWithDefaultValue(defaultText).TWithEnableAnalyzer(true).TWithAnalyzerParams(analyzerParams))
	function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, hp.TNewSchemaOption().TWithFunction(function))

	// insert data with all null
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = false
	}
	insertOption := hp.TNewColumnOptions().WithColumnOption(common.DefaultTextFieldName, hp.TNewDataOption().TWithTextLang(common.DefaultTextLang).TWithValidData(validData))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	indexParams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
	prepare.CreateIndex(ctx, t, mc, indexParams)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// full text search
	vectors := make([]entity.Vector, 0, common.DefaultNq)
	for i := 0; i < common.DefaultNq; i++ {
		vectors = append(vectors, entity.Text(defaultText))
	}

	resSearch, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields(common.DefaultTextFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultTextFieldName}, resSearch[0].Fields)
	for _, field := range resSearch[0].Fields {
		if field.Name() == common.DefaultTextFieldName {
			for i := 0; i < field.Len(); i++ {
				fieldData, _ := field.GetAsString(i)
				require.EqualValues(t, defaultText, fieldData)
			}
		}
	}
}
