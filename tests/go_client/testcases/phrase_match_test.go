package testcases

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestPhraseMatchDefault tests basic phrase match functionality with slop=0
func TestPhraseMatchDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	analyzerParams := map[string]any{"tokenizer": "standard"}
	fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
	function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
	query := common.GenText(common.DefaultTextLang)
	insertOption := hp.TNewDataOption().TWithTextLang(common.DefaultTextLang).TWithTextData([]string{query})
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
	prepare.CreateIndex(ctx, t, mc, indexparams)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Test exact phrase match (slop=0)

	expr := fmt.Sprintf("phrase_match(%s, \"%s\", 0)", common.DefaultTextFieldName, query)
	queryRes, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).WithFilter(expr))
	common.CheckErr(t, err, true)
	// Results may vary as we're using auto-generated data, but it should >= 1, since query text has been inserted
	require.GreaterOrEqual(t, queryRes.ResultCount, 1)
}

// TestPhraseMatchWithSlop tests phrase match with different slop values
func TestPhraseMatchWithSlop(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	analyzerParams := map[string]any{"tokenizer": "standard"}
	fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
	function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)

	// Insert test data with varying distances between words
	query := common.GenText(common.DefaultTextLang)
	insertOption := hp.TNewDataOption().TWithTextLang(common.DefaultTextLang).TWithTextData([]string{query})
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
	prepare.CreateIndex(ctx, t, mc, indexparams)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Test cases with different slop values
	testCases := []struct {
		name string
		slop int
	}{
		{"ExactMatch", 0},                 // Matches only exact phrase
		{"SmallSlop", 1},                  // Matches phrases with 1 word between
		{"MediumSlop", 2},                 // Matches phrases with 2 words between
		{"LargeSlop", 3},                  // Matches phrases with up to 3 words between
		{"VeryLargeSlop", math.MaxUint32}, // Matches phrases with up to max u32 words between
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expr := fmt.Sprintf("phrase_match(%s, \"%s\", %d)", common.DefaultTextFieldName, query, tc.slop)
			queryRes, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).WithFilter(expr))
			common.CheckErr(t, err, true)
			require.GreaterOrEqual(t, queryRes.ResultCount, 1)
		})
	}
}

// TestPhraseMatchWithDiffLang tests phrase match with different languages
func TestPhraseMatchWithDiffLang(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Test cases for different languages and analyzers
	testCases := []struct {
		name     string
		language string
		analyzer string
		slop     int
	}{
		{
			name:     "English_Standard",
			language: common.English,
			analyzer: "standard",
			slop:     3,
		},
		{
			name:     "Chinese_Jieba",
			language: common.Chinese,
			analyzer: "jieba",
			slop:     3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			analyzerParams := map[string]any{"tokenizer": tc.analyzer}
			fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
			function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)
			query := common.GenText(tc.language)
			insertOption := hp.TNewDataOption().TWithTextLang(tc.language).TWithTextData([]string{query})
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
			prepare.FlushData(ctx, t, mc, schema.CollectionName)

			indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
			prepare.CreateIndex(ctx, t, mc, indexparams)
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			expr := fmt.Sprintf("phrase_match(%s, \"%s\", %d)", common.DefaultTextFieldName, query, tc.slop)
			queryRes, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).WithFilter(expr))
			common.CheckErr(t, err, true)
			require.GreaterOrEqual(t, queryRes.ResultCount, 1)
		})
	}
}

// TestPhraseMatchWithEmptyData tests phrase match with empty data
func TestPhraseMatchWithEmptyData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	analyzerParams := map[string]any{"tokenizer": "standard"}
	fieldsOption := hp.TNewFieldsOption().TWithAnalyzerParams(analyzerParams)
	function := hp.TNewBM25Function(common.DefaultTextFieldName, common.DefaultTextSparseVecFieldName)
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.FullTextSearch), fieldsOption, schemaOption)

	insertOption := hp.TNewDataOption().TWithTextLang(common.DefaultTextLang).TWithTextEmptyPercent(100)
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), insertOption)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	indexparams := hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultTextSparseVecFieldName: index.NewSparseInvertedIndex(entity.BM25, 0.1)})
	prepare.CreateIndex(ctx, t, mc, indexparams)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Test phrase match with empty data
	query := common.GenText(common.DefaultTextLang)
	expr := fmt.Sprintf("phrase_match(%s, \"%s\", 0)", common.DefaultTextFieldName, query)
	queryRes, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).WithFilter(expr))
	common.CheckErr(t, err, true)
	require.Equal(t, 0, queryRes.ResultCount)
}
