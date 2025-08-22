package testcases

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// newTextEmbeddingFieldsOption creates fields option with text embedding settings
func newTextEmbeddingFieldsOption(autoId bool) hp.FieldOptions {
	fieldOpts := hp.TNewFieldOptions().
		WithFieldOption("document", hp.TNewFieldsOption().TWithMaxLen(common.MaxLength)).
		WithFieldOption("dense", hp.TNewFieldsOption().TWithDim(int64(hp.GetTEIModelDim()))).
		WithFieldOption(common.DefaultInt64FieldName, hp.TNewFieldsOption().TWithAutoID(autoId))
	return fieldOpts
}

// TestCreateCollectionWithTextEmbedding tests basic collection creation with text embedding function
func TestCreateCollectionWithTextEmbedding(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.TextEmbedding), newTextEmbeddingFieldsOption(true), hp.TNewTextEmbeddingSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// verify collection creation
	require.NotNil(t, prepare)
	require.NotNil(t, schema)

	// describe collection to verify function
	descRes, err := mc.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.Len(t, descRes.Schema.Functions, 1)
	require.Equal(t, "document_text_emb", descRes.Schema.Functions[0].Name)
	require.Equal(t, entity.FunctionTypeTextEmbedding, descRes.Schema.Functions[0].Type)
	require.Equal(t, []string{"document"}, descRes.Schema.Functions[0].InputFieldNames)
	require.Equal(t, []string{"dense"}, descRes.Schema.Functions[0].OutputFieldNames)
}

// TestCreateCollectionWithTextEmbeddingTwice tests creating collection twice with same schema
func TestCreateCollectionWithTextEmbeddingTwice(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function
	function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
	})
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	fieldsOption := newTextEmbeddingFieldsOption(true)

	collectionName := common.GenRandomString("text_embedding", 6)
	createParams := hp.NewCreateCollectionParams(hp.TextEmbedding)

	// first creation
	prepare1, schema1 := hp.CollPrepare.CreateCollection(
		ctx, t, mc, createParams, fieldsOption,
		schemaOption.TWithName(collectionName),
		hp.TWithConsistencyLevel(entity.ClStrong),
	)
	require.NotNil(t, prepare1)
	require.NotNil(t, schema1)

	// second creation with same name should succeed (idempotent)
	prepare2, schema2 := hp.CollPrepare.CreateCollection(
		ctx, t, mc, createParams, fieldsOption,
		schemaOption.TWithName(collectionName),
		hp.TWithConsistencyLevel(entity.ClStrong),
	)
	require.NotNil(t, prepare2)
	require.NotNil(t, schema2)

	// verify function exists
	descRes, err := mc.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collectionName))
	common.CheckErr(t, err, true)
	require.Len(t, descRes.Schema.Functions, 1)
}

// TestCreateCollectionUnsupportedEndpoint tests creation with unsupported endpoint
func TestCreateCollectionUnsupportedEndpoint(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with invalid endpoint
	function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": "http://unsupported_endpoint",
	})
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)

	// this should fail during collection creation
	fieldOpts := hp.TNewFieldOptions().
		WithFieldOption("document", hp.TNewFieldsOption().TWithMaxLen(common.MaxLength)).
		WithFieldOption("dense", hp.TNewFieldsOption().TWithDim(int64(hp.GetTEIModelDim()))).
		WithFieldOption(common.DefaultInt64FieldName, hp.TNewFieldsOption().TWithAutoID(true))
	err := mc.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(
		common.GenRandomString("text_embedding", 6),
		hp.GenSchema(schemaOption.TWithFields(hp.FieldsFact.GenFieldsForCollection(hp.TextEmbedding, fieldOpts))),
	))

	// expect error due to unsupported endpoint
	common.CheckErr(t, err, false, "unsupported_endpoint")
}

// TestCreateCollectionUnmatchedDim tests creation with mismatched dimension
func TestCreateCollectionUnmatchedDim(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with wrong dimension (512 instead of expected 768 from TEI model)
	wrongDim := int64(512)
	function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
	})
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	fieldsOption := hp.TNewFieldOptions().
		WithFieldOption("document", hp.TNewFieldsOption().TWithMaxLen(common.MaxLength)).
		WithFieldOption("dense", hp.TNewFieldsOption().TWithDim(wrongDim)).
		WithFieldOption(common.DefaultInt64FieldName, hp.TNewFieldsOption().TWithAutoID(true))

	collectionName := common.GenRandomString("text_embedding", 6)

	// collection creation should fail with dimension mismatch error
	err := mc.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(
		collectionName,
		hp.GenSchema(schemaOption.TWithFields(hp.FieldsFact.GenFieldsForCollection(hp.TextEmbedding, fieldsOption))),
	))

	// Expect error with specific dimension mismatch message
	expectedError := fmt.Sprintf("required embedding dim is [%d], but the embedding obtained from the model is [%d]", wrongDim, hp.GetTEIModelDim())
	common.CheckErr(t, err, false, expectedError)
}

// TestInsertWithTextEmbedding tests basic data insertion with text embedding
func TestInsertWithTextEmbedding(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.TextEmbedding), newTextEmbeddingFieldsOption(true), hp.TNewTextEmbeddingSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// prepare test data - only provide text, embedding will be auto-generated
	nb := 10
	documents := make([]string, nb)
	for i := 0; i < nb; i++ {
		documents[i] = fmt.Sprintf("This is test document number %d with some content for embedding", i)
	}

	// insert data using only text field
	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(nb), res.InsertCount)

	// create index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{"dense": index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query to verify vectors were generated
	resQuery, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).WithFilter("").WithOutputFields("dense").WithLimit(10))
	common.CheckErr(t, err, true)
	require.Greater(t, len(resQuery.Fields), 0)

	// verify vector dimension - check first result
	if resQuery.Len() > 0 {
		// Query results structure is different - need to check the actual field structure
		denseColumn := resQuery.GetColumn("dense")
		require.NotNil(t, denseColumn)
		// Field should contain vectors for all results
	}
}

// TestInsertWithTruncateParams tests insertion with different truncate parameters
func TestInsertWithTruncateParams(t *testing.T) {
	testCases := []struct {
		name                string
		truncate            bool
		truncationDirection string
		shouldSucceed       bool
	}{
		{"truncate_true_right", true, "Right", true},
		{"truncate_true_left", true, "Left", true},
		{"truncate_false", false, "", false}, // should fail with long text
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			// create TEI function with truncate parameters
			params := map[string]any{}
			if tc.truncate {
				params["truncate"] = "true"
				params["truncation_direction"] = tc.truncationDirection
			} else {
				params["truncate"] = "false"
			}

			params["provider"] = "TEI"
			params["endpoint"] = hp.GetTEIEndpoint()
			function := hp.TNewTextEmbeddingFunction("document", "dense", params)
			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			fieldsOption := newTextEmbeddingFieldsOption(true)

			_, schema := hp.CollPrepare.CreateCollection(
				ctx, t, mc,
				hp.NewCreateCollectionParams(hp.TextEmbedding),
				fieldsOption,
				schemaOption,
				hp.TWithConsistencyLevel(entity.ClStrong),
			)

			// prepare long text data that would need truncation
			// Generate distinctly different left and right parts that will exceed token limits when combined
			leftPart := "artificial intelligence machine learning deep learning neural networks computer vision natural language processing data science algorithms " + strings.Repeat("technology innovation science research development analysis ", 100)
			rightPart := "database systems vector search embeddings similarity matching retrieval information storage indexing " + strings.Repeat("query performance optimization scalability distributed computing ", 100)
			longText := leftPart + " " + rightPart // This will exceed 512 tokens and need truncation

			documents := []string{longText, leftPart, rightPart}

			// insert data
			res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))

			if tc.shouldSucceed {
				common.CheckErr(t, err, true)
				require.Equal(t, int64(len(documents)), res.InsertCount)

				// create index and load for embedding comparison
				_, err = mc.CreateIndex(ctx, milvusclient.NewCreateIndexOption(schema.CollectionName, "dense", index.NewAutoIndex(entity.COSINE)))
				common.CheckErr(t, err, true)

				_, err = mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(schema.CollectionName))
				common.CheckErr(t, err, true)

				// Query embeddings from Milvus
				resQuery, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).
					WithFilter("").
					WithOutputFields("dense", "document").
					WithConsistencyLevel(entity.ClStrong).
					WithLimit(10))
				common.CheckErr(t, err, true)
				require.Equal(t, len(documents), resQuery.Len())

				// Extract Milvus embeddings
				denseColumn := resQuery.GetColumn("dense")
				require.NotNil(t, denseColumn)
				floatVecColumn, ok := denseColumn.(*column.ColumnFloatVector)
				require.True(t, ok, "Dense column should be a float vector column")

				// Truncation validation using similarity comparison approach
				// This follows the Python test logic: compare similarity between combined text and parts
				// to verify that truncation direction works correctly

				require.Equal(t, 3, resQuery.Len(), "Should have 3 documents: longText, leftPart, rightPart")

				// Get embeddings for: [0]=longText, [1]=leftPart, [2]=rightPart
				embeddings := make([][]float32, 3)
				for i := 0; i < 3; i++ {
					embedding := floatVecColumn.Data()[i]
					require.Equal(t, hp.GetTEIModelDim(), len(embedding), "Embedding should have correct dimension")

					// Check that embedding is not all zeros (would indicate a failure)
					var sum float32
					for _, val := range embedding {
						sum += val * val
					}
					require.Greater(t, sum, float32(0.01), "Embedding should not be all zeros for document %d", i)

					embeddings[i] = embedding
				}

				// Calculate cosine similarities
				// similarity_left: longText vs leftPart
				// similarity_right: longText vs rightPart
				similarityLeft := hp.CosineSimilarity(embeddings[0], embeddings[1])
				similarityRight := hp.CosineSimilarity(embeddings[0], embeddings[2])

				t.Logf("Similarity longText vs leftPart: %.6f", similarityLeft)
				t.Logf("Similarity longText vs rightPart: %.6f", similarityRight)

				// Validation based on truncation direction:
				// - If truncation_direction = "Left", we keep the right part, so longText should be more similar to rightPart
				// - If truncation_direction = "Right", we keep the left part, so longText should be more similar to leftPart
				if tc.truncationDirection == "Left" {
					require.Greater(t, similarityRight, similarityLeft,
						"With Left truncation, longText should be more similar to rightPart (%.6f) than leftPart (%.6f)",
						similarityRight, similarityLeft)
					t.Logf("Left truncation verified: rightPart similarity (%.6f) > leftPart similarity (%.6f)",
						similarityRight, similarityLeft)
				} else { // "Right"
					require.Greater(t, similarityLeft, similarityRight,
						"With Right truncation, longText should be more similar to leftPart (%.6f) than rightPart (%.6f)",
						similarityLeft, similarityRight)
					t.Logf("Right truncation verified: leftPart similarity (%.6f) > rightPart similarity (%.6f)",
						similarityLeft, similarityRight)
				}

				t.Logf("Successfully inserted %d documents with truncate=%v, direction=%s", len(documents), tc.truncate, tc.truncationDirection)
			} else {
				common.CheckErr(t, err, false, "Payload Too Large")
			}
		})
	}
}

// TestVerifyEmbeddingConsistency verifies that Milvus text embedding function produces same results as direct TEI calls
func TestVerifyEmbeddingConsistency(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function (custom fields for autoID=false)
	function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
	})
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	fieldsOption := newTextEmbeddingFieldsOption(false)

	prepare, schema := hp.CollPrepare.CreateCollection(
		ctx, t, mc,
		hp.NewCreateCollectionParams(hp.TextEmbedding),
		fieldsOption,
		schemaOption,
		hp.TWithConsistencyLevel(entity.ClStrong),
	)

	// Test documents
	testDocs := []string{
		"This is a test document about artificial intelligence",
		"Vector databases enable semantic search capabilities",
		"Text embeddings transform language into numbers",
	}

	// Insert documents into Milvus (will use text embedding function)
	ids := []int64{1, 2, 3}
	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).
		WithInt64Column(common.DefaultInt64FieldName, ids).
		WithVarcharColumn("document", testDocs))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(len(testDocs)), res.InsertCount)

	// Create index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{"dense": index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Query vectors from Milvus
	resQuery, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).
		WithFilter("").
		WithOutputFields("dense", "document", common.DefaultInt64FieldName).
		WithConsistencyLevel(entity.ClStrong).
		WithLimit(10))
	common.CheckErr(t, err, true)
	require.Equal(t, len(testDocs), resQuery.Len())

	// Get embeddings directly from TEI
	teiEmbeddings, err := hp.CallTEIDirectly(hp.GetTEIEndpoint(), testDocs)
	if err != nil {
		t.Skipf("Skip consistency test - could not connect to TEI endpoint: %v", err)
		return
	}
	require.Equal(t, len(testDocs), len(teiEmbeddings))

	// Compare embeddings
	denseColumn := resQuery.GetColumn("dense")
	require.NotNil(t, denseColumn)

	// Get ID column to match embeddings with documents
	idColumn := resQuery.GetColumn(common.DefaultInt64FieldName)
	require.NotNil(t, idColumn)

	// Extract and compare embeddings - need to handle column type properly
	floatVecColumn, ok := denseColumn.(*column.ColumnFloatVector)
	require.True(t, ok, "Dense column should be a float vector column")

	for i := 0; i < resQuery.Len(); i++ {
		// Get ID to find corresponding TEI embedding
		id, err := idColumn.GetAsInt64(i)
		require.NoError(t, err)
		teiIdx := id - 1 // IDs are 1-based, array is 0-based

		// Get Milvus embedding from the float vector column
		milvusEmbedding := floatVecColumn.Data()[i]

		require.NotNil(t, milvusEmbedding)
		require.Equal(t, hp.GetTEIModelDim(), len(milvusEmbedding), "Embedding dimension should match")

		// Calculate cosine similarity
		similarity := hp.CosineSimilarity(milvusEmbedding, teiEmbeddings[teiIdx])

		t.Logf("Document %d (ID=%d) similarity between Milvus and TEI: %.6f", i, id, similarity)

		// Embeddings should be nearly identical (similarity > 0.99)
		require.Greater(t, similarity, float32(0.99),
			"Milvus embedding should be nearly identical to TEI embedding for document ID %d", id)
	}

	t.Log("Embedding consistency verified: Milvus text embedding function produces same results as direct TEI calls")
}

// TestUpsertTextFieldUpdatesEmbedding tests that upserting text field updates embedding
func TestUpsertTextFieldUpdatesEmbedding(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function (custom fields for autoID=false for upsert)
	function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
	})
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	fieldsOption := newTextEmbeddingFieldsOption(false)

	prepare, schema := hp.CollPrepare.CreateCollection(
		ctx, t, mc,
		hp.NewCreateCollectionParams(hp.TextEmbedding),
		fieldsOption,
		schemaOption,
		hp.TWithConsistencyLevel(entity.ClStrong),
	)

	// create index and load first
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{"dense": index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert initial data with specific ID
	oldText := "This is the original text content"
	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).
		WithInt64Column(common.DefaultInt64FieldName, []int64{1}).
		WithVarcharColumn("document", []string{oldText}))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1), res.InsertCount)

	// query original embedding before upsert
	resQueryBefore, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).
		WithFilter("int64 == 1").
		WithOutputFields("document", "dense").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, resQueryBefore.Len())

	// extract original embedding
	originalDenseColumn := resQueryBefore.GetColumn("dense")
	require.NotNil(t, originalDenseColumn)
	originalFloatVecColumn, ok := originalDenseColumn.(*column.ColumnFloatVector)
	require.True(t, ok, "Dense column should be a float vector column")
	originalEmbedding := originalFloatVecColumn.Data()[0]
	require.Equal(t, hp.GetTEIModelDim(), len(originalEmbedding), "Original embedding dimension should match")

	// verify original text
	originalDocColumn := resQueryBefore.GetColumn("document")
	require.NotNil(t, originalDocColumn)
	originalVarCharColumn, ok := originalDocColumn.(*column.ColumnVarChar)
	require.True(t, ok, "Document column should be a varchar column")
	require.Equal(t, oldText, originalVarCharColumn.Data()[0], "Original text should match")

	// upsert with new text
	newText := "This is completely different updated text content"
	res2, err := mc.Upsert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).
		WithInt64Column(common.DefaultInt64FieldName, []int64{1}).
		WithVarcharColumn("document", []string{newText}))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1), res2.UpsertCount)

	// query updated embedding after upsert
	resQueryAfter, err := mc.Query(ctx, milvusclient.NewQueryOption(schema.CollectionName).
		WithFilter("int64 == 1").
		WithOutputFields("document", "dense").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, resQueryAfter.Len())

	// extract updated embedding
	updatedDenseColumn := resQueryAfter.GetColumn("dense")
	require.NotNil(t, updatedDenseColumn)
	updatedFloatVecColumn, ok := updatedDenseColumn.(*column.ColumnFloatVector)
	require.True(t, ok, "Dense column should be a float vector column")
	updatedEmbedding := updatedFloatVecColumn.Data()[0]
	require.Equal(t, hp.GetTEIModelDim(), len(updatedEmbedding), "Updated embedding dimension should match")

	// verify updated text
	updatedDocColumn := resQueryAfter.GetColumn("document")
	require.NotNil(t, updatedDocColumn)
	updatedVarCharColumn, ok := updatedDocColumn.(*column.ColumnVarChar)
	require.True(t, ok, "Document column should be a varchar column")
	require.Equal(t, newText, updatedVarCharColumn.Data()[0], "Updated text should match")

	// verify embeddings are different (key assertion)
	similarity := hp.CosineSimilarity(originalEmbedding, updatedEmbedding)
	require.Less(t, similarity, float32(0.95),
		"Embeddings should be significantly different after text update (similarity=%.6f)", similarity)

	t.Logf("Upsert verification complete: Original and updated embeddings have cosine similarity %.6f (< 0.95)", similarity)
	t.Logf("   Original text: %s", oldText)
	t.Logf("   Updated text: %s", newText)
}

// TestDeleteAndSearch tests that deleted text cannot be searched
func TestDeleteAndSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function (custom fields for autoID=false)
	function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
	})
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	fieldsOption := newTextEmbeddingFieldsOption(false)

	prepare, schema := hp.CollPrepare.CreateCollection(
		ctx, t, mc,
		hp.NewCreateCollectionParams(hp.TextEmbedding),
		fieldsOption,
		schemaOption,
		hp.TWithConsistencyLevel(entity.ClStrong),
	)

	// insert test data
	documents := []string{
		"This is test document 0",
		"This is test document 1",
		"This is test document 2",
	}
	ids := []int64{0, 1, 2}

	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).
		WithInt64Column(common.DefaultInt64FieldName, ids).
		WithVarcharColumn("document", documents))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(3), res.InsertCount)

	// create index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{"dense": index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete document with ID 1
	res2, err := mc.Delete(ctx, milvusclient.NewDeleteOption(schema.CollectionName).WithExpr("int64 in [1]"))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1), res2.DeleteCount)

	// search and verify document 1 is not in results
	searchRes, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, 3, []entity.Vector{entity.Text("test document 1")}).
		WithANNSField("dense").
		WithOutputFields("document", common.DefaultInt64FieldName))
	common.CheckErr(t, err, true)

	// verify deleted document is not in results
	require.Greater(t, len(searchRes), 0)
	for _, hits := range searchRes {
		for i := 0; i < hits.Len(); i++ {
			id, err := hits.IDs.GetAsInt64(i)
			require.NoError(t, err)
			require.NotEqual(t, int64(1), id, "Deleted document should not appear in search results")
		}
	}
}

// TestSearchWithTextEmbedding tests search functionality with text embedding
func TestSearchWithTextEmbedding(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.TextEmbedding), newTextEmbeddingFieldsOption(true), hp.TNewTextEmbeddingSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// prepare test data
	nb := 10
	documents := make([]string, nb)
	for i := 0; i < nb; i++ {
		documents[i] = fmt.Sprintf("This is test document number %d about artificial intelligence and machine learning", i)
	}

	// insert data using only text field
	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(nb), res.InsertCount)

	// create index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{"dense": index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search using text query
	queryText := "artificial intelligence machine learning"
	searchRes, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, 5, []entity.Vector{entity.Text(queryText)}).
		WithANNSField("dense").
		WithOutputFields("document"))
	common.CheckErr(t, err, true)

	require.Greater(t, len(searchRes), 0)
	for _, hits := range searchRes {
		require.Greater(t, hits.Len(), 0, "Should find relevant documents")
		require.LessOrEqual(t, hits.Len(), 5, "Should respect limit")

		// verify results contain the search terms (semantic similarity)
		for i := 0; i < hits.Len(); i++ {
			score := hits.Scores[i]
			require.Greater(t, score, float32(0), "Score should be positive")
		}
	}
}

// TestSearchWithEmptyQuery tests search with empty query (should fail)
func TestSearchWithEmptyQuery(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.TextEmbedding), newTextEmbeddingFieldsOption(true), hp.TNewTextEmbeddingSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// insert some test data
	documents := []string{"test document"}
	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1), res.InsertCount)

	// create index and load
	_, err = mc.CreateIndex(ctx, milvusclient.NewCreateIndexOption(schema.CollectionName, "dense", index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)

	_, err = mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)

	// search with empty query should fail
	_, err = mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, 3, []entity.Vector{entity.Text("")}).
		WithANNSField("dense"))

	common.CheckErr(t, err, false, "TextEmbedding function does not support empty text")
}

// TestHybridSearchTextEmbeddingBM25 tests hybrid search combining TEI text embedding and BM25
func TestHybridSearchTextEmbeddingBM25(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with both TEI text embedding and BM25 functions
	collectionName := common.GenRandomString("hybrid_search", 6)

	// create fields manually to support both dense and sparse vectors
	fields := []*entity.Field{
		entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true),
		entity.NewField().WithName("document").WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535).WithEnableAnalyzer(true).WithAnalyzerParams(map[string]any{"tokenizer": "standard"}),
		entity.NewField().WithName("dense").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(hp.GetTEIModelDim())),
		entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector),
	}

	// create TEI text embedding function
	teiFunction := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
	})

	// create BM25 function
	bm25Function := hp.TNewBM25Function("document", "sparse")

	// create schema with both functions
	schema := entity.NewSchema().
		WithName(collectionName).
		WithDescription("Hybrid search collection with TEI and BM25").
		WithFunction(teiFunction).
		WithFunction(bm25Function)

	for _, field := range fields {
		schema.WithField(field)
	}

	// create collection
	err := mc.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema))
	common.CheckErr(t, err, true)

	// insert test data with diverse content
	documents := []string{
		"Artificial intelligence and machine learning are transforming technology",
		"Vector databases enable semantic search capabilities for AI applications",
		"Text embeddings capture semantic meaning in numerical representations",
		"BM25 is a traditional keyword-based search algorithm",
		"Hybrid search combines semantic and keyword-based retrieval methods",
		"Large language models use transformer architectures for text understanding",
		"Information retrieval systems help users find relevant documents",
		"Natural language processing enables computers to understand human language",
		"Database systems store and retrieve structured information efficiently",
		"Search engines use ranking algorithms to order results by relevance",
	}

	// insert data - both embeddings will be generated automatically
	res, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).WithVarcharColumn("document", documents))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(len(documents)), res.InsertCount)

	// create indexes
	_, err = mc.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "dense", index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "sparse", index.NewSparseInvertedIndex(entity.BM25, 0.1)))
	common.CheckErr(t, err, true)

	// load collection
	_, err = mc.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	common.CheckErr(t, err, true)

	// test 1: Dense vector search (TEI semantic search)
	t.Run("DenseVectorSearch", func(t *testing.T) {
		queryText := "machine learning artificial intelligence"
		searchRes, err := mc.Search(ctx, milvusclient.NewSearchOption(collectionName, 3, []entity.Vector{entity.Text(queryText)}).
			WithANNSField("dense").
			WithOutputFields("document"))
		common.CheckErr(t, err, true)

		require.Greater(t, len(searchRes), 0)
		for _, hits := range searchRes {
			require.Greater(t, hits.Len(), 0, "Should find semantically similar documents")
			t.Logf("Dense search found %d results for query: %s", hits.Len(), queryText)
		}
	})

	// test 2: Sparse vector search (BM25 keyword search)
	t.Run("SparseVectorSearch", func(t *testing.T) {
		queryText := "database systems"
		searchRes, err := mc.Search(ctx, milvusclient.NewSearchOption(collectionName, 3, []entity.Vector{entity.Text(queryText)}).
			WithANNSField("sparse").
			WithOutputFields("document"))
		common.CheckErr(t, err, true)

		require.Greater(t, len(searchRes), 0)
		for _, hits := range searchRes {
			require.Greater(t, hits.Len(), 0, "Should find keyword-matching documents")
			t.Logf("Sparse search found %d results for query: %s", hits.Len(), queryText)
		}
	})

	// test 3: Both search types work independently
	t.Run("IndependentSearches", func(t *testing.T) {
		queryText := "vector search"

		// Dense search
		denseRes, err := mc.Search(ctx, milvusclient.NewSearchOption(collectionName, 5, []entity.Vector{entity.Text(queryText)}).
			WithANNSField("dense").
			WithOutputFields("document"))
		common.CheckErr(t, err, true)

		// Sparse search
		sparseRes, err := mc.Search(ctx, milvusclient.NewSearchOption(collectionName, 5, []entity.Vector{entity.Text(queryText)}).
			WithANNSField("sparse").
			WithOutputFields("document"))
		common.CheckErr(t, err, true)

		// Both should return results
		require.Greater(t, len(denseRes), 0, "Dense search should return results")
		require.Greater(t, len(sparseRes), 0, "Sparse search should return results")

		for _, hits := range denseRes {
			require.Greater(t, hits.Len(), 0, "Dense search should find documents")
		}

		for _, hits := range sparseRes {
			require.Greater(t, hits.Len(), 0, "Sparse search should find documents")
		}

		t.Logf("Dense search found %d results, Sparse search found %d results",
			len(denseRes), len(sparseRes))
	})
}

// TestInsertEmptyDocument tests insertion with empty document
func TestInsertEmptyDocument(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.TextEmbedding), newTextEmbeddingFieldsOption(true), hp.TNewTextEmbeddingSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// try to insert empty document
	documents := []string{"", "normal document"}

	_, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))

	// should fail with empty document
	common.CheckErr(t, err, false, "TextEmbedding function does not support empty text")
}

// TestInsertLongDocument tests insertion with very long document
func TestInsertLongDocument(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function (no truncate)
	params := map[string]any{
		"provider": "TEI",
		"endpoint": hp.GetTEIEndpoint(),
		"truncate": "false",
	}
	function := hp.TNewTextEmbeddingFunction("document", "dense", params)
	schemaOption := hp.TNewSchemaOption().TWithFunction(function)
	fieldsOption := newTextEmbeddingFieldsOption(true)

	_, schema := hp.CollPrepare.CreateCollection(
		ctx, t, mc,
		hp.NewCreateCollectionParams(hp.TextEmbedding),
		fieldsOption,
		schemaOption,
		hp.TWithConsistencyLevel(entity.ClStrong),
	)

	// try to insert very long document that exceeds model limits
	longDocument := hp.GenLongText(8192, "english") // Very long text
	documents := []string{longDocument}

	_, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))

	// should fail with long document when truncate is false
	common.CheckErr(t, err, false, "Call service failed")
}

// TestInvalidEndpointHandling tests various invalid endpoint scenarios
func TestInvalidEndpointHandling(t *testing.T) {
	testCases := []struct {
		name     string
		endpoint string
		errMsg   string
	}{
		{"NonExistentHost", "http://nonexistent-host:8080", "nonexistent-host"},
		{"InvalidPort", "http://localhost:99999", "99999"},
		{"InvalidProtocol", "ftp://localhost:8080", "ftp"},
		{"EmptyEndpoint", "", "endpoint"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			// create collection with invalid endpoint
			function := hp.TNewTextEmbeddingFunction("document", "dense", map[string]any{
				"provider": "TEI",
				"endpoint": tc.endpoint,
			})
			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			fieldOpts := hp.TNewFieldOptions().
				WithFieldOption("document", hp.TNewFieldsOption().TWithMaxLen(common.MaxLength)).
				WithFieldOption("dense", hp.TNewFieldsOption().TWithDim(int64(hp.GetTEIModelDim()))).
				WithFieldOption(common.DefaultInt64FieldName, hp.TNewFieldsOption().TWithAutoID(true))

			// collection creation should fail for invalid endpoints
			collectionName := common.GenRandomString("test_invalid", 6)
			err := mc.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(
				collectionName,
				hp.GenSchema(schemaOption.TWithFields(hp.FieldsFact.GenFieldsForCollection(hp.TextEmbedding, fieldOpts))),
			))

			common.CheckErr(t, err, false, tc.errMsg)
			t.Logf("Expected error for %s: %v", tc.name, err)
		})
	}
}

// TestMissingRequiredParameters tests creation with missing required parameters
func TestMissingRequiredParameters(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	testCases := []struct {
		name   string
		params map[string]any
		errMsg string
	}{
		{"MissingProvider", map[string]any{"endpoint": hp.GetTEIEndpoint()}, "provider"},
		{"MissingEndpoint", map[string]any{"provider": "TEI"}, "endpoint"},
		{"WrongProvider", map[string]any{"provider": "InvalidProvider", "endpoint": hp.GetTEIEndpoint()}, "invalidprovider"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// create function with incomplete parameters
			function := entity.NewFunction().
				WithName("incomplete_func").
				WithInputFields("document").
				WithOutputFields("dense").
				WithType(entity.FunctionTypeTextEmbedding)

			for key, value := range tc.params {
				function.WithParam(key, value)
			}

			schemaOption := hp.TNewSchemaOption().TWithFunction(function)
			fieldsOption := newTextEmbeddingFieldsOption(true)

			// collection creation should fail
			err := mc.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(
				common.GenRandomString("test_incomplete", 6),
				hp.GenSchema(schemaOption.TWithFields(hp.FieldsFact.GenFieldsForCollection(hp.TextEmbedding, fieldsOption))),
			))

			common.CheckErr(t, err, false, tc.errMsg)
			t.Logf("Expected error for %s: %v", tc.name, err)
		})
	}
}

// TestConcurrentOperations tests concurrent text embedding operations
func TestConcurrentOperations(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2) // longer timeout for concurrent ops
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection with TEI function
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.TextEmbedding), newTextEmbeddingFieldsOption(true), hp.TNewTextEmbeddingSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// create index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{"dense": index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// concurrent inserts
	t.Run("ConcurrentInserts", func(t *testing.T) {
		numRoutines := 5
		documentsPerRoutine := 5

		results := make(chan error, numRoutines)

		for i := 0; i < numRoutines; i++ {
			go func(routineID int) {
				documents := make([]string, documentsPerRoutine)
				for j := 0; j < documentsPerRoutine; j++ {
					documents[j] = fmt.Sprintf("Concurrent document from routine %d, doc %d", routineID, j)
				}

				_, err := mc.Insert(ctx, milvusclient.NewColumnBasedInsertOption(schema.CollectionName).WithVarcharColumn("document", documents))
				results <- err
			}(i)
		}

		// wait for all goroutines to complete
		for i := 0; i < numRoutines; i++ {
			err := <-results
			require.NoError(t, err, "Concurrent insert should succeed")
		}

		t.Logf("Successfully completed %d concurrent inserts with %d documents each", numRoutines, documentsPerRoutine)
	})

	// concurrent searches
	t.Run("ConcurrentSearches", func(t *testing.T) {
		numRoutines := 3

		results := make(chan error, numRoutines)

		for i := 0; i < numRoutines; i++ {
			go func(routineID int) {
				queryText := fmt.Sprintf("document routine %d", routineID)
				_, err := mc.Search(ctx, milvusclient.NewSearchOption(schema.CollectionName, 5, []entity.Vector{entity.Text(queryText)}).
					WithANNSField("dense").
					WithOutputFields("document"))
				results <- err
			}(i)
		}

		// wait for all searches to complete
		for i := 0; i < numRoutines; i++ {
			err := <-results
			require.NoError(t, err, "Concurrent search should succeed")
		}

		t.Logf("Successfully completed %d concurrent searches", numRoutines)
	})
}
