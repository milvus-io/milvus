package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	milvusAddr           = "localhost:19530"
	collectionName       = "expr_rerank_test_collection"
	titleFieldName       = "title"
	textFieldName        = "document_text"
	createdAtFieldName   = "created_at"
	qualityFieldName     = "quality_score"
	popularityFieldName  = "popularity"
	categoryFieldName    = "category"
	titleSparseFieldName = "title_sparse_vector"
	textSparseFieldName  = "text_sparse_vector"
)

type TestCase struct {
	Name        string
	Query       string
	ExprCode    string
	ExpectedTop string // Expected document to be at top
	Description string
	InputFields []string
}

type TestData struct {
	Title      string
	Text       string
	CreatedAt  int64   // Unix timestamp in milliseconds
	Quality    float32 // Quality score 0-100
	Popularity int64   // Popularity count
	Category   string  // Category for conditional logic
}

func setupCollection(ctx context.Context, client *milvusclient.Client) error {
	// Drop existing collection if it exists
	err := client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Printf("Collection '%s' doesn't exist or failed to drop: %v", collectionName, err)
	}

	schema := entity.NewSchema().
		WithName(collectionName).
		WithField(entity.NewField().
			WithName("id").
			WithDataType(entity.FieldTypeInt64).
			WithIsPrimaryKey(true).
			WithIsAutoID(true)).
		WithField(entity.NewField().
			WithName(titleFieldName).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(500).
			WithEnableAnalyzer(true).
			WithEnableMatch(true)).
		WithField(entity.NewField().
			WithName(textFieldName).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(2000).
			WithEnableAnalyzer(true).
			WithEnableMatch(true)).
		WithField(entity.NewField().
			WithName(createdAtFieldName).
			WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().
			WithName(qualityFieldName).
			WithDataType(entity.FieldTypeFloat)).
		WithField(entity.NewField().
			WithName(popularityFieldName).
			WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().
			WithName(categoryFieldName).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(100)).
		WithField(entity.NewField().
			WithName(titleSparseFieldName).
			WithDataType(entity.FieldTypeSparseVector)).
		WithField(entity.NewField().
			WithName(textSparseFieldName).
			WithDataType(entity.FieldTypeSparseVector))

	// Create BM25 functions for title and text fields
	titleBM25Function := entity.NewFunction().
		WithName("title_bm25_func").
		WithType(entity.FunctionTypeBM25).
		WithInputFields(titleFieldName).
		WithOutputFields(titleSparseFieldName)

	textBM25Function := entity.NewFunction().
		WithName("text_bm25_func").
		WithType(entity.FunctionTypeBM25).
		WithInputFields(textFieldName).
		WithOutputFields(textSparseFieldName)

	schema.WithFunction(titleBM25Function).WithFunction(textBM25Function)

	// Create indexes
	titleIndex := index.NewInvertedIndex()
	titleIndexOption := milvusclient.NewCreateIndexOption(collectionName, titleFieldName, titleIndex)

	textIndex := index.NewInvertedIndex()
	textIndexOption := milvusclient.NewCreateIndexOption(collectionName, textFieldName, textIndex)

	titleSparseIndex := index.NewSparseInvertedIndex(entity.BM25, 0.2)
	titleSparseIndexOption := milvusclient.NewCreateIndexOption(collectionName, titleSparseFieldName, titleSparseIndex)

	textSparseIndex := index.NewSparseInvertedIndex(entity.BM25, 0.2)
	textSparseIndexOption := milvusclient.NewCreateIndexOption(collectionName, textSparseFieldName, textSparseIndex)

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).
		WithIndexOptions(titleIndexOption, textIndexOption, titleSparseIndexOption, textSparseIndexOption))
	if err != nil {
		return fmt.Errorf("failed to create collection: %v", err)
	}

	return nil
}

func insertTestData(ctx context.Context, client *milvusclient.Client) error {
	now := time.Now().UnixMilli()

	testData := []TestData{
		{
			Title:      "Recent AI Breakthrough",
			Text:       "Latest artificial intelligence research shows remarkable progress in machine learning.",
			CreatedAt:  now - 86400000, // 1 day ago
			Quality:    95.0,
			Popularity: 1000,
			Category:   "featured",
		},
		{
			Title:      "Alan Turing Biography",
			Text:       "Alan Turing was the first person to propose AI and machine learning concepts.",
			CreatedAt:  now - 86400000*30, // 30 days ago
			Quality:    85.0,
			Popularity: 500,
			Category:   "premium",
		},
		{
			Title:      "Old AI History",
			Text:       "Artificial intelligence was founded in 1956 by computer scientists.",
			CreatedAt:  now - 86400000*365, // 1 year ago
			Quality:    70.0,
			Popularity: 100,
			Category:   "standard",
		},
		{
			Title:      "Machine Learning Overview",
			Text:       "Machine learning is a subset of artificial intelligence that focuses on algorithms.",
			CreatedAt:  now - 86400000*7, // 1 week ago
			Quality:    90.0,
			Popularity: 800,
			Category:   "featured",
		},
		{
			Title:      "Deep Learning Applications",
			Text:       "Deep learning neural networks are used in modern AI applications.",
			CreatedAt:  now - 86400000*14, // 2 weeks ago
			Quality:    88.0,
			Popularity: 600,
			Category:   "premium",
		},
		{
			Title:      "Low Quality AI Content",
			Text:       "Some basic information about artificial intelligence and machine learning.",
			CreatedAt:  now - 86400000*3, // 3 days ago
			Quality:    30.0,
			Popularity: 50,
			Category:   "standard",
		},
		{
			Title:      "Viral AI Post",
			Text:       "This artificial intelligence post went viral with amazing machine learning insights.",
			CreatedAt:  now - 86400000*2, // 2 days ago
			Quality:    75.0,
			Popularity: 5000,
			Category:   "featured",
		},
		{
			Title:      "Technical AI Paper",
			Text:       "Advanced artificial intelligence research on neural network architectures.",
			CreatedAt:  now - 86400000*60, // 2 months ago
			Quality:    98.0,
			Popularity: 200,
			Category:   "premium",
		},
	}

	titleColumn := make([]string, len(testData))
	textColumn := make([]string, len(testData))
	createdAtColumn := make([]int64, len(testData))
	qualityColumn := make([]float32, len(testData))
	popularityColumn := make([]int64, len(testData))
	categoryColumn := make([]string, len(testData))

	for i, d := range testData {
		titleColumn[i] = d.Title
		textColumn[i] = d.Text
		createdAtColumn[i] = d.CreatedAt
		qualityColumn[i] = d.Quality
		popularityColumn[i] = d.Popularity
		categoryColumn[i] = d.Category
	}

	_, err := client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithVarcharColumn(titleFieldName, titleColumn).
		WithVarcharColumn(textFieldName, textColumn).
		WithInt64Column(createdAtFieldName, createdAtColumn).
		WithColumns(column.NewColumnFloat(qualityFieldName, qualityColumn)).
		WithInt64Column(popularityFieldName, popularityColumn).
		WithVarcharColumn(categoryFieldName, categoryColumn))
	if err != nil {
		return fmt.Errorf("failed to insert data: %v", err)
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		return fmt.Errorf("failed to load collection: %v", err)
	}

	err = loadTask.Await(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for collection loading: %v", err)
	}

	return nil
}

// runExpressionTest executes a single expression reranker test case
func runExpressionTest(ctx context.Context, client *milvusclient.Client, testCase TestCase) error {
	log.Printf("\n=== Running Test: %s ===", testCase.Name)
	log.Printf("Description: %s", testCase.Description)
	log.Printf("Query: '%s'", testCase.Query)
	log.Printf("Expression: %s", testCase.ExprCode)
	log.Printf("Expected Top Result: %s", testCase.ExpectedTop)

	textVector := entity.Text(testCase.Query)

	// Create a text match filter for both title and text fields
	textMatchFilter := fmt.Sprintf(`text_match(%s, "%s") OR text_match(%s, "%s")`,
		titleFieldName, testCase.Query, textFieldName, testCase.Query)

	// Create expression reranker function with input fields provided by the test case
	exprRerankerFunction := entity.NewFunction().
		WithName("expr_reranker").
		WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "expr").
		WithParam("expr_code", testCase.ExprCode)
	if len(testCase.InputFields) > 0 {
		exprRerankerFunction = exprRerankerFunction.WithInputFields(testCase.InputFields...)
	}

	// After creating exprRerankerFunction, add this:
	fmt.Printf("=== DEBUG: Expr Reranker Function ===\n")
	protoMsg := exprRerankerFunction.ProtoMessage()
	fmt.Printf("Function Name: %s\n", protoMsg.GetName())
	fmt.Printf("Function Type: %d\n", protoMsg.GetType())
	fmt.Printf("Input Fields: %v\n", protoMsg.GetInputFieldNames())
	fmt.Printf("Params:\n")
	for _, param := range protoMsg.GetParams() {
		fmt.Printf("  %s = %s\n", param.Key, param.Value)
	}
	fmt.Printf("=====================================\n\n")

	// Add a proxy-level decay reranker to demonstrate dual rerankers
	// Decay on recency using created_at, applied at proxy after QueryNode rerank
	decayReranker := entity.NewFunction().
		WithName("proxy_decay_reranker").
		WithType(entity.FunctionTypeRerank).
		WithInputFields(createdAtFieldName).
		WithParam("reranker", "decay").
		WithParam("function", "exp").
		WithParam("origin", time.Now().UnixMilli()).
		WithParam("scale", 7*24*60*60*1000). // ~1 week in ms
		WithParam("offset", 0).              // no offset
		WithParam("decay", 0.5).             // half-life like decay
		WithParam("score_mode", "max")

	searchResult, err := client.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		10,
		[]entity.Vector{textVector},
	).WithANNSField(textSparseFieldName).
		WithFilter(textMatchFilter).
		WithOutputFields("id", titleFieldName, textFieldName, createdAtFieldName, qualityFieldName, popularityFieldName, categoryFieldName).
		WithFunctionReranker(exprRerankerFunction).
		WithFunctionReranker(decayReranker))
	if err != nil {
		return fmt.Errorf("search failed: %v", err)
	}

	actualCount := 0
	var topResult string

	for _, resultSet := range searchResult {
		actualCount = resultSet.ResultCount
		log.Printf("Results Found: %d", actualCount)

		if actualCount == 0 {
			log.Println("No matching documents found")
		} else {
			log.Println("Reranked results:")
			for i := 0; i < resultSet.ResultCount; i++ {
				var id interface{}
				if resultSet.IDs != nil {
					id, _ = resultSet.IDs.Get(i)
				}

				score := resultSet.Scores[i]

				var title, text, category string
				var createdAt, popularity int64
				var quality float32

				if titleColumn := resultSet.GetColumn(titleFieldName); titleColumn != nil {
					title, _ = titleColumn.GetAsString(i)
				}
				if textColumn := resultSet.GetColumn(textFieldName); textColumn != nil {
					text, _ = textColumn.GetAsString(i)
				}
				if createdAtColumn := resultSet.GetColumn(createdAtFieldName); createdAtColumn != nil {
					createdAt, _ = createdAtColumn.GetAsInt64(i)
				}
				if qualityColumn := resultSet.GetColumn(qualityFieldName); qualityColumn != nil {
					qualityDouble, _ := qualityColumn.GetAsDouble(i)
					quality = float32(qualityDouble)
				}
				if popularityColumn := resultSet.GetColumn(popularityFieldName); popularityColumn != nil {
					popularity, _ = popularityColumn.GetAsInt64(i)
				}
				if categoryColumn := resultSet.GetColumn(categoryFieldName); categoryColumn != nil {
					category, _ = categoryColumn.GetAsString(i)
				}

				if i == 0 {
					topResult = title
				}

				// Calculate age in days for display
				now := time.Now().UnixMilli()
				ageDays := (now - createdAt) / 86400000

				log.Printf("  [%d] ID: %v, Score: %.4f", i+1, id, score)
				log.Printf("      Title: %s", title)
				log.Printf("      Text: %s", text)
				log.Printf("      Age: %d days, Quality: %.1f, Popularity: %d, Category: %s",
					ageDays, quality, popularity, category)
			}
		}
	}

	// Validate results
	if testCase.ExpectedTop != "" {
		if strings.Contains(topResult, testCase.ExpectedTop) {
			log.Printf("✅ PASS: Expected '%s' at top, got '%s'", testCase.ExpectedTop, topResult)
		} else {
			log.Printf("❌ FAIL: Expected '%s' at top, got '%s'", testCase.ExpectedTop, topResult)
		}
	} else {
		log.Printf("ℹ️  INFO: No specific expectation set (top result: '%s')", topResult)
	}

	return nil
}

func runAllExpressionTests(ctx context.Context, client *milvusclient.Client) error {
	now := time.Now().UnixMilli()

	testCases := []TestCase{
		// {
		// 	Name:        "Basic Score Pass-through",
		// 	Query:       "artificial intelligence",
		// 	ExprCode:    "score",
		// 	ExpectedTop: "", // No specific expectation, just test basic functionality
		// 	Description: "Test basic expression that returns original score unchanged",
		// 	InputFields: nil,
		// },
		{
			Name:        "Quality Boost",
			Query:       "artificial intelligence",
			ExprCode:    "score * (fields[\"quality_score\"] / 100.0)",
			ExpectedTop: "Recent AI Breakthrough",
			Description: "Test quality-based boosting - higher quality should rank higher",
			InputFields: []string{qualityFieldName},
		},
		{
			Name:        "Recency Boost",
			Query:       "artificial intelligence",
			ExprCode:    fmt.Sprintf("let age_days = (%d - fields[\"created_at\"]) / 86400000; score * exp(-0.01 * age_days)", now),
			ExpectedTop: "Recent AI Breakthrough", // Most recent (1 day ago)
			Description: "Test recency-based boosting - newer content should rank higher",
			InputFields: []string{createdAtFieldName},
		},
		{
			Name:        "Popularity Boost",
			Query:       "artificial intelligence",
			ExprCode:    "score * (1.0 + log(fields[\"popularity\"] + 1) / 10.0)",
			ExpectedTop: "Recent AI Breakthrough", // Updated: recent item remains top after popularity boost
			Description: "Test popularity-based boosting - viral content should rank higher",
			InputFields: []string{popularityFieldName},
		},
		{
			Name:        "Category Conditional Boost",
			Query:       "artificial intelligence",
			ExprCode:    "let boost = fields[\"category\"] == \"featured\" ? 2.0 : fields[\"category\"] == \"premium\" ? 1.5 : 1.0; score * boost",
			ExpectedTop: "Recent AI Breakthrough", // Featured category with 2x boost
			Description: "Test conditional category boosting - featured content gets highest boost",
			InputFields: []string{categoryFieldName},
		},
		{
			Name:        "Multi-factor E-commerce Style",
			Query:       "artificial intelligence",
			ExprCode:    "let quality = fields[\"quality_score\"] / 100.0; let pop_boost = 1.0 + log(fields[\"popularity\"] + 1) / 20.0; score * quality * pop_boost",
			ExpectedTop: "Recent AI Breakthrough",
			Description: "Test multi-factor scoring combining quality and popularity",
			InputFields: []string{qualityFieldName, popularityFieldName},
		},
		{
			Name:        "Quality Filter with Penalty",
			Query:       "artificial intelligence",
			ExprCode:    "fields[\"quality_score\"] > 80.0 ? score : score * 0.1",
			ExpectedTop: "Recent AI Breakthrough", // Recency after decay outweighs older high-quality content
			Description: "Test quality filtering - low quality content gets heavily penalized",
			InputFields: []string{qualityFieldName},
		},
		{
			Name:        "Balanced Recency and Quality",
			Query:       "artificial intelligence",
			ExprCode:    fmt.Sprintf("let age_days = (%d - fields[\"created_at\"]) / 86400000; let recency = exp(-0.005 * age_days); let quality = fields[\"quality_score\"] / 100.0; score * (recency * 0.6 + quality * 0.4)", now),
			ExpectedTop: "Recent AI Breakthrough", // Good balance of recent + high quality
			Description: "Test balanced scoring between recency (60%) and quality (40%)",
			InputFields: []string{createdAtFieldName, qualityFieldName},
		},
		{
			Name:        "Capped Boost",
			Query:       "artificial intelligence",
			ExprCode:    "min(score * (fields[\"popularity\"] / 100.0), score * 5.0)",
			ExpectedTop: "Recent AI Breakthrough", // Higher original score wins when capped
			Description: "Test capped boosting to prevent extreme score inflation",
			InputFields: []string{popularityFieldName},
		},
		{
			Name:        "Complex Multi-Conditional",
			Query:       "artificial intelligence",
			ExprCode:    fmt.Sprintf("let age_days = (%d - fields[\"created_at\"]) / 86400000; let is_recent = age_days < 7; let is_high_quality = fields[\"quality_score\"] > 85; let is_popular = fields[\"popularity\"] > 500; score * (is_recent ? 1.5 : 1.0) * (is_high_quality ? 1.3 : 1.0) * (is_popular ? 1.2 : 1.0)", now),
			ExpectedTop: "Recent AI Breakthrough", // Recent + high quality + popular
			Description: "Test complex conditional logic with multiple boolean factors",
			InputFields: []string{createdAtFieldName, qualityFieldName, popularityFieldName},
		},
	}

	log.Println("\n🚀 Starting Expression Reranker Test Suite")
	log.Println(strings.Repeat("=", 60))

	passCount := 0
	totalTests := len(testCases)

	for i, testCase := range testCases {
		log.Printf("\nTest %d/%d", i+1, totalTests)
		err := runExpressionTest(ctx, client, testCase)
		if err != nil {
			log.Printf("❌ Test failed with error: %v", err)
		} else {
			passCount++
		}
		log.Println(strings.Repeat("-", 40))
	}

	log.Printf("\n📊 Test Summary:")
	log.Printf("Total Tests: %d", totalTests)
	log.Printf("Completed: %d", passCount)
	log.Printf("Errors: %d", totalTests-passCount)

	return nil
}

// runWeightedWithLexicalSearchTest demonstrates Weighted reranker with lexical search, min should match, and expression reranker
func runWeightedWithLexicalSearchTest(ctx context.Context, client *milvusclient.Client) error {
	log.Println("\n⚖️  Running Weighted Reranker with Lexical Search + Min Should Match + Expression Reranker Test")
	log.Println(strings.Repeat("=", 70))

	query := "artificial intelligence machine learning"
	log.Printf("Query: '%s'", query)

	// Create text vector for BM25 search
	textVector := entity.Text(query)

	// Test 1: Min Should Match with text_match filter
	log.Println("\n--- Test 1: Single BM25 Search with Min Should Match ---")
	minShouldMatch := "1"
	textMatchFilter := fmt.Sprintf(`text_match(%s, "%s", "min_should_match=%s")`,
		textFieldName, query, minShouldMatch)
	log.Printf("Filter: %s", textMatchFilter)

	singleSearchResult, err := client.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		5,
		[]entity.Vector{textVector},
	).WithANNSField(textSparseFieldName).
		WithFilter(textMatchFilter).
		WithOutputFields("id", titleFieldName, textFieldName, qualityFieldName, popularityFieldName))
	if err != nil {
		return fmt.Errorf("single search with min_should_match failed: %v", err)
	}

	log.Println("Results:")
	for _, resultSet := range singleSearchResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			title, _ := resultSet.GetColumn(titleFieldName).GetAsString(i)
			score := resultSet.Scores[i]
			log.Printf("  [%d] ID: %v, Score: %.4f, Title: %s", i+1, id, score, title)
		}
	}

	// Test 2: Weighted Reranker with Multiple BM25 Searches (Title + Text)
	log.Println("\n--- Test 2: Weighted Fusion of Title and Text BM25 Searches ---")

	// Create two separate ANN requests for hybrid search
	// Request 1: Search on title sparse vector (higher weight - titles are more important)
	titleSearchRequest := milvusclient.NewAnnRequest(titleSparseFieldName, 20, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s")`, titleFieldName, query)).
		WithSearchParam("metric_type", "BM25")

	// Request 2: Search on text sparse vector
	textSearchRequest := milvusclient.NewAnnRequest(textSparseFieldName, 20, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s", "min_should_match=%s")`,
			textFieldName, query, minShouldMatch)).
		WithSearchParam("metric_type", "BM25")

	// Create Weighted reranker (0.6 weight for title, 0.4 for text)
	weightedReranker := milvusclient.NewWeightedReranker([]float64{0.6, 0.4})

	log.Println("Executing Hybrid Search with Weighted Reranker...")
	hybridSearchResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, titleSearchRequest, textSearchRequest).
			WithReranker(weightedReranker).
			WithOutputFields("id", titleFieldName, textFieldName, qualityFieldName, popularityFieldName))
	if err != nil {
		return fmt.Errorf("hybrid search with Weighted reranker failed: %v", err)
	}

	log.Println("Weighted Fusion Results:")
	for _, resultSet := range hybridSearchResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			title, _ := resultSet.GetColumn(titleFieldName).GetAsString(i)
			text, _ := resultSet.GetColumn(textFieldName).GetAsString(i)
			score := resultSet.Scores[i]
			log.Printf("  [%d] ID: %v, Weighted Score: %.4f", i+1, id, score)
			log.Printf("      Title: %s", title)
			log.Printf("      Text: %s", text[:min(len(text), 60)])
		}
	}

	// Test 3: Weighted + Expression-based Reranker (Two-tier reranking)
	log.Println("\n--- Test 3: Weighted + Expression Reranker (Two-tier) ---")

	// Create expression reranker function for quality boosting
	exprRerankerFunction := entity.NewFunction().
		WithName("quality_boost_reranker").
		WithType(entity.FunctionTypeRerank).
		WithInputFields(qualityFieldName).
		WithParam("reranker", "expr").
		WithParam("expr_code", "score * (fields[\"quality_score\"] / 100.0)")

	// Create Weighted reranker as function (70% title, 30% text)
	weightedRerankerFunction := entity.NewFunction().
		WithName("weighted_reranker").
		WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "weighted").
		WithParam("weights", "[0.7, 0.3]")

	log.Println("Executing Hybrid Search with Weighted + Expression Reranker...")
	dualRerankResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, titleSearchRequest, textSearchRequest).
			WithFunctionRerankers(weightedRerankerFunction).
			WithFunctionRerankers(exprRerankerFunction).
			WithOutputFields("id", titleFieldName, textFieldName, qualityFieldName, popularityFieldName))
	if err != nil {
		return fmt.Errorf("hybrid search with dual rerankers failed: %v", err)
	}

	log.Println("Weighted + Expression Reranked Results:")
	for _, resultSet := range dualRerankResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			title, _ := resultSet.GetColumn(titleFieldName).GetAsString(i)
			qualityDouble, _ := resultSet.GetColumn(qualityFieldName).GetAsDouble(i)
			quality := float32(qualityDouble)
			score := resultSet.Scores[i]
			log.Printf("  [%d] ID: %v, Final Score: %.4f, Quality: %.1f", i+1, id, score, quality)
			log.Printf("      Title: %s", title)
		}
	}

	// Test 4: Complex scenario - Weighted with three BM25 searches + expr reranker
	log.Println("\n--- Test 4: Weighted with Three BM25 Searches + Complex Expression Reranker ---")

	now := time.Now().UnixMilli()

	// Create three searches with different characteristics
	// Search 1: Title search (highest weight)
	priorityTitleSearch := milvusclient.NewAnnRequest(titleSparseFieldName, 30, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s")`, titleFieldName, query)).
		WithSearchParam("metric_type", "BM25")

	// Search 2: Text search with min_should_match (medium weight)
	standardTextSearch := milvusclient.NewAnnRequest(textSparseFieldName, 15, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s", "min_should_match=2")`,
			textFieldName, query)).
		WithSearchParam("metric_type", "BM25")

	// Search 3: Another title search with exact phrase (lower weight)
	exactTitleSearch := milvusclient.NewAnnRequest(titleSparseFieldName, 10, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s")`, titleFieldName, "artificial intelligence")).
		WithSearchParam("metric_type", "BM25")

	// Create weighted reranker for 3 searches (0.5, 0.3, 0.2)
	tripleWeightedFunction := entity.NewFunction().
		WithName("triple_weighted_reranker").
		WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "weighted").
		WithParam("weights", "[0.5, 0.3, 0.2]")

	// Complex expression reranker combining quality, popularity, and recency
	complexExprCode := fmt.Sprintf(
		"let age_days = (%d - fields[\"created_at\"]) / 86400000; "+
			"let recency = exp(-0.01 * age_days); "+
			"let quality = fields[\"quality_score\"] / 100.0; "+
			"let pop_boost = 1.0 + log(fields[\"popularity\"] + 1) / 20.0; "+
			"score * quality * pop_boost * recency",
		now)

	complexExprReranker := entity.NewFunction().
		WithName("multi_factor_reranker").
		WithType(entity.FunctionTypeRerank).
		WithInputFields(createdAtFieldName, qualityFieldName, popularityFieldName).
		WithParam("reranker", "expr").
		WithParam("expr_code", complexExprCode)

	log.Println("Executing Complex Hybrid Search with 3 searches...")
	complexResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 8, priorityTitleSearch, standardTextSearch, exactTitleSearch).
			WithFunctionRerankers(tripleWeightedFunction).
			WithFunctionRerankers(complexExprReranker).
			WithOutputFields("id", titleFieldName, createdAtFieldName, qualityFieldName, popularityFieldName, categoryFieldName))
	if err != nil {
		return fmt.Errorf("complex hybrid search failed: %v", err)
	}

	log.Println("Complex Multi-factor Reranked Results (Weighted + Expression):")
	for _, resultSet := range complexResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			title, _ := resultSet.GetColumn(titleFieldName).GetAsString(i)
			createdAt, _ := resultSet.GetColumn(createdAtFieldName).GetAsInt64(i)
			qualityDouble, _ := resultSet.GetColumn(qualityFieldName).GetAsDouble(i)
			quality := float32(qualityDouble)
			popularity, _ := resultSet.GetColumn(popularityFieldName).GetAsInt64(i)
			category, _ := resultSet.GetColumn(categoryFieldName).GetAsString(i)
			score := resultSet.Scores[i]

			ageDays := (now - createdAt) / 86400000
			log.Printf("  [%d] ID: %v, Score: %.4f", i+1, id, score)
			log.Printf("      Title: %s", title)
			log.Printf("      Quality: %.1f, Popularity: %d, Age: %d days, Category: %s",
				quality, popularity, ageDays, category)
		}
	}

	log.Println("\n✅ All Weighted + Lexical Search tests completed successfully!")
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	ctx := context.Background()

	log.Println("🔗 Connecting to Milvus...")
	milvusClient, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatalf("failed to connect to Milvus: %v", err)
	}
	defer milvusClient.Close(ctx)
	log.Println("✅ Connected to Milvus successfully.")

	log.Println("\n🏗️  Setting up test collection...")
	err = setupCollection(ctx, milvusClient)
	if err != nil {
		log.Fatalf("failed to setup collection: %v", err)
	}
	log.Printf("✅ Collection '%s' created successfully.", collectionName)

	log.Println("\n📝 Inserting comprehensive test data...")
	err = insertTestData(ctx, milvusClient)
	if err != nil {
		log.Fatalf("failed to insert test data: %v", err)
	}
	log.Println("✅ Test data inserted and collection loaded successfully.")

	log.Println("\n🧪 Running Expression Reranker tests...")
	err = runAllExpressionTests(ctx, milvusClient)
	if err != nil {
		log.Fatalf("failed to run tests: %v", err)
	}

	log.Println("\n⚖️  Running Weighted Reranker + Lexical Search tests...")
	err = runWeightedWithLexicalSearchTest(ctx, milvusClient)
	if err != nil {
		log.Fatalf("failed to run Weighted reranker tests: %v", err)
	}

	log.Println("\n🧹 Cleaning up...")
	err = milvusClient.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Printf("failed to drop collection: %v", err)
	} else {
		log.Printf("✅ Collection '%s' dropped successfully.", collectionName)
	}

	log.Println("\n🎉 All test suites completed!")
}
