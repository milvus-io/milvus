package main

import (
	"context"
	"fmt"
	"log"
	"math"
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
	Name             string
	Query            string
	ExprCode         string
	ExpectedTop      string             // Expected document to be at top
	ExpectedTopScore float64            // Expected score for the top document
	ExpectedScores   map[string]float64 // Expected scores for all documents (title -> score)
	Description      string
	InputFields      []string
	ScoreCalculator  func(baseScore float64, doc TestData, now int64) float64 // Function to calculate expected score
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

// Global test data accessible for score calculations
var globalTestData []TestData

// Global map to store base BM25 scores (document title -> base score)
var baseScores map[string]float64

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

	// Store test data globally for score calculations
	globalTestData = testData

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

// Score calculator functions for each test case
// These calculate the expected reranked score based on the expression
func qualityBoostCalculator(baseScore float64, doc TestData, now int64) float64 {
	// score * (fields["quality_score"] / 100.0)
	return baseScore * (float64(doc.Quality) / 100.0)
}

func recencyBoostCalculator(baseScore float64, doc TestData, now int64) float64 {
	// let age_days = (now - fields["created_at"]) / 86400000; score * exp(-0.01 * age_days)
	ageDays := float64(now-doc.CreatedAt) / 86400000.0
	return baseScore * math.Exp(-0.01*ageDays)
}

func popularityBoostCalculator(baseScore float64, doc TestData, now int64) float64 {
	// score * (1.0 + log(fields["popularity"] + 1) / 10.0)
	return baseScore * (1.0 + math.Log(float64(doc.Popularity+1))/10.0)
}

func categoryConditionalCalculator(baseScore float64, doc TestData, now int64) float64 {
	// let boost = fields["category"] == "featured" ? 2.0 : fields["category"] == "premium" ? 1.5 : 1.0; score * boost
	var boost float64
	switch doc.Category {
	case "featured":
		boost = 2.0
	case "premium":
		boost = 1.5
	default:
		boost = 1.0
	}
	return baseScore * boost
}

func multiFactorCalculator(baseScore float64, doc TestData, now int64) float64 {
	// let quality = fields["quality_score"] / 100.0; let pop_boost = 1.0 + log(fields["popularity"] + 1) / 20.0; score * quality * pop_boost
	quality := float64(doc.Quality) / 100.0
	popBoost := 1.0 + math.Log(float64(doc.Popularity+1))/20.0
	return baseScore * quality * popBoost
}

func qualityFilterCalculator(baseScore float64, doc TestData, now int64) float64 {
	// fields["quality_score"] > 80.0 ? score : score * 0.1
	if doc.Quality > 80.0 {
		return baseScore
	}
	return baseScore * 0.1
}

func balancedRecencyQualityCalculator(baseScore float64, doc TestData, now int64) float64 {
	// let age_days = (now - fields["created_at"]) / 86400000; let recency = exp(-0.005 * age_days); let quality = fields["quality_score"] / 100.0; score * (recency * 0.6 + quality * 0.4)
	ageDays := float64(now-doc.CreatedAt) / 86400000.0
	recency := math.Exp(-0.005 * ageDays)
	quality := float64(doc.Quality) / 100.0
	return baseScore * (recency*0.6 + quality*0.4)
}

func cappedBoostCalculator(baseScore float64, doc TestData, now int64) float64 {
	// min(score * (fields["popularity"] / 100.0), score * 5.0)
	boosted := baseScore * (float64(doc.Popularity) / 100.0)
	capped := baseScore * 5.0
	if boosted < capped {
		return boosted
	}
	return capped
}

func complexMultiConditionalCalculator(baseScore float64, doc TestData, now int64) float64 {
	// let age_days = (now - fields["created_at"]) / 86400000; let is_recent = age_days < 7; let is_high_quality = fields["quality_score"] > 85; let is_popular = fields["popularity"] > 500; score * (is_recent ? 1.5 : 1.0) * (is_high_quality ? 1.3 : 1.0) * (is_popular ? 1.2 : 1.0)
	ageDays := float64(now-doc.CreatedAt) / 86400000.0

	recentBoost := 1.0
	if ageDays < 7 {
		recentBoost = 1.5
	}

	qualityBoost := 1.0
	if doc.Quality > 85 {
		qualityBoost = 1.3
	}

	popularityBoost := 1.0
	if doc.Popularity > 500 {
		popularityBoost = 1.2
	}

	return baseScore * recentBoost * qualityBoost * popularityBoost
}

// captureBaseScores runs a BM25 search WITHOUT reranker to get pure base BM25 scores
func captureBaseScores(ctx context.Context, client *milvusclient.Client, query string) error {
	log.Println("\n📊 Capturing base BM25 scores (BM25 only, no reranking)...")

	textVector := entity.Text(query)

	// Create a text match filter for both title and text fields
	textMatchFilter := fmt.Sprintf(`text_match(%s, "%s") OR text_match(%s, "%s")`,
		titleFieldName, query, textFieldName, query)

	// Search WITHOUT any reranker to get pure BM25 scores
	searchResult, err := client.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		20, // Request more to ensure we get all docs
		[]entity.Vector{textVector},
	).WithANNSField(textSparseFieldName).
		WithFilter(textMatchFilter).
		WithOutputFields(titleFieldName, textFieldName)) // Explicitly request output fields
	if err != nil {
		return fmt.Errorf("failed to capture base scores: %v", err)
	}

	baseScores = make(map[string]float64)

	for _, resultSet := range searchResult {
		log.Printf("Captured %d base scores:", resultSet.ResultCount)
		log.Printf("Fields available: %d", len(resultSet.Fields))
		for idx, field := range resultSet.Fields {
			if field != nil {
				log.Printf("  Field[%d]: %s (type: %d)", idx, field.Name(), field.Type())
			}
		}

		titleColumn := resultSet.GetColumn(titleFieldName)
		if titleColumn == nil {
			return fmt.Errorf("title column '%s' not found in results", titleFieldName)
		}

		for i := 0; i < resultSet.ResultCount; i++ {
			title, err := titleColumn.GetAsString(i)
			if err != nil {
				log.Printf("  Warning: Failed to get title at index %d: %v", i, err)
				continue
			}
			if title == "" {
				log.Printf("  Warning: Empty title at index %d", i)
				continue
			}
			score := float64(resultSet.Scores[i])
			baseScores[title] = score
			log.Printf("  %s: %.6f", title, score)
		}
	}

	if len(baseScores) == 0 {
		return fmt.Errorf("no base scores captured - title field may not be returned correctly")
	}

	log.Printf("\n✅ Base scores captured for %d documents\n", len(baseScores))
	return nil
}

// runExpressionTest executes a single expression reranker test case
func runExpressionTest(ctx context.Context, client *milvusclient.Client, testCase TestCase, now int64) error {
	log.Printf("\n=== Running Test: %s ===", testCase.Name)
	log.Printf("Description: %s", testCase.Description)
	log.Printf("Query: '%s'", testCase.Query)
	log.Printf("Expression: %s", testCase.ExprCode)
	log.Printf("Expected Top Result: %s", testCase.ExpectedTop)

	// Create a map of test data by title for quick lookup
	testDataMap := make(map[string]TestData)
	for _, data := range globalTestData {
		testDataMap[data.Title] = data
	}

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

	// Note: We only use the expression reranker here (no additional decay reranker)
	// to ensure clean validation of the expression reranker logic
	searchResult, err := client.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		10,
		[]entity.Vector{textVector},
	).WithANNSField(textSparseFieldName).
		WithFilter(textMatchFilter).
		WithOutputFields("id", titleFieldName, textFieldName, createdAtFieldName, qualityFieldName, popularityFieldName, categoryFieldName).
		WithFunctionReranker(exprRerankerFunction))
	if err != nil {
		return fmt.Errorf("search failed: %v", err)
	}

	actualCount := 0
	var topResult string
	var scoreValidationErrors []string

	for _, resultSet := range searchResult {
		actualCount = resultSet.ResultCount
		log.Printf("Results Found: %d", actualCount)

		if actualCount == 0 {
			log.Println("No matching documents found")
		} else {
			log.Println("Reranked results with score validation:")

			// First pass: collect all scores to infer base BM25 scores
			type DocScore struct {
				Title       string
				ActualScore float64
				TestData    TestData
			}
			var docScores []DocScore

			for i := 0; i < resultSet.ResultCount; i++ {
				var id interface{}
				if resultSet.IDs != nil {
					id, _ = resultSet.IDs.Get(i)
				}

				score := float64(resultSet.Scores[i])

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
				ageDays := (now - createdAt) / 86400000

				// Get test data for this document
				testData, exists := testDataMap[title]
				if !exists {
					log.Printf("  [%d] ID: %v, Score: %.4f - WARNING: No test data found for title: %s", i+1, id, score, title)
					continue
				}

				docScores = append(docScores, DocScore{
					Title:       title,
					ActualScore: score,
					TestData:    testData,
				})

				log.Printf("  [%d] ID: %v, Final Score: %.6f", i+1, id, score)
				log.Printf("      Title: %s", title)
				log.Printf("      Text: %s", text)
				log.Printf("      Age: %d days, Quality: %.1f, Popularity: %d, Category: %s",
					ageDays, quality, popularity, category)

				// Calculate expected score if calculator is provided and we have base scores
				if testCase.ScoreCalculator != nil {
					baseScore, hasBaseScore := baseScores[title]
					if !hasBaseScore {
						log.Printf("      ⚠️  WARNING: No base score found for '%s'", title)
						continue
					}

					// Calculate what the score should be using the actual base score
					expectedScore := testCase.ScoreCalculator(baseScore, testData, now)
					scoreDiff := math.Abs(expectedScore - score)
					scorePercDiff := 0.0
					if score != 0 {
						scorePercDiff = (scoreDiff / score) * 100
					}

					// Calculate the effective multiplier/transformation
					multiplier := 0.0
					if baseScore != 0 {
						multiplier = expectedScore / baseScore
					}

					log.Printf("      Raw BM25 Score (before reranking): %.6f", baseScore)

					// Show calculation breakdown based on test case
					log.Printf("      Calculation Breakdown:")
					switch testCase.Name {
					case "Quality Boost":
						qualityMultiplier := float64(testData.Quality) / 100.0
						log.Printf("        - Quality factor: %.6f (quality=%.1f / 100)", qualityMultiplier, testData.Quality)
						log.Printf("        - Formula: raw_score × quality_factor = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, qualityMultiplier, expectedScore)
					case "Recency Boost":
						ageDays := float64(now-testData.CreatedAt) / 86400000.0
						recencyFactor := math.Exp(-0.01 * ageDays)
						log.Printf("        - Age: %.2f days", ageDays)
						log.Printf("        - Recency factor: %.6f (exp(-0.01 × %.2f))", recencyFactor, ageDays)
						log.Printf("        - Formula: raw_score × recency_factor = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, recencyFactor, expectedScore)
					case "Popularity Boost":
						popBoost := 1.0 + math.Log(float64(testData.Popularity+1))/10.0
						log.Printf("        - Popularity: %d", testData.Popularity)
						log.Printf("        - Popularity boost: %.6f (1.0 + ln(%d) / 10)", popBoost, testData.Popularity+1)
						log.Printf("        - Formula: raw_score × popularity_boost = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, popBoost, expectedScore)
					case "Category Conditional Boost":
						categoryBoost := 1.0
						switch testData.Category {
						case "featured":
							categoryBoost = 2.0
						case "premium":
							categoryBoost = 1.5
						default:
							categoryBoost = 1.0
						}
						log.Printf("        - Category: %s", testData.Category)
						log.Printf("        - Category boost: %.6f", categoryBoost)
						log.Printf("        - Formula: raw_score × category_boost = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, categoryBoost, expectedScore)
					case "Multi-factor E-commerce Style":
						qualityFactor := float64(testData.Quality) / 100.0
						popBoost := 1.0 + math.Log(float64(testData.Popularity+1))/20.0
						combinedMultiplier := qualityFactor * popBoost
						log.Printf("        - Quality factor: %.6f (quality=%.1f / 100)", qualityFactor, testData.Quality)
						log.Printf("        - Popularity boost: %.6f (1.0 + ln(%d) / 20)", popBoost, testData.Popularity+1)
						log.Printf("        - Combined multiplier: %.6f (quality × pop_boost)", combinedMultiplier)
						log.Printf("        - Formula: raw_score × quality × pop_boost = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, combinedMultiplier, expectedScore)
					case "Quality Filter with Penalty":
						penalty := "keep (quality > 80)"
						factor := 1.0
						if testData.Quality <= 80.0 {
							penalty = "penalize × 0.1 (quality ≤ 80)"
							factor = 0.1
						}
						log.Printf("        - Quality: %.1f", testData.Quality)
						log.Printf("        - Action: %s", penalty)
						log.Printf("        - Formula: quality > 80 ? raw_score : raw_score × 0.1")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, factor, expectedScore)
					case "Balanced Recency and Quality":
						ageDays := float64(now-testData.CreatedAt) / 86400000.0
						recencyFactor := math.Exp(-0.005 * ageDays)
						qualityFactor := float64(testData.Quality) / 100.0
						combinedFactor := recencyFactor*0.6 + qualityFactor*0.4
						log.Printf("        - Age: %.2f days", ageDays)
						log.Printf("        - Recency factor: %.6f (exp(-0.005 × %.2f))", recencyFactor, ageDays)
						log.Printf("        - Quality factor: %.6f (quality=%.1f / 100)", qualityFactor, testData.Quality)
						log.Printf("        - Combined factor: %.6f (recency×0.6 + quality×0.4)", combinedFactor)
						log.Printf("        - Formula: raw_score × (recency×0.6 + quality×0.4) = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, combinedFactor, expectedScore)
					case "Capped Boost":
						popFactor := float64(testData.Popularity) / 100.0
						boostedValue := baseScore * popFactor
						cappedValue := baseScore * 5.0
						effectiveMultiplier := popFactor
						if boostedValue > cappedValue {
							effectiveMultiplier = 5.0
						}
						log.Printf("        - Popularity: %d", testData.Popularity)
						log.Printf("        - Popularity multiplier: %.6f (popularity / 100)", popFactor)
						log.Printf("        - Boosted: %.6f (raw × %.6f)", boostedValue, popFactor)
						log.Printf("        - Cap: %.6f (raw × 5.0)", cappedValue)
						log.Printf("        - Effective multiplier: %.6f (min of %.6f and 5.0)", effectiveMultiplier, popFactor)
						log.Printf("        - Formula: min(raw_score × popularity/100, raw_score × 5) = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, effectiveMultiplier, expectedScore)
					case "Complex Multi-Conditional":
						ageDays := float64(now-testData.CreatedAt) / 86400000.0
						recentBoost := 1.0
						if ageDays < 7 {
							recentBoost = 1.5
						}
						qualityBoost := 1.0
						if testData.Quality > 85 {
							qualityBoost = 1.3
						}
						popularityBoost := 1.0
						if testData.Popularity > 500 {
							popularityBoost = 1.2
						}
						combinedMultiplier := recentBoost * qualityBoost * popularityBoost
						log.Printf("        - Age: %.2f days → Recent boost: %.1f (%s)", ageDays, recentBoost,
							map[bool]string{true: "< 7 days", false: "≥ 7 days"}[ageDays < 7])
						log.Printf("        - Quality: %.1f → Quality boost: %.1f (%s)", testData.Quality, qualityBoost,
							map[bool]string{true: "> 85", false: "≤ 85"}[testData.Quality > 85])
						log.Printf("        - Popularity: %d → Popularity boost: %.1f (%s)", testData.Popularity, popularityBoost,
							map[bool]string{true: "> 500", false: "≤ 500"}[testData.Popularity > 500])
						log.Printf("        - Combined multiplier: %.6f (%.1f × %.1f × %.1f)", combinedMultiplier,
							recentBoost, qualityBoost, popularityBoost)
						log.Printf("        - Formula: raw_score × recent × quality × popularity = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, combinedMultiplier, expectedScore)
					default:
						log.Printf("        - Effective multiplier: %.6f", multiplier)
						log.Printf("        - Formula: raw_score × multiplier = final_score")
						log.Printf("        - Verification: %.6f × %.6f = %.6f ✓", baseScore, multiplier, expectedScore)
					}

					log.Printf("      Expected Score: %.6f, Actual Score: %.6f", expectedScore, score)
					log.Printf("      Difference: %.6f (%.2f%%)", scoreDiff, scorePercDiff)

					matchStatus := "✓ MATCH"
					// Allow 0.1% tolerance for floating point errors
					if scorePercDiff > 0.1 {
						matchStatus = "✗ MISMATCH"
						errMsg := fmt.Sprintf("Score mismatch for '%s': expected %.6f, got %.6f (%.2f%% difference)",
							title, expectedScore, score, scorePercDiff)
						scoreValidationErrors = append(scoreValidationErrors, errMsg)
					}
					log.Printf("      %s", matchStatus)
				}
			}

			// Log summary table
			if testCase.ScoreCalculator != nil && len(docScores) > 1 {
				log.Println("\n  === Score Validation Summary ===")
				log.Println("  Document | Base Score | Expected | Actual | Match?")
				log.Println("  " + strings.Repeat("-", 80))

				for _, ds := range docScores {
					baseScore, hasBase := baseScores[ds.Title]
					if !hasBase {
						continue
					}
					expectedScore := testCase.ScoreCalculator(baseScore, ds.TestData, now)
					scoreDiff := math.Abs(expectedScore - ds.ActualScore)
					scorePercDiff := 0.0
					if ds.ActualScore != 0 {
						scorePercDiff = (scoreDiff / ds.ActualScore) * 100
					}
					matchStatus := "✓"
					if scorePercDiff > 0.1 {
						matchStatus = "✗"
					}
					log.Printf("  %-30s | %.6f | %.6f | %.6f | %s",
						ds.Title[:min(len(ds.Title), 30)], baseScore, expectedScore, ds.ActualScore, matchStatus)
				}
				log.Println("\n  Note: Base scores captured from BM25 search WITHOUT any reranker.")
				log.Println("  Expected scores calculated independently using base score and expression.")
				log.Println("  This is a non-circular validation approach!")
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

	// Report score validation errors
	if len(scoreValidationErrors) > 0 {
		log.Printf("\n⚠️  Score Validation Errors:")
		for _, errMsg := range scoreValidationErrors {
			log.Printf("  - %s", errMsg)
		}
		return fmt.Errorf("%d score validation error(s) found", len(scoreValidationErrors))
	}

	return nil
}

func runAllExpressionTests(ctx context.Context, client *milvusclient.Client) error {
	now := time.Now().UnixMilli()

	// First, capture base BM25 scores with pass-through expression
	// This gives us ground truth scores to validate against
	query := "artificial intelligence"
	err := captureBaseScores(ctx, client, query)
	if err != nil {
		return fmt.Errorf("failed to capture base scores: %v", err)
	}

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
			Name:            "Quality Boost",
			Query:           "artificial intelligence",
			ExprCode:        "score * (fields[\"quality_score\"] / 100.0)",
			ExpectedTop:     "Technical AI Paper",
			Description:     "Test quality-based boosting - higher quality should rank higher",
			InputFields:     []string{qualityFieldName},
			ScoreCalculator: qualityBoostCalculator,
		},
		{
			Name:            "Recency Boost",
			Query:           "artificial intelligence",
			ExprCode:        fmt.Sprintf("let age_days = (%d - fields[\"created_at\"]) / 86400000; score * exp(-0.01 * age_days)", now),
			ExpectedTop:     "Low Quality AI Content", // Higher base score + recent (3 days) wins
			Description:     "Test recency-based boosting - newer content should rank higher",
			InputFields:     []string{createdAtFieldName},
			ScoreCalculator: recencyBoostCalculator,
		},
		{
			Name:            "Popularity Boost",
			Query:           "artificial intelligence",
			ExprCode:        "score * (1.0 + log(fields[\"popularity\"] + 1) / 10.0)",
			ExpectedTop:     "Viral AI Post", // Highest popularity (5000) wins
			Description:     "Test popularity-based boosting - viral content should rank higher",
			InputFields:     []string{popularityFieldName},
			ScoreCalculator: popularityBoostCalculator,
		},
		{
			Name:            "Category Conditional Boost",
			Query:           "artificial intelligence",
			ExprCode:        "let boost = fields[\"category\"] == \"featured\" ? 2.0 : fields[\"category\"] == \"premium\" ? 1.5 : 1.0; score * boost",
			ExpectedTop:     "Recent AI Breakthrough", // Featured category with 2x boost
			Description:     "Test conditional category boosting - featured content gets highest boost",
			InputFields:     []string{categoryFieldName},
			ScoreCalculator: categoryConditionalCalculator,
		},
		{
			Name:            "Multi-factor E-commerce Style",
			Query:           "artificial intelligence",
			ExprCode:        "let quality = fields[\"quality_score\"] / 100.0; let pop_boost = 1.0 + log(fields[\"popularity\"] + 1) / 20.0; score * quality * pop_boost",
			ExpectedTop:     "Technical AI Paper",
			Description:     "Test multi-factor scoring combining quality and popularity",
			InputFields:     []string{qualityFieldName, popularityFieldName},
			ScoreCalculator: multiFactorCalculator,
		},
		{
			Name:            "Quality Filter with Penalty",
			Query:           "artificial intelligence",
			ExprCode:        "fields[\"quality_score\"] > 80.0 ? score : score * 0.1",
			ExpectedTop:     "Technical AI Paper", // Highest base score among quality>80
			Description:     "Test quality filtering - low quality content gets heavily penalized",
			InputFields:     []string{qualityFieldName},
			ScoreCalculator: qualityFilterCalculator,
		},
		{
			Name:            "Balanced Recency and Quality",
			Query:           "artificial intelligence",
			ExprCode:        fmt.Sprintf("let age_days = (%d - fields[\"created_at\"]) / 86400000; let recency = exp(-0.005 * age_days); let quality = fields[\"quality_score\"] / 100.0; score * (recency * 0.6 + quality * 0.4)", now),
			ExpectedTop:     "Recent AI Breakthrough", // Good balance of recent + high quality
			Description:     "Test balanced scoring between recency (60%) and quality (40%)",
			InputFields:     []string{createdAtFieldName, qualityFieldName},
			ScoreCalculator: balancedRecencyQualityCalculator,
		},
		{
			Name:            "Capped Boost",
			Query:           "artificial intelligence",
			ExprCode:        "min(score * (fields[\"popularity\"] / 100.0), score * 5.0)",
			ExpectedTop:     "Recent AI Breakthrough", // Higher original score wins when capped
			Description:     "Test capped boosting to prevent extreme score inflation",
			InputFields:     []string{popularityFieldName},
			ScoreCalculator: cappedBoostCalculator,
		},
		{
			Name:            "Complex Multi-Conditional",
			Query:           "artificial intelligence",
			ExprCode:        fmt.Sprintf("let age_days = (%d - fields[\"created_at\"]) / 86400000; let is_recent = age_days < 7; let is_high_quality = fields[\"quality_score\"] > 85; let is_popular = fields[\"popularity\"] > 500; score * (is_recent ? 1.5 : 1.0) * (is_high_quality ? 1.3 : 1.0) * (is_popular ? 1.2 : 1.0)", now),
			ExpectedTop:     "Recent AI Breakthrough", // Recent + high quality + popular
			Description:     "Test complex conditional logic with multiple boolean factors",
			InputFields:     []string{createdAtFieldName, qualityFieldName, popularityFieldName},
			ScoreCalculator: complexMultiConditionalCalculator,
		},
	}

	log.Println("\n🚀 Starting Expression Reranker Test Suite")
	log.Println(strings.Repeat("=", 60))

	passCount := 0
	totalTests := len(testCases)

	for i, testCase := range testCases {
		log.Printf("\nTest %d/%d", i+1, totalTests)
		err := runExpressionTest(ctx, client, testCase, now)
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
	textVector := entity.Text(query)
	log.Printf("Query: '%s'", query)

	exactPhraseBaseBM25Scores := make(map[string]float64)
	log.Println("  Capturing Exact Phrase BM25 scores...")
	exactPhraseQuery := "artificial intelligence"
	exactPhraseFilter := fmt.Sprintf(`text_match(%s, "%s")`, titleFieldName, exactPhraseQuery)
	exactPhraseVector := entity.Text(exactPhraseQuery)
	exactPhraseSearchResult, err := client.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		20,
		[]entity.Vector{exactPhraseVector},
	).WithANNSField(titleSparseFieldName).
		WithFilter(exactPhraseFilter).
		WithOutputFields(titleFieldName))

	if err == nil {
		for _, resultSet := range exactPhraseSearchResult {
			titleColumn := resultSet.GetColumn(titleFieldName)
			if titleColumn != nil {
				for i := 0; i < resultSet.ResultCount; i++ {
					title, _ := titleColumn.GetAsString(i)
					if title != "" {
						score := float64(resultSet.Scores[i])
						exactPhraseBaseBM25Scores[title] = score
					}
				}
			}
		}
		log.Printf("    ✓ Captured %d exact phrase BM25 scores", len(exactPhraseBaseBM25Scores))
	} else {
		log.Printf("    ⚠️  Warning: Failed to capture exact phrase scores: %v", err)
	}

	// // Test 1: Min Should Match with text_match filter
	log.Println("\n--- Test 1: Single BM25 Search with Min Should Match ---")
	minShouldMatch := "1"
	textMatchFilter := fmt.Sprintf(`text_match(%s, "%s", minimum_should_match=%s)`,
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
		return fmt.Errorf("single search with minimum_should_match failed: %v", err)
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
		WithFilter(fmt.Sprintf(`text_match(%s, "%s", minimum_should_match=%s)`,
			textFieldName, query, minShouldMatch)).
		WithSearchParam("metric_type", "BM25")

	hybridSearchResultWithoutReranker, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, titleSearchRequest, textSearchRequest).
			WithOutputFields("id", titleFieldName, textFieldName, qualityFieldName, popularityFieldName))
	if err != nil {
		return fmt.Errorf("hybrid search with Weighted reranker failed: %v", err)
	}
	withoutReranker := make(map[string]float64)
	for _, resultSet := range hybridSearchResultWithoutReranker {
		titleColumn := resultSet.GetColumn(titleFieldName)
		if titleColumn != nil {
			for i := 0; i < resultSet.ResultCount; i++ {
				title, _ := titleColumn.GetAsString(i)
				if title != "" {
					withoutReranker[title] = float64(resultSet.Scores[i])
				}
			}
		}
	}

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
	log.Println("  Formula: Final Score = (title_BM25_score × 0.6) + (text_BM25_score × 0.4)")
	log.Println()
	for _, resultSet := range hybridSearchResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			score := float64(resultSet.Scores[i])
			// Guard GetColumn usage to avoid nil deref
			var title, text string
			titleCol := resultSet.GetColumn(titleFieldName)
			if titleCol != nil {
				title, _ = titleCol.GetAsString(i)
			}

			textCol := resultSet.GetColumn(textFieldName)
			if textCol != nil {
				text, _ = textCol.GetAsString(i)
			}
			log.Printf("  [%d] ID: %v, Final Weighted Score: %.6f Base Score: %.6f", i+1, id, score, withoutReranker[title])
			log.Printf("      Title: %s", title)
			log.Printf("      Text: %s...", text[:min(len(text), 60)])
		}
	}

	// Test 3: Weighted + Expression-based Reranker (Two-tier reranking)
	log.Println("\n--- Test 3: Weighted + Expression Reranker (Two-tier reranking) ---")

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

	// First, capture the base scores for comparison
	log.Println("Capturing intermediate weighted scores (before expression reranker)...")
	baseResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, titleSearchRequest, textSearchRequest).
			WithOutputFields("id", titleFieldName, qualityFieldName))
	if err != nil {
		return fmt.Errorf("failed to capture intermediate scores: %v", err)
	}

	baseScores := make(map[string]float64)
	for _, resultSet := range baseResult {
		titleColumn := resultSet.GetColumn(titleFieldName)
		if titleColumn != nil {
			for i := 0; i < resultSet.ResultCount; i++ {
				title, _ := titleColumn.GetAsString(i)
				if title != "" {
					baseScores[title] = float64(resultSet.Scores[i])
				}
			}
		}
	}
	fmt.Println("baseScores", len(baseScores))
	for title := range baseScores {
		fmt.Printf("title: %s score %f\n", title, baseScores[title])
	}

	intermediateResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, titleSearchRequest, textSearchRequest).
			WithFunctionRerankers(weightedRerankerFunction).
			WithOutputFields("id", titleFieldName, textFieldName, qualityFieldName, popularityFieldName))
	if err != nil {
		return fmt.Errorf("failed to capture intermediate scores: %v", err)
	}

	intermediateScores := make(map[string]float64)
	for _, resultSet := range intermediateResult {
		titleColumn := resultSet.GetColumn(titleFieldName)
		if titleColumn != nil {
			for i := 0; i < resultSet.ResultCount; i++ {
				title, _ := titleColumn.GetAsString(i)
				if title != "" {
					intermediateScores[title] = float64(resultSet.Scores[i])
				}
			}
		}
	}
	fmt.Println("weighted ranker intermediateScores count", len(intermediateScores))
	for title := range intermediateScores {
		fmt.Printf("title: %s score %f\n", title, intermediateScores[title])
	}

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
	log.Println("  Reranker Chain: Weighted (0.7 title, 0.3 text) -> Expression (score × quality/100)")
	for _, resultSet := range dualRerankResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			title, _ := resultSet.GetColumn(titleFieldName).GetAsString(i)
			qualityDouble, _ := resultSet.GetColumn(qualityFieldName).GetAsDouble(i)
			quality := float32(qualityDouble)
			finalScore := float64(resultSet.Scores[i])

			qualityFactor := float64(quality) / 100.0

			// Show calculation breakdown
			log.Printf("  [%d] ID: %v, Final Score: %.6f", i+1, id, finalScore)
			log.Printf("      Title: %s", title)
			log.Printf("      Quality: %.1f", quality)

			intermediateBM25Score, _ := intermediateScores[title]
			baseBM25Score, _ := baseScores[title]

			log.Printf("      Base Weighted Score before any ranking: %.6f", baseBM25Score)
			expectedFinal := intermediateBM25Score * qualityFactor
			log.Printf("      Reranking Steps:")
			log.Printf("        1. Weighted Fusion: (title_score × 0.7) + (text_score × 0.3) = %.6f", intermediateBM25Score)
			log.Printf("        2. Expression Rerank: Apply quality multiplier")
			log.Printf("           - Quality factor: %.6f (%.1f / 100)", qualityFactor, quality)
			log.Printf("           - Verification: %.6f × %.6f = %.6f got %.6f", intermediateBM25Score, qualityFactor, expectedFinal, finalScore)
			if finalScore-expectedFinal > 0.0001 {
				log.Printf("      Verification failed: %.6f != %.6f ❌", finalScore, expectedFinal)
			} else {
				log.Printf("      Verification passed: %.6f == %.6f ✅", finalScore, expectedFinal)
			}
		}
	}

	// Test 4: Complex scenario - Weighted with three BM25 searches + expr reranker
	log.Println("\n--- Test 4: Weighted with Three BM25 Searches + Complex Expression Reranker  ---")

	now := time.Now().UnixMilli()

	// Create three searches with different characteristics
	// Search 1: Title search (highest weight)
	priorityTitleSearch := milvusclient.NewAnnRequest(titleSparseFieldName, 30, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s")`, titleFieldName, query)).
		WithSearchParam("metric_type", "BM25")

	// Example of using a function reranker to boost the title search with a phrase match filter
	// priorityTitleSearch.WithFunctionReranker(entity.NewFunction().
	// 	WithName("priority_title_reranker").
	// 	WithType(entity.FunctionTypeRerank).
	// 	WithParam("reranker", "boost").
	// 	WithParam("filter", fmt.Sprintf(`phrase_match(%s, "%s")`, titleFieldName, query)).
	// 	WithParam("weight", "2.0"))

	// Search 2: Text search with minimum_should_match (medium weight)
	standardTextSearch := milvusclient.NewAnnRequest(textSparseFieldName, 15, textVector).
		WithFilter(fmt.Sprintf(`text_match(%s, "%s", minimum_should_match=2)`,
			textFieldName, query)).
		WithSearchParam("metric_type", "BM25")

	// Search 3: Another title search with exact phrase (lower weight)
	exactTitleSearch := milvusclient.NewAnnRequest(titleSparseFieldName, 10, textVector).
		WithFilter(fmt.Sprintf(`phrase_match(%s, "%s")`, titleFieldName, "artificial intelligence")).
		WithSearchParam("metric_type", "BM25")

	// Create weighted reranker for 3 searches (0.5, 0.3, 0.2)
	tripleWeightedFunction := entity.NewFunction().
		WithName("triple_weighted_reranker").
		WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "weighted").
		WithParam("weights", "[0.5, 0.3, 0.2]")

	// Boost Ranker for featured category items
	// This will boost documents with category="featured" by 1.5x
	boostReranker := entity.NewFunction().
		WithName("category_boost_reranker").
		WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "boost").
		WithParam("filter", "category == \"featured\"").
		WithParam("weight", "1.5")

	// Complex expression reranker combining quality, popularity, recency, and category
	complexExprCode := fmt.Sprintf(
		"let age_days = (%d - fields[\"created_at\"]) / 86400000; "+
			"let recency = exp(-0.01 * age_days); "+
			"let quality = fields[\"quality_score\"] / 100.0; "+
			"let pop_boost = 1.0 + log(fields[\"popularity\"] + 1) / 20.0; "+
			"score * quality * pop_boost * recency ",
		now)

	complexExprReranker := entity.NewFunction().
		WithName("multi_factor_reranker").
		WithType(entity.FunctionTypeRerank).
		WithInputFields(createdAtFieldName, qualityFieldName, popularityFieldName, categoryFieldName).
		WithParam("reranker", "expr").
		WithParam("expr_code", complexExprCode)

	// First, capture base weighted scores (before step 1)
	log.Println("Capturing base weighted scores (after weighted fusion)...")
	baseWeightedResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, priorityTitleSearch, standardTextSearch, exactTitleSearch).
			WithOutputFields("id", titleFieldName, titleFieldName))
	if err != nil {
		return fmt.Errorf("failed to capture base weighted scores: %v", err)
	}

	baseWeightedScores := make(map[string]float64)
	for _, resultSet := range baseWeightedResult {
		titleColumn := resultSet.GetColumn(titleFieldName)
		if titleColumn != nil {
			for i := 0; i < resultSet.ResultCount; i++ {
				title, _ := titleColumn.GetAsString(i)
				if title != "" {
					baseWeightedScores[title] = float64(resultSet.Scores[i])
				}
			}
		}
	}

	// Second, capture intermediate  weighted scores (after step 1)
	log.Println("Capturing intermediate weighted scores (after weighted fusion)...")
	intermediateWeightedResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 10, priorityTitleSearch, standardTextSearch, exactTitleSearch).
			WithFunctionRerankers(tripleWeightedFunction).
			WithOutputFields("id", titleFieldName, titleFieldName))
	if err != nil {
		return fmt.Errorf("failed to capture intermediate weighted scores: %v", err)
	}

	complexIntermediateScores := make(map[string]float64)
	for _, resultSet := range intermediateWeightedResult {
		titleColumn := resultSet.GetColumn(titleFieldName)
		if titleColumn != nil {
			for i := 0; i < resultSet.ResultCount; i++ {
				title, _ := titleColumn.GetAsString(i)
				if title != "" {
					complexIntermediateScores[title] = float64(resultSet.Scores[i])
				}
			}
		}
	}

	log.Println("Executing Complex Hybrid Search with 3 searches + Boost Ranker...")
	complexResult, err := client.HybridSearch(ctx,
		milvusclient.NewHybridSearchOption(collectionName, 8, priorityTitleSearch, standardTextSearch, exactTitleSearch).
			WithFunctionRerankers(tripleWeightedFunction).
			WithFunctionRerankers(complexExprReranker).
			WithFunctionRerankers(boostReranker).
			WithOutputFields("id", titleFieldName, createdAtFieldName, qualityFieldName, popularityFieldName, categoryFieldName))
	if err != nil {
		return fmt.Errorf("complex hybrid search failed: %v", err)
	}

	log.Println("Complex Multi-factor Reranked Results (Weighted + Expression + Boost):")
	log.Println("  Reranker Chain:")
	log.Println("    1. Weighted Fusion: (title × 0.5) + (text × 0.3) + (exact × 0.2)")
	log.Println("    2. Expression: score × quality × pop_boost × recency × boost")
	log.Println()

	for _, resultSet := range complexResult {
		for i := 0; i < resultSet.ResultCount; i++ {
			id, _ := resultSet.IDs.Get(i)
			title, _ := resultSet.GetColumn(titleFieldName).GetAsString(i)
			createdAt, _ := resultSet.GetColumn(createdAtFieldName).GetAsInt64(i)
			qualityDouble, _ := resultSet.GetColumn(qualityFieldName).GetAsDouble(i)
			quality := float32(qualityDouble)
			popularity, _ := resultSet.GetColumn(popularityFieldName).GetAsInt64(i)
			category, _ := resultSet.GetColumn(categoryFieldName).GetAsString(i)
			finalScore := float64(resultSet.Scores[i])

			ageDays := (now - createdAt) / 86400000

			// Calculate the factors that were applied
			ageDaysFloat := float64(now-createdAt) / 86400000.0
			recencyFactor := math.Exp(-0.01 * ageDaysFloat)
			qualityFactor := float64(quality) / 100.0
			popBoost := 1.0 + math.Log(float64(popularity+1))/20.0
			isFeatured := category == "featured"
			boostFactor := 1.0
			if isFeatured {
				boostFactor = 1.5
			}

			combinedMultiplier := qualityFactor * popBoost * recencyFactor * boostFactor
			expectedFinal := complexIntermediateScores[title] * combinedMultiplier
			log.Printf("  [%d] ID: %v, Final Score: %.6f", i+1, id, finalScore)
			log.Printf("      Title: %s", title)
			log.Printf("      Calculation Steps:")
			log.Printf("      Base Weighted Score before any ranking: %.6f", baseWeightedScores[title])
			log.Printf("        1. Weighted Fusion: (title × 0.5) + (text × 0.3) + (exact × 0.2) = %.6f", complexIntermediateScores[title])
			log.Printf("        2. Expression: score × quality × pop_boost × recency × boost = %.6f", finalScore)
			log.Printf("        3. Combined Multiplier: quality × pop_boost × recency × boost = %.6f", combinedMultiplier)
			log.Printf("        4. Verification: %.6f × %.6f × %.6f × %.6f × %.6f = %.6f", complexIntermediateScores[title], qualityFactor, popBoost, recencyFactor, boostFactor, finalScore)
			if math.Abs(finalScore-expectedFinal) > 0.0001 {
				log.Printf("      Verification failed: %.6f != %.6f ❌", finalScore, expectedFinal)
			} else {
				log.Printf("      Verification passed: %.6f == %.6f ✅", finalScore, expectedFinal)
			}
			log.Printf("      Metadata: Quality: %.1f, Popularity: %d, Age: %d days Boost: %.1f", quality, popularity, ageDays, boostFactor)
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

	/*log.Println("\n⚖️  Running Weighted Reranker + Lexical Search tests...")
	err = runWeightedWithLexicalSearchTest(ctx, milvusClient)
	if err != nil {
		log.Fatalf("failed to run Weighted reranker tests: %v", err)
	}
	*/
	log.Println("\n🧹 Cleaning up...")
	err = milvusClient.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Printf("failed to drop collection: %v", err)
	} else {
		log.Printf("✅ Collection '%s' dropped successfully.", collectionName)
	}

	log.Println("\n🎉 All test suites completed!")
}
