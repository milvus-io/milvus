package main

import (
	"context"
	"fmt"
	"log"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	milvusAddr      = "localhost:19530"
	collectionName  = "text_search_collection"
	textFieldName   = "document_text"
	sparseFieldName = "sparse_vector"
)

func main() {
	ctx := context.Background()

	log.Println("Connecting to Milvus...")
	milvusClient, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatalf("failed to connect to Milvus: %v", err)
	}
	defer milvusClient.Close(ctx)
	log.Println("Connected to Milvus successfully.")

	err = milvusClient.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Printf("Collection '%s' doesn't exist or failed to drop: %v", collectionName, err)
	} else {
		log.Printf("Dropped existing collection '%s'", collectionName)
	}

	schema := entity.NewSchema().
		WithName(collectionName).
		WithField(entity.NewField().
			WithName("id").
			WithDataType(entity.FieldTypeInt64).
			WithIsPrimaryKey(true).
			WithIsAutoID(true)).
		WithField(entity.NewField().
			WithName(textFieldName).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1000).
			WithEnableAnalyzer(true).
			WithEnableMatch(true)).
		WithField(entity.NewField().
			WithName(sparseFieldName).
			WithDataType(entity.FieldTypeSparseVector))

	bm25Function := entity.NewFunction().
		WithName("text_bm25_func").
		WithType(entity.FunctionTypeBM25).
		WithInputFields(textFieldName).
		WithOutputFields(sparseFieldName)
	schema.WithFunction(bm25Function)

	log.Println("Creating collection with text index and BM25 index...")

	textIndex := index.NewInvertedIndex()
	textIndexOption := milvusclient.NewCreateIndexOption(collectionName, textFieldName, textIndex)

	sparseIndex := index.NewSparseInvertedIndex(entity.BM25, 0.2) // BM25 metric with 0.2 drop ratio
	sparseIndexOption := milvusclient.NewCreateIndexOption(collectionName, sparseFieldName, sparseIndex)

	err = milvusClient.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).
		WithIndexOptions(textIndexOption, sparseIndexOption))
	if err != nil {
		log.Fatalf("failed to create collection: %v", err)
	}
	log.Printf("Collection '%s' created successfully.", collectionName)

	data := []struct {
		Text string
	}{
		{"Artificial intelligence was founded in 1956."},
		{"Alan Turing was the first person to propose AI."},
		{"Born in Maida Vale, London, Turing was a brilliant mathematician. XI is the future."},
	}

	textColumn := make([]string, len(data))
	for i, d := range data {
		textColumn[i] = d.Text
	}

	_, err = milvusClient.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithVarcharColumn(textFieldName, textColumn))
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}
	log.Println("Data inserted successfully.")

	loadTask, err := milvusClient.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		log.Fatalf("failed to load collection: %v", err)
	}

	// Wait for the collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		log.Fatalf("failed to wait for collection loading: %v", err)
	}
	log.Println("Collection loaded successfully.")

	query := "Artificial Alan Turing London first"
	minShouldMatch := 3

	textVector := entity.Text(query)

	log.Printf("Query: %s", query)
	log.Printf("Minimum Should Match: %d terms", minShouldMatch)

	textMatchFilter := fmt.Sprintf(`text_match(%s, "%s")`, textFieldName, query)
	log.Printf("Using text filter: %s", textMatchFilter)

	searchResult, err := milvusClient.Search(ctx, milvusclient.NewSearchOption(
		collectionName,
		10,
		[]entity.Vector{textVector}, // text vector for BM25 search
	).WithANNSField(sparseFieldName). // Search on sparse vector field for BM25 scoring
						WithFilter(textMatchFilter).
						WithMinShouldMatch(minShouldMatch).
						WithOutputFields("id", textFieldName))
	if err != nil {
		log.Fatalf("failed to search: %v", err)
	}

	log.Println("\nSearch Results (Full-text search):")
	for _, resultSet := range searchResult {
		log.Printf("Found %d results:\n", resultSet.ResultCount)

		if resultSet.ResultCount == 0 {
			log.Println("No matching documents found")
			continue
		}

		for i := 0; i < resultSet.ResultCount; i++ {
			log.Printf("\nResult %d:\n", i+1)

			if resultSet.IDs != nil {
				id, err := resultSet.IDs.Get(i)
				if err == nil {
					log.Printf("  ID: %v\n", id)
				}
			}

			log.Printf("  Score: %.4f\n", resultSet.Scores[i])

			textColumn := resultSet.GetColumn(textFieldName)
			if textColumn != nil {
				text, err := textColumn.GetAsString(i)
				if err == nil {
					log.Printf("  Text: %s\n", text)
				}
			}
		}
	}

	err = milvusClient.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Printf("failed to drop collection: %v", err)
	} else {
		log.Printf("Collection '%s' dropped successfully.", collectionName)
	}
}
