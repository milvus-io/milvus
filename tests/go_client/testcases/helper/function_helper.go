package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
)

// TNewBM25Function creates a new BM25 function with the given input and output fields
func TNewBM25Function(inputField, outputField string) *entity.Function {
	return entity.NewFunction().
		WithName(inputField + "_bm25_emb").
		WithInputFields(inputField).
		WithOutputFields(outputField).
		WithType(entity.FunctionTypeBM25)
}

// TNewTextEmbeddingFunction creates a text embedding function for different providers
func TNewTextEmbeddingFunction(inputField, outputField string, params map[string]any) *entity.Function {
	function := entity.NewFunction().
		WithName(inputField + "_text_emb").
		WithInputFields(inputField).
		WithOutputFields(outputField).
		WithType(entity.FunctionTypeTextEmbedding)

	// Add all parameters including provider
	for key, value := range params {
		function.WithParam(key, value)
	}

	return function
}
