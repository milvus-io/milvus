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
