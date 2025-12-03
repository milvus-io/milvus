package interfaces

import "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

//go:generate mockery --name=TokenStream --with-expecter
type TokenStream interface {
	Advance() bool
	Token() string
	DetailedToken() *milvuspb.AnalyzerToken
	Destroy()
}

type Analyzer interface {
	NewTokenStream(text string) TokenStream
	Clone() (Analyzer, error)
	Destroy()
}

//go:generate mockery --name=MinHash --with-expecter
type MinHash interface {
	// Compute computes MinHash signature for the given text
	// Returns the signature as []uint32
	Compute(text string) []uint32

	// ComputeBatch computes MinHash signatures for multiple texts
	// Returns signatures as [][]uint32
	ComputeBatch(texts []string) [][]uint32

	// Destroy releases resources
	Destroy()
}
