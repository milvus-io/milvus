package tokenizerapi

import "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

//go:generate mockery --name=TokenStream --with-expecter
type TokenStream interface {
	Advance() bool
	Token() string
	DetailedToken() *milvuspb.AnalyzerToken
	Destroy()
}
