package analyzer

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Run creates a request-local analyzer and releases all native resources.
func Run(ctx context.Context, params string, texts [][]byte, withDetail, withHash bool) ([]*milvuspb.AnalyzerResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tokenizer, err := NewAnalyzer(params, "")
	if err != nil {
		return nil, err
	}

	defer tokenizer.Destroy()

	results := make([]*milvuspb.AnalyzerResult, len(texts))
	for i, text := range texts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stream, err := tokenizer.NewTokenStream(string(text))
		if err != nil {
			return nil, err
		}

		results[i] = &milvuspb.AnalyzerResult{
			Tokens: make([]*milvuspb.AnalyzerToken, 0),
		}

		for stream.Advance() {
			var token *milvuspb.AnalyzerToken
			if withDetail {
				token = stream.DetailedToken()
			} else {
				token = &milvuspb.AnalyzerToken{Token: stream.Token()}
			}

			if withHash {
				token.Hash = typeutil.HashString2LessUint32(token.GetToken())
			}
			results[i].Tokens = append(results[i].Tokens, token)
		}
		stream.Destroy()
	}
	return results, nil
}
