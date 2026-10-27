package analyzer

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RunOperation shares native tokenization and its resource lifetime between
// token output and phrase matching. The callback owns analyzer selection and
// returns detailed positions when requested.
func RunOperation(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest,
	run func(texts [][]byte, withDetail, withHash bool, names []string) ([]*milvuspb.AnalyzerResult, error),
) *streamingpb.StreamingNodeRunAnalyzerResponse {
	response := &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success()}
	if err := ctx.Err(); err != nil {
		response.Status = merr.Status(err)
		return response
	}
	names := req.GetFieldAnalyzer().GetAnalyzerNames()
	switch operation := req.GetOperation().(type) {
	case *streamingpb.StreamingNodeRunAnalyzerRequest_Tokenize:
		results, err := run(req.GetPlaceholder(), operation.Tokenize.GetWithDetail(), operation.Tokenize.GetWithHash(), names)
		if err != nil {
			response.Status = merr.Status(err)
			return response
		}
		response.Result = &streamingpb.StreamingNodeRunAnalyzerResponse_Tokenize{Tokenize: &streamingpb.TokenizeResult{Results: results}}
	case *streamingpb.StreamingNodeRunAnalyzerRequest_PhraseMatch:
		result := &streamingpb.PhraseMatchResult{}
		response.Result = &streamingpb.StreamingNodeRunAnalyzerResponse_PhraseMatch{PhraseMatch: result}
		// Retain the public API's empty-query/empty-batch result.
		if operation.PhraseMatch.GetQueryText() == "" || len(req.GetPlaceholder()) == 0 {
			return response
		}
		texts := make([][]byte, 0, 2*len(req.GetPlaceholder()))
		query := []byte(operation.PhraseMatch.GetQueryText())
		for _, data := range req.GetPlaceholder() {
			texts = append(texts, query, data)
		}
		// Each query/data pair must use the same named analyzer.
		if len(names) > 1 {
			pairedNames := make([]string, 0, 2*len(names))
			for _, name := range names {
				pairedNames = append(pairedNames, name, name)
			}
			names = pairedNames
		}
		results, err := run(texts, true, false, names)
		if err != nil {
			response.Status = merr.Status(err)
			response.Result = nil
			return response
		}
		if len(results) != len(texts) {
			response.Status = merr.Status(merr.WrapErrServiceInternalMsg("phrase analyzer returned %d results for %d texts", len(results), len(texts)))
			response.Result = nil
			return response
		}
		result.IsMatch = make([]bool, len(req.GetPlaceholder()))
		result.Slops = make([]int64, len(req.GetPlaceholder()))
		for i := 0; i < len(results); i += 2 {
			if err := ctx.Err(); err != nil {
				response.Status = merr.Status(err)
				response.Result = nil
				return response
			}
			slop := phraseMatchSlop(results[i].GetTokens(), results[i+1].GetTokens())
			result.IsMatch[i/2] = slop != unmatchedPhraseSlop
			result.Slops[i/2] = slop
		}
	default:
		response.Status = merr.Status(merr.WrapErrParameterInvalidMsg("analyzer operation is required"))
	}
	return response
}
