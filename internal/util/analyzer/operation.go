package analyzer

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/function/highlight"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// BatchRun tokenizes using the analyzer pinned by the enclosing operation.
type BatchRun func(texts [][]byte, withDetail, withHash bool, names []string) ([]*milvuspb.AnalyzerResult, error)

// RunOperation executes within one analyzer/resource binding. withAnalyzer must
// hold that binding until use returns and provide normalized per-input names,
// or nil when analyzer selection does not depend on names.
func RunOperation(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest,
	withAnalyzer func(use func(BatchRun, []string) error) error,
) *streamingpb.StreamingNodeRunAnalyzerResponse {
	response := &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success()}
	if err := ctx.Err(); err != nil {
		response.Status = merr.Status(err)
		return response
	}
	var err error
	switch operation := req.GetOperation().(type) {
	case *streamingpb.StreamingNodeRunAnalyzerRequest_Tokenize:
		err = withAnalyzer(func(run BatchRun, names []string) error {
			results, err := run(req.GetPlaceholder(), operation.Tokenize.GetWithDetail(), operation.Tokenize.GetWithHash(), names)
			if err == nil {
				response.Result = &streamingpb.StreamingNodeRunAnalyzerResponse_Tokenize{Tokenize: &streamingpb.TokenizeResult{Results: results}}
			}
			return err
		})
	case *streamingpb.StreamingNodeRunAnalyzerRequest_Highlight:
		err = withAnalyzer(func(run BatchRun, names []string) error {
			analyzed, err := run(req.GetPlaceholder(), true, false, names)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			options := operation.Highlight
			if options.GetQueryTextNum() < 0 || options.GetQueryTextNum() > int64(len(req.GetPlaceholder())) {
				return merr.WrapErrServiceInternalMsg("highlight query text count does not match texts")
			}
			task := &querypb.HighlightTask{
				SearchTextNum: options.GetSearchTextNum(), CorpusTextNum: options.GetCorpusTextNum(),
				Queries: make([]*querypb.HighlightQuery, int(options.GetQueryTextNum())), Options: &querypb.HighlightOptions{
					FragmentSize: options.GetFragmentSize(), FragmentOffset: options.GetFragmentOffset(), NumOfFragments: options.GetNumOfFragments(),
				},
				Texts: make([]string, len(req.GetPlaceholder())),
			}
			for i, text := range req.GetPlaceholder() {
				task.Texts[i] = string(text)
			}
			tokens := make([][]*milvuspb.AnalyzerToken, len(analyzed))
			for i, result := range analyzed {
				tokens[i] = result.GetTokens()
			}
			fragments, err := highlight.BuildFragments(task, options.GetTopks(), tokens)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			result := &streamingpb.HighlightResult{Results: make([]*streamingpb.HighlightRowResult, len(fragments))}
			for i, row := range fragments {
				converted := &streamingpb.HighlightRowResult{}
				for _, fragment := range row.GetFragments() {
					converted.Fragments = append(converted.Fragments, &streamingpb.HighlightFragment{StartOffset: fragment.GetStartOffset(), EndOffset: fragment.GetEndOffset(), Offsets: fragment.GetOffsets()})
				}
				result.Results[i] = converted
			}
			response.Result = &streamingpb.StreamingNodeRunAnalyzerResponse_Highlight{Highlight: result}
			return nil
		})
	case *streamingpb.StreamingNodeRunAnalyzerRequest_PhraseMatch:
		result := &streamingpb.PhraseMatchResult{}
		response.Result = &streamingpb.StreamingNodeRunAnalyzerResponse_PhraseMatch{PhraseMatch: result}
		// Preserve the public API's empty result without preparing an analyzer.
		if operation.PhraseMatch.GetQueryText() == "" || len(req.GetPlaceholder()) == 0 {
			return response
		}
		err = withAnalyzer(func(run BatchRun, names []string) error {
			return runPhraseMatch(ctx, req, run, names, result)
		})
	default:
		err = merr.WrapErrParameterInvalidMsg("analyzer operation is required")
	}
	if err != nil {
		response.Status = merr.Status(err)
		response.Result = nil
	}
	return response
}

const (
	phraseBatchRows  = 32
	phraseBatchBytes = 64 * 1024
)

func runPhraseMatch(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest,
	run BatchRun, names []string, result *streamingpb.PhraseMatchResult,
) error {
	// Group row indexes rather than caching query tokens for every analyzer: even
	// a request with a different analyzer per row retains only one query at a time.
	groups := make(map[string][]int)
	order := make([]string, 0)
	for i := range req.GetPlaceholder() {
		name := ""
		if len(names) != 0 {
			name = names[i]
		}
		if _, ok := groups[name]; !ok {
			order = append(order, name)
		}
		groups[name] = append(groups[name], i)
	}
	result.IsMatch = make([]bool, len(req.GetPlaceholder()))
	result.Slops = make([]int64, len(req.GetPlaceholder()))
	for _, name := range order {
		if err := runPhraseGroup(ctx, req, run, name, len(names) != 0, groups[name], result); err != nil {
			return err
		}
	}
	return nil
}

func runPhraseGroup(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest,
	run BatchRun, name string, named bool, rows []int, result *streamingpb.PhraseMatchResult,
) error {
	analyze := func(texts [][]byte) ([]*milvuspb.AnalyzerResult, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var names []string
		if named {
			names = make([]string, len(texts))
			for i := range names {
				names[i] = name
			}
		}
		tokens, err := run(texts, true, false, names)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(tokens) != len(texts) {
			return nil, merr.WrapErrServiceInternalMsg("phrase analyzer returned %d results for %d texts", len(tokens), len(texts))
		}
		return tokens, nil
	}
	query, err := analyze([][]byte{[]byte(req.GetPhraseMatch().GetQueryText())})
	if err != nil {
		return err
	}
	for start := 0; start < len(rows); {
		texts := make([][]byte, 0, min(phraseBatchRows, len(rows)-start))
		bytes := 0
		for end := start; end < len(rows) && len(texts) < phraseBatchRows; end++ {
			text := req.GetPlaceholder()[rows[end]]
			// Admit an oversized individual text alone; batching must not introduce a
			// new input limit. The next batch cannot accumulate more large texts.
			if len(texts) > 0 && len(text) > phraseBatchBytes-bytes {
				break
			}
			texts = append(texts, text)
			bytes += len(text)
		}
		tokens, err := analyze(texts)
		if err != nil {
			return err
		}
		for i, data := range tokens {
			if err := ctx.Err(); err != nil {
				return err
			}
			slop := phraseMatchSlop(query[0].GetTokens(), data.GetTokens())
			row := rows[start+i]
			result.IsMatch[row] = slop != unmatchedPhraseSlop
			result.Slops[row] = slop
		}
		start += len(texts)
	}
	return nil
}
