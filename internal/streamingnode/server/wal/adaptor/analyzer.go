package adaptor

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAdaptorImpl) RunAnalyzer(ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("WAL is closing")
	}
	defer w.lifetime.Done()
	if w.rwWALImpls.Channel().AccessMode != types.AccessModeRW {
		return nil, status.NewUnrecoverableError("analyzer requires the primary WAL")
	}
	field := req.GetFieldAnalyzer()
	key := shard.WALFunctionRunnerKey(field.GetVchannel())
	return analyzer.RunOperation(ctx, req, func(inputs [][]byte, withDetail, withHash bool, names []string) ([]*milvuspb.AnalyzerResult, error) {
		texts := make([]string, len(inputs))
		for i, text := range inputs {
			texts[i] = string(text)
		}
		var tokens [][]*milvuspb.AnalyzerToken
		ok, err := function.GetManager().RunWithAnalyzer(ctx, field.GetCollectionId(), key, field.GetFieldId(), func(a function.Analyzer) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			if len(a.GetInputFields()) == 1 {
				tokens, err = a.BatchAnalyze(withDetail, withHash, texts)
			} else {
				normalized, nameErr := function.NormalizeAnalyzerNames(names, len(texts))
				if nameErr != nil {
					return nameErr
				}
				tokens, err = a.BatchAnalyze(withDetail, withHash, texts, normalized)
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("analyzer is not enabled for field %d", field.GetFieldId())
		}
		results := make([]*milvuspb.AnalyzerResult, len(tokens))
		for i, token := range tokens {
			results[i] = &milvuspb.AnalyzerResult{Tokens: token}
		}
		return results, nil
	}), nil
}
