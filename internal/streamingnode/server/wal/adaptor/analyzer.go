package adaptor

import (
	"context"
	"fmt"
	"sync/atomic"

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

var readAnalyzerSequence atomic.Uint64

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
	if field.ReadSchemaVersion != nil {
		snapshot, err := w.GetCollectionSchema(ctx, &streamingpb.StreamingNodeGetCollectionSchemaRequest{
			Vchannel: field.GetVchannel(),
			Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: field.GetReadSchemaVersion()},
		})
		if err != nil {
			return nil, err
		}
		key = fmt.Sprintf("read-analyzer/%s/%d", field.GetVchannel(), readAnalyzerSequence.Add(1))
		if err := function.GetManager().Alloc(field.GetCollectionId(), key, snapshot.GetSchema()); err != nil {
			return nil, err
		}
		defer function.GetManager().Release(field.GetCollectionId(), key)
	}
	return analyzer.RunOperation(ctx, req, func(use func(analyzer.BatchRun, []string) error) error {
		ok, err := function.GetManager().RunWithAnalyzer(ctx, field.GetCollectionId(), key, field.GetFieldId(), func(a function.Analyzer) error {
			multiAnalyzer := len(a.GetInputFields()) != 1
			var names []string
			if multiAnalyzer {
				var err error
				names, err = function.NormalizeAnalyzerNames(field.GetAnalyzerNames(), len(req.GetPlaceholder()))
				if err != nil {
					return err
				}
			}
			return use(func(inputs [][]byte, withDetail, withHash bool, names []string) ([]*milvuspb.AnalyzerResult, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				texts := make([]string, len(inputs))
				for i, text := range inputs {
					texts[i] = string(text)
				}
				var tokens [][]*milvuspb.AnalyzerToken
				var err error
				if !multiAnalyzer {
					tokens, err = a.BatchAnalyze(withDetail, withHash, texts)
				} else {
					tokens, err = a.BatchAnalyze(withDetail, withHash, texts, names)
				}
				if err != nil {
					return nil, err
				}
				results := make([]*milvuspb.AnalyzerResult, len(tokens))
				for i, token := range tokens {
					results[i] = &milvuspb.AnalyzerResult{Tokens: token}
				}
				return results, nil
			}, names)
		})
		if err != nil {
			return err
		}
		if !ok {
			return merr.WrapErrParameterInvalidMsg("analyzer is not enabled for field %d", field.GetFieldId())
		}
		return nil
	}), nil
}
