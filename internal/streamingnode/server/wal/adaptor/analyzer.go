package adaptor

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
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
			CollectionId: field.GetCollectionId(), Vchannel: field.GetVchannel(),
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
	texts := make([]string, len(req.GetPlaceholder()))
	for i, text := range req.GetPlaceholder() {
		texts[i] = string(text)
	}
	var tokens [][]*milvuspb.AnalyzerToken
	ok, err := function.GetManager().RunWithAnalyzer(ctx, field.GetCollectionId(), key, field.GetFieldId(), func(a function.Analyzer) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		if len(a.GetInputFields()) == 1 {
			tokens, err = a.BatchAnalyze(req.GetWithDetail(), req.GetWithHash(), texts)
		} else {
			names, nameErr := function.NormalizeAnalyzerNames(field.GetAnalyzerNames(), len(texts))
			if nameErr != nil {
				return nameErr
			}
			tokens, err = a.BatchAnalyze(req.GetWithDetail(), req.GetWithHash(), texts, names)
		}
		return err
	})
	if err != nil {
		return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(err)}, nil
	}
	if !ok {
		return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("analyzer is not enabled for field %d", field.GetFieldId()))}, nil
	}
	results := make([]*milvuspb.AnalyzerResult, len(tokens))
	for i, token := range tokens {
		results[i] = &milvuspb.AnalyzerResult{Tokens: token}
	}
	return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success(), Results: results}, nil
}
