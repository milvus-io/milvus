package adaptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type analyzerWALTest struct{ walimpls.WALImpls }

func (*analyzerWALTest) Channel() types.PChannelInfo { panic("unpatched Channel") }

func TestWALFieldAnalyzerCurrentSchema(t *testing.T) {
	mockey.PatchConvey("field analyzer uses the current WAL schema without a requested version", t, func() {
		mockey.Mock((*analyzerWALTest).Channel).Return(types.PChannelInfo{Name: "p", Term: 1, AccessMode: types.AccessModeRW}).Build()
		fm := function.GetManager()
		const collectionID = 734991
		key := shard.WALFunctionRunnerKey("p_1v0")
		schema := &schemapb.CollectionSchema{Version: 3, Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}}}}
		require.NoError(t, fm.Alloc(collectionID, key, schema))
		defer fm.Release(collectionID, key)
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, rwWALImpls: &analyzerWALTest{}}
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Placeholder: [][]byte{[]byte("hello-world")}, Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{CollectionId: collectionID, Vchannel: "p_1v0", FieldId: 101}}, Operation: &streamingpb.StreamingNodeRunAnalyzerRequest_Tokenize{Tokenize: &streamingpb.TokenizeOptions{}}}
		resp, err := w.RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetTokenize().GetResults()[0].Tokens, 2)
		phrase := proto.Clone(req).(*streamingpb.StreamingNodeRunAnalyzerRequest)
		phrase.Operation = &streamingpb.StreamingNodeRunAnalyzerRequest_PhraseMatch{PhraseMatch: &streamingpb.PhraseMatchOptions{QueryText: "hello world"}}
		phraseResp, err := w.RunAnalyzer(t.Context(), phrase)
		require.NoError(t, err)
		require.NoError(t, merr.Error(phraseResp.GetStatus()))
		require.Equal(t, []int64{0}, phraseResp.GetPhraseMatch().GetSlops())
		updated := proto.Clone(schema).(*schemapb.CollectionSchema)
		updated.Version++
		updated.Fields[0].TypeParams = append(updated.Fields[0].TypeParams, &commonpb.KeyValuePair{Key: "analyzer_params", Value: `{"tokenizer":"whitespace"}`})
		require.NoError(t, fm.Update(collectionID, key, updated))
		resp, err = w.RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Len(t, resp.GetTokenize().GetResults()[0].Tokens, 1)
		require.Equal(t, "hello-world", resp.GetTokenize().GetResults()[0].Tokens[0].Token)
		phraseResp, err = w.RunAnalyzer(t.Context(), phrase)
		require.NoError(t, err)
		require.NoError(t, merr.Error(phraseResp.GetStatus()))
		require.Equal(t, []int64{-1}, phraseResp.GetPhraseMatch().GetSlops())
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = w.RunAnalyzer(context.Background(), req)
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
	})
}

type fieldAnalyzerManager struct{ function.FunctionRunnerManager }

func (*fieldAnalyzerManager) RunWithAnalyzer(context.Context, int64, string, int64, func(function.Analyzer) error) (bool, error) {
	panic("unpatched")
}

type fieldAnalyzerRunner struct{ function.Analyzer }

func (*fieldAnalyzerRunner) GetInputFields() []*schemapb.FieldSchema { panic("unpatched") }
func (*fieldAnalyzerRunner) BatchAnalyze(bool, bool, ...any) ([][]*milvuspb.AnalyzerToken, error) {
	panic("unpatched")
}

func TestWALFieldAnalyzerLifetimeAndNames(t *testing.T) {
	mockey.PatchConvey("field requests pin WAL and normalize multi-analyzer names", t, func() {
		mockey.Mock((*analyzerWALTest).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
		mockey.Mock(function.GetManager).Return(&fieldAnalyzerManager{}).Build()
		mockey.Mock((*fieldAnalyzerManager).RunWithAnalyzer).To(func(_ *fieldAnalyzerManager, ctx context.Context, id int64, key string, field int64, run func(function.Analyzer) error) (bool, error) {
			require.Equal(t, int64(7), id)
			require.Equal(t, "WAL-p_7v0", key)
			return true, run(&fieldAnalyzerRunner{})
		}).Build()
		mockey.Mock((*fieldAnalyzerRunner).GetInputFields).Return([]*schemapb.FieldSchema{{}, {}}).Build()
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		batch := mockey.Mock((*fieldAnalyzerRunner).BatchAnalyze).To(func(_ *fieldAnalyzerRunner, detail, hash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
			require.Equal(t, []string{"default", "default"}, inputs[1])
			close(entered)
			<-release
			return [][]*milvuspb.AnalyzerToken{{{Token: "one"}}, {{Token: "two"}}}, nil
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, rwWALImpls: &analyzerWALTest{}}
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Placeholder: [][]byte{[]byte("one"), []byte("two")}, Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{CollectionId: 7, Vchannel: "p_7v0"}}, Operation: &streamingpb.StreamingNodeRunAnalyzerRequest_Tokenize{Tokenize: &streamingpb.TokenizeOptions{}}}
		req.GetFieldAnalyzer().AnalyzerNames = []string{"one", "two", "three"}
		resp, err := w.RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrParameterInvalid)
		require.Zero(t, batch.Times())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp, err = w.RunAnalyzer(ctx, req)
		require.NoError(t, err)
		require.Equal(t, merr.Code(context.Canceled), resp.GetStatus().GetCode())
		req.GetFieldAnalyzer().AnalyzerNames = []string{""}
		done := make(chan error, 1)
		go func() {
			resp, err := w.RunAnalyzer(context.Background(), req)
			if err == nil && len(resp.GetTokenize().Results) != 2 {
				err = merr.ErrServiceInternal
			}
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("analyzer did not start")
		}
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		stopped := make(chan struct{})
		go func() { w.lifetime.Wait(); close(stopped) }()
		select {
		case <-stopped:
			t.Fatal("WAL did not wait for analyzer")
		case <-time.After(50 * time.Millisecond):
		}
		once.Do(func() { close(release) })
		require.NoError(t, <-done)
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("WAL pin leaked")
		}
	})
}

func TestWALPhraseAnalyzerKeepsSchemaAcrossBatches(t *testing.T) {
	mockey.PatchConvey("a schema update between query and data cannot change the pinned analyzer", t, func() {
		mockey.Mock((*analyzerWALTest).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
		fm := function.GetManager()
		const collectionID = 734992
		key := shard.WALFunctionRunnerKey("p_2v0")
		schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}}}}
		require.NoError(t, fm.Alloc(collectionID, key, schema))
		defer fm.Release(collectionID, key)
		updated := proto.Clone(schema).(*schemapb.CollectionSchema)
		updated.Version++
		updated.Fields[0].TypeParams = append(updated.Fields[0].TypeParams, &commonpb.KeyValuePair{Key: "analyzer_params", Value: `{"tokenizer":"whitespace"}`})
		var original func(*function.BM25FunctionRunner, bool, bool, ...any) ([][]*milvuspb.AnalyzerToken, error)
		batches := 0
		mockey.Mock((*function.BM25FunctionRunner).BatchAnalyze).Origin(&original).To(func(a *function.BM25FunctionRunner, detail, hash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
			tokens, err := original(a, detail, hash, inputs...)
			batches++
			if batches == 1 {
				require.NoError(t, fm.Update(collectionID, key, updated))
			}
			return tokens, err
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, rwWALImpls: &analyzerWALTest{}}
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{
			Placeholder: make([][]byte, 100),
			Source:      &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{CollectionId: collectionID, Vchannel: "p_2v0", FieldId: 101, AnalyzerNames: []string{"ignored", "for single field"}}},
			Operation:   &streamingpb.StreamingNodeRunAnalyzerRequest_PhraseMatch{PhraseMatch: &streamingpb.PhraseMatchOptions{QueryText: "hello world"}},
		}
		for i := range req.Placeholder {
			req.Placeholder[i] = []byte("hello-world")
		}
		resp, err := w.RunAnalyzer(t.Context(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Greater(t, batches, 2)
		require.Len(t, resp.GetPhraseMatch().GetSlops(), 100)
		for _, slop := range resp.GetPhraseMatch().GetSlops() {
			require.Zero(t, slop)
		}
		// A subsequent operation observes the new schema.
		resp, err = w.RunAnalyzer(t.Context(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		for _, slop := range resp.GetPhraseMatch().GetSlops() {
			require.EqualValues(t, -1, slop)
		}
	})
}

func TestWALPhraseAnalyzerPinsRunnerOnce(t *testing.T) {
	mockey.PatchConvey("all named phrase batches share one managed runner pin", t, func() {
		mockey.Mock((*analyzerWALTest).Channel).Return(types.PChannelInfo{AccessMode: types.AccessModeRW}).Build()
		mockey.Mock(function.GetManager).Return(&fieldAnalyzerManager{}).Build()
		pinned, released := false, false
		pin := mockey.Mock((*fieldAnalyzerManager).RunWithAnalyzer).To(func(_ *fieldAnalyzerManager, _ context.Context, _ int64, _ string, _ int64, use func(function.Analyzer) error) (bool, error) {
			pinned = true
			defer func() { pinned = false; released = true }()
			return true, use(&fieldAnalyzerRunner{})
		}).Build()
		mockey.Mock((*fieldAnalyzerRunner).GetInputFields).Return([]*schemapb.FieldSchema{{}, {}}).Build()
		queries := map[string]int{}
		mockey.Mock((*fieldAnalyzerRunner).BatchAnalyze).To(func(_ *fieldAnalyzerRunner, detail, hash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
			require.True(t, pinned)
			texts, names := inputs[0].([]string), inputs[1].([]string)
			require.Len(t, names, len(texts))
			tokens := make([][]*milvuspb.AnalyzerToken, len(texts))
			for i, text := range texts {
				if text == "query" {
					queries[names[i]]++
				}
				tokens[i] = []*milvuspb.AnalyzerToken{{Token: names[i]}}
			}
			return tokens, nil
		}).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, rwWALImpls: &analyzerWALTest{}}
		field := &streamingpb.StreamingFieldAnalyzer{CollectionId: 7, Vchannel: "p_7v0", AnalyzerNames: make([]string, 100)}
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Placeholder: make([][]byte, 100), Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: field}, Operation: &streamingpb.StreamingNodeRunAnalyzerRequest_PhraseMatch{PhraseMatch: &streamingpb.PhraseMatchOptions{QueryText: "query"}}}
		for i := range req.Placeholder {
			req.Placeholder[i] = []byte("data")
			switch i % 3 {
			case 0:
				field.AnalyzerNames[i] = ""
			case 1:
				field.AnalyzerNames[i] = "default"
			case 2:
				field.AnalyzerNames[i] = "other"
			}
		}
		resp, err := w.RunAnalyzer(t.Context(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Equal(t, 1, pin.Times())
		require.True(t, released)
		require.Equal(t, map[string]int{"default": 1, "other": 1}, queries)
		for _, matched := range resp.GetPhraseMatch().GetIsMatch() {
			require.True(t, matched)
		}
	})
}
