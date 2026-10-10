package adaptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

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

func TestWALFieldAnalyzerAdmission(t *testing.T) {
	mockey.PatchConvey("field analyzer needs only WAL schema and works without a QueryView", t, func() {
		mockey.Mock((*analyzerWALTest).Channel).Return(types.PChannelInfo{Name: "p", Term: 1, AccessMode: types.AccessModeRW}).Build()
		fm := function.GetManager()
		const collectionID = 734991
		key := shard.WALFunctionRunnerKey("p_1v0")
		schema := &schemapb.CollectionSchema{Version: 3, Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}}}}
		require.NoError(t, fm.Alloc(collectionID, key, schema))
		defer fm.Release(collectionID, key)
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, rwWALImpls: &analyzerWALTest{}}
		version := int32(3)
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Placeholder: [][]byte{[]byte("hello world")}, Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{CollectionId: collectionID, Vchannel: "p_1v0", FieldId: 101, SchemaVersion: &version}}}
		resp, err := w.RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetResults()[0].Tokens, 2)
		version = 4
		resp, err = w.RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionSchemaVersionNotReady)
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = w.RunAnalyzer(context.Background(), req)
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
	})
}

type fieldAnalyzerManager struct{ function.FunctionRunnerManager }

func (*fieldAnalyzerManager) RunWithAnalyzerAtSchemaVersion(context.Context, int64, string, int64, int32, func(function.Analyzer) error) (bool, error) {
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
		mockey.Mock((*fieldAnalyzerManager).RunWithAnalyzerAtSchemaVersion).To(func(_ *fieldAnalyzerManager, ctx context.Context, id int64, key string, field int64, version int32, run func(function.Analyzer) error) (bool, error) {
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
		version := int32(3)
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Placeholder: [][]byte{[]byte("one"), []byte("two")}, Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{CollectionId: 7, Vchannel: "p_7v0", SchemaVersion: &version}}}
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
			if err == nil && len(resp.Results) != 2 {
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
