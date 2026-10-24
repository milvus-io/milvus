// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

func TestHighlightTask(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	t.Run("basic methods", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		t.Run("traceCtx", func(t *testing.T) {
			traceCtx := task.TraceCtx()
			assert.NotNil(t, traceCtx)
			assert.Equal(t, ctx, traceCtx)
		})

		t.Run("id", func(t *testing.T) {
			id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
			task.SetID(id)
			assert.Equal(t, id, task.ID())
		})

		t.Run("name", func(t *testing.T) {
			assert.Equal(t, HighlightTaskName, task.Name())
		})

		t.Run("type", func(t *testing.T) {
			assert.Equal(t, commonpb.MsgType_Undefined, task.Type())
		})

		t.Run("ts", func(t *testing.T) {
			ts := Timestamp(time.Now().UnixNano())
			task.SetTs(ts)
			assert.Equal(t, ts, task.BeginTs())
			assert.Equal(t, ts, task.EndTs())
		})
	})

	t.Run("OnEnqueue", func(t *testing.T) {
		task := &HighlightTask{
			ctx:                 ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{},
			Condition:           NewTaskCondition(ctx),
		}

		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.NotNil(t, task.Base)
		assert.Equal(t, commonpb.MsgType_Undefined, task.Base.MsgType)
		assert.Equal(t, paramtable.GetNodeID(), task.Base.SourceID)
	})

	t.Run("PreExecute", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		err := task.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("PostExecute", func(t *testing.T) {
		task := &HighlightTask{
			ctx: ctx,
			GetHighlightRequest: &querypb.GetHighlightRequest{
				Base: commonpbutil.NewMsgBase(),
			},
			Condition: NewTaskCondition(ctx),
		}

		err := task.PostExecute(ctx)
		assert.NoError(t, err)
	})
}

// Runtime-patched clients deliberately expose no QueryNode or delegator path.
type highlightWAL struct{ streaming.WALAccesser }

func (*highlightWAL) AnalyzerClient() streaming.AnalyzerClient { panic("unpatched") }

type highlightAnalyzer struct{ streaming.AnalyzerClient }

func (*highlightAnalyzer) RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	panic("unpatched")
}

func TestHighlightStreamingAnalyzer(t *testing.T) {
	for _, scenario := range []string{"success", "metadata", "no channel", "business", "transport", "canceled", "malformed"} {
		mockey.PatchConvey(scenario, t, func() {
			mockey.Mock(streaming.WAL).Return(&highlightWAL{}).Build()
			mockey.Mock((*highlightWAL).AnalyzerClient).Return(&highlightAnalyzer{}).Build()
			mockey.Mock((*metacache.MetaCache).GetCollectionInfo).To(func(_ *metacache.MetaCache, _ context.Context, db, name string, id int64) (*collectionInfo, error) {
				require.Empty(t, name)
				require.Equal(t, int64(7), id)
				if scenario == "metadata" {
					return nil, merr.ErrCollectionNotFound
				}
				info := &collectionInfo{VChannels: []string{"p_7v0", "p_7v1"}}
				if scenario == "no channel" {
					info.VChannels = nil
				}
				return info, nil
			}).Build()
			calls := 0
			mockey.Mock((*highlightAnalyzer).RunAnalyzer).To(func(_ *highlightAnalyzer, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				require.Equal(t, int64(100+calls), req.GetFieldAnalyzer().GetFieldId())
				require.Equal(t, int64(7), req.GetFieldAnalyzer().GetCollectionId())
				require.Equal(t, "p_7v0", req.GetFieldAnalyzer().GetVchannel())
				require.Equal(t, []string{"default", "zh", "default", "zh"}, req.GetFieldAnalyzer().GetAnalyzerNames())
				require.True(t, req.GetWithDetail())
				require.False(t, req.GetWithHash())
				require.Equal(t, [][]byte{[]byte("hello"), []byte("世界"), []byte("hello 👋"), []byte("你好世界")}, req.GetPlaceholder())
				if scenario == "transport" {
					return nil, streamingstatus.NewOnShutdownError("WAL is closing")
				}
				if scenario == "canceled" {
					return nil, context.DeadlineExceeded
				}
				if scenario == "business" {
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.ErrParameterInvalid)}, nil
				}
				resp := &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success(), Results: []*milvuspb.AnalyzerResult{
					{Tokens: []*milvuspb.AnalyzerToken{{Token: "hello", StartOffset: 0, EndOffset: 5}}},
					{Tokens: []*milvuspb.AnalyzerToken{{Token: "世界", StartOffset: 0, EndOffset: 6}}},
					{Tokens: []*milvuspb.AnalyzerToken{{Token: "hello", StartOffset: 0, EndOffset: 5}}},
					{Tokens: []*milvuspb.AnalyzerToken{{Token: "世界", StartOffset: 6, EndOffset: 12}}},
				}}
				if scenario == "malformed" {
					resp.Results = resp.Results[:1]
				}
				return resp, nil
			}).Build()
			task := &HighlightTask{baseTask: baseTask{MetaCache: &metacache.MetaCache{}}, collectionID: 7, dbName: "default", GetHighlightRequest: &querypb.GetHighlightRequest{Topks: []int64{1, 1}}}
			for _, id := range []int64{101, 102} {
				task.Tasks = append(task.Tasks, &querypb.HighlightTask{FieldId: id, Texts: []string{"hello", "世界", "hello 👋", "你好世界"}, SearchTextNum: 2, CorpusTextNum: 2, AnalyzerNames: []string{"default", "zh", "default", "zh"}, Options: &querypb.HighlightOptions{FragmentSize: 100, NumOfFragments: 5}})
			}
			err := task.Execute(t.Context())
			if scenario != "success" {
				require.Error(t, err)
				require.Nil(t, task.result, "do not publish partial highlight results")
				if scenario == "business" {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
				}
				if scenario == "transport" {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				}
				if scenario == "canceled" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.Len(t, task.result.Results, 4)
			require.Equal(t, []int64{0, 5}, task.result.Results[0].Fragments[0].Offsets)
			require.Equal(t, []int64{2, 4}, task.result.Results[1].Fragments[0].Offsets)
		})
	}
}

// Pipeline tests exercise the streaming analyzer boundary; QueryNode RPCs are
// no longer part of lexical highlighting.
func mockPipelineAnalyzer(t *testing.T, expectedTexts []string) {
	t.Helper()
	mockTest(t, streaming.WAL, &highlightWAL{})
	mockTest(t, (*highlightWAL).AnalyzerClient, &highlightAnalyzer{})
	mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{VChannels: []string{"p_1v0"}}, nil)
	mockTestTo(t, (*highlightAnalyzer).RunAnalyzer, func(_ *highlightAnalyzer, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
		texts := make([]string, len(req.GetPlaceholder()))
		results := make([]*milvuspb.AnalyzerResult, len(texts))
		for i, text := range req.GetPlaceholder() {
			texts[i] = string(text)
			results[i] = &milvuspb.AnalyzerResult{}
		}
		require.Equal(t, expectedTexts, texts)
		require.Equal(t, "p_1v0", req.GetFieldAnalyzer().GetVchannel())
		require.NotNil(t, req.GetFieldAnalyzer().ReadSchemaVersion)
		return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success(), Results: results}, nil
	})
}
