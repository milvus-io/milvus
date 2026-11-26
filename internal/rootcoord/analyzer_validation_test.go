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

package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Resource-lifecycle tests replace only native validation; reservation and DDL
// publication continue through their real implementation.
func mockAnalyzerValidation(t *testing.T) func(func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error)) {
	var validate func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error)
	patch := mockey.Mock((*Core).validateAnalyzerInfos).To(func(_ *Core, ctx context.Context, infos []*querypb.AnalyzerInfo) ([]int64, error) {
		if len(infos) == 0 {
			return nil, nil
		}
		return validate(ctx, infos)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return func(f func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error)) { validate = f }
}

type validationWAL struct{ streaming.WALAccesser }

func (*validationWAL) AnalyzerClient() streaming.AnalyzerClient { panic("unpatched") }

type validationClient struct{ streaming.AnalyzerClient }

func (*validationClient) ValidateAnalyzer(context.Context, *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
	panic("unpatched")
}

func TestValidateAnalyzerInfosUsesStreaming(t *testing.T) {
	mockey.PatchConvey("validation does not require QN inventory or a collection", t, func() {
		const params = `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}]}`
		wal := mockey.Mock(streaming.WAL).Return(&validationWAL{}).Build()
		mockey.Mock((*validationWAL).AnalyzerClient).Return(&validationClient{}).Build()
		var validationErr error
		mockey.Mock((*validationClient).ValidateAnalyzer).To(func(_ *validationClient, _ context.Context, req *streamingpb.StreamingNodeValidateAnalyzerRequest) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
			require.Len(t, req.AnalyzerInfos, 1)
			info := req.AnalyzerInfos[0]
			require.Equal(t, "body", info.Field)
			require.Equal(t, "english", info.Name)
			require.Equal(t, params, info.Params)
			if validationErr != nil {
				return nil, validationErr
			}
			return &streamingpb.StreamingNodeValidateAnalyzerResponse{ResourceIds: []int64{11}}, nil
		}).Build()
		core := &Core{} // No metadata access: SN resolves resource_name from the forwarded params.
		ids, err := core.validateAnalyzerInfos(context.Background(), nil)
		require.NoError(t, err)
		require.Empty(t, ids)
		require.Zero(t, wal.Times())
		infos := []*querypb.AnalyzerInfo{{Field: "body", Name: "english", Params: params}}
		ids, err = core.validateAnalyzerInfos(context.Background(), infos)
		require.NoError(t, err)
		require.Equal(t, []int64{11}, ids)
		validationErr = merr.ErrServiceUnavailable
		_, err = core.validateAnalyzerInfos(context.Background(), infos)
		require.ErrorIs(t, err, validationErr)
	})
}

func TestAnalyzerAdmissionOnlyValidatesChangedDefinitions(t *testing.T) {
	paramtable.Init()
	previous := `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"/legacy/stop.txt"}}]}`
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
		{Key: "enable_analyzer", Value: "true"}, {Key: "analyzer_params", Value: previous},
	}}
	coll := &model.Collection{Fields: []*schemapb.FieldSchema{field}}
	core := &Core{}
	calls := 0
	mockAnalyzerValidation(t)(func(_ context.Context, infos []*querypb.AnalyzerInfo) ([]int64, error) {
		calls++
		require.Len(t, infos, 1)
		require.Equal(t, "text", infos[0].Field)
		return nil, merr.WrapErrParameterInvalidMsg("outside current policy")
	})
	schema := coll.ToCollectionSchemaPB()
	_, err := core.validateChangedAnalyzers(context.Background(), coll, schema)
	require.NoError(t, err)
	require.Zero(t, calls)
	require.Equal(t, previous, schema.Fields[0].TypeParams[1].Value)
	schema.Fields[0].TypeParams[1].Value = `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"/different/stop.txt"}}]}`
	_, err = core.validateChangedAnalyzers(context.Background(), coll, schema)
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestResolveMultiAnalyzerPreservesUnchangedLegacyRelativePath(t *testing.T) {
	paramtable.Init()
	field := func(value string) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{Name: "text", TypeParams: []*commonpb.KeyValuePair{
			{Key: "enable_analyzer", Value: "true"}, {Key: "multi_analyzer_params", Value: value},
		}}
	}
	old := `{"analyzers":{"default":{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"legacy.txt"}}]}}}`
	changed := `{"analyzers":{"default":{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"legacy.txt"}}]},"new":{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"new.txt"}}]}}}`
	previous := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field(old)}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field(changed)}}
	require.NoError(t, resolveSchemaAnalyzerPaths(schema, previous))
	result := schema.Fields[0].TypeParams[1].Value
	require.Contains(t, result, `"path":"legacy.txt"`)
	require.NotContains(t, result, `"path":"new.txt"`)
	require.Contains(t, result, "/new.txt")
	require.False(t, sameAnalyzerParams(`{"tokenizer":"standard"} trailing`, `{"tokenizer":"standard"}`))
}

func TestMinHashAnalyzerAdmissionWithoutEnableAnalyzer(t *testing.T) {
	paramtable.Init()
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
		{Key: "analyzer_params", Value: `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"dict.txt"}}]}`},
	}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}, Functions: []*schemapb.FunctionSchema{
		{Name: "minhash", Type: schemapb.FunctionType_MinHash, InputFieldNames: []string{"text"}, InputFieldIds: []int64{999}},
	}}
	calls := 0
	mockAnalyzerValidation(t)(func(_ context.Context, infos []*querypb.AnalyzerInfo) ([]int64, error) {
		calls++
		require.Len(t, infos, 1)
		require.Contains(t, infos[0].Params, "/dict.txt")
		return nil, merr.WrapErrParameterInvalidMsg("rejected by admission")
	})
	core := &Core{}
	_, err := core.validateSchemaAnalyzerFileResources(context.Background(), schema)
	require.Error(t, err)
	require.Equal(t, 1, calls)
	// Adding a MinHash function makes a previously unused definition subject to admission.
	old := &model.Collection{Fields: []*schemapb.FieldSchema{field}}
	_, err = core.validateChangedAnalyzers(context.Background(), old, schema)
	require.Error(t, err)
	require.Equal(t, 2, calls)
}

func TestAnalyzerRetentionIgnoresUnconsumedResourceCandidates(t *testing.T) {
	paramtable.Init()
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
		{Key: "enable_analyzer", Value: "true"},
		{Key: "analyzer_params", Value: `{"type":"standard","unused":{"type":"remote","resource_name":"unused"}}`},
	}}
	coll := &model.Collection{Fields: []*schemapb.FieldSchema{field}}
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().FindFileResource(mock.Anything, "unused").Return(nil).Once()
	core := &Core{meta: meta}
	mockAnalyzerValidation(t)(func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error) {
		t.Fatal("unchanged definition must not be revalidated")
		return nil, nil
	})
	ids, err := core.validateChangedAnalyzers(context.Background(), coll, coll.ToCollectionSchemaPB())
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestAnalyzerAdmissionOnlyExemptsStoredSnapshots(t *testing.T) {
	paramtable.Init()
	const params = `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"legacy.txt"}}]}`
	schema := &schemapb.CollectionSchema{Name: "restored", Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "100"}, {Key: "enable_analyzer", Value: "true"}, {Key: "analyzer_params", Value: params}},
	}}}
	calls := 0
	mockAnalyzerValidation(t)(func(context.Context, []*querypb.AnalyzerInfo) ([]int64, error) {
		calls++
		return nil, merr.WrapErrParameterInvalidMsg("current policy rejects definition")
	})
	task := &createCollectionTask{Core: &Core{}, Req: &milvuspb.CreateCollectionRequest{CollectionName: "restored"}, preserveFieldID: true, restoreAnalyzerDefinitions: true}
	restored := proto.Clone(schema).(*schemapb.CollectionSchema)
	require.NoError(t, task.validateSchema(context.Background(), restored))
	require.Equal(t, params, restored.Fields[0].TypeParams[2].Value)
	require.Zero(t, calls)
	// A client-provided preserve_field_ids request is still new admission.
	task.restoreAnalyzerDefinitions = false
	require.Error(t, task.validateSchema(context.Background(), proto.Clone(schema).(*schemapb.CollectionSchema)))
	require.Equal(t, 1, calls)
}
