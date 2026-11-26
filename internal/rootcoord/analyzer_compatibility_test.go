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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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

func TestAnalyzerAdmissionOnlyValidatesChangedDefinitions(t *testing.T) {
	paramtable.Init()
	previous := `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"/legacy/stop.txt"}}]}`
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
		{Key: "enable_analyzer", Value: "true"}, {Key: "analyzer_params", Value: previous},
	}}
	coll := &model.Collection{Fields: model.UnmarshalFieldModels([]*schemapb.FieldSchema{field})}
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
	old := &model.Collection{Fields: model.UnmarshalFieldModels([]*schemapb.FieldSchema{field})}
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
	coll := &model.Collection{Fields: model.UnmarshalFieldModels([]*schemapb.FieldSchema{field})}
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
