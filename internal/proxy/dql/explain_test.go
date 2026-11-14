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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// explainTestNode is the taskmodel.TaskNode an explain needs: a meta cache
// and nothing else. The other dependencies are what Execute would use, so
// they stay nil and an explain that reached them would panic the test.
type explainTestNode struct {
	cache metacache.Cache
}

func (n *explainTestNode) GetMetaCache() metacache.Cache        { return n.cache }
func (n *explainTestNode) MixCoord() types.MixCoordClient       { return nil }
func (n *explainTestNode) LBPolicy() shardclient.LBPolicy       { return nil }
func (n *explainTestNode) ShardMgr() shardclient.ShardClientMgr { return nil }
func (n *explainTestNode) ChMgr() channelmgr.ChannelsMgr        { return nil }
func (n *explainTestNode) TsoAllocator() taskmodel.TsoAllocator { return nil }

func (n *explainTestNode) CheckClusterPrivilege(_ context.Context, _ interface{}, _, _ string) error {
	return nil
}

var _ taskmodel.TaskNode = (*explainTestNode)(nil)

func TestExplainSearch(t *testing.T) {
	ctx := context.TODO()
	node := &explainTestNode{cache: newTestCache()}
	collName := "explain_search" + funcutil.GenRandomStr()
	schema := mustNewSchemaInfo(constructCollectionSchema(testInt64Field, testFloatVecField, testVecDim, collName))
	// A second collection whose vector field is filled by a text embedding
	// function, so a search on it would call the function's model provider.
	funcCollName := "explain_search_function" + funcutil.GenRandomStr()
	funcSchema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: funcCollName,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "200"}}},
			{FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "embed",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldIds:    []int64{101},
			InputFieldNames:  []string{"text"},
			OutputFieldIds:   []int64{102},
			OutputFieldNames: []string{"vector"},
			Params: []*commonpb.KeyValuePair{
				{Key: "provider", Value: "openai"},
				{Key: "model_name", Value: "text-embedding-ada-002"},
				{Key: "credential", Value: "mock"},
				{Key: "dim", Value: "4"},
			},
		}},
	})
	// A third collection with a VarChar field a highlighter can point at.
	textCollName := "explain_search_text" + funcutil.GenRandomStr()
	textSchema := mustNewSchemaInfo(constructCollectionSchemaByDataType(textCollName, map[string]schemapb.DataType{
		testInt64Field:    schemapb.DataType_Int64,
		testFloatVecField: schemapb.DataType_FloatVector,
		"text":            schemapb.DataType_VarChar,
	}, testInt64Field, false))
	infos := map[string]*collectionInfo{
		// A TTL makes PreExecute derive the expiry window from the task
		// timestamp, which ExplainSearch sets because no scheduler does.
		collName:     {CollID: 1000, Schema: schema, CollectionTTL: uint64(15 * time.Second)},
		funcCollName: {CollID: 1001, Schema: funcSchema},
		textCollName: {CollID: 1002, Schema: textSchema},
	}
	mockTestTo(t, (*metacache.MetaCache).GetCollectionID, func(_ *metacache.MetaCache, _ context.Context, _, name string) (int64, error) {
		info, ok := infos[name]
		if !ok {
			return 0, merr.WrapErrCollectionNotFound(name)
		}
		return info.CollID, nil
	})
	mockTestTo(t, (*metacache.MetaCache).GetCollectionInfo, func(_ *metacache.MetaCache, _ context.Context, _, name string, _ int64) (*collectionInfo, error) {
		info, ok := infos[name]
		if !ok {
			return nil, merr.WrapErrCollectionNotFound(name)
		}
		return info, nil
	})
	mockTest(t, (*metacache.MetaCache).GetPartitions, map[string]int64{"_default": 1}, nil)

	placeholderGroup, err := proto.Marshal(constructPlaceholderGroup(1, testVecDim))
	require.NoError(t, err)
	newRequest := func() *milvuspb.SearchRequest {
		return &milvuspb.SearchRequest{
			CollectionName: collName,
			Dsl:            testInt64Field + " > {v}",
			DslType:        commonpb.DslType_BoolExprV1,
			ExprTemplateValues: map[string]*schemapb.TemplateValue{
				"v": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 3}},
			},
			SearchParams: getValidSearchParams(),
			SearchInput:  &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholderGroup},
			Nq:           1,
			OutputFields: []string{testInt64Field},
		}
	}

	t.Run("describes the plan PreExecute built", func(t *testing.T) {
		out, err := ExplainSearch(ctx, node, newRequest())
		require.NoError(t, err)
		assert.Equal(t, planparserv2.ExplainVersion, out.Version)
		assert.Equal(t, planparserv2.PlanTypeVectorSearch, out.PlanType)
		require.NotNil(t, out.VectorSearch)
		assert.Equal(t, testFloatVecField, out.VectorSearch.Field.Name)
		assert.Equal(t, metric.L2, out.VectorSearch.MetricType)
		assert.Equal(t, int64(10), out.VectorSearch.TopK)
		require.NotNil(t, out.Predicates)
		assert.Equal(t, planparserv2.NodeUnaryRange, out.Predicates.Node)
		assert.Equal(t, "GreaterThan", out.Predicates.Op)
		assert.Equal(t, testInt64Field, out.Predicates.Field.Name)
		assert.True(t, out.Predicates.Field.PrimaryKey)
		assert.Equal(t, "v", out.Predicates.Template)
		assert.Equal(t, int64(3), out.Predicates.Value)
		assert.Equal(t, 0, out.UnknownNodeCount)
		// output_fields is what the task put on the plan, here the primary
		// key the task always adds.
		require.Len(t, out.OutputFields, 1)
		assert.Equal(t, testInt64Field, out.OutputFields[0].Name)
	})

	t.Run("invalid expression returns the search error", func(t *testing.T) {
		req := newRequest()
		req.Dsl = "no_such_field > 3"
		req.ExprTemplateValues = nil
		_, err := ExplainSearch(ctx, node, req)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("unknown collection returns the search error", func(t *testing.T) {
		req := newRequest()
		req.CollectionName = "missing"
		_, err := ExplainSearch(ctx, node, req)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("hybrid search is not supported", func(t *testing.T) {
		req := newRequest()
		req.SubReqs = []*milvuspb.SubSearchRequest{{}}
		_, err := ExplainSearch(ctx, node, req)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.ErrorContains(t, err, "hybrid search")
	})

	t.Run("search by primary key is not supported", func(t *testing.T) {
		req := newRequest()
		req.SearchInput = &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}},
		}}
		_, err := ExplainSearch(ctx, node, req)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.ErrorContains(t, err, "search by primary key")
	})

	t.Run("semantic highlighting is not supported", func(t *testing.T) {
		req := newRequest()
		req.Highlighter = &commonpb.Highlighter{Type: commonpb.HighlightType_Semantic}
		_, err := ExplainSearch(ctx, node, req)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.ErrorContains(t, err, "semantic highlighting")
	})

	t.Run("lexical highlighting adds its field to the plan, as in the search", func(t *testing.T) {
		req := newRequest()
		req.CollectionName = textCollName
		req.Highlighter = &commonpb.Highlighter{Type: commonpb.HighlightType_Lexical, Params: []*commonpb.KeyValuePair{
			{Key: HighlightQueryKey, Value: `[{"text": "a", "type": "TextMatch", "field": "text"}]`},
		}}
		out, err := ExplainSearch(ctx, node, req)
		require.NoError(t, err)
		names := make([]string, 0, len(out.OutputFields))
		for _, f := range out.OutputFields {
			names = append(names, f.Name)
		}
		assert.ElementsMatch(t, []string{testInt64Field, "text"}, names)
	})

	t.Run("empty ids are an ordinary search, as in the search preflight", func(t *testing.T) {
		req := newRequest()
		req.SearchInput = &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{}}
		out, err := ExplainSearch(ctx, node, req)
		require.NoError(t, err)
		assert.Equal(t, planparserv2.PlanTypeVectorSearch, out.PlanType)
	})

	t.Run("the embedding function's model provider is not called", func(t *testing.T) {
		var calls atomic.Int32
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			// an answer with no embeddings fails the caller at once; an error
			// status would be retried with backoff
			_, _ = w.Write([]byte("{}"))
		}))
		defer provider.Close()
		paramtable.Init()
		credentials := &paramtable.Get().CredentialCfg.Credential
		providers := &paramtable.Get().FunctionCfg.TextEmbeddingProviders
		prevCredentials, prevProviders := credentials.GetFunc, providers.GetFunc
		credentials.GetFunc = func() map[string]string { return map[string]string{"mock.apikey": "mock"} }
		providers.GetFunc = func() map[string]string { return map[string]string{"openai.url": provider.URL} }
		t.Cleanup(func() { credentials.GetFunc, providers.GetFunc = prevCredentials, prevProviders })

		texts, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
			Tag: "$0", Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("a sentence")},
		}}})
		require.NoError(t, err)
		newTextRequest := func() *milvuspb.SearchRequest {
			return &milvuspb.SearchRequest{
				CollectionName: funcCollName,
				SearchParams:   []*commonpb.KeyValuePair{{Key: AnnsFieldKey, Value: "vector"}, {Key: TopKKey, Value: "10"}},
				SearchInput:    &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: texts},
				Nq:             1,
			}
		}

		out, err := ExplainSearch(ctx, node, newTextRequest())
		require.NoError(t, err)
		assert.Equal(t, "vector", out.VectorSearch.Field.Name)
		assert.Equal(t, int32(0), calls.Load())

		// Control: the same request as a search task does call the provider,
		// so the assertion above holds because explain skips the call.
		task := NewSearchTask(ctx, node, nil, newTextRequest(), false, false, nil)
		task.SetTs(tsoutil.ComposeTS(time.Now().UnixMilli(), 0))
		assert.Error(t, task.PreExecute(ctx))
		assert.Equal(t, int32(1), calls.Load())
	})
}
