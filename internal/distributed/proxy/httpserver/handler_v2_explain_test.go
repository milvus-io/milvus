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

package httpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// explainProxy adds the Go-only ExplainSearch the explain route asserts for
// on top of the generated mock, and keeps the request it was handed.
type explainProxy struct {
	*mocks.MockProxy
	explanation *planparserv2.PlanExplanation
	err         error
	received    *milvuspb.SearchRequest
}

func (p *explainProxy) ExplainSearch(_ context.Context, request *milvuspb.SearchRequest) (*planparserv2.PlanExplanation, error) {
	p.received = request
	return p.explanation, p.err
}

// explainTestBody sets no limit, so the parity test compares the default each
// route sets on its own.
const explainTestBody = `{"collectionName": "book", "data": [[0.1, 0.2]], "filter": "book_id in {list}", "exprParams": {"list": [2, 4, 6, 8]}, "outputFields": ["word_count"]}`

func newExplainProxy(t *testing.T, describeTimes int) *explainProxy {
	mp := mocks.NewMockProxy(t)
	if describeTimes > 0 {
		mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			CollectionName: DefaultCollectionName,
			Schema:         generateCollectionSchema(schemapb.DataType_Int64, false, true),
			ShardsNum:      ShardNumDefault,
			Status:         &StatusSuccess,
		}, nil).Times(describeTimes)
	}
	return &explainProxy{
		MockProxy: mp,
		explanation: &planparserv2.PlanExplanation{
			Version:  planparserv2.ExplainVersion,
			PlanType: planparserv2.PlanTypeVectorSearch,
			VectorSearch: &planparserv2.VectorSearchExplanation{
				Field:      planparserv2.FieldRef{ID: 102, Name: FieldBookIntro},
				MetricType: "COSINE",
				TopK:       4,
			},
		},
	}
}

type explainReturn struct {
	Code    int32           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// refusingHook refuses the search request from Before, as a hook plugin may;
// the schema lookup before it goes through the same interceptor and passes.
type refusingHook struct{ hookutil.DefaultHook }

func (refusingHook) Before(ctx context.Context, req interface{}, _ string) (context.Context, error) {
	if _, ok := req.(*milvuspb.SearchRequest); ok {
		return ctx, errors.New("refused by the hook")
	}
	return ctx, nil
}

// mockingHook answers search requests itself, as a hook plugin may; keyed on
// the request type, it cannot tell explain from search.
type mockingHook struct{ hookutil.DefaultHook }

func (mockingHook) Mock(_ context.Context, req interface{}, _ string) (bool, interface{}, error) {
	if _, ok := req.(*milvuspb.SearchRequest); ok {
		return true, &milvuspb.SearchResults{Status: &StatusSuccess}, nil
	}
	return false, nil, nil
}

func postExplain(t *testing.T, engine http.Handler, body string, auth func(*http.Request)) (*httptest.ResponseRecorder, explainReturn) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, ExplainAction), strings.NewReader(body))
	if auth != nil {
		auth(req)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	ret := explainReturn{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &ret), w.Body.String())
	return w, ret
}

func TestExplainV2(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	t.Run("explain hands the proxy the request search would", func(t *testing.T) {
		p := newExplainProxy(t, 2)
		var searched *milvuspb.SearchRequest
		p.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			searched = req
			return &milvuspb.SearchResults{Status: commonSuccessStatus, Results: &schemapb.SearchResultData{TopK: 0}}, nil
		}).Once()
		engine := initHTTPServerV2(p, false)
		// a named database, so the comparison covers the database too
		body := strings.TrimSuffix(explainTestBody, "}") + `, "dbName": "db1"}`

		searchReq := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, SearchAction), strings.NewReader(body))
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, searchReq)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, searched)

		w, ret := postExplain(t, engine, body, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, merr.Code(nil), ret.Code, ret.Message)
		require.NotNil(t, p.received)
		assert.True(t, proto.Equal(searched, p.received), "search sent %v, explain sent %v", searched, p.received)
		assert.Equal(t, "book_id in {list}", p.received.GetDsl())
		assert.Equal(t, "db1", p.received.GetDbName())
		topK, err := funcutil.GetAttrByKeyFromRepeatedKV(common.TopKKey, p.received.GetSearchParams())
		require.NoError(t, err)
		assert.Equal(t, "100", topK, "the default limit is the search route's")

		// data is the explanation as PlanExplanation.JSON() renders it: same
		// keys, same presence rules
		want, err := p.explanation.JSON()
		require.NoError(t, err)
		assert.JSONEq(t, string(want), string(ret.Data))
	})

	t.Run("text format returns the text rendering", func(t *testing.T) {
		p := newExplainProxy(t, 1)
		engine := initHTTPServerV2(p, false)
		body := strings.TrimSuffix(explainTestBody, "}") + `, "format": "text"}`
		w, ret := postExplain(t, engine, body, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, merr.Code(nil), ret.Code, ret.Message)
		var text string
		require.NoError(t, json.Unmarshal(ret.Data, &text))
		assert.Equal(t, p.explanation.Text(), text)
	})

	t.Run("unknown format is rejected before the proxy is asked", func(t *testing.T) {
		p := newExplainProxy(t, 0)
		engine := initHTTPServerV2(p, false)
		body := strings.TrimSuffix(explainTestBody, "}") + `, "format": "yaml"}`
		w, ret := postExplain(t, engine, body, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, merr.Code(merr.ErrParameterInvalid), ret.Code)
		assert.Contains(t, ret.Message, `format must be "json" or "text", got "yaml"`)
		assert.Nil(t, p.received)
	})

	t.Run("the proxy's error keeps its code", func(t *testing.T) {
		p := newExplainProxy(t, 1)
		p.err = merr.WrapErrParameterInvalidMsg("cannot parse expression: book_id >")
		engine := initHTTPServerV2(p, false)
		w, ret := postExplain(t, engine, explainTestBody, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, merr.Code(merr.ErrParameterInvalid), ret.Code)
		assert.Contains(t, ret.Message, "cannot parse expression: book_id >")
	})

	t.Run("the route is counted like the other v2 routes, a failure as a failure", func(t *testing.T) {
		methodTag, ok := routeToMethod[versionalV2(EntityCategory, ExplainAction)]
		require.True(t, ok, "explain has no ProxyFunctionCall method tag")
		nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
		series := func(label, cause string) float64 {
			return testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(nodeID, methodTag, label, cause, DefaultDbName, DefaultCollectionName))
		}
		total, success := series(metrics.TotalLabel, metrics.CauseNA), series(metrics.SuccessLabel, metrics.CauseNA)
		failed, rejected := series(metrics.FailLabel, metrics.CauseUser), series(metrics.RejectedLabel, metrics.CauseUser)

		p := newExplainProxy(t, 1)
		_, ret := postExplain(t, initHTTPServerV2(p, false), explainTestBody, nil)
		require.Equal(t, merr.Code(nil), ret.Code, ret.Message)
		assert.Equal(t, total+1, series(metrics.TotalLabel, metrics.CauseNA))
		assert.Equal(t, success+1, series(metrics.SuccessLabel, metrics.CauseNA))

		// the same body fails on /search as fail/user: the proxy's error was
		// processed, not rejected around processing
		p = newExplainProxy(t, 1)
		p.err = merr.WrapErrParameterInvalidMsg("cannot parse expression: book_id >")
		_, ret = postExplain(t, initHTTPServerV2(p, false), explainTestBody, nil)
		require.Equal(t, merr.Code(merr.ErrParameterInvalid), ret.Code)
		assert.Equal(t, total+2, series(metrics.TotalLabel, metrics.CauseNA))
		assert.Equal(t, failed+1, series(metrics.FailLabel, metrics.CauseUser))
		assert.Equal(t, rejected, series(metrics.RejectedLabel, metrics.CauseUser))
	})

	t.Run("a hook sees the ExplainSearch name, and its refusal is a rejection", func(t *testing.T) {
		hookutil.InitOnceHook()
		hookutil.SetTestHook(refusingHook{})
		t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })
		methodTag := routeToMethod[versionalV2(EntityCategory, ExplainAction)]
		nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
		// ProxyHookFunc is labeled with the name the hook was called with
		refused := func() float64 {
			return testutil.ToFloat64(metrics.ProxyHookFunc.WithLabelValues(metrics.HookBefore, "/milvus.proto.milvus.MilvusService/"+methodTag))
		}
		rejected := func() float64 {
			return testutil.ToFloat64(metrics.ProxyFunctionCall.WithLabelValues(nodeID, methodTag, metrics.RejectedLabel, metrics.CauseUser, DefaultDbName, DefaultCollectionName))
		}
		byHook, byRoute := refused(), rejected()

		p := newExplainProxy(t, 1)
		_, ret := postExplain(t, initHTTPServerV2(p, false), explainTestBody, nil)
		require.NotEqual(t, merr.Code(nil), ret.Code)
		assert.Contains(t, ret.Message, "refused by the hook")
		assert.Nil(t, p.received)
		assert.Equal(t, byHook+1, refused())
		assert.Equal(t, byRoute+1, rejected())
	})

	t.Run("a hook's mocked search results are an error, not a panic", func(t *testing.T) {
		hookutil.InitOnceHook()
		hookutil.SetTestHook(mockingHook{})
		t.Cleanup(func() { hookutil.SetTestHook(hookutil.DefaultHook{}) })

		p := newExplainProxy(t, 1)
		w, ret := postExplain(t, initHTTPServerV2(p, false), explainTestBody, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, merr.Code(merr.ErrServiceInternal), ret.Code)
		assert.Contains(t, ret.Message, "instead of a plan explanation")
		assert.Nil(t, p.received)
	})

	t.Run("a proxy without ExplainSearch is unimplemented", func(t *testing.T) {
		// the generated mock alone: no ExplainSearch
		engine := initHTTPServerV2(newExplainProxy(t, 1).MockProxy, false)
		w, ret := postExplain(t, engine, explainTestBody, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, merr.Code(merr.ErrServiceUnimplemented), ret.Code)
	})
}

// Explain authorizes the SearchRequest it builds, so the Search privilege
// decides, exactly as it does for a search.
func TestExplainV2Authorization(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
	const (
		username = "explainer"
		role     = "explain_reader"
	)
	proxy.InitEmptyMetaCacheForTest()
	privCache := privilege.GetPrivilegeCache()
	require.NotNil(t, privCache)
	grant := func(objectType commonpb.ObjectType, objectName string, priv commonpb.ObjectPrivilege) {
		require.NoError(t, privCache.RefreshPolicyInfo(typeutil.CacheOp{
			OpType: typeutil.CacheGrantPrivilege,
			OpKey:  funcutil.PolicyForPrivilege(role, objectType.String(), objectName, priv.String(), util.DefaultDBName),
		}))
	}
	require.NoError(t, privCache.RefreshPolicyInfo(typeutil.CacheOp{
		OpType: typeutil.CacheAddUserToRole,
		OpKey:  funcutil.EncodeUserRoleCache(username, role),
	}))
	// the schema lookup that precedes the privilege check must pass on its
	// own; DescribeCollection is a database-wide (Global) privilege
	grant(commonpb.ObjectType_Global, util.AnyWord, commonpb.ObjectPrivilege_PrivilegeDescribeCollection)

	p := newExplainProxy(t, 2)
	engine := newRESTV2ServerForRBACTest(t, p)
	basicAuth := func(req *http.Request) { req.SetBasicAuth(username, "pwd") }

	w, ret := postExplain(t, engine, explainTestBody, basicAuth)
	assert.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	assert.Contains(t, ret.Message, commonpb.ObjectPrivilege_PrivilegeSearch.String())
	assert.Nil(t, p.received)

	grant(commonpb.ObjectType_Collection, DefaultCollectionName, commonpb.ObjectPrivilege_PrivilegeSearch)
	w, ret = postExplain(t, engine, explainTestBody, basicAuth)
	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, merr.Code(nil), ret.Code, ret.Message)
	assert.NotNil(t, p.received)
}
