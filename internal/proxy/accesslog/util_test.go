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

package accesslog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestJoin(t *testing.T) {
	assert.Equal(t, "a/b", join("a", "b"))
	assert.Equal(t, "a/b", join("a/", "b"))
}

func newTestGrpcAccessInfo(req any) *info.GrpcAccessInfo {
	return info.NewGrpcAccessInfo(context.Background(), &grpc.UnaryServerInfo{}, req)
}

func TestConsistencyLevelHelperFallbackToCarrier(t *testing.T) {
	carrier := &milvuspb.SearchRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Bounded}
	want := "REQ-" + commonpb.ConsistencyLevel_Bounded.String()

	// ctx without access info
	helper := NewConsistencyLevelHelper(context.Background(), carrier)
	assert.Equal(t, want, helper.String())

	// nil ctx
	//nolint:staticcheck // exercising the nil-context fallback path
	helper = NewConsistencyLevelHelper(nil, carrier)
	assert.Equal(t, want, helper.String())

	// ctx carries a value that is not an AccessInfo
	ctx := context.WithValue(context.Background(), AccessKey{}, "not-an-access-info")
	helper = NewConsistencyLevelHelper(ctx, carrier)
	assert.Equal(t, want, helper.String())

	// nil carrier
	helper = NewConsistencyLevelHelper(context.Background(), nil)
	assert.Equal(t, info.Unknown, helper.String())
}

func TestConsistencyLevelHelperUsesActualLevel(t *testing.T) {
	req := &milvuspb.SearchRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}
	accessInfo := newTestGrpcAccessInfo(req)
	accessInfo.SetActualConsistencyLevel(commonpb.ConsistencyLevel_Eventually)
	ctx := context.WithValue(context.Background(), AccessKey{}, accessInfo)

	helper := NewConsistencyLevelHelper(ctx, req)
	assert.Equal(t, "ACT-"+commonpb.ConsistencyLevel_Eventually.String(), helper.String())
}

func TestConsistencyLevelHelperAccessInfoFallback(t *testing.T) {
	// actual level not set, access info itself falls back to the request level
	req := &milvuspb.QueryRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Bounded}
	accessInfo := newTestGrpcAccessInfo(req)
	ctx := context.WithValue(context.Background(), AccessKey{}, accessInfo)

	helper := NewConsistencyLevelHelper(ctx, req)
	assert.Equal(t, "ACT-"+commonpb.ConsistencyLevel_Bounded.String(), helper.String())
}

func TestConsistencyLevelHelperQueryRequestCarrier(t *testing.T) {
	// QueryRequest implements ConsistencyLevelCarrier
	req := &milvuspb.QueryRequest{ConsistencyLevel: commonpb.ConsistencyLevel_Eventually}
	helper := NewConsistencyLevelHelper(context.Background(), req)
	assert.Equal(t, "REQ-"+commonpb.ConsistencyLevel_Eventually.String(), helper.String())
}

func newRESTTestContext(t *testing.T) *gin.Context {
	gin.SetMode(gin.TestMode)
	paramtable.Init()
	if _globalL == nil {
		_globalL = NewAccessLogger()
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v2/vectordb/entities/search", nil)
	c.Keys = make(map[any]any)
	AccessLogMiddleware(c)
	return c
}

func TestAccessLogMiddlewareBridgesAccessKey(t *testing.T) {
	c := newRESTTestContext(t)

	v, ok := c.Request.Context().Value(AccessKey{}).(info.AccessInfo)
	assert.True(t, ok)
	assert.NotNil(t, v)
	got, _ := c.Get(ContextLogKey)
	assert.Equal(t, got, v)
}

func TestConsistencyLevelHelperRESTPath(t *testing.T) {
	// REST request relying on the default consistency: the protobuf field is
	// the zero value (Strong), while the collection default resolves to Bounded.
	req := &milvuspb.SearchRequest{
		ConsistencyLevel:      commonpb.ConsistencyLevel_Strong,
		UseDefaultConsistency: true,
	}
	c := newRESTTestContext(t)

	// task PreExecute resolves the actual level and records it on the REST access info
	SetActualConsistencyLevel(c.Request.Context(), commonpb.ConsistencyLevel_Bounded)

	helper := NewConsistencyLevelHelper(c.Request.Context(), req)
	assert.Equal(t, "ACT-"+commonpb.ConsistencyLevel_Bounded.String(), helper.String())
	assert.NotEqual(t, "REQ-"+commonpb.ConsistencyLevel_Strong.String(), helper.String())
}
