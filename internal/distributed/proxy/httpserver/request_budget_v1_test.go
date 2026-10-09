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
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
)

type v1BudgetContextProbe struct {
	types.ProxyComponent
	deadline time.Time
}

func (p *v1BudgetContextProbe) ShowCollections(ctx context.Context, _ *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	p.deadline, _ = ctx.Deadline()
	return &milvuspb.ShowCollectionsResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
}

func TestV1RequestDeadlineReachesProxy(t *testing.T) {
	probe := &v1BudgetContextProbe{}
	h := &HandlersV1{proxy: probe}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	deadline := time.Now().Add(time.Minute)
	requestCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/vector/collections", nil).WithContext(requestCtx)
	c.Set(ContextUsername, "root")
	h.listCollections(c)
	if probe.deadline.IsZero() || !probe.deadline.Equal(deadline) {
		t.Fatalf("Proxy received deadline %v, want %v", probe.deadline, deadline)
	}
}
