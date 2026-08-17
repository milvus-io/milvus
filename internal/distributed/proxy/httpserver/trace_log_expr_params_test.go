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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proxy"
)

// A RESTful request binds its expression templates through exprParams, which is
// a plain Go map rather than a protobuf field -- so the gRPC-side elision does
// not reach it. The trace log must still show only which placeholders were
// bound, never the values (a membership filter ships a multi-MiB blob there).
func TestTraceLogRedactsExprParams(t *testing.T) {
	const secret = "918273645546372819"

	redact := func(req any) any {
		return getTraceLogRequestFieldWithoutSensitiveInfo(req).Interface
	}
	assertAllRedacted := func(t *testing.T, params ...map[string]interface{}) {
		t.Helper()
		seen := 0
		for _, m := range params {
			for name, v := range m {
				assert.Equal(t, proxy.RedactedValue, v, "exprParams[%s]", name)
				seen++
			}
		}
		assert.Positive(t, seen, "expected at least one redacted parameter")
	}

	t.Run("query", func(t *testing.T) {
		out := redact(&QueryReqV2{
			CollectionName: "c",
			Filter:         "roaring_match(id, {ids})",
			ExprParams:     map[string]interface{}{"ids": secret},
		}).(*QueryReqV2)
		assertAllRedacted(t, out.ExprParams)
		// The placeholder name and the expression text stay visible.
		assert.Contains(t, out.ExprParams, "ids")
		assert.Equal(t, "roaring_match(id, {ids})", out.Filter)
	})

	t.Run("delete by filter", func(t *testing.T) {
		out := redact(&CollectionFilterReq{
			CollectionName: "c",
			Filter:         "roaring_match(id, {ids})",
			ExprParams:     map[string]interface{}{"ids": secret},
		}).(*CollectionFilterReq)
		assertAllRedacted(t, out.ExprParams)
	})

	t.Run("search", func(t *testing.T) {
		out := redact(&SearchReqV2{
			CollectionName: "c",
			Filter:         "roaring_match(id, {ids})",
			ExprParams:     map[string]interface{}{"ids": secret},
		}).(*SearchReqV2)
		assertAllRedacted(t, out.ExprParams)
	})

	t.Run("hybrid search sub-requests", func(t *testing.T) {
		out := redact(&HybridSearchReq{
			CollectionName: "c",
			Search: []SubSearchReq{
				{Filter: "roaring_match(id, {ids})", ExprParams: map[string]interface{}{"ids": secret}},
				{Filter: "id > 0"},
			},
		}).(*HybridSearchReq)
		assertAllRedacted(t, out.Search[0].ExprParams)
		// A sub-request without exprParams is untouched.
		assert.Empty(t, out.Search[1].ExprParams)
		assert.Equal(t, "id > 0", out.Search[1].Filter)
	})

	// Redacting must not mutate the caller's request: it is logged before being
	// handled, and the handler still needs the real values.
	t.Run("the original request is not modified", func(t *testing.T) {
		hybrid := &HybridSearchReq{
			Search: []SubSearchReq{{Filter: "f", ExprParams: map[string]interface{}{"ids": secret}}},
		}
		_ = redact(hybrid)
		require.Equal(t, secret, hybrid.Search[0].ExprParams["ids"])

		q := &QueryReqV2{ExprParams: map[string]interface{}{"ids": secret}}
		_ = redact(q)
		require.Equal(t, secret, q.ExprParams["ids"])
	})

	t.Run("a request without exprParams is unaffected", func(t *testing.T) {
		req := &QueryReqV2{CollectionName: "c", Filter: "id > 0"}
		assert.Same(t, req, redact(req))
	})
}
