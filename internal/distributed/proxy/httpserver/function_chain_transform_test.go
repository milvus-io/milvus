// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRESTTransformFieldsAreNotSilentlyDiscarded(t *testing.T) {
	for _, body := range []string{
		`{"schema":{"functionChains":[{"stage":"FunctionChainStageIngestion"}]}}`,
		`{"schema":{"function_chains":[]}}`,
		`{"function_chains":[{"stage":"FunctionChainStagePreProcess"}]}`,
		`{"search":[{"function_chains":[]}]}`,
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Set(gin.BodyBytesKey, []byte(body))
		require.ErrorIs(t, rejectIgnoredRESTChainFields(c, false), merr.ErrOperationNotSupported)
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(gin.BodyBytesKey, []byte(`{"functionChains":[{"stage":"FunctionChainStageL2Rerank"}],"data":[{"function_chains":"ordinary JSON payload"}]}`))
	require.NoError(t, rejectIgnoredRESTChainFields(c, true), "valid rerank camelCase request and ordinary JSON payload remain supported")
	for _, stage := range []string{"FunctionChainStageIngestion", "FunctionChainStagePreProcess", "FunctionChainStagePostProcess"} {
		_, err := genFunctionChains([]FunctionChainReq{{Stage: stage}})
		require.ErrorIs(t, err, merr.ErrOperationNotSupported)
		require.Equal(t, merr.InputError, merr.GetErrorType(err))
	}
}

func TestRESTInsertCannotBypassCollectionIngestion(t *testing.T) {
	require.NoError(t, rejectRESTIngestion(&schemapb.CollectionSchema{}))
	err := rejectRESTIngestion(&schemapb.CollectionSchema{FunctionChains: []*schemapb.FunctionChain{{Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion}}})
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	require.Equal(t, merr.InputError, merr.GetErrorType(err))
}

func TestRESTV1SearchRejectsIgnoredTransformationBeforeProxy(t *testing.T) {
	for _, key := range []string{"functionChains", "function_chains"} {
		t.Run(key, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/vector/search", strings.NewReader(`{"collectionName":"docs","vector":[1,2],"`+key+`":[{"stage":"FunctionChainStagePreProcess"}]}`))
			c.Request.Header.Set("Content-Type", "application/json")
			handler := &HandlersV1{} // Any attempted Proxy invocation fails this regression.
			handler.search(c)
			require.True(t, c.IsAborted())
			require.Contains(t, recorder.Body.String(), "unsupported")
			_, proxyRequestBuilt := c.Get(ContextRequest)
			require.False(t, proxyRequestBuilt)
		})
	}
}
