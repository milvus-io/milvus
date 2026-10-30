/*
 * # Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rerank

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

func (s *RerankModelSuite) TestSGLangProviderProtocol() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Equal(http.MethodPost, r.Method)
		s.Equal("/v1/rerank", r.URL.Path)
		s.Equal("Bearer mock", r.Header.Get("Authorization"))

		var request struct {
			Query           string   `json:"query"`
			Documents       []string `json:"documents"`
			ReturnDocuments bool     `json:"return_documents"`
		}
		s.Require().NoError(json.NewDecoder(r.Body).Decode(&request))
		s.Equal("query", request.Query)
		s.Equal([]string{"first", "second", "third"}, request.Documents)
		s.False(request.ReturnDocuments)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"index":2,"score":0.3},{"index":0,"score":0.1},{"index":1,"score":0.2}]`))
	}))
	defer ts.Close()

	provider, err := newSGLangProvider([]*commonpb.KeyValuePair{
		{Key: models.CredentialParamKey, Value: "mock"},
	}, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}))
	s.Require().NoError(err)

	scores, err := provider.Rerank(context.Background(), "query", []string{"first", "second", "third"})
	s.Require().NoError(err)
	s.Equal([]float32{0.1, 0.2, 0.3}, scores)
}

func (s *RerankModelSuite) TestSGLangProviderHonorsTimeoutAndCancellation() {
	for _, tc := range []struct {
		name    string
		timeout int64
		cancel  bool
		err     error
	}{
		{name: "timeout", timeout: 20, err: context.DeadlineExceeded},
		{name: "cancellation", timeout: 1000, cancel: true, err: context.Canceled},
	} {
		s.Run(tc.name, func() {
			started := make(chan struct{})
			aborted := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				<-r.Context().Done()
				close(aborted)
			}))
			defer ts.Close()

			provider, err := newSGLangProvider([]*commonpb.KeyValuePair{
				{Key: models.EndpointParamKey, Value: ts.URL},
				{Key: models.TimeoutMsParamKey, Value: strconv.FormatInt(tc.timeout, 10)},
			}, nil, &credentials.Credentials{})
			s.Require().NoError(err)

			ctx := context.Background()
			var cancel context.CancelFunc
			if tc.cancel {
				ctx, cancel = context.WithCancel(ctx)
			}
			result := make(chan error, 1)
			go func() {
				_, err := provider.Rerank(ctx, "query", []string{"document"})
				result <- err
			}()

			select {
			case <-started:
			case <-time.After(time.Second):
				s.Fail("SGLang service was not called")
			}
			if cancel != nil {
				cancel()
			}

			select {
			case err := <-result:
				s.ErrorIs(err, tc.err)
			case <-time.After(time.Second):
				s.Fail("rerank call did not finish")
			}
			select {
			case <-aborted:
			case <-time.After(time.Second):
				s.Fail("SGLang request context was not canceled")
			}
		})
	}
}
