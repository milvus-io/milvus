/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package rerank

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

// rerankPair is one (index, score) pair as a rerank service reports it: index is
// the position of the document in the request, not the position of the result in
// the response.
type rerankPair struct {
	index int
	score float32
}

func renderItems(pairs []rerankPair, indexKey string, scoreKey string) string {
	items := make([]string, 0, len(pairs))
	for _, p := range pairs {
		items = append(items, fmt.Sprintf(`{"%s":%d,"%s":%v}`, indexKey, p.index, scoreKey, p.score))
	}
	return "[" + strings.Join(items, ",") + "]"
}

// rerankProviderCase describes one provider: how its service renders a response
// and how to build the provider against a test server.
type rerankProviderCase struct {
	name        string
	body        func(pairs []rerankPair) string
	newProvider func(url string) (ModelProvider, error)
}

func rerankProviderCases() []rerankProviderCase {
	creds := func() *credentials.Credentials {
		return credentials.NewCredentials(map[string]string{"mock.apikey": "mock"})
	}
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}
	namedParams := []*commonpb.KeyValuePair{
		{Key: models.CredentialParamKey, Value: "mock"},
		{Key: models.ModelNameParamKey, Value: "mock-model"},
	}
	endpointParams := func(url string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.EndpointParamKey, Value: url},
		}
	}
	return []rerankProviderCase{
		{
			name: "ali",
			body: func(pairs []rerankPair) string {
				return `{"output":{"results":` + renderItems(pairs, "index", "relevance_score") + `}}`
			},
			newProvider: func(url string) (ModelProvider, error) {
				return newAliProvider(namedParams, map[string]string{models.URLParamKey: url}, creds(), extraInfo)
			},
		},
		{
			name: "cohere",
			body: func(pairs []rerankPair) string {
				return `{"results":` + renderItems(pairs, "index", "relevance_score") + `}`
			},
			newProvider: func(url string) (ModelProvider, error) {
				return newCohereProvider(namedParams, map[string]string{models.URLParamKey: url}, creds(), extraInfo)
			},
		},
		{
			name: "siliconflow",
			body: func(pairs []rerankPair) string {
				return `{"results":` + renderItems(pairs, "index", "relevance_score") + `}`
			},
			newProvider: func(url string) (ModelProvider, error) {
				return newSiliconflowProvider(namedParams, map[string]string{models.URLParamKey: url}, creds(), extraInfo)
			},
		},
		{
			name: "voyageai",
			body: func(pairs []rerankPair) string {
				return `{"data":` + renderItems(pairs, "index", "relevance_score") + `}`
			},
			newProvider: func(url string) (ModelProvider, error) {
				return newVoyageaiProvider(namedParams, map[string]string{models.URLParamKey: url}, creds(), extraInfo)
			},
		},
		{
			name: "vllm",
			body: func(pairs []rerankPair) string {
				return `{"results":` + renderItems(pairs, "index", "relevance_score") + `}`
			},
			newProvider: func(url string) (ModelProvider, error) {
				return newVllmProvider(endpointParams(url), nil, creds())
			},
		},
		{
			name: "tei",
			body: func(pairs []rerankPair) string {
				return renderItems(pairs, "index", "score")
			},
			newProvider: func(url string) (ModelProvider, error) {
				return newTeiProvider(endpointParams(url), nil, creds())
			},
		},
	}
}

// TestRerankScoresFollowResultIndex pins the contract between a rerank service
// and a ModelProvider: a score belongs to the document named by the result's
// index, and a response that does not answer for every document exactly once is
// an error rather than a silent zero score on the unanswered documents.
func (s *RerankModelSuite) TestRerankScoresFollowResultIndex() {
	docs := []string{"d0", "d1", "d2"}
	cases := []struct {
		name     string
		results  []rerankPair
		expected []float32
		errMsg   string
	}{
		{
			name:     "complete response in request order",
			results:  []rerankPair{{0, 0.1}, {1, 0.2}, {2, 0.3}},
			expected: []float32{0.1, 0.2, 0.3},
		},
		{
			name:     "complete response in relevance order",
			results:  []rerankPair{{2, 0.3}, {0, 0.1}, {1, 0.2}},
			expected: []float32{0.1, 0.2, 0.3},
		},
		{
			name:    "partial response leaves documents unscored",
			results: []rerankPair{{1, 0.2}, {2, 0.3}},
			errMsg:  "the number of docs and scores does not match docs:[3], scores:[2]",
		},
		{
			name:    "duplicated result index",
			results: []rerankPair{{0, 0.1}, {0, 0.9}, {1, 0.2}},
			errMsg:  "invalid or duplicated result index",
		},
		{
			name:    "result index out of range",
			results: []rerankPair{{0, 0.1}, {1, 0.2}, {7, 0.3}},
			errMsg:  "invalid or duplicated result index",
		},
	}

	for _, provider := range rerankProviderCases() {
		for _, tc := range cases {
			s.Run(provider.name+"/"+tc.name, func() {
				body := provider.body(tc.results)
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(body))
				}))
				defer ts.Close()

				p, err := provider.newProvider(ts.URL)
				s.Require().NoError(err)
				scores, err := p.Rerank(context.Background(), "q", docs)
				if tc.errMsg != "" {
					s.Require().Error(err)
					s.Contains(err.Error(), tc.errMsg)
					return
				}
				s.Require().NoError(err)
				s.Equal(tc.expected, scores)
			})
		}
	}
}

// TestRerankRejectsOversizedResponse covers the other end of the same contract:
// a service answering with more results than there were documents must produce
// an error, not an out-of-range write.
func (s *RerankModelSuite) TestRerankRejectsOversizedResponse() {
	docs := []string{"d0", "d1"}
	results := []rerankPair{{0, 0.1}, {1, 0.2}, {2, 0.3}}

	for _, provider := range rerankProviderCases() {
		s.Run(provider.name, func() {
			body := provider.body(results)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(body))
			}))
			defer ts.Close()

			p, err := provider.newProvider(ts.URL)
			s.Require().NoError(err)
			_, err = p.Rerank(context.Background(), "q", docs)
			s.Require().Error(err)
			s.Contains(err.Error(), "the number of docs and scores does not match docs:[2], scores:[3]")
		})
	}
}
