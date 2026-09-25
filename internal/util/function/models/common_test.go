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

package models

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CommonSuite struct {
	suite.Suite
}

func TestCommon(t *testing.T) {
	suite.Run(t, new(CommonSuite))
}

func (s *CommonSuite) TestParseAKAndURL() {
	{
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "")
	}
	{
		os.Setenv(OpenaiAKEnvStr, "TEST")
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "TEST")
		os.Unsetenv(OpenaiAKEnvStr)
	}
	{
		os.Setenv("MILVUSAI_OPENAI_API_KEY", "OLD_TEST")
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "OLD_TEST")
		os.Unsetenv("MILVUSAI_OPENAI_API_KEY")
	}
	{
		os.Setenv(OpenaiAKEnvStr, "TEST")
		os.Setenv("MILVUSAI_OPENAI_API_KEY", "OLD_TEST")
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "TEST")
		os.Unsetenv("MILVUSAI_OPENAI_API_KEY")
		os.Unsetenv(OpenaiAKEnvStr)
	}
	{
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{{Key: "integration_id", Value: "test-integration"}}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "test-integration|test-cluster|test-db")
	}
}

func (s *CommonSuite) TestParseTimeoutMs() {
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{}, 45))

	s.Equal(int64(90), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "90"}}, 45))

	// an invalid override falls back to the default instead of failing
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "invalid"}}, 45))

	// non-positive overrides fall back to the default
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "0"}}, 45))
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "-1"}}, 45))

	// a non-positive default falls back to 30000
	s.Equal(int64(30000), ParseTimeoutMs([]*commonpb.KeyValuePair{}, 0))

	// param key match is case-insensitive
	s.Equal(int64(120), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: "Timeout_MS", Value: "120"}}, 45))
}

func (s *CommonSuite) TestResolveTimeoutMs() {
	paramtable.Init()
	params := paramtable.Get()

	// falls back to the global function model timeout when no param is set
	params.Save(params.FunctionCfg.ModelRequestTimeout.Key, "12s")
	defer params.Reset(params.FunctionCfg.ModelRequestTimeout.Key)
	s.Equal(int64(12000), ResolveTimeoutMs([]*commonpb.KeyValuePair{}))

	// per-function param overrides the global default
	s.Equal(int64(777), ResolveTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "777"}}))
}

// TestRetrySendClassification pins retrySend to the classification send()
// already makes: only transient failures (429, 5xx, transport errors) are
// worth another attempt. A permanent 4xx -- a bad credential, a rejected
// request body -- must surface on the first attempt instead of being replayed
// against the model service.
func (s *CommonSuite) TestRetrySendClassification() {
	cases := []struct {
		name             string
		status           int
		expectedAttempts int
		retryable        bool
	}{
		{name: "bad request is permanent", status: http.StatusBadRequest, expectedAttempts: 1, retryable: false},
		{name: "unauthorized is permanent", status: http.StatusUnauthorized, expectedAttempts: 1, retryable: false},
		{name: "forbidden is permanent", status: http.StatusForbidden, expectedAttempts: 1, retryable: false},
		{name: "not found is permanent", status: http.StatusNotFound, expectedAttempts: 1, retryable: false},
		{name: "payload too large is permanent", status: http.StatusRequestEntityTooLarge, expectedAttempts: 1, retryable: false},
		{name: "too many requests is transient", status: http.StatusTooManyRequests, expectedAttempts: 3, retryable: true},
		{name: "internal server error is transient", status: http.StatusInternalServerError, expectedAttempts: 3, retryable: true},
		{name: "bad gateway is transient", status: http.StatusBadGateway, expectedAttempts: 3, retryable: true},
		{name: "service unavailable is transient", status: http.StatusServiceUnavailable, expectedAttempts: 3, retryable: true},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			attempts := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts++
				w.WriteHeader(c.status)
				w.Write([]byte(`{"error": "boom"}`))
			}))
			defer ts.Close()

			// A context deadline shorter than the 1s+2s backoff would mask the
			// difference, so give the retrying cases room to finish.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			_, err := retrySend(ctx, []byte(`{}`), http.MethodPost, ts.URL, nil, 3)
			s.Error(err)
			s.Equal(c.expectedAttempts, attempts)
			s.Equal(c.retryable, merr.IsRetryableErr(err))
		})
	}
}

// TestRetrySendRetriesUntilSuccess keeps the transient path intact: a request
// that fails once with a retryable status still succeeds on the next attempt.
func (s *CommonSuite) TestRetrySendRetriesUntilSuccess() {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"ok": true}`))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := retrySend(ctx, []byte(`{}`), http.MethodPost, ts.URL, nil, 3)
	s.NoError(err)
	s.Equal(2, attempts)
	s.Equal(`{"ok": true}`, string(body))
}
