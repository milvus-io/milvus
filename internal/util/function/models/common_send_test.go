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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSendDoesNotFollowRedirect(t *testing.T) {
	for _, code := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		var targetHits atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			targetHits.Add(1)
			_, _ = w.Write([]byte(`{}`))
		}))
		var sourceHits atomic.Int32
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sourceHits.Add(1)
			http.Redirect(w, r, target.URL+"/internal", code)
		}))

		_, err := retrySend(context.Background(), []byte(`{"k":"v"}`), http.MethodPost, source.URL, map[string]string{"Authorization": "Bearer secret"}, 1)
		require.Error(t, err, code)
		// a redirect is classified as permanent
		require.ErrorIs(t, err, merr.ErrFunctionFailed, code)
		require.Equal(t, int32(1), sourceHits.Load(), code)
		require.Equal(t, int32(0), targetHits.Load(), code)
		source.Close()
		target.Close()
	}
}

func TestSendDoesNotReturnErrorBody(t *testing.T) {
	const canary = "UPSTREAM-BODY-CANARY"
	const reason = "REASON-PHRASE-CANARY"
	for _, code := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusMethodNotAllowed} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(canary))
		}))
		_, err := retrySend(context.Background(), []byte(`{}`), http.MethodPost, server.URL, nil, 1)
		require.Error(t, err, code)
		require.ErrorIs(t, err, merr.ErrFunctionFailed, code)
		require.NotContains(t, err.Error(), canary, code)
		require.Contains(t, err.Error(), http.StatusText(code), code)
		server.Close()
	}

	// a custom reason phrase is not returned either, and 5xx stays retriable
	var hits atomic.Int32
	raw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buf.WriteString("HTTP/1.1 502 " + reason + "\r\nContent-Length: 20\r\nConnection: close\r\n\r\n" + canary)
		_ = buf.Flush()
	}))
	defer raw.Close()
	_, err := retrySend(context.Background(), []byte(`{}`), http.MethodPost, raw.URL, nil, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.NotContains(t, err.Error(), canary)
	require.NotContains(t, err.Error(), reason)
	require.Contains(t, err.Error(), "502")
	require.Equal(t, int32(1), hits.Load())
}
