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

package vertexai

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type oauthContextFixture struct {
	client        *VertexAIEmbedding
	modelURL      string
	tokenCalls    atomic.Int32
	modelCalls    atomic.Int32
	tokenStarted  chan struct{}
	tokenCanceled chan struct{}
	releaseToken  func()
}

// Exercise the actual service-account JWT TokenSource, rather than supplying
// the mock access token used by the existing model client tests.
func newOAuthContextFixture(t *testing.T, blockToken bool, partialResponse ...bool) *oauthContextFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	f := &oauthContextFixture{
		tokenStarted:  make(chan struct{}),
		tokenCanceled: make(chan struct{}),
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	f.releaseToken = func() { releaseOnce.Do(func() { close(release) }) }
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume the request body so the server can observe connection closure.
		_, _ = io.Copy(io.Discard, r.Body)
		f.tokenCalls.Add(1)
		if len(partialResponse) > 0 && partialResponse[0] {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":`)
			w.(http.Flusher).Flush()
		}
		close(f.tokenStarted)
		if blockToken {
			select {
			case <-r.Context().Done():
				close(f.tokenCanceled)
				return
			case <-release:
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"context-test-token","token_type":"Bearer","expires_in":3600}`)
	}))
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.modelCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer context-test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// Each endpoint ignores the response field belonging to the other API.
		_, _ = io.WriteString(w, `{"predictions":[{"embeddings":{"values":[1,2]}}],"embedding":{"values":[1,2]}}`)
	}))
	t.Cleanup(func() {
		// Also release an uncancelable baseline request on any assertion failure.
		f.releaseToken()
		tokenServer.Close()
		modelServer.Close()
	})
	credentialsJSON, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "context-test-project",
		"private_key_id": "context-test-key",
		"private_key":    string(keyPEM),
		"client_email":   "context-test@context-test-project.iam.gserviceaccount.com",
		"token_uri":      tokenServer.URL,
	})
	require.NoError(t, err)
	f.modelURL = modelServer.URL
	f.client = NewVertexAIEmbedding(modelServer.URL, credentialsJSON, "https://www.googleapis.com/auth/cloud-platform", "")
	return f
}

func oauthContextCalls() []struct {
	name string
	call func(context.Context, *oauthContextFixture) (any, error)
} {
	return []struct {
		name string
		call func(context.Context, *oauthContextFixture) (any, error)
	}{
		{
			name: "embedding",
			call: func(ctx context.Context, f *oauthContextFixture) (any, error) {
				return f.client.Embedding(ctx, "text-embedding-005", []string{"text"}, 2, "RETRIEVAL_QUERY", 10000)
			},
		},
		{
			name: "gemini",
			call: func(ctx context.Context, f *oauthContextFixture) (any, error) {
				return f.client.GeminiEmbedding(ctx, f.modelURL, "text", 2, "RETRIEVAL_QUERY", 10000)
			},
		},
	}
}

func TestVertexAIOAuthCanceledBeforeRequest(t *testing.T) {
	for _, tc := range oauthContextCalls() {
		t.Run(tc.name, func(t *testing.T) {
			f := newOAuthContextFixture(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := tc.call(ctx, f)
			require.True(t, errors.Is(err, context.Canceled), "got %v", err)
			require.Zero(t, f.tokenCalls.Load(), "canceled callers must not start OAuth requests")
			require.Zero(t, f.modelCalls.Load())
		})
	}
}

func TestVertexAIOAuthInFlightCancellation(t *testing.T) {
	for _, tc := range oauthContextCalls() {
		for _, deadline := range []bool{false, true} {
			name := "cancel"
			if deadline {
				name = "parent_deadline"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				f := newOAuthContextFixture(t, true)
				ctx, cancel := context.WithCancel(context.Background())
				expectedErr := context.Canceled
				if deadline {
					cancel()
					// Generate the RSA key and start the servers before starting the
					// deadline; synchronize cancellation with the actual token request.
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					expectedErr = context.DeadlineExceeded
				}
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := tc.call(ctx, f)
					done <- err
				}()
				select {
				case <-f.tokenStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("OAuth request did not start")
				}
				if !deadline {
					cancel()
				}
				<-ctx.Done()

				var err error
				stoppedBeforeRelease := false
				select {
				case err = <-done:
					stoppedBeforeRelease = true
				case <-time.After(time.Second):
					// Unblock the old implementation so this regression fails
					// promptly without leaving an HTTP handler or goroutine stuck.
					f.releaseToken()
					select {
					case err = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("OAuth call did not stop after cleanup")
					}
				}
				require.True(t, stoppedBeforeRelease, "OAuth request ignored %v", expectedErr)
				require.True(t, errors.Is(err, expectedErr), "got %v", err)
				require.EqualValues(t, 1, f.tokenCalls.Load())
				require.Zero(t, f.modelCalls.Load())
				select {
				case <-f.tokenCanceled:
				case <-time.After(time.Second):
					t.Fatal("token endpoint did not observe cancellation")
				}
			})
		}
	}
}

func TestVertexAIOAuthSuccessfulExchange(t *testing.T) {
	for _, tc := range oauthContextCalls() {
		t.Run(tc.name, func(t *testing.T) {
			f := newOAuthContextFixture(t, false)
			response, err := tc.call(context.Background(), f)
			require.NoError(t, err)
			require.EqualValues(t, 1, f.tokenCalls.Load())
			require.EqualValues(t, 1, f.modelCalls.Load())
			switch r := response.(type) {
			case *EmbeddingResponse:
				require.Len(t, r.Predictions, 1)
				require.Equal(t, []float32{1, 2}, r.Predictions[0].Embeddings.Values)
			case *GeminiEmbedContentResponse:
				require.Equal(t, []float32{1, 2}, r.Embedding.Values)
			default:
				t.Fatalf("unexpected response type %T", response)
			}
		})
	}
}

type oauthContextTestTransport func(*http.Request) (*http.Response, error)

func (f oauthContextTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type oauthContextTestBody struct {
	io.ReadCloser
	readStarted chan struct{}
	once        sync.Once
}

func (b *oauthContextTestBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.readStarted) })
	return b.ReadCloser.Read(p)
}

func TestVertexAIOAuthResponseBodyCancellation(t *testing.T) {
	f := newOAuthContextFixture(t, true, true)
	bodyRead := make(chan struct{})
	transport := oauthContextTestTransport(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err == nil {
			resp.Body = &oauthContextTestBody{ReadCloser: resp.Body, readStarted: bodyRead}
		}
		return resp, err
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: transport})
	done := make(chan error, 1)
	go func() {
		_, err := f.client.Embedding(ctx, "text-embedding-005", []string{"text"}, 2, "RETRIEVAL_QUERY", 10000)
		done <- err
	}()
	// Wait until the client has received headers and is reading the incomplete
	// JSON body. Cancellation before headers would miss this lifecycle boundary.
	select {
	case <-bodyRead:
	case <-time.After(5 * time.Second):
		t.Fatal("token response body was not read")
	}
	cancel()
	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt the token response body")
	}
	require.EqualValues(t, 1, f.tokenCalls.Load())
	require.Zero(t, f.modelCalls.Load())
	select {
	case <-f.tokenCanceled:
	case <-time.After(time.Second):
		t.Fatal("token endpoint did not observe cancellation during body reads")
	}
}

func TestVertexAIOAuthPreservesCustomHTTPClientTimeout(t *testing.T) {
	f := newOAuthContextFixture(t, false)
	cleanup := make(chan struct{})
	defer close(cleanup)
	transportErr := make(chan error, 1)
	var calls atomic.Int32
	var hasDeadline atomic.Bool
	transportFunc := oauthContextTestTransport(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		_, ok := req.Context().Deadline()
		hasDeadline.Store(ok)
		// Intentionally ignore legacy req.Cancel: preserving only that channel
		// would not protect custom transports that use the request context.
		select {
		case <-req.Context().Done():
			err := req.Context().Err()
			transportErr <- err
			return nil, err
		case <-cleanup:
			return nil, errors.New("test cleanup")
		}
	})
	transport := &transportFunc
	customClient := &http.Client{Transport: transport, Timeout: 100 * time.Millisecond}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, customClient)
	_, hasCallerDeadline := ctx.Deadline()
	require.False(t, hasCallerDeadline)
	done := make(chan error, 1)
	go func() {
		_, err := f.client.Embedding(ctx, "text-embedding-005", []string{"text"}, 2, "RETRIEVAL_QUERY", 10000)
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("custom HTTP client timeout was not retained")
	}
	require.True(t, hasDeadline.Load(), "custom transport lost the HTTP client deadline")
	requestErr := <-transportErr
	require.True(t, errors.Is(requestErr, context.DeadlineExceeded), "custom transport received %v", requestErr)
	require.NoError(t, ctx.Err(), "HTTP client timeout must not cancel its caller")
	require.EqualValues(t, 1, calls.Load())
	require.Zero(t, f.modelCalls.Load())
	// Wrapping authentication must copy the client, leaving shared settings intact.
	require.Equal(t, 100*time.Millisecond, customClient.Timeout)
	require.Same(t, transport, customClient.Transport)
}
