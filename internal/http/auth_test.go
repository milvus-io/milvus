// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// setVerifyFunc swaps the package-level passwordVerifyFunc for a test, returning
// a cleanup that restores the previous value. Sequential tests must use this
// to avoid leaking verifier state between cases.
func setVerifyFunc(t *testing.T, fn func(ctx context.Context, username, password string) bool) {
	t.Helper()
	prev := passwordVerifyFunc
	passwordVerifyFunc = fn
	t.Cleanup(func() { passwordVerifyFunc = prev })
}

// invoked records whether the wrapped handler was reached.
type invoked struct{ called bool }

func (i *invoked) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i.called = true
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"msg":"ok"}`)
	})
}

func TestWrapBasicRootAuth_NilPolicyPassThrough(t *testing.T) {
	// A nil policy means "no auth required" — the wrapper should return the
	// inner handler unchanged. This guards every endpoint that registers with
	// AuthPolicy: nil (healthz, metrics, etc.).
	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "handler must be invoked when policy is nil")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapBasicRootAuth_PolicyFalseSkipsCheck(t *testing.T) {
	// Policy returning false means "this request doesn't require auth right
	// now" — the wrapped handler runs even without credentials.
	setVerifyFunc(t, func(context.Context, string, string) bool {
		t.Fatal("verifier must not be called when policy returns false")
		return false
	})

	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), func() bool { return false })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapBasicRootAuth_NoCredentialsReturns401(t *testing.T) {
	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called, "handler must NOT be invoked when auth fails")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "authentication required")
}

func TestWrapBasicRootAuth_NonRootUserReturns401(t *testing.T) {
	// passwordVerifyFunc must NOT be consulted for non-root usernames — the
	// wrapper short-circuits to 401 before calling it. This prevents leaking
	// timing differences between "user exists with wrong password" and
	// "user doesn't exist", and also makes the auth gate cheaper.
	setVerifyFunc(t, func(context.Context, string, string) bool {
		t.Fatal("verifier must not be called for non-root usernames")
		return false
	})

	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("alice", "anything")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "only root")
}

func TestWrapBasicRootAuth_WrongPasswordReturns401(t *testing.T) {
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "correct-horse"
	})

	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "wrong")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid root password")
}

func TestWrapBasicRootAuth_ValidRootCredentialsPass(t *testing.T) {
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "correct-horse"
	})

	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "handler must run when credentials are valid")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapBasicRootAuth_VerifierUnavailable(t *testing.T) {
	// passwordVerifyFunc may legitimately be nil on a node that doesn't host
	// the proxy package and hasn't wired in its own verifier (e.g. on early
	// 2.5 deployments where mix coord didn't register a verifier). The wrapper
	// must fail closed in this case rather than silently allow access.
	setVerifyFunc(t, nil)

	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "anything")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "not available")
}

func TestWrapBasicRootAuth_NoWWWAuthenticateHeader(t *testing.T) {
	// We deliberately do not set WWW-Authenticate: Basic on 401 responses —
	// callers are API clients, not browsers, and we don't want to trigger
	// the browser's basic-auth login prompt.
	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, rec.Header().Get("WWW-Authenticate"))
}

func TestWrapBasicRootAuth_JSONBodyShape(t *testing.T) {
	// The 401 body shape is part of the contract with API clients — keep it
	// as a JSON object with a single "msg" string field so clients can parse.
	inv := &invoked{}
	wrapped := wrapBasicRootAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.True(t, strings.HasPrefix(body, `{"msg":`), "body should start with JSON: %s", body)
	assert.True(t, strings.HasSuffix(strings.TrimRight(body, "\n"), `"}`), "body should end with quote+brace: %s", body)
}
