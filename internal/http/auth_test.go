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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

// setVerifyFunc swaps the package-level passwordVerifyFunc for a test, returning
// a cleanup that restores the previous value. Sequential tests must use this
// to avoid leaking verifier state between cases.
func setVerifyFunc(t *testing.T, fn func(ctx context.Context, username, password string) bool) {
	t.Helper()
	prevPrimary, prevFallback := passwordVerifyFunc, fallbackPasswordVerifyFunc
	passwordVerifyFunc = fn
	// Clear the fallback too: getPasswordVerifyFunc falls through to it, so a
	// leftover fallback from another test would mask a nil primary and turn the
	// "no verifier on this node" case into a silent pass.
	fallbackPasswordVerifyFunc = nil
	t.Cleanup(func() {
		passwordVerifyFunc, fallbackPasswordVerifyFunc = prevPrimary, prevFallback
	})
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

func TestWrapAdminAuth_NilPolicyPassThrough(t *testing.T) {
	// A nil policy means "no auth required" — the wrapper should return the
	// inner handler unchanged. This guards every endpoint that registers with
	// AuthPolicy: nil (healthz, metrics, etc.).
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "handler must be invoked when policy is nil")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_PolicyFalseSkipsCheck(t *testing.T) {
	// Policy returning false means "this request doesn't require auth right
	// now" — the wrapped handler runs even without credentials.
	setVerifyFunc(t, func(context.Context, string, string) bool {
		t.Fatal("verifier must not be called when policy returns false")
		return false
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), func() bool { return false })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_NoCredentialsReturns401(t *testing.T) {
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called, "handler must NOT be invoked when auth fails")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "authentication required")
}

func TestWrapAdminAuth_NonRootUserReturns403(t *testing.T) {
	// passwordVerifyFunc must NOT be consulted for non-root usernames — the
	// wrapper short-circuits before calling it. This prevents leaking timing
	// differences between "user exists with wrong password" and "user doesn't
	// exist", and also makes the auth gate cheaper.
	//
	// The status is 403, not 401: retrying with different credentials for this
	// username will never succeed, so telling the caller "unauthenticated"
	// would invite a pointless retry loop.
	setVerifyFunc(t, func(context.Context, string, string) bool {
		t.Fatal("verifier must not be called for non-root usernames")
		return false
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("alice", "anything")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "only root")
}

func TestWrapAdminAuth_WrongPasswordReturns401(t *testing.T) {
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "correct-horse"
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "wrong")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid root password")
}

func TestWrapAdminAuth_ValidRootCredentialsPass(t *testing.T) {
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "correct-horse"
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "handler must run when credentials are valid")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_VerifierUnavailableReturns503(t *testing.T) {
	// No verifier at all means this node cannot judge the credential. The
	// wrapper must fail closed rather than allow access — but report 503, not
	// 401: the password may well be correct, and answering "unauthorized" would
	// send operators hunting a credential problem that does not exist.
	setVerifyFunc(t, nil)

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "anything")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "not available")
}

func TestWrapAdminAuth_NoWWWAuthenticateHeader(t *testing.T) {
	// We deliberately do not set WWW-Authenticate: Basic on 401 responses —
	// callers are API clients, not browsers, and we don't want to trigger
	// the browser's basic-auth login prompt.
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, rec.Header().Get("WWW-Authenticate"))
}

func TestWrapAdminAuth_JSONBodyShape(t *testing.T) {
	// The 401 body shape is part of the contract with API clients — keep it
	// as a JSON object with a single "msg" string field so clients can parse.
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.True(t, strings.HasPrefix(body, `{"msg":`), "body should start with JSON: %s", body)
	assert.True(t, strings.HasSuffix(strings.TrimRight(body, "\n"), `"}`), "body should end with quote+brace: %s", body)
}

func TestGetPasswordVerifyFunc_PrimaryWinsOverFallback(t *testing.T) {
	// Standalone runs proxy, mix coord and the worker nodes in one process, so
	// all three register a verifier into the same globals. The primary slot
	// (in-process credential lookup) must win deterministically, otherwise the
	// winner would depend on goroutine scheduling and standalone would
	// sometimes pay an RPC to verify a password it can check locally.
	setVerifyFunc(t, func(_ context.Context, _, password string) bool {
		return password == "primary-accepts"
	})
	RegisterFallbackPasswordVerifyFunc(func(_ context.Context, _, _ string) error {
		t.Fatal("fallback verifier must not be consulted while a primary exists")
		return nil
	})

	assert.NoError(t, verifyPassword(context.Background(), "root", "primary-accepts", "/test"))
	assert.True(t, IsAuthenticationError(
		verifyPassword(context.Background(), "root", "fallback-accepts", "/test")))
}

func TestGetPasswordVerifyFunc_FallbackUsedWhenNoPrimary(t *testing.T) {
	// On a worker node nothing registers a primary verifier; the fallback is
	// what keeps /management/* and /debug/pprof reachable by root once
	// adminAuthEnabled is on.
	setVerifyFunc(t, nil)
	RegisterFallbackPasswordVerifyFunc(func(_ context.Context, _, password string) error {
		if password == "fallback-accepts" {
			return nil
		}
		return ErrInvalidCredential
	})

	assert.NoError(t, verifyPassword(context.Background(), "root", "fallback-accepts", "/test"))
	assert.True(t, IsAuthenticationError(
		verifyPassword(context.Background(), "root", "nope", "/test")))
}

func TestWrapAdminAuth_FallbackVerifierAuthenticates(t *testing.T) {
	// End-to-end through the wrapper: a worker node with only a fallback
	// verifier must admit correct root credentials rather than answering 503.
	// This is the regression the reviewer flagged — before the fallback slot
	// existed, every worker node rejected even valid root credentials.
	setVerifyFunc(t, nil)
	RegisterFallbackPasswordVerifyFunc(func(_ context.Context, username, password string) error {
		if username == "root" && password == "correct-horse" {
			return nil
		}
		return ErrInvalidCredential
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/management/stop", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "valid root credentials must pass via the fallback verifier")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_UnreachableCredentialStoreReturns503(t *testing.T) {
	// A worker node whose mix coord is down cannot judge the password. It must
	// answer 503, not 401: the operator is usually holding a correct password
	// and debugging a half-down cluster, and "invalid root password" would send
	// them chasing a credential problem that does not exist.
	//
	// This is a regression test for real behavior observed against a live
	// cluster: with the verifier returning a bare bool, an unreachable coord was
	// indistinguishable from a wrong password and surfaced as 401.
	setVerifyFunc(t, nil)
	RegisterFallbackPasswordVerifyFunc(func(_ context.Context, _, _ string) error {
		return errors.New("mix coord client unavailable: connection refused")
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), AuthAlways)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotContains(t, rec.Body.String(), "invalid root password")
}

func TestWrapAdminAuth_ErrorBodyLeaksNoInternals(t *testing.T) {
	// The 503 body is returned to a caller who has not authenticated, so it must
	// not carry the underlying cause. merr and cockroachdb/errors render wrapped
	// errors with a stack trace containing absolute build paths; a live cluster
	// returned exactly that before this was fixed.
	setVerifyFunc(t, nil)
	RegisterFallbackPasswordVerifyFunc(func(_ context.Context, _, _ string) error {
		return errors.Wrap(
			errors.New("stack trace: /home/builder/milvus/pkg/tracer/stack_trace.go:51"),
			"GetCredential failed")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapAdminAuth((&invoked{}).handler(), AuthAlways).ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotContains(t, body, "stack trace")
	assert.NotContains(t, body, "/home/builder")
	assert.NotContains(t, body, "GetCredential")
	assert.Contains(t, body, "credential store is unreachable")
}
