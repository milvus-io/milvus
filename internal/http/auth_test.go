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
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// enableAdminAuth turns the gate on for the duration of a test. The wrapper
// reads the flag per request, so tests exercise the same path production does
// rather than a stand-in policy.
func enableAdminAuth(t *testing.T) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().CommonCfg.AdminAuthEnabled.Key
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

// setVerifyFunc gives this node a proxy-shaped credential check written as the
// data plane's bool verifier, and restores every slot during cleanup.
// Sequential tests must use it to avoid leaking verifier state between cases.
func setVerifyFunc(t *testing.T, fn func(ctx context.Context, username, password string) bool) {
	t.Helper()
	var typed CredentialVerifier
	if fn != nil {
		typed = func(ctx context.Context, username, password string) error {
			if !fn(ctx, username, password) {
				return NewAuthenticationError("invalid root password")
			}
			return nil
		}
	}
	passwordVerifyMu.Lock()
	prev := managementVerifiers
	prevPrimary := passwordVerifyFunc
	// Clear every management slot: resolveManagementVerifier falls through
	// them, so a leftover verifier from another test would turn the "no
	// verifier on this node" case into a silent pass.
	managementVerifiers = [numManagementVerifierSlots]CredentialVerifier{}
	managementVerifiers[VerifierSlotProxy] = typed
	passwordVerifyFunc = fn
	passwordVerifyMu.Unlock()
	t.Cleanup(func() {
		passwordVerifyMu.Lock()
		defer passwordVerifyMu.Unlock()
		managementVerifiers, passwordVerifyFunc = prev, prevPrimary
	})
}

// setManagementVerifier installs fn in one slot and clears the others.
func setManagementVerifier(t *testing.T, slot ManagementVerifierSlot, fn CredentialVerifier) {
	t.Helper()
	passwordVerifyMu.Lock()
	prev := managementVerifiers
	managementVerifiers = [numManagementVerifierSlots]CredentialVerifier{}
	managementVerifiers[slot] = fn
	passwordVerifyMu.Unlock()
	t.Cleanup(func() {
		passwordVerifyMu.Lock()
		defer passwordVerifyMu.Unlock()
		managementVerifiers = prev
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

func TestWrapAdminAuth_GateOffSkipsCheck(t *testing.T) {
	// With the flag at its default the wrapper is inert, which is what keeps
	// an upgrade transparent.
	paramtable.Init()
	paramtable.Get().Reset(paramtable.Get().CommonCfg.AdminAuthEnabled.Key)
	setVerifyFunc(t, func(context.Context, string, string) bool {
		t.Fatal("verifier must not be called while the gate is off")
		return false
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_NoCredentialsReturns401(t *testing.T) {
	enableAdminAuth(t)
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called, "handler must NOT be invoked when auth fails")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "authentication required")
}

func TestWrapAdminAuth_NonRootUserReturns403(t *testing.T) {
	enableAdminAuth(t)
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
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("alice", "anything")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "only root")
}

func TestWrapAdminAuth_WrongPasswordReturns401(t *testing.T) {
	enableAdminAuth(t)
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "correct-horse"
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "wrong")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid root password")
}

func TestWrapAdminAuth_ValidRootCredentialsPass(t *testing.T) {
	enableAdminAuth(t)
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "correct-horse"
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "handler must run when credentials are valid")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_VerifierUnavailableReturns503(t *testing.T) {
	enableAdminAuth(t)
	// No verifier at all means this node cannot judge the credential. The
	// wrapper must fail closed rather than allow access — but report 503, not
	// 401: the password may well be correct, and answering "unauthorized" would
	// send operators hunting a credential problem that does not exist.
	setVerifyFunc(t, nil)

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.SetBasicAuth("root", "anything")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "not available")
}

func TestWrapAdminAuth_NoWWWAuthenticateHeader(t *testing.T) {
	enableAdminAuth(t)
	// API surfaces deliberately do not set WWW-Authenticate: Basic on 401 —
	// callers are API clients, not browsers, and we don't want to trigger
	// the browser's basic-auth login prompt on a JSON endpoint.
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, rec.Header().Get("WWW-Authenticate"))
}

// Browser surfaces need the opposite: without the challenge the console takes a
// 401, nothing prompts, and the operator has no way to supply credentials.
func TestWrapAdminAuth_ChallengeOnlyOn401(t *testing.T) {
	enableAdminAuth(t)
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "right"
	})

	for _, tc := range []struct {
		name         string
		user, pass   string
		wantStatus   int
		wantChalleng bool
	}{
		{name: "no credentials", wantStatus: http.StatusUnauthorized, wantChalleng: true},
		{name: "wrong password", user: "root", pass: "wrong", wantStatus: http.StatusUnauthorized, wantChalleng: true},
		// 403 must not carry a challenge: re-prompting a non-root user loops
		// the browser dialog forever on a request no password can satisfy.
		{name: "non-root user", user: "alice", pass: "right", wantStatus: http.StatusForbidden, wantChalleng: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv := &invoked{}
			wrapped := wrapAdminAuth(inv.handler(), "/test", true)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/webui/", nil)
			if tc.user != "" {
				req.SetBasicAuth(tc.user, tc.pass)
			}
			wrapped.ServeHTTP(rec, req)

			assert.False(t, inv.called)
			assert.Equal(t, tc.wantStatus, rec.Code)
			challenge := rec.Header().Get("WWW-Authenticate")
			if tc.wantChalleng {
				assert.Contains(t, challenge, `realm="`+BasicAuthRealm+`"`)
			} else {
				assert.Empty(t, challenge)
			}
		})
	}
}

// The challenge that makes the console work also makes the browser replay
// root's credential at this origin, so a page the operator visits later could
// otherwise drive management endpoints without the attacker ever reaching the
// port. Requests a browser marks as cross-site are refused before the
// credential is even looked at.
func TestWrapAdminAuth_RejectsCrossSiteRequests(t *testing.T) {
	enableAdminAuth(t)
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == "root" && password == "right"
	})

	for _, tc := range []struct {
		name       string
		document   bool // the handler is a browser page, not an API
		fetchSite  string
		fetchMode  string
		fetchDest  string
		origin     string
		wantCalled bool
	}{
		{name: "browser cross-site", fetchSite: "cross-site"},
		{name: "browser same-origin", fetchSite: "same-origin", wantCalled: true},
		{name: "browser address bar", fetchSite: "none", wantCalled: true},
		// A sibling subdomain is a different origin. Trusting it would extend
		// the management plane to whoever controls one.
		{name: "browser same-site", fetchSite: "same-site"},
		{name: "foreign origin without fetch metadata", origin: "http://evil.example"},
		{name: "matching origin without fetch metadata", origin: "http://milvus.local:9091", wantCalled: true},
		// curl, the operator's scripts and the Milvus operator send neither
		// header; they must keep working exactly as before.
		{name: "non-browser client", wantCalled: true},
		// A link to the console from a wiki or a chat message: showing a page
		// is not an action, so it is allowed — but only because the handler
		// says it is a document.
		{
			name: "cross-site link to a document surface", document: true,
			fetchSite: "cross-site", fetchMode: "navigate", fetchDest: "document",
			wantCalled: true,
		},
		// The same click aimed at a route that stops a component is not. This
		// must not depend on that route happening to send no challenge.
		{
			name:      "cross-site link to a mutating route",
			fetchSite: "cross-site", fetchMode: "navigate", fetchDest: "document",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv := &invoked{}
			wrapped := wrapAdminAuth(inv.handler(), "/test", tc.document)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/management/stop", nil)
			req.Host = "milvus.local:9091"
			req.SetBasicAuth("root", "right")
			for h, v := range map[string]string{
				"Sec-Fetch-Site": tc.fetchSite,
				"Sec-Fetch-Mode": tc.fetchMode,
				"Sec-Fetch-Dest": tc.fetchDest,
				"Origin":         tc.origin,
			} {
				if v != "" {
					req.Header.Set(h, v)
				}
			}
			wrapped.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantCalled, inv.called)
			if !tc.wantCalled {
				assert.Equal(t, http.StatusForbidden, rec.Code)
				// No challenge on the refusal: prompting would invite the
				// browser to attach a credential to the very request being
				// refused for carrying one.
				assert.Empty(t, rec.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

// Register must not be able to publish an operator endpoint anonymously. The
// gate defaults to open, so without this the next /management route added
// would ship unauthenticated and nothing would say so.
func TestRegisterRejectsUngatedOperatorPaths(t *testing.T) {
	noop := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	for _, path := range []string{
		"/management/some/new/route", "/debug/pprof/newprofile",
		LogLevelRouterPath, EventLogRouterPath, RouteWebUI, TelemetryUIPath,
	} {
		t.Run(path, func(t *testing.T) {
			// The message is asserted, not just the panic: it is what tells
			// whoever hits this at startup what to do about it.
			assert.PanicsWithValue(t,
				"http.Register: \""+path+"\" is an operator endpoint and must set AdminAuth "+
					"(or be added to openOperatorPaths if it has to answer Kubernetes probes)",
				func() { Register(&Handler{Path: path, Handler: noop}) })
		})
	}

	t.Run("readiness stays open", func(t *testing.T) {
		assert.False(t, mustBeGated(RouteCheckComponentReady),
			"k8s probes cannot present credentials")
	})

	t.Run("non-operator paths are unaffected", func(t *testing.T) {
		for _, path := range []string{HealthzRouterPath, LivezRouterPath, MetricsPath, MetricsDefaultPath, RootPath} {
			assert.False(t, mustBeGated(path), path)
		}
	})

	// The guard is only worth having if it covers what the config file
	// promises. Every path adminAuthEnabled documents as root-only must be one
	// Register refuses to publish anonymously.
	t.Run("covers every documented operator path", func(t *testing.T) {
		for _, path := range []string{
			RouteTriggerStopPath,
			LogLevelRouterPath,
			EventLogRouterPath,
			RouteWebUI,
			TelemetryUIPath,
			"/debug/pprof/",
			"/management/datacoord/garbage_collection/pause",
		} {
			assert.True(t, mustBeGated(path), path)
		}
	})
}

func TestWrapAdminAuth_JSONBodyShape(t *testing.T) {
	enableAdminAuth(t)
	// The 401 body shape is part of the contract with API clients — keep it
	// as a JSON object with a single "msg" string field so clients can parse.
	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	wrapped.ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.True(t, strings.HasPrefix(body, `{"msg":`), "body should start with JSON: %s", body)
	assert.True(t, strings.HasSuffix(strings.TrimRight(body, "\n"), `"}`), "body should end with quote+brace: %s", body)
}

func TestGetPasswordVerifyFunc_PrimaryWinsOverFallback(t *testing.T) {
	// Standalone runs proxy, mix coord and the worker nodes in one process, so
	// all three register a verifier into the same globals. The lowest slot
	// (in-process credential lookup) must win deterministically, otherwise the
	// winner would depend on goroutine scheduling and standalone would
	// sometimes pay an RPC to verify a password it can check locally.
	setVerifyFunc(t, func(_ context.Context, _, password string) bool {
		return password == "primary-accepts"
	})
	previousWorker := managementVerifiers[VerifierSlotWorker]
	managementVerifiers[VerifierSlotWorker] = func(_ context.Context, _, _ string) error {
		t.Fatal("fallback verifier must not be consulted while a primary exists")
		return nil
	}
	t.Cleanup(func() { managementVerifiers[VerifierSlotWorker] = previousWorker })

	assert.NoError(t, verifyManagementPassword(context.Background(), "root", "primary-accepts", "/test"))
	assert.True(t, IsAuthenticationError(
		verifyManagementPassword(context.Background(), "root", "fallback-accepts", "/test")))
}

func TestGetPasswordVerifyFunc_FallbackUsedWhenNoPrimary(t *testing.T) {
	// On a worker node nothing registers a primary verifier; the fallback is
	// what keeps /management/* and /debug/pprof reachable by root once
	// adminAuthEnabled is on.
	setVerifyFunc(t, nil)
	setManagementVerifier(t, VerifierSlotWorker, func(_ context.Context, _, password string) error {
		if password == "fallback-accepts" {
			return nil
		}
		return NewAuthenticationError("invalid root password")
	})

	assert.NoError(t, verifyManagementPassword(context.Background(), "root", "fallback-accepts", "/test"))
	assert.True(t, IsAuthenticationError(
		verifyManagementPassword(context.Background(), "root", "nope", "/test")))
}

func TestWrapAdminAuth_FallbackVerifierAuthenticates(t *testing.T) {
	enableAdminAuth(t)
	// End-to-end through the wrapper: a worker node with only a fallback
	// verifier must admit correct root credentials rather than answering 503.
	// This is the regression the reviewer flagged — before the fallback slot
	// existed, every worker node rejected even valid root credentials.
	setVerifyFunc(t, nil)
	RegisterManagementVerifier(VerifierSlotWorker, func(_ context.Context, username, password string) error {
		if username == "root" && password == "correct-horse" {
			return nil
		}
		return NewAuthenticationError("invalid root password")
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/management/stop", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.True(t, inv.called, "valid root credentials must pass via the fallback verifier")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestWrapAdminAuth_UnreachableCredentialStoreReturns503(t *testing.T) {
	enableAdminAuth(t)
	// A worker node whose mix coord is down cannot judge the password. It must
	// answer 503, not 401: the operator is usually holding a correct password
	// and debugging a half-down cluster, and "invalid root password" would send
	// them chasing a credential problem that does not exist.
	//
	// This is a regression test for real behavior observed against a live
	// cluster: with the verifier returning a bare bool, an unreachable coord was
	// indistinguishable from a wrong password and surfaced as 401.
	setVerifyFunc(t, nil)
	RegisterManagementVerifier(VerifierSlotWorker, func(_ context.Context, _, _ string) error {
		return errors.New("mix coord client unavailable: connection refused")
	})

	inv := &invoked{}
	wrapped := wrapAdminAuth(inv.handler(), "/test", false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapped.ServeHTTP(rec, req)

	assert.False(t, inv.called)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotContains(t, rec.Body.String(), "invalid root password")
}

func TestWrapAdminAuth_ErrorBodyLeaksNoInternals(t *testing.T) {
	enableAdminAuth(t)
	// The 503 body is returned to a caller who has not authenticated, so it must
	// not carry the underlying cause. merr and cockroachdb/errors render wrapped
	// errors with a stack trace containing absolute build paths; a live cluster
	// returned exactly that before this was fixed.
	setVerifyFunc(t, nil)
	RegisterManagementVerifier(VerifierSlotWorker, func(_ context.Context, _, _ string) error {
		return errors.Wrap(
			errors.New("stack trace: /home/builder/milvus/pkg/tracer/stack_trace.go:51"),
			"GetCredential failed")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.SetBasicAuth("root", "correct-horse")
	wrapAdminAuth((&invoked{}).handler(), "/test", false).ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotContains(t, body, "stack trace")
	assert.NotContains(t, body, "/home/builder")
	assert.NotContains(t, body, "GetCredential")
	assert.Contains(t, body, "cannot verify credentials on this node")
}

// The counter is the only way an operator can tell a node that is rejecting
// credentials from one that cannot reach its credential store, because both
// answer 503 and both log lines are rate limited. Pin the label values and the
// route label's stated safety property: it is the registered pattern, never the
// caller-controlled path.
func TestAdminAuthMetricRecordsEachOutcomeOnce(t *testing.T) {
	enableAdminAuth(t)
	setVerifyFunc(t, func(_ context.Context, username, password string) bool {
		return username == util.UserRoot && password == "s3cr3t"
	})

	const route = "/debug/pprof/"
	wrapped := wrapAdminAuth((&invoked{}).handler(), route, false)
	count := func(result string) float64 {
		return testutil.ToFloat64(metrics.AdminAuthTotal.WithLabelValues(route, result))
	}

	for _, tc := range []struct {
		result  string
		prepare func(*http.Request)
	}{
		{metrics.AdminAuthUnauthenticated, func(*http.Request) {}},
		{metrics.AdminAuthUnauthenticated, func(r *http.Request) { r.SetBasicAuth(util.UserRoot, "wrong") }},
		{metrics.AdminAuthForbidden, func(r *http.Request) { r.SetBasicAuth("alice", "s3cr3t") }},
		{metrics.AdminAuthCrossSite, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }},
		{metrics.AdminAuthAllowed, func(r *http.Request) { r.SetBasicAuth(util.UserRoot, "s3cr3t") }},
	} {
		before := count(tc.result)
		// The caller-controlled part of the path must not reach the label.
		req := httptest.NewRequest(http.MethodGet, route+"heap?seconds=1", nil)
		tc.prepare(req)
		wrapped.ServeHTTP(httptest.NewRecorder(), req)
		assert.Equal(t, before+1, count(tc.result), tc.result)
	}

	assert.Zero(t, testutil.ToFloat64(
		metrics.AdminAuthTotal.WithLabelValues(route+"heap", metrics.AdminAuthAllowed)),
		"the metric label must be the registered route, not the request path")
}

// The endpoint and username reach the log from an unauthenticated request, so
// an unbounded field would let one cheap request write as many log bytes as the
// caller cares to send.
func TestTruncateForLog(t *testing.T) {
	assert.Equal(t, "short", truncateForLog("short"))

	exact := strings.Repeat("a", maxLoggedFieldLen)
	assert.Equal(t, exact, truncateForLog(exact), "a field at the bound is kept whole")

	long := truncateForLog(strings.Repeat("a", maxLoggedFieldLen+10))
	assert.Equal(t, strings.Repeat("a", maxLoggedFieldLen)+"...(truncated)", long)

	// Cutting mid-rune would put invalid UTF-8 into the log stream, which some
	// collectors drop and others escape byte by byte.
	multibyte := truncateForLog(strings.Repeat("é", maxLoggedFieldLen))
	assert.True(t, utf8.ValidString(multibyte), "truncation must land on a rune boundary")
	assert.True(t, strings.HasSuffix(multibyte, "...(truncated)"))
}

// The cross-site check is the only thing standing between a browser that has
// cached root's credential for this origin and any page the operator later
// visits, so it has to fail closed on anything it does not understand.
func TestRejectCrossSiteFailsClosed(t *testing.T) {
	request := func(setHeaders func(*http.Request)) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/management/stop", nil)
		req.Host = "milvus.example.com:9091"
		setHeaders(req)
		return req
	}

	for _, tc := range []struct {
		name     string
		headers  func(*http.Request)
		rejected bool
	}{
		{"no headers at all is a non-browser client", func(*http.Request) {}, false},
		{"same-origin", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-origin") }, false},
		{"none, a user-initiated navigation", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "none") }, false},
		{"cross-site", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }, true},
		{"same-site is a sibling subdomain", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-site") }, true},
		{"upper case is still a verdict", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "Cross-Site") }, true},
		{"an unrecognized value must not fall through", func(r *http.Request) {
			r.Header.Set("Sec-Fetch-Site", "something-new")
		}, true},
		{"matching Origin", func(r *http.Request) {
			r.Header.Set("Origin", "http://milvus.example.com:9091")
		}, false},
		{"other Origin", func(r *http.Request) { r.Header.Set("Origin", "http://evil.example.com") }, true},
		{"Origin null is a sandboxed frame, not this origin", func(r *http.Request) {
			r.Header.Set("Origin", "null")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.rejected, RejectCrossSite(request(tc.headers), false))
		})
	}

	// A document surface lets a top-level navigation through, and only that:
	// showing a page is not an action, but a fetch from another page still is.
	navigation := func(r *http.Request) {
		r.Header.Set("Sec-Fetch-Site", "cross-site")
		r.Header.Set("Sec-Fetch-Mode", "navigate")
		r.Header.Set("Sec-Fetch-Dest", "document")
	}
	assert.False(t, RejectCrossSite(request(navigation), true))
	assert.True(t, RejectCrossSite(request(navigation), false),
		"only surfaces that opt in may be linked to")

	subresource := func(r *http.Request) {
		r.Header.Set("Sec-Fetch-Site", "cross-site")
		r.Header.Set("Sec-Fetch-Mode", "no-cors")
		r.Header.Set("Sec-Fetch-Dest", "image")
	}
	assert.True(t, RejectCrossSite(request(subresource), true))

	post := httptest.NewRequest(http.MethodPost, "/management/stop", nil)
	navigation(post)
	assert.True(t, RejectCrossSite(post, true),
		"a navigation that mutates state is the CSRF this guards against")
}

// A route's error body must not change shape depending on which rule answered
// it: the gate and the data plane's own check both reply on the same paths, and
// a script parsing one of them cannot be expected to know which ran.
func TestAuthErrorCodeMatchesTheDataPlaneCodes(t *testing.T) {
	assert.Equal(t, merr.Code(merr.ErrNeedAuthenticate), AuthErrorCode(http.StatusUnauthorized))
	assert.Equal(t, merr.Code(merr.ErrPrivilegeNotPermitted), AuthErrorCode(http.StatusForbidden))
	assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), AuthErrorCode(http.StatusServiceUnavailable))
	// Anything the gate has no name for still carries a code, not a zero that a
	// client would read as success.
	assert.NotZero(t, AuthErrorCode(http.StatusInternalServerError))
}

// The registry is written by component startup and shutdown while requests are
// reading it, which is why it is behind a mutex. Nothing exercised that
// concurrently before, so -race never saw it.
func TestManagementVerifierRegistryIsRaceFree(t *testing.T) {
	enableAdminAuth(t)
	restore := installVerifier(t, "proxy", func(context.Context, string, string) error { return nil })
	t.Cleanup(restore)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: a proxy and a coordinator starting and stopping repeatedly.
	for _, slot := range []ManagementVerifierSlot{VerifierSlotProxy, VerifierSlotCoordinator} {
		wg.Add(1)
		go func(slot ManagementVerifierSlot) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				RegisterManagementVerifier(slot, func(context.Context, string, string) error { return nil })
				RegisterManagementVerifier(slot, nil)
			}
		}(slot)
	}

	// Readers: gated requests resolving a verifier and running the whole gate.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				req := httptest.NewRequest(http.MethodGet, "/management/stop", nil)
				req.SetBasicAuth(util.UserRoot, "whatever")
				// Either verdict is fine: which one depends on whether a
				// verifier happens to be registered. Not crashing, and not
				// tripping the race detector, is the point.
				status, _, _ := CheckAdminRequest(req, "/management/stop", false)
				assert.Contains(t, []int{http.StatusOK, http.StatusServiceUnavailable}, status)
			}
		}()
	}
	close(stop)
	wg.Wait()
}
