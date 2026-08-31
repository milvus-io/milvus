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
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// AdminAuthEnabled is read per request, so the flag takes effect without a
// restart.
func AdminAuthEnabled() bool {
	return paramtable.Get().CommonCfg.AdminAuthEnabled.GetAsBool()
}

// BasicAuthRealm names the protection space in WWW-Authenticate. Browsers key
// cached credentials on (origin, realm), so it must stay stable across releases.
const BasicAuthRealm = "milvus"

type authenticatedAdminContextKey struct{}

// AuthDecision is the complete result of one metrics-port authentication
// check. Keeping the HTTP status, response code, metric result and authenticated
// principal together prevents the net/http and Gin adapters from translating
// the same outcome independently.
type AuthDecision struct {
	Status  int
	Message string

	code      int32
	result    string
	principal string
}

// Allowed reports whether the request may proceed.
func (d AuthDecision) Allowed() bool {
	return d.Status == http.StatusOK
}

// ErrorCode is the merr code emitted by the metrics-port JSON API.
func (d AuthDecision) ErrorCode() int32 {
	return d.code
}

// AuthenticatedRequest projects a successful root-auth decision into a typed
// request context. Downstream in-process RPC handlers can trust this marker: it
// cannot arrive over HTTP or gRPC, and it avoids copying the root password into
// metadata merely to authenticate the next function call in the same process.
func (d AuthDecision) AuthenticatedRequest(req *http.Request) *http.Request {
	if !d.Allowed() || d.principal == "" {
		return req
	}
	ctx := context.WithValue(req.Context(), authenticatedAdminContextKey{}, d.principal)
	return req.WithContext(ctx)
}

// AuthenticatedAdminFromContext returns the management-plane principal that
// was verified at the HTTP boundary, if one exists.
func AuthenticatedAdminFromContext(ctx context.Context) (string, bool) {
	username, ok := ctx.Value(authenticatedAdminContextKey{}).(string)
	return username, ok && username != ""
}

func allowedAuthDecision(principal string) AuthDecision {
	return AuthDecision{
		Status:    http.StatusOK,
		result:    metrics.AdminAuthAllowed,
		principal: principal,
	}
}

func rejectedAuthDecision(status int, message string) AuthDecision {
	decision := AuthDecision{Status: status, Message: message}
	switch status {
	case http.StatusUnauthorized:
		decision.code = merr.Code(merr.ErrNeedAuthenticate)
		decision.result = metrics.AdminAuthUnauthenticated
	case http.StatusForbidden:
		decision.code = merr.Code(merr.ErrPrivilegeNotPermitted)
		decision.result = metrics.AdminAuthForbidden
	case http.StatusServiceUnavailable:
		decision.code = merr.Code(merr.ErrServiceUnavailable)
		decision.result = metrics.AdminAuthUnavailable
	default:
		decision.code = merr.Code(merr.ErrServiceUnavailable)
		decision.result = metrics.AdminAuthError
	}
	return decision
}

const crossSiteRejection = "cross-site requests are not accepted on this endpoint; " +
	"open it directly rather than following a link from another site"

// RejectCrossSite reports whether a browser says another site initiated this
// request.
//
// WriteBasicAuthChallenge is what makes this necessary: the challenge teaches
// the browser to hold root's credential for this origin, so any page the
// operator later visits can fire a request here and have it attached.
// Management handlers read their parameters from the query string and do not
// check the method, and a cross-site form post to /api/v1/collection is a
// top-level navigation, which carries cached credentials and needs no
// preflight. same-site is refused as well: it is a different origin under the
// same registrable domain, so trusting it would extend the management plane to
// whoever controls a sibling subdomain.
func RejectCrossSite(req *http.Request, allowTopLevelNavigation bool) bool {
	if allowTopLevelNavigation && isTopLevelNavigation(req) {
		return false
	}
	// Default-deny, lower-cased: an unrecognized value is not a browser
	// following the spec, and must not fall through to Origin, which a
	// cross-site GET navigation does not carry. same-site counts as cross:
	// it is a sibling subdomain, not this origin.
	switch strings.ToLower(req.Header.Get("Sec-Fetch-Site")) {
	case "same-origin", "none":
		return false
	case "":
		// Not a browser, or one predating Fetch Metadata: try Origin.
	default:
		return true
	}
	origin := req.Header.Get("Origin")
	switch {
	case origin == "":
		return false
	case strings.EqualFold(origin, "null"):
		// Sandboxed frame or cross-origin redirect: cannot be checked.
		return true
	}
	return !strings.EqualFold(originHost(origin), req.Host)
}

// isTopLevelNavigation reports whether the browser is loading this URL as a
// document rather than fetching it from a page. GET is required: a navigation
// that mutates state is the CSRF this guards against.
func isTopLevelNavigation(req *http.Request) bool {
	return req.Method == http.MethodGet &&
		req.Header.Get("Sec-Fetch-Mode") == "navigate" &&
		req.Header.Get("Sec-Fetch-Dest") == "document"
}

// originHost is compared against req.Host, which carries no scheme, so http and
// https on the same authority are indistinguishable here. A reverse proxy that
// rewrites Host defeats it; those deployments have Sec-Fetch-Site.
func originHost(origin string) string {
	if i := strings.Index(origin, "://"); i >= 0 {
		origin = origin[i+3:]
	}
	return strings.TrimSuffix(origin, "/") // some clients send one anyway
}

// CheckCrossSite refuses a request another site initiated, before any
// credential is considered. Separate from CheckAdminRequest because every route
// on this port needs it once the gate is on, including the ones the data plane's
// own rule authenticates. route is the registered route pattern, used as a
// metric label and therefore never the raw request path.
func CheckCrossSite(req *http.Request, route string, allowTopLevelNavigation bool) AuthDecision {
	if RejectCrossSite(req, allowTopLevelNavigation) {
		metrics.AdminAuthTotal.WithLabelValues(route, metrics.AdminAuthCrossSite).Inc()
		decision := rejectedAuthDecision(http.StatusForbidden, crossSiteRejection)
		decision.result = metrics.AdminAuthCrossSite
		return decision
	}
	return allowedAuthDecision("")
}

// CheckAdminRequest is the whole root gate for one request: cross-site refusal,
// then root authentication. Every surface carrying the gate goes through here,
// so the net/http handlers and the proxy's gin routes cannot drift apart.
// allowTopLevelNavigation is for document surfaces only; see Handler.AuthChallenge.
func CheckAdminRequest(req *http.Request, route string, allowTopLevelNavigation bool) AuthDecision {
	if decision := CheckCrossSite(req, route, allowTopLevelNavigation); !decision.Allowed() {
		return decision
	}
	// Management endpoints act on process lifecycle and cluster-wide runtime
	// state, so they are root-only rather than "any valid user": accepting a
	// caller-chosen username means one credential lookup per name, and the
	// proxy's credential cache does not cache misses.
	if err := CheckRootAuth(req.Context(), req, req.URL.Path); err != nil {
		status := HTTPStatusFromPrivilegeError(err)
		decision := rejectedAuthDecision(status, err.Error())
		metrics.AdminAuthTotal.WithLabelValues(route, decision.result).Inc()
		return decision
	}
	metrics.AdminAuthTotal.WithLabelValues(route, metrics.AdminAuthAllowed).Inc()
	return allowedAuthDecision(util.UserRoot)
}

// ApplyGinAuthDecision is the single Gin projection for both the management
// gate and its cross-site-only data-plane branch.
func ApplyGinAuthDecision(c *gin.Context, decision AuthDecision, challenge bool) bool {
	if decision.Allowed() {
		c.Request = decision.AuthenticatedRequest(c.Request)
		return true
	}
	if challenge && decision.Status == http.StatusUnauthorized {
		WriteBasicAuthChallenge(c.Writer)
	}
	c.AbortWithStatusJSON(decision.Status, gin.H{
		HTTPReturnCode:    decision.ErrorCode(),
		HTTPReturnMessage: decision.Message,
	})
	return false
}

// GinAdminAuthMiddleware applies the root gate without advancing the Gin
// chain. That matches Gin's middleware loop and lets a route-level backstop
// reuse a principal already verified by the parent group without a second
// bcrypt comparison or metric increment.
func GinAdminAuthMiddleware(challenge bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, ok := AuthenticatedAdminFromContext(c.Request.Context()); ok {
			return
		}
		ApplyGinAuthDecision(c,
			CheckAdminRequest(c.Request, c.FullPath(), false), challenge)
	}
}

// wrapAdminAuth wraps next with the gate. route is the registered pattern;
// document is Handler.AuthChallenge, which for a page a human opens means both
// "send the challenge" and "a link to it is not an action".
func wrapAdminAuth(next http.Handler, route string, document bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if AdminAuthEnabled() {
			decision := CheckAdminRequest(req, route, document)
			if !decision.Allowed() {
				if document && decision.Status == http.StatusUnauthorized {
					WriteBasicAuthChallenge(w)
				}
				writeJSONError(w, decision.Status, decision.Message)
				return
			}
			req = decision.AuthenticatedRequest(req)
		}
		next.ServeHTTP(w, req)
	})
}

// WriteBasicAuthChallenge tells a browser to prompt for credentials; without it
// the console takes a 401 and nothing prompts. Only alongside a 401, and only on
// surfaces a human opens.
func WriteBasicAuthChallenge(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="`+BasicAuthRealm+`", charset="UTF-8"`)
}
