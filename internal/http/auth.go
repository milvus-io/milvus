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
	"net/http"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
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

// AdminAuthCheckedKey marks, in a gin context, that the gate already ran, so a
// route carrying its own copy neither repeats the work nor counts it twice.
const AdminAuthCheckedKey = "milvus-admin-auth-checked"

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
func CheckCrossSite(req *http.Request, route string, allowTopLevelNavigation bool) (int, string, bool) {
	if RejectCrossSite(req, allowTopLevelNavigation) {
		metrics.AdminAuthTotal.WithLabelValues(route, metrics.AdminAuthCrossSite).Inc()
		return http.StatusForbidden, crossSiteRejection, false
	}
	return http.StatusOK, "", true
}

// CheckAdminRequest is the whole root gate for one request: cross-site refusal,
// then root authentication. Every surface carrying the gate goes through here,
// so the net/http handlers and the proxy's gin routes cannot drift apart.
// allowTopLevelNavigation is for document surfaces only; see Handler.AuthChallenge.
func CheckAdminRequest(req *http.Request, route string, allowTopLevelNavigation bool) (int, string, bool) {
	if status, msg, ok := CheckCrossSite(req, route, allowTopLevelNavigation); !ok {
		return status, msg, ok
	}
	// Management endpoints act on process lifecycle and cluster-wide runtime
	// state, so they are root-only rather than "any valid user": accepting a
	// caller-chosen username means one credential lookup per name, and the
	// proxy's credential cache does not cache misses.
	if err := CheckRootAuth(req.Context(), req, req.URL.Path); err != nil {
		status := HTTPStatusFromPrivilegeError(err)
		metrics.AdminAuthTotal.WithLabelValues(route, adminAuthResult(status)).Inc()
		return status, err.Error(), false
	}
	metrics.AdminAuthTotal.WithLabelValues(route, metrics.AdminAuthAllowed).Inc()
	return http.StatusOK, "", true
}

func adminAuthResult(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return metrics.AdminAuthUnauthenticated
	case http.StatusForbidden:
		return metrics.AdminAuthForbidden
	case http.StatusServiceUnavailable:
		return metrics.AdminAuthUnavailable
	default:
		return metrics.AdminAuthError
	}
}

// wrapAdminAuth wraps next with the gate. route is the registered pattern;
// document is Handler.AuthChallenge, which for a page a human opens means both
// "send the challenge" and "a link to it is not an action".
func wrapAdminAuth(next http.Handler, route string, document bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if AdminAuthEnabled() {
			status, msg, ok := CheckAdminRequest(req, route, document)
			if !ok {
				if document && status == http.StatusUnauthorized {
					WriteBasicAuthChallenge(w)
				}
				writeJSONError(w, status, msg)
				return
			}
		}
		next.ServeHTTP(w, req)
	})
}

// AuthErrorCode maps a gate status onto the merr code clients already parse out
// of this port's error bodies, so a route's reply keeps one shape whether the
// gate answered it or the data plane's own rule did.
func AuthErrorCode(status int) int32 {
	switch status {
	case http.StatusUnauthorized:
		return merr.Code(merr.ErrNeedAuthenticate)
	case http.StatusForbidden:
		return merr.Code(merr.ErrPrivilegeNotPermitted)
	default:
		return merr.Code(merr.ErrServiceUnavailable)
	}
}

// WriteBasicAuthChallenge tells a browser to prompt for credentials; without it
// the console takes a 401 and nothing prompts. Only alongside a 401, and only on
// surfaces a human opens.
func WriteBasicAuthChallenge(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="`+BasicAuthRealm+`", charset="UTF-8"`)
}
