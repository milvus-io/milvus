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
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// AuthPolicy decides, per request, whether the wrapped handler should require
// HTTP Basic Auth with root credentials. Returning true triggers the check;
// returning false bypasses it (handler invoked as if no wrapper were present).
//
// The two predefined policies cover all current call sites:
//   - AuthAlways: for endpoints whose mere existence implies authentication is
//     required (e.g. /expr, which executes arbitrary Go expressions).
//   - AuthByAdminFlag: for endpoints gated by common.security.adminAuthEnabled
//     (e.g. /management/*, /debug/pprof/*).
type AuthPolicy func() bool

// AuthAlways forces authentication on every request regardless of any flag.
// Used by /expr where the consequence of unauthenticated access is RCE-class.
var AuthAlways AuthPolicy = func() bool { return true }

// AuthByAdminFlag enables authentication when common.security.adminAuthEnabled
// is true. Used by /management/* and /debug/pprof/* — endpoints that exist for
// operator use but may be reachable from untrusted networks depending on
// deployment posture. Default value of adminAuthEnabled is false, preserving
// historical behavior; production deployments are expected to enable it.
var AuthByAdminFlag AuthPolicy = func() bool {
	return paramtable.Get().CommonCfg.AdminAuthEnabled.GetAsBool()
}

// wrapBasicRootAuth wraps next with an HTTP Basic Auth check that demands the
// milvus root user's credentials. The policy callback decides whether the
// check fires; if policy() returns false the handler is invoked directly.
//
// Authentication failures return HTTP 401 with a JSON body and never invoke
// the wrapped handler. No WWW-Authenticate header is emitted by design —
// callers are API clients, not browsers.
func wrapBasicRootAuth(next http.Handler, policy AuthPolicy) http.Handler {
	if policy == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if policy() {
			if err := checkBasicRootAuth(req); err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprintf(w, `{"msg": %q}`, err.Error())
				return
			}
		}
		next.ServeHTTP(w, req)
	})
}

// checkBasicRootAuth verifies HTTP Basic Auth with the root user's credentials.
// It returns nil on success and an error describing the failure otherwise.
//
// The error string is returned verbatim in the 401 response body, so it must
// not leak sensitive details (e.g. whether the username exists). The current
// messages distinguish "no credentials", "wrong username", "wrong password",
// and "verifier not available" — sufficient for operator diagnosis without
// aiding offline attacks against the root account.
func checkBasicRootAuth(req *http.Request) error {
	username, password, ok := req.BasicAuth()
	if !ok || username == "" || password == "" {
		return fmt.Errorf("authentication required: use HTTP Basic Auth with root credentials")
	}
	if username != "root" {
		log.Warn("non-root user attempted to access protected endpoint",
			zap.String("username", username), zap.String("path", req.URL.Path))
		return fmt.Errorf("only root user can access this endpoint")
	}
	if passwordVerifyFunc == nil {
		// passwordVerifyFunc is registered by Proxy (always) and MixCoord
		// (since the auth refactor). A nil value here means the request hit
		// a node type that doesn't host credential metadata.
		return fmt.Errorf("password verification is not available on this node")
	}
	if !passwordVerifyFunc(context.Background(), username, password) {
		log.Warn("invalid root password", zap.String("path", req.URL.Path))
		return fmt.Errorf("invalid root password")
	}
	return nil
}
