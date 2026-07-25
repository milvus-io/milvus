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

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// AuthPolicy decides, per request, whether the wrapped handler must
// authenticate the caller. Returning false invokes the handler as if no
// wrapper were present.
//
// The two predefined policies cover all current call sites:
//   - AuthAlways: endpoints whose mere existence implies authentication.
//   - AuthByAdminFlag: endpoints gated by common.security.adminAuthEnabled.
type AuthPolicy func() bool

// AuthAlways forces authentication on every request regardless of any flag.
var AuthAlways AuthPolicy = func() bool { return true }

// AuthByAdminFlag enables authentication when common.security.adminAuthEnabled
// is true. Used by /management/*, /log/level, /eventlog and /debug/pprof/* —
// operator-facing endpoints that may be reachable from untrusted networks
// depending on deployment posture. The flag defaults to false, preserving
// historical behavior; production deployments are expected to enable it.
var AuthByAdminFlag AuthPolicy = func() bool {
	return paramtable.Get().CommonCfg.AdminAuthEnabled.GetAsBool()
}

// CheckAdminAuth authenticates a management-plane request.
//
// Management endpoints act on process lifecycle and cluster-wide runtime state
// (stop a component, pause GC, move segments, mutate config, change log level),
// so they are restricted to the root user via CheckRootAuth — the same
// primitive /expr uses in its rootOnly mode.
//
// RBAC mode is deliberately not offered here yet, unlike
// common.security.exprAuthMode: it would require a management privilege in
// milvus-proto, and util.PrivilegeExpr is still a Go-side special case carrying
// a TODO to be promoted into the proto enum. Adding a second such special case
// inside a security fix would compound that debt. Because the config surface is
// a single boolean, an adminAuthMode with an rbac option can be introduced
// later without breaking it.
func CheckAdminAuth(ctx context.Context, req *http.Request) error {
	return CheckRootAuth(ctx, req, req.URL.Path)
}

// wrapAdminAuth wraps next with the management-plane authentication check. The
// policy callback decides whether the check fires; a nil policy leaves the
// handler untouched, which is how open endpoints (/healthz, /livez, /metrics,
// /management/check/ready) stay reachable by k8s probes and Prometheus.
//
// Failures are rendered from the typed errors in rbac.go, so callers see 401
// for bad credentials, 403 for a valid non-root user, and 503 on a node with no
// credential verifier. No WWW-Authenticate header is emitted by design: callers
// are API clients, and the header would make browsers pop a login dialog on
// what is an API surface.
func wrapAdminAuth(next http.Handler, policy AuthPolicy) http.Handler {
	if policy == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if policy() {
			if err := CheckAdminAuth(req.Context(), req); err != nil {
				writeJSONError(w, HTTPStatusFromPrivilegeError(err), err.Error())
				return
			}
		}
		next.ServeHTTP(w, req)
	})
}
