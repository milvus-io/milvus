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

	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/bcrypt"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// AuthPolicy decides, per request, whether the wrapped handler must
// authenticate the caller. Returning false invokes the handler as if no
// wrapper were present.
//
// AuthByAdminFlag is the production policy used by endpoints gated by
// common.security.adminAuthEnabled. Tests can supply an always-true policy
// directly when exercising the wrapper independently of configuration.
type AuthPolicy func() bool

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

// VerifyStoredRootPassword compares a plaintext password with the stored
// bcrypt hash while preserving the distinction between caller error and
// credential-store corruption. Only ErrMismatchedHashAndPassword means the
// supplied password is wrong; malformed hashes are system failures and must
// surface as 503 at the management boundary, not as a misleading 401.
func VerifyStoredRootPassword(storedHash, password string) error {
	err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password))
	if err == nil {
		return nil
	}
	if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		return NewAuthenticationError("invalid root password")
	}
	return merr.WrapErrServiceInternalErr(err, "stored root credential hash is invalid")
}

// wrapAdminAuth wraps next with the management-plane authentication check. The
// policy callback decides whether the check fires; a nil policy leaves the
// handler untouched, which is how open endpoints (/healthz, /livez, /metrics,
// /management/check/ready) stay reachable by k8s probes and Prometheus.
//
// Failures are rendered from the typed errors in rbac.go, so callers see 401
// for missing credentials or a wrong root password, 403 for any non-root
// username (rejected before password verification), and 503 when the root
// credential cannot be checked. No WWW-Authenticate header is emitted by
// design: callers are API clients, and the header would make browsers pop a
// login dialog on what is an API surface.
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
