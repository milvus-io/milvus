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

package http

import (
	"context"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// getUserRoleFunc is a callback function to get user roles.
// This is set by the proxy package to avoid circular dependency.
var getUserRoleFunc func(username string) ([]string, error)

// RegisterGetUserRoleFunc registers a function to get user roles.
// This should be called by the proxy package during initialization.
func RegisterGetUserRoleFunc(fn func(username string) ([]string, error)) {
	getUserRoleFunc = fn
}

// ErrAuthentication represents an authentication error (invalid credentials)
type ErrAuthentication struct {
	msg string
}

func (e *ErrAuthentication) Error() string {
	return e.msg
}

// ErrPermissionDenied represents a permission denied error (valid credentials but no permission)
type ErrPermissionDenied struct {
	msg string
}

func (e *ErrPermissionDenied) Error() string {
	return e.msg
}

// ErrServiceUnavailable represents an unavailable dependency needed for authz.
type ErrServiceUnavailable struct {
	msg string
}

func (e *ErrServiceUnavailable) Error() string {
	return e.msg
}

// maxLoggedFieldLen bounds a caller-supplied value in the log. The gated
// endpoints answer unauthenticated callers, and both the username and the
// endpoint are caller-controlled (/debug/pprof/ and /webui/ are subtree
// patterns), so an unbounded field would let one cheap request write as many
// log bytes as the caller cares to send.
const maxLoggedFieldLen = 64

func truncateForLog(s string) string {
	if len(s) <= maxLoggedFieldLen {
		return s
	}
	// Trim back to a rune boundary so the result is still valid UTF-8.
	cut := maxLoggedFieldLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "...(truncated)"
}

// parseHTTPAuth extracts username and password from HTTP request.
// Supports HTTP Basic Auth format only.
func parseHTTPAuth(req *http.Request) (username, password string, ok bool) {
	return req.BasicAuth()
}

// CheckRootAuth authenticates a management-plane request as the milvus root
// user via HTTP Basic Auth. endpoint names the protected resource for the log
// only, never for the response body.
//
// Render the result with HTTPStatusFromPrivilegeError:
//
//   - missing/blank credentials or wrong root password -> 401
//   - any non-root username (password is not checked)  -> 403
//   - the credential could not be checked at all       -> 503
//
// The 503 case matters: a node that cannot reach its credential store must not
// report a correct password as invalid.
func CheckRootAuth(ctx context.Context, req *http.Request, endpoint string) error {
	return checkRootAuth(ctx, req, endpoint, verifyManagementPassword)
}

func checkRootAuth(ctx context.Context, req *http.Request, endpoint string,
	verify func(ctx context.Context, username, password, endpoint string) error,
) error {
	username, password, ok := parseHTTPAuth(req)
	if !ok || username == "" || password == "" {
		return &ErrAuthentication{msg: "authentication required. Use HTTP Basic Auth with root credentials"}
	}
	if username != util.UserRoot {
		mlog.RatedWarn(ctx, 1.0, "non-root user attempted to access protected endpoint",
			mlog.String("username", truncateForLog(username)),
			mlog.String("endpoint", truncateForLog(endpoint)))
		return &ErrPermissionDenied{msg: "only root user can access this endpoint"}
	}
	if err := verify(ctx, username, password, endpoint); err != nil {
		if IsAuthenticationError(err) {
			mlog.RatedWarn(ctx, 1.0, "invalid root password",
				mlog.String("endpoint", truncateForLog(endpoint)))
		}
		return err
	}
	mlog.RatedDebug(ctx, 1.0, "root user authenticated", mlog.String("endpoint", truncateForLog(endpoint)))
	return nil
}

// CheckPrivilege checks if the authenticated user has the specified privilege.
func CheckPrivilege(ctx context.Context, req *http.Request, objectType commonpb.ObjectType,
	objectPrivilege string, objectName string, dbName string,
) error {
	// Check if authorization is enabled
	if !paramtable.Get().CommonCfg.AuthorizationEnabled.GetAsBool() {
		return &ErrPermissionDenied{msg: "authorization must be enabled for RBAC privilege checks"}
	}

	// Parse authentication from request
	username, password, ok := parseHTTPAuth(req)
	if !ok || username == "" || password == "" {
		return &ErrAuthentication{msg: "authentication required"}
	}

	// Verify password with the proxy-owned RBAC verifier. The management-plane
	// verifier is deliberately not consulted: it is root-only, and MixCoord may
	// have registered one in this same process.
	if err := verifyRBACPassword(ctx, username, password, ""); err != nil {
		if IsAuthenticationError(err) {
			mlog.Warn(ctx, "invalid credentials for HTTP RBAC check", mlog.String("username", username))
		}
		return err
	}

	// Root bypass (unless RootShouldBindRole is enabled)
	if !paramtable.Get().CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		mlog.Info(ctx, "root user authenticated for HTTP access", mlog.String("privilege", objectPrivilege))
		return nil
	}

	// Get user roles
	if getUserRoleFunc == nil {
		return &ErrServiceUnavailable{msg: "role lookup not available"}
	}
	roleNames, err := getUserRoleFunc(username)
	if err != nil {
		mlog.Warn(ctx, "failed to get user roles", mlog.String("username", username), mlog.Err(err))
		return &ErrServiceUnavailable{msg: fmt.Sprintf("failed to get user roles: %v", err)}
	}
	roleNames = append(roleNames, util.RolePublic)

	// Check privilege using Casbin enforcer
	e := privilege.GetEnforcer()
	object := funcutil.PolicyForResource(dbName, objectType.String(), objectName)
	privilegeName := objectPrivilege

	for _, roleName := range roleNames {
		// Check cache first
		isPermit, cached, version := privilege.GetResultCache(roleName, object, privilegeName)
		if cached {
			if isPermit {
				return nil
			}
			continue
		}

		// Enforce with Casbin
		isPermit, err := e.Enforce(roleName, object, privilegeName)
		if err != nil {
			mlog.Warn(ctx, "privilege check failed", mlog.Err(err))
			return errors.Wrapf(err, "privilege check failed")
		}
		privilege.SetResultCache(roleName, object, privilegeName, isPermit, version)
		if isPermit {
			return nil
		}
	}

	mlog.Info(ctx, "HTTP permission denied",
		mlog.String("username", username),
		mlog.Strings("roles", roleNames),
		mlog.String("privilege", util.MetaStore2API(privilegeName)))

	return &ErrPermissionDenied{
		msg: fmt.Sprintf("permission denied: user %s requires %s privilege", username, util.MetaStore2API(privilegeName)),
	}
}

// IsAuthenticationError returns true if the error is an authentication error
func IsAuthenticationError(err error) bool {
	var target *ErrAuthentication
	return errors.As(err, &target)
}

// IsPermissionDeniedError returns true if the error is a permission denied error
func IsPermissionDeniedError(err error) bool {
	var target *ErrPermissionDenied
	return errors.As(err, &target)
}

// IsServiceUnavailableError returns true if an auth dependency is unavailable.
func IsServiceUnavailableError(err error) bool {
	var target *ErrServiceUnavailable
	return errors.As(err, &target)
}

func HTTPStatusFromPrivilegeError(err error) int {
	switch {
	case IsAuthenticationError(err):
		return http.StatusUnauthorized
	case IsPermissionDeniedError(err):
		return http.StatusForbidden
	case IsServiceUnavailableError(err):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
