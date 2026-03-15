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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	// getUserRoleFunc is a callback function to get user roles.
	// This is set by the proxy package to avoid circular dependency.
	getUserRoleFunc func(username string) ([]string, error)
)

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

// parseHTTPAuth extracts username and password from HTTP request.
// Supports HTTP Basic Auth format only.
func parseHTTPAuth(req *http.Request) (username, password string, ok bool) {
	return req.BasicAuth()
}

// CheckPrivilege checks if the authenticated user has the specified privilege.
func CheckPrivilege(ctx context.Context, req *http.Request, objectType commonpb.ObjectType,
	objectPrivilege commonpb.ObjectPrivilege, objectName string, dbName string) error {
	// Check if authorization is enabled
	if !paramtable.Get().CommonCfg.AuthorizationEnabled.GetAsBool() {
		return nil
	}

	// Parse authentication from request
	username, password, ok := parseHTTPAuth(req)
	if !ok || username == "" || password == "" {
		return &ErrAuthentication{msg: "authentication required"}
	}

	// Verify password
	if passwordVerifyFunc == nil {
		return &ErrAuthentication{msg: "password verification not available"}
	}
	if !passwordVerifyFunc(ctx, username, password) {
		log.Warn("invalid credentials for HTTP RBAC check", zap.String("username", username))
		return &ErrAuthentication{msg: "invalid credentials"}
	}

	// Root bypass (unless RootShouldBindRole is enabled)
	if !paramtable.Get().CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		log.Info("root user authenticated for HTTP access", zap.String("privilege", objectPrivilege.String()))
		return nil
	}

	// Get user roles
	if getUserRoleFunc == nil {
		return &ErrAuthentication{msg: "role lookup not available"}
	}
	roleNames, err := getUserRoleFunc(username)
	if err != nil {
		log.Warn("failed to get user roles", zap.String("username", username), zap.Error(err))
		return &ErrAuthentication{msg: fmt.Sprintf("failed to get user roles: %v", err)}
	}
	roleNames = append(roleNames, util.RolePublic)

	// Check privilege using Casbin enforcer
	e := privilege.GetEnforcer()
	object := funcutil.PolicyForResource(dbName, objectType.String(), objectName)
	privilegeName := objectPrivilege.String()

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
			log.Warn("privilege check failed", zap.Error(err))
			return fmt.Errorf("privilege check failed: %w", err)
		}
		privilege.SetResultCache(roleName, object, privilegeName, isPermit, version)
		if isPermit {
			return nil
		}
	}

	log.Info("HTTP permission denied",
		zap.String("username", username),
		zap.Strings("roles", roleNames),
		zap.String("privilege", util.MetaStore2API(privilegeName)))

	return &ErrPermissionDenied{
		msg: fmt.Sprintf("permission denied: user %s requires %s privilege", username, util.MetaStore2API(privilegeName)),
	}
}

// IsAuthenticationError returns true if the error is an authentication error
func IsAuthenticationError(err error) bool {
	_, ok := err.(*ErrAuthentication)
	return ok
}

// IsPermissionDeniedError returns true if the error is a permission denied error
func IsPermissionDeniedError(err error) bool {
	_, ok := err.(*ErrPermissionDenied)
	return ok
}
