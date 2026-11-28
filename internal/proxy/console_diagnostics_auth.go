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

package proxy

import (
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"

	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// consoleUsernameKey is the Gin key under which the metrics port's
// authentication stores the verified caller (httpserver.ContextUsername,
// which this package cannot import).
const consoleUsernameKey = "username"

// ConsoleDiagnosticsAuthMiddleware guards the web console routes that read
// coordinator metrics. They report the same cluster-wide data as GetMetrics
// with system_info, and their query parameters can select other metrics
// actions, so they need the grant authorizeClusterDiagnostics requires.
//
// The management gate (common.security.adminAuthEnabled) admits root only,
// and such requests pass. Without it, the console admits any authenticated
// user while authorization is enabled; only an authenticated caller holding a
// cluster admin grant passes. A request that carries no verified caller is
// refused (403, like the other console routes) rather than treated as
// internal. Nothing changes with both gates off.
func ConsoleDiagnosticsAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, ok := mhttp.AuthenticatedAdminFromContext(c.Request.Context()); ok {
			c.Next()
			return
		}
		if mhttp.AdminAuthEnabled() {
			// The metrics port runs the management gate before any console
			// route. Reaching here without its principal means the route was
			// mounted elsewhere.
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				mhttp.HTTPReturnCode:    merr.Code(merr.ErrNeedAuthenticate),
				mhttp.HTTPReturnMessage: merr.ErrNeedAuthenticate.Error(),
			})
			return
		}
		if authorizationDisabled() {
			c.Next()
			return
		}
		user := c.GetString(consoleUsernameKey)
		if user == "" {
			// Refused the way other console routes refuse a request without a
			// verified caller.
			err := merr.WrapErrPrivilegeNotPermitted("console request requires an authenticated user")
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				mhttp.HTTPReturnCode:    merr.Code(err),
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}
		// Build the caller's identity from what the port authenticated, on a
		// fresh incoming context so that nothing the request itself carries
		// can name the user.
		ctx := metadata.NewIncomingContext(c.Request.Context(), metadata.MD{})
		ctx = NewContextWithMetadata(ctx, user, "")
		if err := authorizeClusterDiagnostics(ctx, nil, c.FullPath()); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, merr.ErrPrivilegeNotPermitted) {
				status = http.StatusForbidden
			}
			c.AbortWithStatusJSON(status, gin.H{
				mhttp.HTTPReturnCode:    merr.Code(err),
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}
		c.Next()
	}
}
