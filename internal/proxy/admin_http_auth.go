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
	"github.com/gin-gonic/gin"

	mhttp "github.com/milvus-io/milvus/internal/http"
)

// adminAuthMiddleware adapts the management-plane root authentication policy
// to routes served by the proxy's Gin tree. Most management routes are served
// by net/http and use Handler.AuthPolicy directly, but a few operator-facing
// endpoints live under /api/v1 and need the same policy there.
func adminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !mhttp.AuthByAdminFlag() {
			c.Next()
			return
		}

		if err := mhttp.CheckAdminAuth(c.Request.Context(), c.Request); err != nil {
			c.AbortWithStatusJSON(mhttp.HTTPStatusFromPrivilegeError(err), gin.H{
				mhttp.HTTPReturnMessage: err.Error(),
			})
			return
		}

		c.Next()
	}
}
