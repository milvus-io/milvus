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

package requestbudget

import (
	"context"
	"net/http"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/h2transport/http2"
)

type upgradeStartKey struct{}

// WithUpgradeStart carries the time the HTTP/1 headers completed for the
// request that established an h2c Upgrade. It is not a deadline on the HTTP/2
// connection.
func WithUpgradeStart(r *http.Request, startedAt time.Time) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), upgradeStartKey{}, startedAt))
}

// StartedAt returns the post-header budget start. The h2c Upgrade path starts
// its budget in the outer HTTP/1 handler, before the upgrade reads its body.
func StartedAt(r *http.Request) time.Time {
	if startedAt, ok := r.Context().Value(upgradeStartKey{}).(time.Time); ok {
		return startedAt
	}
	if completedAt, ok := http2.RequestHeadersCompletedAt(r); ok {
		return completedAt
	}
	return time.Now()
}
