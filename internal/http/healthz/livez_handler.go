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

package healthz

import (
	"fmt"
	"net/http"
)

// LivenessHandler returns the HTTP Handler for the LIGHTWEIGHT liveness check (/livez).
// This is designed to be the target for the Kubernetes Liveness Probe.
func LivenessHandler() http.Handler {
	return http.HandlerFunc(LivezHandlerFunc)
}

// LivezHandlerFunc executes the minimal, non-blocking check for process activity.
// It confirms the Go scheduler is active and the HTTP server can respond immediately.
func LivezHandlerFunc(w http.ResponseWriter, r *http.Request) {
	// IMPORTANT: This check must be non-blocking. No external calls (Etcd, MinIO)
	// or checks against other internal Milvus component states are allowed here.

	w.WriteHeader(http.StatusOK)
	// Return a simple string to confirm scheduler activity.
	fmt.Fprintln(w, "Milvus Process Active")
}
