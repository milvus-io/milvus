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

package httpserver

import (
	"context"
	"net/http"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// WrapWithBodyDeadline wraps next with a per-request deadline for reading the
// request body and writing the response (proxy.http.readTimeout /
// proxy.http.writeTimeout), for every request that is not already-negotiated
// HTTP/2.
//
// This must wrap the OUTERMOST handler assigned to http.Server.Handler --
// specifically, it must sit outside h2c.NewHandler(...), not inside it, and
// therefore also outside gin entirely. An earlier version of this deadline
// was a gin middleware (gin.Engine.Use(...)), which missed two paths that
// never reach gin's middleware chain at all:
//
//  1. gin's own automatic trailing-slash/fixed-path redirect
//     (RedirectTrailingSlash, default true) writes its response and returns
//     before ever running registered middleware -- see gin's
//     handleHTTPRequest, which only invokes the middleware chain (c.Next())
//     on an exact route match, never on the redirect branch.
//  2. golang.org/x/net/http2/h2c's own h2cUpgrade function reads the entire
//     declared request body itself (io.ReadAll(r.Body)), to validate an
//     "Upgrade: h2c" handshake, BEFORE ever invoking the wrapped handler --
//     i.e. before gin, before this proxy's gRPC/REST dispatch (httpHandler),
//     before anything this package controls.
//
// Both of those paths are still only reachable by requests that are not yet
// negotiated HTTP/2 (gin's redirect only fires for plain HTTP/1.1 REST
// requests that reached gin; an h2c Upgrade attempt is, by definition, still
// an HTTP/1.1 request until/unless the handshake succeeds). Real gRPC always
// arrives as already-negotiated HTTP/2 ("prior knowledge" -- gRPC clients
// send the HTTP/2 client preface directly and never use the Upgrade
// mechanism), so r.ProtoMajor == 2 reliably means "this is not something we
// need to protect against, and it may be a long-running legitimate gRPC
// call" at this, the earliest possible point -- before h2c.NewHandler, gin's
// dispatch, or gin's routing have done anything at all.
func WrapWithBodyDeadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 {
			next.ServeHTTP(w, r)
			return
		}

		cfg := &paramtable.Get().HTTPCfg
		rc := http.NewResponseController(w)
		now := time.Now()

		if readTimeout := cfg.ReadTimeout.GetAsDurationByParse(); readTimeout > 0 {
			if err := rc.SetReadDeadline(now.Add(readTimeout)); err != nil {
				mlog.Debug(context.TODO(), "failed to set REST body read deadline", mlog.Err(err))
			}
		}
		if writeTimeout := cfg.WriteTimeout.GetAsDurationByParse(); writeTimeout > 0 {
			if err := rc.SetWriteDeadline(now.Add(writeTimeout)); err != nil {
				mlog.Debug(context.TODO(), "failed to set REST response write deadline", mlog.Err(err))
			}
		}
		next.ServeHTTP(w, r)
	})
}
