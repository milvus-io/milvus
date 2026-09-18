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

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// BodyDeadlineMiddleware bounds how long a single REST request may take to
// read its declared body and to write its response, using
// proxy.http.readTimeout / proxy.http.writeTimeout.
//
// This is deliberately NOT done via http.Server.ReadTimeout/WriteTimeout:
// in the default deployment (proxy.http.port empty), this proxy's
// http.Server also serves external gRPC over the same shared HTTP/2
// listener (see (*grpcproxy.Server).httpHandler / httpHandler's
// grpcExternalServer.ServeHTTP dispatch in service.go), and Go's HTTP/2
// implementation arms per-stream deadlines directly from
// Server.ReadTimeout/WriteTimeout (golang.org/x/net/http2 server.go).
// A Server-wide deadline would therefore also cut long-running gRPC RPCs.
//
// Using http.ResponseController.SetReadDeadline/SetWriteDeadline instead
// scopes the deadline to this one request/connection. It also cannot ever
// affect gRPC traffic: gRPC requests are diverted to
// grpcExternalServer.ServeHTTP before reaching gin at all, so gin
// middleware -- this one included -- never runs for them regardless of
// this middleware's registration point.
//
// Must be registered before any middleware that swaps gin.Context.Writer
// for a value that does not unwrap to the real, connection-backed
// http.ResponseWriter (e.g. timeoutMiddleware's timeoutResponseRecorder) --
// http.ResponseController needs to reach the real writer to set a deadline
// on the underlying connection.
func BodyDeadlineMiddleware() gin.HandlerFunc {
	return func(gCtx *gin.Context) {
		cfg := &paramtable.Get().HTTPCfg
		rc := http.NewResponseController(gCtx.Writer)
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
		gCtx.Next()
	}
}
