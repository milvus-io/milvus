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

package accesslog

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
)

type AccessKey struct{}

const ContextLogKey = "accesslog"

func UnaryAccessLogInterceptor(ctx context.Context, req any, rpcInfo *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	accessInfo := info.NewGrpcAccessInfo(ctx, rpcInfo, req)
	newCtx := context.WithValue(ctx, AccessKey{}, accessInfo)
	resp, err := handler(newCtx, req)
	accessInfo.SetResult(resp, err)
	_globalL.Write(accessInfo)
	return resp, err
}

func UnaryUpdateAccessInfoInterceptor(ctx context.Context, req any, rpcInfonfo *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	accessInfo := ctx.Value(AccessKey{}).(*info.GrpcAccessInfo)
	accessInfo.UpdateCtx(ctx)
	return handler(ctx, req)
}

func AccessLogMiddleware(ctx *gin.Context) {
	accessInfo := info.NewRestfulInfo()
	ctx.Set(ContextLogKey, accessInfo)
	ctx.Next()
	accessInfo.InitReq()
	_globalL.Write(accessInfo)
}

func SetHTTPParams(p *gin.LogFormatterParams) {
	value, ok := p.Keys[ContextLogKey]
	if !ok {
		return
	}

	info := value.(*info.RestfulInfo)
	info.SetParams(p)
}

func join(path1, path2 string) string {
	if strings.HasSuffix(path1, "/") {
		return path1 + path2
	}
	return path1 + "/" + path2
}

func timeFromName(filename, prefix, ext string) (time.Time, error) {
	if !strings.HasPrefix(filename, prefix) {
		return time.Time{}, errors.New("mismatched prefix")
	}
	if !strings.HasSuffix(filename, ext) {
		return time.Time{}, errors.New("mismatched extension")
	}
	ts := filename[len(prefix) : len(filename)-len(ext)]
	return time.Parse(timeNameFormat, ts)
}
