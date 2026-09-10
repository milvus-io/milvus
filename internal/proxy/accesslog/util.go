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
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	accessInfo := info.NewRestfulInfo(ctx)
	ctx.Set(ContextLogKey, accessInfo)
	// Bridge the access info into the standard request context. The gRPC
	// interceptor injects the same AccessKey value for grpc traffic; without
	// this, REST handlers reach the Proxy through the hook interceptor with a
	// context lacking AccessKey, so task PreExecute (SetActualConsistencyLevel)
	// and slow logs fall back to the raw request consistency level.
	if ctx.Request != nil {
		reqCtx := context.WithValue(ctx.Request.Context(), AccessKey{}, accessInfo)
		ctx.Request = ctx.Request.WithContext(reqCtx)
	}
	ctx.Next()
	accessInfo.InitReq()
	_globalL.Write(accessInfo)

	// Plugin pulls req / status / err / path from the gin context — the
	// middleware only needs to surface it through the stdlib context.
	reqCtx := context.WithValue(ctx.Request.Context(), hook.GinParamsKey, ctx)
	hookutil.GetExtension().ReportAction(reqCtx, nil, nil, nil, "", hookutil.ActionRestfulReturn)
}

func SetHTTPParams(ctx *gin.Context, p *gin.LogFormatterParams) {
	value, ok := ctx.Get(ContextLogKey)
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
		return time.Time{}, merr.WrapErrParameterInvalidMsg("mismatched prefix")
	}
	if !strings.HasSuffix(filename, ext) {
		return time.Time{}, merr.WrapErrParameterInvalidMsg("mismatched extension")
	}
	ts := filename[len(prefix) : len(filename)-len(ext)]
	return time.Parse(timeNameFormat, ts)
}

func SetActualConsistencyLevel(ctx context.Context, acl commonpb.ConsistencyLevel) {
	if ctx != nil {
		v := ctx.Value(AccessKey{})
		info, ok := v.(info.AccessInfo)
		if ok && info != nil {
			info.SetActualConsistencyLevel(acl)
		}
	}
}

type ConsistencyLevelCarrier interface {
	GetConsistencyLevel() commonpb.ConsistencyLevel
}

type ConsistencyLevelHelper struct {
	accessInfo  info.AccessInfo
	clvlCarrier ConsistencyLevelCarrier
}

func (clHelper *ConsistencyLevelHelper) String() string {
	if clHelper.accessInfo != nil {
		return fmt.Sprintf("ACT-%s", clHelper.accessInfo.ConsistencyLevel())
	}
	if clHelper.clvlCarrier == nil {
		return info.Unknown
	}
	return fmt.Sprintf("REQ-%s", clHelper.clvlCarrier.GetConsistencyLevel().String())
}

func NewConsistencyLevelHelper(ctx context.Context, clvlCarrier ConsistencyLevelCarrier) *ConsistencyLevelHelper {
	cc := &ConsistencyLevelHelper{clvlCarrier: clvlCarrier}
	if ctx != nil {
		v := ctx.Value(AccessKey{})
		info, ok := v.(info.AccessInfo)
		if ok && info != nil {
			cc.accessInfo = info
		}
	}
	return cc
}
