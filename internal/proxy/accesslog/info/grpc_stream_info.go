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

package info

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GrpcStreamAccessInfo is the access-log info for a streaming RPC. Streaming
// calls have no request object at the interceptor layer, so request-derived
// fields (db/collection/partition/expr/...) return Unknown; the audit-relevant
// fields (method, user, address, time, status, error) are populated.
type GrpcStreamAccessInfo struct {
	ctx      context.Context
	grpcInfo *grpc.StreamServerInfo
	err      error
	start    time.Time
	end      time.Time
}

func NewGrpcStreamAccessInfo(ctx context.Context, grpcInfo *grpc.StreamServerInfo) *GrpcStreamAccessInfo {
	return &GrpcStreamAccessInfo{
		ctx:      ctx,
		grpcInfo: grpcInfo,
		start:    time.Now(),
	}
}

func (i *GrpcStreamAccessInfo) UpdateCtx(ctx context.Context) {
	i.ctx = ctx
}

func (i *GrpcStreamAccessInfo) SetResult(_ interface{}, err error) {
	i.err = err
	i.end = time.Now()
}

func (i *GrpcStreamAccessInfo) TimeCost() string {
	if i.end.IsZero() {
		return Unknown
	}
	return fmt.Sprint(i.end.Sub(i.start))
}

func (i *GrpcStreamAccessInfo) TimeNow() string {
	return time.Now().Format(timeFormat)
}

func (i *GrpcStreamAccessInfo) TimeStart() string {
	if i.start.IsZero() {
		return Unknown
	}
	return i.start.Format(timeFormat)
}

func (i *GrpcStreamAccessInfo) TimeEnd() string {
	if i.end.IsZero() {
		return Unknown
	}
	return i.end.Format(timeFormat)
}

func (i *GrpcStreamAccessInfo) MethodName() string {
	_, methodName := path.Split(i.grpcInfo.FullMethod)
	return methodName
}

func (i *GrpcStreamAccessInfo) Address() string {
	ip, ok := peer.FromContext(i.ctx)
	if !ok {
		return Unknown
	}
	return fmt.Sprintf("%s-%s", ip.Addr.Network(), ip.Addr.String())
}

func (i *GrpcStreamAccessInfo) TraceID() string {
	meta, ok := metadata.FromIncomingContext(i.ctx)
	if ok {
		values := meta.Get(ClientRequestIDKey)
		if len(values) > 0 {
			return values[0]
		}
	}

	traceID := trace.SpanFromContext(i.ctx).SpanContext().TraceID()
	if !traceID.IsValid() {
		return Unknown
	}
	return traceID.String()
}

func (i *GrpcStreamAccessInfo) MethodStatus() string {
	code := status.Code(i.err)
	if code != codes.OK && code != codes.Unknown {
		return fmt.Sprintf("Grpc%s", code.String())
	}
	if i.err != nil {
		return "Failed"
	}
	return "Successful"
}

func (i *GrpcStreamAccessInfo) UserName() string {
	username, err := getCurUserFromContext(i.ctx)
	if err != nil {
		return Unknown
	}
	return username
}

func (i *GrpcStreamAccessInfo) ResponseSize() string {
	return NotAny
}

func (i *GrpcStreamAccessInfo) ErrorCode() string {
	return fmt.Sprint(merr.Code(i.err))
}

func (i *GrpcStreamAccessInfo) ErrorMsg() string {
	if i.err != nil {
		return strings.ReplaceAll(i.err.Error(), "\n", "\\n")
	}
	return Unknown
}

func (i *GrpcStreamAccessInfo) ErrorType() string {
	if i.err != nil {
		return merr.GetErrorType(i.err).String()
	}
	return ""
}

func (i *GrpcStreamAccessInfo) DbName() string                                      { return Unknown }
func (i *GrpcStreamAccessInfo) CollectionName() string                              { return Unknown }
func (i *GrpcStreamAccessInfo) PartitionName() string                               { return Unknown }
func (i *GrpcStreamAccessInfo) Expression() string                                  { return Unknown }
func (i *GrpcStreamAccessInfo) OutputFields() string                                { return NotAny }
func (i *GrpcStreamAccessInfo) AnnsField() string                                   { return NotAny }
func (i *GrpcStreamAccessInfo) NQ() string                                          { return NotAny }
func (i *GrpcStreamAccessInfo) SearchParams() string                                { return NotAny }
func (i *GrpcStreamAccessInfo) QueryParams() string                                 { return NotAny }
func (i *GrpcStreamAccessInfo) TemplateValueLength() string                         { return NotAny }
func (i *GrpcStreamAccessInfo) PartialUpdate() string                               { return NotAny }
func (i *GrpcStreamAccessInfo) SetActualConsistencyLevel(commonpb.ConsistencyLevel) {}

func (i *GrpcStreamAccessInfo) ConsistencyLevel() string { return Unknown }

func (i *GrpcStreamAccessInfo) SdkVersion() string {
	clientInfo := connection.GetManager().Get(i.ctx)
	if clientInfo != nil {
		return clientInfo.GetSdkType() + "-" + clientInfo.GetSdkVersion()
	}
	return getSdkVersionByUserAgent(i.ctx)
}

func (i *GrpcStreamAccessInfo) ClientRequestTime() string {
	unixmsec, ok := logutil.GetClientReqUnixmsecGrpc(i.ctx)
	if !ok {
		return Unknown
	}
	return time.UnixMilli(unixmsec).Format(timeFormat)
}

// Ensure GrpcStreamAccessInfo implements AccessInfo.
var _ AccessInfo = (*GrpcStreamAccessInfo)(nil)
