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

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
)

type GrpcAccessInfo struct {
	ctx    context.Context
	status *commonpb.Status
	req    interface{}
	resp   interface{}
	err    error

	grpcInfo *grpc.UnaryServerInfo
	start    time.Time
	end      time.Time
}

func NewGrpcAccessInfo(ctx context.Context, grpcInfo *grpc.UnaryServerInfo, req interface{}) *GrpcAccessInfo {
	accessInfo := &GrpcAccessInfo{
		ctx:      ctx,
		grpcInfo: grpcInfo,
		req:      req,
		start:    time.Now(),
	}

	return accessInfo
}

// update context for more info
func (i *GrpcAccessInfo) UpdateCtx(ctx context.Context) {
	i.ctx = ctx
}

func (i *GrpcAccessInfo) SetResult(resp interface{}, err error) {
	i.resp = resp
	i.err = err
	i.end = time.Now()

	// extract status from response
	baseResp, ok := i.resp.(BaseResponse)
	if ok {
		i.status = baseResp.GetStatus()
		return
	}

	status, ok := i.resp.(*commonpb.Status)
	if ok {
		i.status = status
		return
	}
}

func (i *GrpcAccessInfo) TimeCost() string {
	if i.end.IsZero() {
		return Unknown
	}
	return fmt.Sprint(i.end.Sub(i.start))
}

func (i *GrpcAccessInfo) TimeNow() string {
	return time.Now().Format(timeFormat)
}

func (i *GrpcAccessInfo) TimeStart() string {
	if i.start.IsZero() {
		return Unknown
	}
	return i.start.Format(timeFormat)
}

func (i *GrpcAccessInfo) TimeEnd() string {
	if i.end.IsZero() {
		return Unknown
	}
	return i.end.Format(timeFormat)
}

func (i *GrpcAccessInfo) MethodName() string {
	_, methodName := path.Split(i.grpcInfo.FullMethod)
	return methodName
}

func (i *GrpcAccessInfo) Address() string {
	ip, ok := peer.FromContext(i.ctx)
	if !ok {
		return "Unknown"
	}
	return fmt.Sprintf("%s-%s", ip.Addr.Network(), ip.Addr.String())
}

func (i *GrpcAccessInfo) TraceID() string {
	meta, ok := metadata.FromOutgoingContext(i.ctx)
	if ok {
		return meta.Get(ClientRequestIDKey)[0]
	}

	traceID := trace.SpanFromContext(i.ctx).SpanContext().TraceID()
	if !traceID.IsValid() {
		return Unknown
	}

	return traceID.String()
}

func (i *GrpcAccessInfo) MethodStatus() string {
	code := status.Code(i.err)
	if code != codes.OK && code != codes.Unknown {
		return fmt.Sprintf("Grpc%s", code.String())
	}

	if i.status.GetCode() != 0 || i.err != nil {
		return "Failed"
	}

	return "Successful"
}

func (i *GrpcAccessInfo) UserName() string {
	username, err := getCurUserFromContext(i.ctx)
	if err != nil {
		return Unknown
	}
	return username
}

type SizeResponse interface {
	XXX_Size() int
}

func (i *GrpcAccessInfo) ResponseSize() string {
	var size int
	switch r := i.resp.(type) {
	case SizeResponse:
		size = r.XXX_Size()
	case proto.Message:
		size = proto.Size(r)
	default:
		return Unknown
	}

	return fmt.Sprint(size)
}

type BaseResponse interface {
	GetStatus() *commonpb.Status
}

func (i *GrpcAccessInfo) ErrorCode() string {
	if i.status != nil {
		return fmt.Sprint(i.status.GetCode())
	}

	return fmt.Sprint(merr.Code(i.err))
}

func (i *GrpcAccessInfo) respStatus() *commonpb.Status {
	baseResp, ok := i.resp.(BaseResponse)
	if ok {
		return baseResp.GetStatus()
	}

	status, ok := i.resp.(*commonpb.Status)
	if ok {
		return status
	}
	return nil
}

func (i *GrpcAccessInfo) ErrorMsg() string {
	if i.err != nil {
		return strings.ReplaceAll(i.err.Error(), "\n", "\\n")
	}

	if status := i.respStatus(); status != nil {
		return strings.ReplaceAll(status.GetReason(), "\n", "\\n")
	}

	return Unknown
}

func (i *GrpcAccessInfo) ErrorType() string {
	if i.err != nil {
		return merr.GetErrorType(i.err).String()
	}

	if status := i.respStatus(); status.GetCode() > 0 {
		if _, ok := status.ExtraInfo[merr.InputErrorFlagKey]; ok {
			return merr.InputError.String()
		}
		return merr.SystemError.String()
	}

	return ""
}

func (i *GrpcAccessInfo) DbName() string {
	name, ok := requestutil.GetDbNameFromRequest(i.req)
	if !ok {
		return Unknown
	}
	return name.(string)
}

func (i *GrpcAccessInfo) CollectionName() string {
	name, ok := requestutil.GetCollectionNameFromRequest(i.req)
	if !ok {
		return Unknown
	}
	return name.(string)
}

func (i *GrpcAccessInfo) PartitionName() string {
	name, ok := requestutil.GetPartitionNameFromRequest(i.req)
	if ok {
		return name.(string)
	}

	names, ok := requestutil.GetPartitionNamesFromRequest(i.req)
	if ok {
		return fmt.Sprint(names.([]string))
	}

	return Unknown
}

func (i *GrpcAccessInfo) Expression() string {
	expr, ok := requestutil.GetExprFromRequest(i.req)
	if ok {
		return expr.(string)
	}

	if req, ok := i.req.(*milvuspb.HybridSearchRequest); ok {
		return listToString(lo.Map(req.GetRequests(), func(req *milvuspb.SearchRequest, _ int) string { return req.GetDsl() }))
	}

	dsl, ok := requestutil.GetDSLFromRequest(i.req)
	if ok {
		return dsl.(string)
	}
	return Unknown
}

func (i *GrpcAccessInfo) SdkVersion() string {
	clientInfo := connection.GetManager().Get(i.ctx)
	if clientInfo != nil {
		return clientInfo.GetSdkType() + "-" + clientInfo.GetSdkVersion()
	}

	if req, ok := i.req.(*milvuspb.ConnectRequest); ok {
		return req.GetClientInfo().GetSdkType() + "-" + req.GetClientInfo().GetSdkVersion()
	}

	return getSdkVersionByUserAgent(i.ctx)
}

func (i *GrpcAccessInfo) OutputFields() string {
	fields, ok := requestutil.GetOutputFieldsFromRequest(i.req)
	if ok {
		return fmt.Sprint(fields.([]string))
	}
	return Unknown
}

func (i *GrpcAccessInfo) ConsistencyLevel() string {
	level, ok := requestutil.GetConsistencyLevelFromRequst(i.req)
	if ok {
		return level.String()
	}
	return Unknown
}

func (i *GrpcAccessInfo) AnnsField() string {
	if req, ok := i.req.(*milvuspb.SearchRequest); ok {
		return getAnnsFieldFromKvs(req.GetSearchParams())
	}

	if req, ok := i.req.(*milvuspb.HybridSearchRequest); ok {
		fields := lo.Map(req.GetRequests(), func(req *milvuspb.SearchRequest, _ int) string { return getAnnsFieldFromKvs(req.GetSearchParams()) })
		return listToString(fields)
	}
	return Unknown
}
