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
	"path"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
)

type AccessInfo interface {
	Get(keys ...string) []string
}

type GrpcAccessInfo struct {
	ctx    context.Context
	info   *grpc.UnaryServerInfo
	status *commonpb.Status
	req    interface{}
	resp   interface{}
	err    error

	start time.Time
	end   time.Time
}

func NewGrpcAccessInfo(ctx context.Context, info *grpc.UnaryServerInfo, req interface{}) *GrpcAccessInfo {
	accessInfo := &GrpcAccessInfo{
		ctx:   ctx,
		info:  info,
		req:   req,
		start: time.Now(),
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

func (i *GrpcAccessInfo) Get(keys ...string) []string {
	result := []string{}
	metricMap := map[string]string{}
	for _, key := range keys {
		if value, ok := metricMap[key]; ok {
			result = append(result, value)
		} else if getFunc, ok := metricFuncMap[key]; ok {
			result = append(result, getFunc(i))
		}
	}
	return result
}

func (i *GrpcAccessInfo) Write() bool {
	if _globalW == nil {
		return false
	}

	formatter, ok := _globalF.GetByMethod(getMethodName(i))
	if !ok {
		return false
	}
	_, err := _globalW.Write([]byte(formatter.Format(i)))
	return err == nil
}

func getTimeCost(i *GrpcAccessInfo) string {
	if i.end.IsZero() {
		return unknownString
	}
	return fmt.Sprint(i.end.Sub(i.start))
}

func getTimeNow(i *GrpcAccessInfo) string {
	return time.Now().Format(timePrintFormat)
}

func getTimeStart(i *GrpcAccessInfo) string {
	if i.start.IsZero() {
		return unknownString
	}
	return i.start.Format(timePrintFormat)
}

func getTimeEnd(i *GrpcAccessInfo) string {
	if i.end.IsZero() {
		return unknownString
	}
	return i.end.Format(timePrintFormat)
}

func getMethodName(i *GrpcAccessInfo) string {
	_, methodName := path.Split(i.info.FullMethod)
	return methodName
}

func getAddr(i *GrpcAccessInfo) string {
	ip, ok := peer.FromContext(i.ctx)
	if !ok {
		return "Unknown"
	}
	return fmt.Sprintf("%s-%s", ip.Addr.Network(), ip.Addr.String())
}

func getTraceID(i *GrpcAccessInfo) string {
	meta, ok := metadata.FromOutgoingContext(i.ctx)
	if ok {
		return meta.Get(clientRequestIDKey)[0]
	}

	traceID := trace.SpanFromContext(i.ctx).SpanContext().TraceID()
	return traceID.String()
}

func getMethodStatus(i *GrpcAccessInfo) string {
	code := status.Code(i.err)
	if code != codes.OK && code != codes.Unknown {
		return fmt.Sprintf("Grpc%s", code.String())
	}

	if i.status.GetCode() != 0 {
		return "Failed"
	}

	return code.String()
}

func getUserName(i *GrpcAccessInfo) string {
	username, err := getCurUserFromContext(i.ctx)
	if err != nil {
		return unknownString
	}
	return username
}

type SizeResponse interface {
	XXX_Size() int
}

func getResponseSize(i *GrpcAccessInfo) string {
	message, ok := i.resp.(SizeResponse)
	if !ok {
		return unknownString
	}

	return fmt.Sprint(message.XXX_Size())
}

type BaseResponse interface {
	GetStatus() *commonpb.Status
}

func getErrorCode(i *GrpcAccessInfo) string {
	if i.status != nil {
		return fmt.Sprint(i.status.GetCode())
	}

	return fmt.Sprint(merr.Code(i.err))
}

func getErrorMsg(i *GrpcAccessInfo) string {
	if i.err != nil {
		return i.err.Error()
	}

	baseResp, ok := i.resp.(BaseResponse)
	if ok {
		status := baseResp.GetStatus()
		return status.GetReason()
	}

	status, ok := i.resp.(*commonpb.Status)
	if ok {
		return status.GetReason()
	}
	return unknownString
}

func getDbName(i *GrpcAccessInfo) string {
	name, ok := requestutil.GetDbNameFromRequest(i.req)
	if !ok {
		return unknownString
	}
	return name.(string)
}

func getCollectionName(i *GrpcAccessInfo) string {
	name, ok := requestutil.GetCollectionNameFromRequest(i.req)
	if !ok {
		return unknownString
	}
	return name.(string)
}

func getPartitionName(i *GrpcAccessInfo) string {
	name, ok := requestutil.GetPartitionNameFromRequest(i.req)
	if ok {
		return name.(string)
	}

	names, ok := requestutil.GetPartitionNamesFromRequest(i.req)
	if ok {
		return fmt.Sprint(names.([]string))
	}

	return unknownString
}

func getExpr(i *GrpcAccessInfo) string {
	expr, ok := requestutil.GetExprFromRequest(i.req)
	if !ok {
		return unknownString
	}
	return expr.(string)
}
