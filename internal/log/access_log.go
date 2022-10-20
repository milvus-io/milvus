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

package log

import (
	"context"
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	clientRequestIDKey = "client_request_id"
)

type BaseResponse interface {
	GetStatus() *commonpb.Status
}

func PrintAccessInfo(ctx context.Context, resp interface{}, err error, rpcInfo *grpc.UnaryServerInfo, timeCost int64) {
	fields := []zap.Field{
		//format time cost of task
		zap.String("timeCost", fmt.Sprintf("%d ms", timeCost)),
	}

	//get trace ID of task
	traceId, ok := getTraceID(ctx)
	if ok {
		fields = append(fields, zap.String("traceId", traceId))
	}

	//get response size of task
	responseSize, ok := getResponseSize(resp)
	if ok {
		fields = append(fields, zap.Int("responseSize", responseSize))
	}

	//get err code of task
	errCode, ok := getErrCode(resp)
	if ok {
		fields = append(fields, zap.Int("errorCode", errCode))
	}

	//get status of grpc
	Status := getGrpcStatus(err)
	if Status == "OK" && errCode > 0 {
		Status = "TaskFailed"
	}

	//get method name of grpc
	_, methodName := path.Split(rpcInfo.FullMethod)

	A().Info(fmt.Sprintf("%v: %s-%s", Status, getAccessAddr(ctx), methodName), fields...)
}

func getAccessAddr(ctx context.Context) string {
	ip, ok := peer.FromContext(ctx)
	if !ok {
		return "Unkonwn"
	}
	return fmt.Sprintf("%s-%s", ip.Addr.Network(), ip.Addr.String())
}

func getTraceID(ctx context.Context) (id string, ok bool) {
	meta, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		return meta.Get(clientRequestIDKey)[0], true
	}

	traceId, _, ok := infoFromContext(ctx)
	if ok {
		return traceId, true
	}
	return "", false
}

func getResponseSize(resq interface{}) (int, bool) {
	message, ok := resq.(proto.Message)
	if !ok {
		return 0, false
	}

	return proto.Size(message), true
}

func getErrCode(resp interface{}) (int, bool) {
	baseResp, ok := resp.(BaseResponse)
	if !ok {
		return 0, false
	}

	status := baseResp.GetStatus()
	return int(status.ErrorCode), true
}

func getGrpcStatus(err error) string {
	code := status.Code(err)
	if code != codes.OK {
		return fmt.Sprintf("Grpc%s", code.String())
	}
	return code.String()
}

// InfoFromSpan is a method return span details.
func infoFromSpan(span opentracing.Span) (traceID string, sampled, found bool) {
	if span != nil {
		if spanContext, ok := span.Context().(jaeger.SpanContext); ok {
			traceID = spanContext.TraceID().String()
			sampled = spanContext.IsSampled()
			return traceID, sampled, true
		}
	}
	return "", false, false
}

// InfoFromContext is a method return details of span associated with context.
func infoFromContext(ctx context.Context) (traceID string, sampled, found bool) {
	if ctx != nil {
		if span := opentracing.SpanFromContext(ctx); span != nil {
			return infoFromSpan(span)
		}
	}
	return "", false, false
}
