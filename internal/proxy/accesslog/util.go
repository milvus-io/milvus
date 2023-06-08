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

	"github.com/cockroachdb/errors"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type BaseResponse interface {
	GetStatus() *commonpb.Status
}

func UnaryAccessLoggerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	starttime := time.Now()
	resp, err := handler(ctx, req)
	PrintAccessInfo(ctx, resp, err, info, time.Since(starttime).Milliseconds())
	return resp, err
}

func Join(path1, path2 string) string {
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
	return time.Parse(timeFormat, ts)
}

func getAccessAddr(ctx context.Context) string {
	ip, ok := peer.FromContext(ctx)
	if !ok {
		return "Unknown"
	}
	return fmt.Sprintf("%s-%s", ip.Addr.Network(), ip.Addr.String())
}

func getTraceID(ctx context.Context) (id string, ok bool) {
	meta, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		return meta.Get(clientRequestIDKey)[0], true
	}

	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	return traceID.String(), traceID.IsValid()
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
	return int(status.GetErrorCode()), true
}

func getGrpcStatus(err error) string {
	code := status.Code(err)
	if code != codes.OK {
		return fmt.Sprintf("Grpc%s", code.String())
	}
	return code.String()
}
