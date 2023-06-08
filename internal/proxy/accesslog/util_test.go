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
	"net"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestGetAccessAddr(t *testing.T) {
	ctx := context.Background()
	addr := getAccessAddr(ctx)
	assert.Equal(t, "Unknown", addr)

	newctx := peer.NewContext(
		ctx,
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})

	addr = getAccessAddr(newctx)
	assert.Equal(t, "ip-0.0.0.0%test", addr)
}

func TestGetTraceID(t *testing.T) {
	paramtable.Init()
	tracer.Init()

	ctx := context.Background()
	traceID, ok := getTraceID(ctx)
	log.Debug("traceID", zap.String("id", traceID))
	assert.False(t, ok)

	traceContext, traceSpan := otel.Tracer("proxy").Start(ctx, "demo")
	trueTraceID := traceSpan.SpanContext().TraceID().String()
	log.Debug("traceID", zap.String("trueTraceID", trueTraceID))
	ID, ok := getTraceID(traceContext)
	assert.True(t, ok)
	assert.Equal(t, trueTraceID, ID)

	ctx = metadata.AppendToOutgoingContext(ctx, clientRequestIDKey, "test")
	ID, ok = getTraceID(ctx)
	assert.True(t, ok)
	assert.Equal(t, "test", ID)
}

func TestGetResponseSize(t *testing.T) {
	resp := &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
		Value: false,
	}

	_, ok := getResponseSize(nil)
	assert.False(t, ok)

	_, ok = getResponseSize(resp)
	assert.True(t, ok)
}

func TestGetErrCode(t *testing.T) {
	resp := &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
		Value: false,
	}

	_, ok := getErrCode(nil)
	assert.False(t, ok)

	code, ok := getErrCode(resp)
	assert.True(t, ok)
	assert.Equal(t, int(commonpb.ErrorCode_UnexpectedError), code)
}
