// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package trace

import (
	"context"
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/baggage"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/metadata"
)

type simpleStruct struct {
	name  string
	value string
}

func TestMain(m *testing.M) {
	baseTbl := paramtable.BaseTable{}
	baseTbl.Init()
	closer := InitTracing("test", &baseTbl, "")
	defer closer.Close()
	os.Exit(m.Run())
}

func TestInjectAndExtract(t *testing.T) {
	t.Run("inject and extract grpc trace", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"log_level":         zapcore.FatalLevel.String(),
			"client_request_id": "test-request-id",
		})
		ctx := metadata.NewOutgoingContext(context.TODO(), md)

		data := make(map[string]string)
		InjectContextToPulsarMsgProperties(ctx, data)
		assert.Equal(t, "fatal", data["log_level"])
		assert.Equal(t, "test-request-id", data["client_request_id"])

		newctx := ExtractContextFromProperties(context.TODO(), data)

		newmd, ok := metadata.FromIncomingContext(newctx)
		assert.True(t, ok)
		assert.Equal(t, []string{"fatal"}, newmd.Get("log_level"))
		assert.Equal(t, []string{"test-request-id"}, newmd.Get("client_request_id"))
	})

	t.Run("inject and extract baggage", func(t *testing.T) {
		property, err := baggage.NewKeyValueProperty("prop_key", "prop_val")
		assert.Nil(t, err)
		member, err := baggage.NewMember("mem_key", "mem_val", property)
		assert.Nil(t, err)
		b, err := baggage.New(member)
		assert.Nil(t, err)

		ctx := baggage.ContextWithBaggage(context.TODO(), b)
		data := make(map[string]string)
		InjectContextToPulsarMsgProperties(ctx, data)
		assert.Equal(t, "mem_key=mem_val;prop_key=prop_val", data["baggage"])

		newctx := ExtractContextFromProperties(ctx, data)
		newb := baggage.FromContext(newctx)

		assert.Equal(t, b, newb)
	})
}

func TestCallerInfo(t *testing.T) {
	fnName, loc := callerInfo(2)

	assert.Equal(t, "github.com/milvus-io/milvus/internal/util/trace.TestCallerInfo", fnName)
	assert.Equal(t, "util_test.go:81", loc)

	fnName, loc = callfn()
	assert.Equal(t, "github.com/milvus-io/milvus/internal/util/trace.callfn", fnName)
	assert.Equal(t, "util_test.go:96", loc)

	fnName, loc = callerInfo(100)
	assert.Equal(t, "unknown", fnName)
	assert.Equal(t, "unknown", loc)
}

func callfn() (string, string) {
	return callerInfo(2)
}

func TestTraceIDFromContext(t *testing.T) {
	ctx, span := StartSpanFromContextWithOperationName(context.TODO(), "test")
	assert.NotNil(t, span)

	spanctx := span.SpanContext()
	trace0, sampled, ok := InfoFromContext(ctx)
	assert.True(t, ok && sampled)
	trace1, sampled2, ok2 := InfoFromSpan(span)
	assert.True(t, ok2 && sampled2)

	assert.True(t, len(trace0) > 0)
	assert.Equal(t, trace0, trace1, "Trace0: %s, Trace1: %s", trace0, trace1)
	assert.Equal(t, spanctx.TraceID().String(), trace0)
}
