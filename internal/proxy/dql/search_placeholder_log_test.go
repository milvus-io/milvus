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

package dql

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func marshalLogPlaceholder(t testing.TB, kind commonpb.PlaceholderType, values ...[]byte) []byte {
	t.Helper()
	data, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: kind, Values: values,
	}}})
	require.NoError(t, err)
	return data
}

func TestPlaceholderLogPreview(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kind  commonpb.PlaceholderType
		value []byte
		want  string
	}{
		{"float", commonpb.PlaceholderType_FloatVector, typeutil.Float32ArrayToBytes([]float32{1.25, -2.5}), "dim=2 preview=[[1.25 -2.5]]"},
		{"float16", commonpb.PlaceholderType_Float16Vector, typeutil.Float32ArrayToFloat16Bytes([]float32{1.25, -2.5}), "dim=2 preview=[[1.25 -2.5]]"},
		{"bfloat16", commonpb.PlaceholderType_BFloat16Vector, typeutil.Float32ArrayToBFloat16Bytes([]float32{1.25, -2.5}), "dim=2 preview=[[1.25 -2.5]]"},
		{"binary", commonpb.PlaceholderType_BinaryVector, []byte{255, 0}, "dim=16 preview=[bytes=[255 0]]"},
		{"int8", commonpb.PlaceholderType_Int8Vector, []byte{255, 0, 127}, "dim=3 preview=[[-1 0 127]]"},
		{"sparse", commonpb.PlaceholderType_SparseFloatVector, []byte{7, 0, 0, 0, 0, 0, 128, 63}, "first_nnz=1 preview=[map[7:1]]"},
		{"int64", commonpb.PlaceholderType_Int64, typeutil.Int64ToBytes(-42), "preview=[-42]"},
		{"invalid int64", commonpb.PlaceholderType_Int64, []byte{1}, "preview=[<invalid int64>]"},
		{"opaque", commonpb.PlaceholderType_EmbListFloatVector, []byte{1, 2}, "first_value_bytes=2 preview=[bytes=[1 2]]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := placeholderGroupToLogString(marshalLogPlaceholder(t, tc.kind, tc.value))
			assert.Contains(t, got, tc.want)
			assert.Contains(t, got, "nq=1")
			assert.Contains(t, got, "truncated=false")
		})
	}
	text := placeholderGroupToLogString(marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar,
		[]byte("hello world\n\"foo\""), []byte("second query")))
	assert.Contains(t, text, `preview=["hello world\n\"foo\""]`)
	assert.Contains(t, text, "nq=2")
	assert.Contains(t, text, "truncated=true")
	assert.NotContains(t, text, "second query")
	assert.NotContains(t, text, "\n")
}

func TestPlaceholderLogLargeInput(t *testing.T) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = make([]byte, 1536*4)
	}
	data := marshalLogPlaceholder(t, commonpb.PlaceholderType_FloatVector, values...)
	got := placeholderGroupToLogString(data)
	assert.Contains(t, got, "nq=1000")
	assert.Contains(t, got, "dim=1536")
	assert.Contains(t, got, "truncated=true")
	assert.Less(t, len(got), 1024)

	// A single oversized text also has to be bounded before quoting/formatting it.
	text := marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar, bytes.Repeat([]byte{0}, 1024*1024))
	got = placeholderGroupToLogString(text)
	assert.Contains(t, got, "first_value_bytes=1048576")
	assert.Contains(t, got, "truncated=true")
	assert.Less(t, len(got), 1024)

	request := &milvuspb.SearchRequest{}
	for i := 0; i < 1024; i++ {
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{PlaceholderGroup: text, Nq: 1})
	}
	got = (searchPlaceholderStringer{request: request}).String()
	assert.Contains(t, got, "sub_requests=1024")
	assert.Contains(t, got, "omitted_sub_requests=")
	assert.LessOrEqual(t, len(got), maxPlaceholderLogBytes)
}

func TestPlaceholderLogWireHandling(t *testing.T) {
	// Protobuf permits reordered and unknown fields. The value precedes its type.
	value := protowire.AppendTag(nil, 3, protowire.BytesType)
	value = protowire.AppendBytes(value, []byte("hello world"))
	value = protowire.AppendTag(value, 100, protowire.Fixed32Type)
	value = protowire.AppendFixed32(value, 42)
	value = protowire.AppendTag(value, 2, protowire.VarintType)
	value = protowire.AppendVarint(value, uint64(commonpb.PlaceholderType_VarChar))
	group := protowire.AppendTag(nil, 1, protowire.BytesType)
	group = protowire.AppendBytes(group, value)
	assert.Contains(t, placeholderGroupToLogString(group), `preview=["hello world"]`)
	assert.Empty(t, placeholderGroupToLogString(nil))
	for _, data := range [][]byte{{0xff}, {0x0a, 0xff}, {0x0a, 2, 0x12, 0}, {0x0a, 2, 0x18, 1}, {0x0b}} {
		assert.NotPanics(t, func() { assert.Contains(t, placeholderGroupToLogString(data), "invalid") })
	}
	assert.Contains(t, placeholderGroupToLogString(bytes.Repeat(group, 10)), "truncated placeholders")
	assert.Contains(t, placeholderValueToLogString(bytes.Repeat([]byte{0x18, 1}, maxPlaceholderLogFields+1)), "invalid")
	assert.Contains(t, placeholderValueToLogString(bytes.Repeat([]byte{0x1a, 0}, maxPlaceholderLogFields+1)), "metadata exceeds preview limit")
}

type placeholderLogBuffer struct{ bytes.Buffer }

func (*placeholderLogBuffer) Sync() error { return nil }

func TestLogSearchPlaceholders(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.ProxyCfg.SearchPlaceholderLogRateLimit.Key
	oldRate := params.ProxyCfg.SearchPlaceholderLogRateLimit.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, oldRate)) })
	buf := &placeholderLogBuffer{}
	logger, _, err := mlog.InitLoggerWithWriteSyncer(&mlog.Config{Level: "info"}, buf)
	require.NoError(t, err)
	oldLogger, oldLevel := mlog.L(), mlog.GetLevel()
	mlog.ReplaceGlobals(logger, nil)
	mlog.SetLevel(mlog.InfoLevel)
	t.Cleanup(func() {
		mlog.ReplaceGlobals(oldLogger, nil)
		mlog.SetLevel(oldLevel)
	})
	log := mlog.With(mlog.FieldCollectionID(100))
	ctx := context.Background()
	// Even a fresh call site must not spend the initial burst token while disabled.
	for _, limit := range []string{"0", "-1", "NaN"} {
		require.NoError(t, params.Save(key, limit))
		logSearchPlaceholders(ctx, log, nil, nil)
		assert.Empty(t, buf.String())
	}

	request := &milvuspb.SearchRequest{}
	infos := []*planpb.QueryInfo{}
	for i := 0; i < 3; i++ {
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{Nq: 1,
			PlaceholderGroup: marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar, []byte(fmt.Sprintf("query-%d", i))),
		})
		infos = append(infos, &planpb.QueryInfo{QueryFieldId: int64(101 + i)})
	}
	// Prime the real limiter so this test also works with -count > 1.
	require.NoError(t, params.Save(key, "+Inf"))
	logSearchPlaceholders(ctx, log, request, infos)
	buf.Reset()
	require.NoError(t, params.Save(key, "0.000000001"))
	for i := 0; i < 100; i++ {
		logSearchPlaceholders(ctx, log, request, infos)
	}
	assert.Equal(t, 1, strings.Count(buf.String(), "proxy search request placeholder"))
	for i := 0; i < 3; i++ {
		assert.Contains(t, buf.String(), fmt.Sprintf("index=%d anns_field_id=%d", i, 101+i))
		assert.Contains(t, buf.String(), fmt.Sprintf("query-%d", i))
	}
	buf.Reset()
	// Search and HybridSearch share one budget. Then exercise runtime refresh.
	ordinary := &milvuspb.SearchRequest{Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{
		PlaceholderGroup: marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar, []byte("ordinary")),
	}}
	logSearchPlaceholders(ctx, log, ordinary, infos[:1])
	assert.Empty(t, buf.String())
	require.NoError(t, params.Save(key, "0"))
	logSearchPlaceholders(ctx, log, ordinary, infos[:1])
	assert.Empty(t, buf.String())
	require.NoError(t, params.Save(key, "+Inf"))
	logSearchPlaceholders(ctx, log, ordinary, infos[:1])
	assert.Contains(t, buf.String(), "ordinary")
	buf.Reset()
	mlog.SetLevel(mlog.WarnLevel)
	logSearchPlaceholders(ctx, log, ordinary, infos[:1])
	assert.Empty(t, buf.String())
	mlog.SetLevel(mlog.InfoLevel)

	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID}))
	large := make([][]byte, 1000)
	for i := range large {
		large[i] = make([]byte, 1536*4)
	}
	ordinary.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{
		PlaceholderGroup: marshalLogPlaceholder(t, commonpb.PlaceholderType_FloatVector, large...),
	}
	ordinary.Nq = 1000
	logSearchPlaceholders(ctx, log, ordinary, infos[:1])
	assert.Contains(t, buf.String(), "nq=1000")
	assert.Contains(t, buf.String(), "dim=1536")
	assert.Contains(t, buf.String(), traceID.String())
	assert.Less(t, buf.Len(), 4096)
}

func BenchmarkPlaceholderLogLargeInput(b *testing.B) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = make([]byte, 1536*4)
	}
	data := marshalLogPlaceholder(b, commonpb.PlaceholderType_FloatVector, values...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		placeholderGroupToLogString(data)
	}
}

func FuzzPlaceholderLogPreview(f *testing.F) {
	f.Add([]byte{0xff})
	f.Add(marshalLogPlaceholder(f, commonpb.PlaceholderType_VarChar, []byte("hello")))
	f.Fuzz(func(t *testing.T, data []byte) {
		require.LessOrEqual(t, len(placeholderGroupToLogString(data)), maxPlaceholderLogBytes)
	})
}
