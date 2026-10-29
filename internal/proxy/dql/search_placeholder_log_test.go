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
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
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

const testPlaceholderLogMaxBytes = 16 * 1024

func decodeLogPlaceholder(t testing.TB, data []byte) *commonpb.PlaceholderGroup {
	t.Helper()
	group := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(data, group))
	return group
}

func renderLogPlaceholder(t testing.TB, data []byte, limit int) string {
	t.Helper()
	preview := &searchPlaceholderStringer{}
	preview.capture(decodeLogPlaceholder(t, data))
	w := newPlaceholderLogWriter(limit)
	writePlaceholderGroup(w, preview.groups[0])
	return w.finish()
}

func captureLogRequest(t testing.TB, preview *searchPlaceholderStringer, request *milvuspb.SearchRequest) {
	t.Helper()
	if preview == nil {
		return
	}
	if len(request.GetSubReqs()) == 0 {
		preview.capture(decodeLogPlaceholder(t, request.GetPlaceholderGroup()))
	} else {
		for _, sub := range request.GetSubReqs() {
			preview.capture(decodeLogPlaceholder(t, sub.GetPlaceholderGroup()))
		}
	}
}

func renderLogRequest(t testing.TB, request *milvuspb.SearchRequest, limit int) string {
	t.Helper()
	preview := &searchPlaceholderStringer{request: request, maxBytes: limit}
	captureLogRequest(t, preview, request)
	return preview.String()
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
			got := renderLogPlaceholder(t, marshalLogPlaceholder(t, tc.kind, tc.value), testPlaceholderLogMaxBytes)
			assert.Contains(t, got, tc.want)
			assert.Contains(t, got, "nq=1")
			assert.Contains(t, got, "omitted_queries=0")
			assert.NotContains(t, got, "<truncated>")
		})
	}
	text := renderLogPlaceholder(t, marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar,
		[]byte("hello world\n\"foo\""), []byte("second query")), testPlaceholderLogMaxBytes)
	assert.Contains(t, text, `preview=["hello world\n\"foo\""]`)
	assert.Contains(t, text, "nq=2")
	assert.Contains(t, text, "omitted_queries=1")
	assert.NotContains(t, text, "second query")
	assert.NotContains(t, text, "\n")
}

func TestPlaceholderLogLargeInput(t *testing.T) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = make([]byte, 1536*4)
	}
	data := marshalLogPlaceholder(t, commonpb.PlaceholderType_FloatVector, values...)
	got := renderLogPlaceholder(t, data, testPlaceholderLogMaxBytes)
	assert.Contains(t, got, "nq=1000")
	assert.Contains(t, got, "dim=1536")
	assert.Contains(t, got, "omitted_queries=999")
	assert.NotContains(t, got, "<truncated>")
	assert.LessOrEqual(t, len(got), testPlaceholderLogMaxBytes)

	// A single oversized text also has to be bounded before quoting/formatting it.
	text := marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar, bytes.Repeat([]byte{0}, 1024*1024))
	got = renderLogPlaceholder(t, text, testPlaceholderLogMaxBytes)
	assert.Contains(t, got, "first_value_bytes=1048576")
	assert.Contains(t, got, "<truncated>")
	assert.LessOrEqual(t, len(got), testPlaceholderLogMaxBytes)

	request := &milvuspb.SearchRequest{}
	for i := 0; i < 16; i++ {
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{PlaceholderGroup: text, Nq: 1})
	}
	got = renderLogRequest(t, request, testPlaceholderLogMaxBytes)
	assert.Contains(t, got, "sub_requests=16")
	assert.Contains(t, got, "<truncated>")
	assert.LessOrEqual(t, len(got), testPlaceholderLogMaxBytes)
}

func TestPlaceholderLogCapture(t *testing.T) {
	first := []byte("original first query")
	ph := &commonpb.PlaceholderValue{Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{first, []byte("second query")}}
	preview := &searchPlaceholderStringer{}
	preview.capture(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{ph, {}}})
	require.Len(t, preview.groups, 1)
	require.Len(t, preview.groups[0], 2)
	value := preview.groups[0][0]
	assert.Equal(t, 2, value.nq)
	assert.Same(t, &first[0], &value.first[0]) // Reuse the decoded first value, without another payload copy.
	ph.Type = commonpb.PlaceholderType_FloatVector
	ph.Values = [][]byte{{0, 0, 0, 0}} // Later conversion must not change the preview.
	w := newPlaceholderLogWriter(testPlaceholderLogMaxBytes)
	writePlaceholderGroup(w, preview.groups[0])
	got := w.finish()
	assert.Contains(t, got, `type=VarChar nq=2`)
	assert.Contains(t, got, `preview=["original first query"]`)
	assert.Contains(t, got, `omitted_queries=1`)
	assert.Contains(t, got, `nq=0`)
	assert.NotContains(t, got, "second query")
	assert.NotContains(t, got, "FloatVector")
}

func TestPlaceholderLogUsesWholeBudgetForFirstQuery(t *testing.T) {
	vector := make([]float32, 1536)
	vector[len(vector)-1] = 123.5
	for _, tc := range []struct {
		name  string
		kind  commonpb.PlaceholderType
		value []byte
		want  string
	}{
		{"float", commonpb.PlaceholderType_FloatVector, typeutil.Float32ArrayToBytes(vector), "123.5"},
		{"float16", commonpb.PlaceholderType_Float16Vector, typeutil.Float32ArrayToFloat16Bytes(vector), "123.5"},
		{"bfloat16", commonpb.PlaceholderType_BFloat16Vector, typeutil.Float32ArrayToBFloat16Bytes(vector), "123.5"},
		{"text", commonpb.PlaceholderType_VarChar, []byte(strings.Repeat("x", 1024) + "END_OF_FIRST_QUERY"), "END_OF_FIRST_QUERY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := marshalLogPlaceholder(t, tc.kind, tc.value, []byte("second query"))
			got := renderLogPlaceholder(t, data, testPlaceholderLogMaxBytes)
			assert.Contains(t, got, tc.want)
			assert.Contains(t, got, "omitted_queries=1")
			assert.NotContains(t, got, "<truncated>")
			assert.NotContains(t, got, "second query")
		})
	}
}

func TestPlaceholderLogByteBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kind  commonpb.PlaceholderType
		value []byte
	}{
		{"float", commonpb.PlaceholderType_FloatVector, bytes.Repeat(typeutil.Float32ToBytes(1.234567), 10000)},
		{"float16", commonpb.PlaceholderType_Float16Vector, bytes.Repeat(typeutil.Float32ToFloat16Bytes(1.234567), 10000)},
		{"bfloat16", commonpb.PlaceholderType_BFloat16Vector, bytes.Repeat(typeutil.Float32ToBFloat16Bytes(1.234567), 10000)},
		{"sparse", commonpb.PlaceholderType_SparseFloatVector, bytes.Repeat([]byte{7, 0, 0, 0, 0, 0, 128, 63}, 10000)},
		{"binary", commonpb.PlaceholderType_BinaryVector, bytes.Repeat([]byte{255}, 10000)},
		{"int8", commonpb.PlaceholderType_Int8Vector, bytes.Repeat([]byte{128}, 10000)},
		{"text", commonpb.PlaceholderType_VarChar, []byte(strings.Repeat("中文\n\"\\", 10000))},
		{"invalid utf8", commonpb.PlaceholderType_VarChar, bytes.Repeat([]byte{255}, 10000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := marshalLogPlaceholder(t, tc.kind, tc.value)
			// Include limits smaller than a summary, a quoted rune or the marker.
			for _, limit := range []int{0, 1, 11, 12, 64, 127, 128, 129, 256, 1024, 16384, 32768} {
				got := renderLogPlaceholder(t, data, limit)
				assert.LessOrEqual(t, len(got), limit)
				assert.True(t, utf8.ValidString(got))
				if limit >= len(placeholderLogTruncated) {
					assert.Contains(t, got, "<truncated>")
				}
			}
		})
	}

	// A hybrid request shares the budget across all sub-requests, rather than
	// allowing each sub-request to grow independently to the configured limit.
	request := &milvuspb.SearchRequest{}
	for i := 0; i < 3; i++ {
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{
			Nq: 1, PlaceholderGroup: marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar,
				[]byte(strings.Repeat("x", 1000)+fmt.Sprintf("QUERY_%d_END", i))),
		})
	}
	got := renderLogRequest(t, request, 2048)
	assert.LessOrEqual(t, len(got), 2048)
	assert.Contains(t, got, "QUERY_0_END")
	assert.NotContains(t, got, "QUERY_1_END")
	assert.NotContains(t, got, "QUERY_2_END")
	assert.Contains(t, got, "<truncated>")
	got = renderLogRequest(t, request, 4096)
	assert.LessOrEqual(t, len(got), 4096)
	assert.Contains(t, got, "QUERY_2_END")
	assert.NotContains(t, got, "<truncated>")

	largeText := marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar,
		[]byte(strings.Repeat("x", 20*1024)+"END_OF_FIRST_QUERY"))
	assert.Contains(t, renderLogPlaceholder(t, largeText, 16384), "<truncated>")
	got = renderLogPlaceholder(t, largeText, 32768)
	assert.Greater(t, len(got), 16384)
	assert.LessOrEqual(t, len(got), 32768)
	assert.Contains(t, got, "END_OF_FIRST_QUERY")
	assert.NotContains(t, got, "<truncated>")
}

type placeholderLogBuffer struct{ bytes.Buffer }

func (*placeholderLogBuffer) Sync() error { return nil }

func TestLogSearchPlaceholders(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.ProxyCfg.SearchPlaceholderLogRateLimit.Key
	oldRate := params.ProxyCfg.SearchPlaceholderLogRateLimit.GetValue()
	sizeKey := params.ProxyCfg.SearchPlaceholderLogMaxBytes.Key
	oldSize := params.ProxyCfg.SearchPlaceholderLogMaxBytes.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(sizeKey, oldSize)) })
	require.NoError(t, params.Save(sizeKey, "16384"))
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
	emit := func(request *milvuspb.SearchRequest, infos []*planpb.QueryInfo) {
		preview := newSearchPlaceholderLog()
		captureLogRequest(t, preview, request)
		logSearchPlaceholders(ctx, log, request, infos, preview)
	}
	// Even a fresh call site must not spend the initial burst token while disabled.
	for _, limit := range []string{"0", "-1", "NaN"} {
		require.NoError(t, params.Save(key, limit))
		assert.Nil(t, newSearchPlaceholderLog())
		emit(nil, nil)
		assert.Empty(t, buf.String())
	}

	request := &milvuspb.SearchRequest{}
	infos := []*planpb.QueryInfo{}
	for i := 0; i < 3; i++ {
		request.SubReqs = append(request.SubReqs, &milvuspb.SubSearchRequest{
			Nq:               1,
			PlaceholderGroup: marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar, []byte(fmt.Sprintf("query-%d", i))),
		})
		infos = append(infos, &planpb.QueryInfo{QueryFieldId: int64(101 + i)})
	}
	// Prime the real limiter so this test also works with -count > 1.
	require.NoError(t, params.Save(key, "+Inf"))
	emit(request, infos)
	buf.Reset()
	require.NoError(t, params.Save(key, "0.000000001"))
	for i := 0; i < 100; i++ {
		emit(request, infos)
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
	emit(ordinary, infos[:1])
	assert.Empty(t, buf.String())
	preview := newSearchPlaceholderLog()
	require.NotNil(t, preview)
	captureLogRequest(t, preview, ordinary)
	require.NoError(t, params.Save(key, "0"))
	logSearchPlaceholders(ctx, log, ordinary, infos[:1], preview)
	emit(ordinary, infos[:1])
	assert.Empty(t, buf.String())
	require.NoError(t, params.Save(key, "+Inf"))
	emit(ordinary, infos[:1])
	assert.Contains(t, buf.String(), "ordinary")
	buf.Reset()
	mlog.SetLevel(mlog.WarnLevel)
	assert.Nil(t, newSearchPlaceholderLog())
	emit(ordinary, infos[:1])
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
	emit(ordinary, infos[:1])
	assert.Contains(t, buf.String(), "nq=1000")
	assert.Contains(t, buf.String(), "dim=1536")
	assert.Contains(t, buf.String(), traceID.String())
	assert.Less(t, buf.Len(), 4096)

	// Changing the size configuration changes actual emitted logs without a restart.
	ordinary.Nq = 1
	ordinary.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{
		PlaceholderGroup: marshalLogPlaceholder(t, commonpb.PlaceholderType_VarChar,
			[]byte(strings.Repeat("x", 1024)+"END_OF_FIRST_QUERY")),
	}
	require.NoError(t, params.Save(sizeKey, "256"))
	buf.Reset()
	emit(ordinary, infos[:1])
	assert.Contains(t, buf.String(), "<truncated>")
	assert.NotContains(t, buf.String(), "END_OF_FIRST_QUERY")
	assert.Contains(t, buf.String(), traceID.String())
	require.NoError(t, params.Save(sizeKey, "16384"))
	buf.Reset()
	emit(ordinary, infos[:1])
	assert.NotContains(t, buf.String(), "<truncated>")
	assert.Contains(t, buf.String(), "END_OF_FIRST_QUERY")
}

func BenchmarkPlaceholderLogLargeInput(b *testing.B) {
	values := make([][]byte, 1000)
	for i := range values {
		values[i] = make([]byte, 1536*4)
	}
	group := decodeLogPlaceholder(b, marshalLogPlaceholder(b, commonpb.PlaceholderType_FloatVector, values...))
	// Search already decoded the group. Measure only the extra capture/render work.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		preview := &searchPlaceholderStringer{}
		preview.capture(group)
		w := newPlaceholderLogWriter(testPlaceholderLogMaxBytes)
		writePlaceholderGroup(w, preview.groups[0])
		_ = w.finish()
	}
}

func FuzzPlaceholderLogBudget(f *testing.F) {
	f.Add([]byte("hello"), int32(commonpb.PlaceholderType_VarChar), uint16(256))
	f.Add([]byte{0xff}, int32(commonpb.PlaceholderType_FloatVector), uint16(1))
	f.Fuzz(func(t *testing.T, value []byte, kind int32, limit uint16) {
		preview := &searchPlaceholderStringer{}
		preview.capture(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType(kind), Values: [][]byte{value}}}})
		w := newPlaceholderLogWriter(int(limit))
		writePlaceholderGroup(w, preview.groups[0])
		got := w.finish()
		require.LessOrEqual(t, len(got), int(limit))
		require.True(t, utf8.ValidString(got))
	})
}
