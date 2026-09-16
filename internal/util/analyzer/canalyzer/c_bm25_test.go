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

package canalyzer

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Independent reference using the original C-string token API and Go hash.
func legacyBM25Rows(t testing.TB, a interfaces.Analyzer, texts []string) [][]byte {
	t.Helper()
	rows := make([][]byte, len(texts))
	for i, text := range texts {
		tf := make(map[uint32]float32)
		if text != "" {
			stream, err := a.NewTokenStream(text)
			require.NoError(t, err)
			for stream.Advance() {
				tf[typeutil.HashString2LessUint32(stream.Token())]++
			}
			stream.Destroy()
		}
		rows[i] = typeutil.CreateAndSortSparseFloatRow(tf)
	}
	return rows
}

func TestBatchTokenizeBM25MatchesLegacy(t *testing.T) {
	texts := []string{
		"", " ", "!!!", "HELLO hello world hello", "the and of running runs",
		"北京大学的学生在北京学习，全文检索测试", "café CAFÉ naïve Straße 👋🏽",
		"한국어 검색 테스트", "日本語の全文検索", "مرحبا بالعالم", "สวัสดีชาวโลก",
		"hello\x00world", "\x00hello",
		"bm25-crc-1295crV", // CRC32 IEEE == UINT32_MAX, which must map to dimension 0.
		strings.Repeat("a", 99), strings.Repeat("b", 100), strings.Repeat("c", 101),
		strings.Repeat("界", 33) + "éx " + strings.Repeat("界", 33) + "éy",
		strings.Repeat("x", 100) + "a " + strings.Repeat("x", 100) + "b",
		strings.Repeat("one two three one ", 1024),
	}
	for _, params := range []string{
		`{}`, `{"tokenizer":"standard"}`, `{"tokenizer":"whitespace"}`,
		`{"tokenizer":"jieba"}`, `{"tokenizer":"icu"}`, `{"type":"english"}`,
		`{"tokenizer":"standard","filter":["lowercase",{"type":"stop","stop_words":["the","and","of"]}]}`,
		`{"tokenizer":"standard","filter":["lowercase",{"type":"synonym","synonyms":["hello, world","running, runs"]}]}`,
	} {
		t.Run(params, func(t *testing.T) {
			a, err := NewAnalyzer(params, "")
			require.NoError(t, err)
			defer a.Destroy()
			native := a.(*CAnalyzer)
			want := legacyBM25Rows(t, a, texts)
			got, err := native.BatchTokenizeBM25(texts)
			require.NoError(t, err)
			require.Equal(t, want, got)
			// Repeated calls must reset TF state and leave previous Go-owned rows valid.
			again, err := native.BatchTokenizeBM25([]string{"different", "", "different"})
			require.NoError(t, err)
			require.Equal(t, legacyBM25Rows(t, a, []string{"different", "", "different"}), again)
			require.Equal(t, want, got)
			empty, err := native.BatchTokenizeBM25(nil)
			require.NoError(t, err)
			require.Empty(t, empty)
			_, err = native.BatchTokenizeBM25([]string{"valid", "\xff", "valid"})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func TestBatchTokenizeBM25MixedCardinalities(t *testing.T) {
	const distinctTokens = 8192
	tokens := make([]string, distinctTokens)
	for i := range tokens {
		tokens[i] = "word" + strconv.Itoa(i)
	}
	long := strings.Join(tokens, " ")
	// Exercise both retaining a large table and releasing it after cardinality
	// drops, then growing it again. The Rust test asserts the capacity policy;
	// this test checks the sparse rows across the C++/Go boundary.
	texts := []string{long, long}
	for i := 0; i < 64; i++ {
		texts = append(texts, "short short tail"+strconv.Itoa(i))
	}
	texts = append(texts, long, "last last")

	a, err := NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(t, err)
	defer a.Destroy()
	want := legacyBM25Rows(t, a, texts)
	// Verify the input really produces a high-cardinality TF map, rather than
	// a long document made up of repeated tokens or collapsed hash dimensions.
	require.Equal(t, distinctTokens, typeutil.SparseFloatRowElementCount(want[0]))
	got, err := a.(*CAnalyzer).BatchTokenizeBM25(texts)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBatchTokenizeBM25NativeFailure(t *testing.T) {
	// An invalid internal handle must propagate a system error through C++/Go
	// status handling, never produce an empty success result or blame the request.
	rows, err := NewCAnalyzer(nil).BatchTokenizeBM25([]string{"hello"})
	require.Nil(t, rows)
	require.ErrorIs(t, err, merr.ErrSegcore)
	require.NotErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestBatchTokenizeBM25UTF8Validation(t *testing.T) {
	a, err := NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(t, err)
	defer a.Destroy()
	var validate func(string) bool
	checks := 0
	patch := mockey.Mock(typeutil.IsUTF8).To(func(text string) bool {
		checks++
		return validate(text)
	}).Origin(&validate).Build()
	defer patch.UnPatch()

	_, err = a.(*CAnalyzer).BatchTokenizeBM25([]string{"hello", "北京 café", "x\x00y"})
	require.NoError(t, err)
	require.Zero(t, checks, "valid native batches must not scan UTF-8 in Go")

	for _, invalid := range []string{"\xff", "x\x00\xff", "\xe4\xb8"} {
		checks = 0
		rows, err := a.(*CAnalyzer).BatchTokenizeBM25([]string{"ok", invalid, "tail"})
		require.Nil(t, rows)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "string data must be utf8 format: "+invalid)
		require.EqualValues(t, 1100, merr.Status(err).GetCode())
		require.Equal(t, merr.InputError, merr.GetErrorType(err))
		require.False(t, merr.Status(err).GetRetriable())
		require.Equal(t, 2, checks, "only failed batches rescan to recover the legacy error")
	}
}

func TestBatchTokenizeBM25RowsReleaseIndependently(t *testing.T) {
	a, err := NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(t, err)
	defer a.Destroy()
	released := make(chan struct{})
	// Return only the repacked small row so the source rows leave the stack.
	retained := func() []byte {
		words := make([]string, 8192)
		for i := range words {
			words[i] = "word" + strconv.Itoa(i)
		}
		rows, err := a.(*CAnalyzer).BatchTokenizeBM25([]string{strings.Join(words, " "), "small"})
		require.NoError(t, err)
		require.Greater(t, len(rows[0]), 32768) // Avoid the runtime's tiny-object batching.
		runtime.AddCleanup(&rows[0][0], func(done chan struct{}) { close(done) }, released)
		src := []*schemapb.FieldData{{
			Type: schemapb.DataType_SparseFloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: rows}},
			}},
		}}
		dst := make([]*schemapb.FieldData, 1)
		typeutil.AppendFieldData(dst, src, 1)
		return dst[0].GetVectors().GetSparseFloatVector().GetContents()[0]
	}()
	require.Eventually(t, func() bool {
		runtime.GC()
		select {
		case <-released:
			return true
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond, "keeping a small row must not keep the large row's allocation alive")
	require.Equal(t, typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{typeutil.HashString2LessUint32("small"): 1}), retained)
	runtime.KeepAlive(retained)
}

func TestBatchTokenizeBM25CgoCalls(t *testing.T) {
	a, err := NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(t, err)
	defer a.Destroy()
	for _, count := range []int{1, 1000} {
		before := runtime.NumCgoCall()
		rows, err := a.(*CAnalyzer).BatchTokenizeBM25([]string{strings.Repeat("word ", count)})
		calls := runtime.NumCgoCall() - before
		require.NoError(t, err)
		require.Equal(t, typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{
			typeutil.HashString2LessUint32("word"): float32(count),
		}), rows[0])
		require.EqualValues(t, 2, calls, "one batch call and one release, independent of token count")
	}
}

func TestBatchTokenizeBM25ReleasesHandleOnError(t *testing.T) {
	a, err := NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(t, err)
	defer a.Destroy()

	// Produce a real native batch, then inject an error at status translation
	// to exercise the failed-status/nonnull-handle combination independently of
	// the native implementation's current no-partial-output guarantee.
	statusMock := mockey.Mock(HandleCStatus).Return(merr.ErrServiceUnavailable).Build()
	defer statusMock.UnPatch()
	var release func(unsafe.Pointer)
	var released []unsafe.Pointer
	releaseMock := mockey.Mock(freeBM25Batch).To(func(handle unsafe.Pointer) {
		released = append(released, handle)
		release(handle)
	}).Origin(&release).Build()
	defer releaseMock.UnPatch()

	rows, err := a.(*CAnalyzer).BatchTokenizeBM25([]string{"word word tail"})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, rows)
	require.Len(t, released, 1)
	require.NotNil(t, released[0])
}
