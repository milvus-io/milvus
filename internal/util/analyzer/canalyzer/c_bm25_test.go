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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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

func TestBatchTokenizeBM25NativeFailure(t *testing.T) {
	// An invalid internal handle must propagate a system error through C++/Go
	// status handling, never produce an empty success result or blame the request.
	rows, err := NewCAnalyzer(nil).BatchTokenizeBM25([]string{"hello"})
	require.Nil(t, rows)
	require.ErrorIs(t, err, merr.ErrSegcore)
	require.NotErrorIs(t, err, merr.ErrParameterInvalid)
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
