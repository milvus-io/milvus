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

package function

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Hide the optional batch capability to exercise the original token API.
type tokenOnlyAnalyzer struct{ analyzer.Analyzer }

type stubBM25BatchTokenizer struct {
	analyzer.Analyzer
	rows [][]byte
	err  error
}

func (s stubBM25BatchTokenizer) BatchTokenizeBM25([]string) ([][]byte, error) {
	return s.rows, s.err
}

func TestRunBM25BatchErrors(t *testing.T) {
	dst := make([][]byte, 1)
	// A failed native call must retain its error, never fall back to token
	// iteration (the embedded Analyzer is deliberately nil).
	err := runBM25(stubBM25BatchTokenizer{err: merr.ErrServiceUnavailable}, []string{"query"}, dst)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, dst[0])
	err = runBM25(stubBM25BatchTokenizer{}, []string{"query"}, dst)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
}

func (a tokenOnlyAnalyzer) Clone() (interfaces.Analyzer, error) {
	clone, err := a.Analyzer.Clone()
	if err != nil {
		return nil, err
	}
	return tokenOnlyAnalyzer{clone}, nil
}

func TestBM25BatchRunEquivalence(t *testing.T) {
	for _, params := range []string{`{}`, `{"tokenizer":"jieba"}`, `{"type":"english"}`} {
		t.Run(params, func(t *testing.T) {
			a, err := analyzer.NewAnalyzer(params, "")
			require.NoError(t, err)
			defer a.Destroy()
			native := &BM25FunctionRunner{tokenizer: a}
			legacy := &BM25FunctionRunner{tokenizer: tokenOnlyAnalyzer{a}}
			texts := []string{"", "test test runs running", "北京大学北京大学", "the and", "café", "x\x00y", "a b a", "!!!", "tail"}
			want, err := legacy.BatchRun(texts)
			require.NoError(t, err)
			got, err := native.BatchRun(texts)
			require.NoError(t, err)
			require.Equal(t, want, got)
			// Exercise concurrent cloning, batch ownership, and multiple worker chunks.
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 10; j++ {
						result, err := native.BatchRun(texts)
						if err != nil {
							t.Error(err)
							return
						}
						if !reflect.DeepEqual(want, result) {
							t.Error("concurrent BM25 result changed")
							return
						}
					}
				}()
			}
			wg.Wait()
			_, err = native.BatchRun([]string{"ok", "\xff"})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func TestMultiAnalyzerBM25BatchRunEquivalence(t *testing.T) {
	base, err := analyzer.NewAnalyzer(`{}`, "")
	require.NoError(t, err)
	english, err := analyzer.NewAnalyzer(`{"type":"english"}`, "")
	require.NoError(t, err)
	defer base.Destroy()
	defer english.Destroy()
	aliases := map[string]string{"en": "english"}
	native := &MultiAnalyzerBM25FunctionRunner{analyzers: map[string]analyzer.Analyzer{"default": base, "english": english}, alias: aliases}
	legacy := &MultiAnalyzerBM25FunctionRunner{analyzers: map[string]analyzer.Analyzer{"default": tokenOnlyAnalyzer{base}, "english": tokenOnlyAnalyzer{english}}, alias: aliases}
	texts := []string{"running runs the", "running runs the", "", "the and", "the and", "running running", "tail", "tail", "tail"}
	names := []string{"default", "en", "missing", "english", "default", "english", "missing", "missing", "default"}
	want, err := legacy.BatchRun(texts, names)
	require.NoError(t, err)
	got, err := native.BatchRun(texts, names)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBM25BatchRunNativeDispatch(t *testing.T) {
	a, err := analyzer.NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(t, err)
	defer a.Destroy()
	single := &BM25FunctionRunner{tokenizer: a}
	multi := &MultiAnalyzerBM25FunctionRunner{analyzers: map[string]analyzer.Analyzer{"default": a}}
	for _, tc := range []struct {
		name string
		run  func(string) ([]any, error)
	}{
		{"single", func(text string) ([]any, error) { return single.BatchRun([]string{text}) }},
		{"multi", func(text string) ([]any, error) {
			return multi.BatchRun([]string{text}, []string{"default"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, count := range []int{1, 1000} {
				text := strings.Repeat("word ", count)
				before := runtime.NumCgoCall()
				result, err := tc.run(text)
				calls := runtime.NumCgoCall() - before
				require.NoError(t, err)
				require.Equal(t, [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{
					typeutil.HashString2LessUint32("word"): float32(count),
				})}, result[0].(*schemapb.SparseFloatArray).Contents)
				// One row means one worker: clone + batch + release + destroy.
				require.EqualValues(t, 4, calls, "runner must dispatch to the native batch capability")
			}
		})
	}
}

// Preserve the pre-optimization scheduling/map/sort placement as the benchmark
// baseline, rather than comparing against a tokenizer-only microbenchmark.
func legacyBM25BatchRun(v *BM25FunctionRunner, inputs ...any) ([]any, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, merr.WrapErrServiceInternalMsg("analyzer receview request after function closed")
	}
	if len(inputs) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("BM25 function received more than one input column")
	}
	texts, ok := inputs[0].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("BM25 function batch input not string list")
	}
	data := make([]map[uint32]float32, len(texts))
	concurrency := getAnalyzerRunnerConcurrency()
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	for i, j := 0, 0; i < concurrency && j < len(texts); i++ {
		start := j
		end := start + len(texts)/concurrency
		if i < len(texts)%concurrency {
			end++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := legacyBM25Run(v, texts[start:end], data[start:end]); err != nil {
				errCh <- err
			}
		}()
		j = end
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}
	dim := int64(0)
	rows := lo.Map(data, func(tf map[uint32]float32, _ int) []byte {
		row := typeutil.CreateAndSortSparseFloatRow(tf)
		if rowDim := typeutil.SparseFloatRowDim(row); rowDim > dim {
			dim = rowDim
		}
		return row
	})
	return []any{&schemapb.SparseFloatArray{Contents: rows, Dim: dim}}, nil
}

func legacyBM25Run(v *BM25FunctionRunner, texts []string, dst []map[uint32]float32) error {
	a, err := v.tokenizer.Clone()
	if err != nil {
		return err
	}
	defer a.Destroy()
	for i, text := range texts {
		if len(text) == 0 {
			dst[i] = map[uint32]float32{}
			continue
		}
		if !typeutil.IsUTF8(text) {
			return merr.WrapErrParameterInvalidMsg("string data must be utf8 format: %v", text)
		}
		tf := map[uint32]float32{}
		stream, err := a.NewTokenStream(text)
		if err != nil {
			return err
		}
		for stream.Advance() {
			tf[typeutil.HashString2LessUint32(stream.Token())]++
		}
		stream.Destroy()
		dst[i] = tf
	}
	return nil
}

func BenchmarkBM25BatchRun(b *testing.B) {
	b.Logf("GOMAXPROCS=%d analyzer_runner_concurrency=%d", runtime.GOMAXPROCS(0), getAnalyzerRunnerConcurrency())
	uniqueWords := make([]string, 256)
	for i := range uniqueWords {
		uniqueWords[i] = fmt.Sprintf("word%d", i)
	}
	for _, tc := range []struct{ name, params, text string }{
		{"standard_empty", `{}`, ""},
		{"standard_single", `{}`, "database"},
		{"standard_short", `{}`, "vector database full text search"},
		{"standard_long", `{}`, strings.Repeat("vector database full text search with efficient tokenization and ranking ", 32)},
		{"standard_unique", `{}`, strings.Join(uniqueWords, " ")},
		{"jieba_short", `{"tokenizer":"jieba"}`, "北京大学全文检索优化"},
		{"jieba_long", `{"tokenizer":"jieba"}`, strings.Repeat("北京大学全文检索优化，向量数据库支持中文搜索。", 32)},
	} {
		a, err := analyzer.NewAnalyzer(tc.params, "")
		require.NoError(b, err)
		runner := &BM25FunctionRunner{tokenizer: a}
		for _, nq := range []int{1, 8, 32} {
			texts := make([]string, nq)
			for i := range texts {
				texts[i] = tc.text
			}
			want, err := legacyBM25BatchRun(runner, texts)
			require.NoError(b, err)
			got, err := runner.BatchRun(texts)
			require.NoError(b, err)
			require.Equal(b, want, got)
			for _, native := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/nq%d/native=%t", tc.name, nq, native), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if native {
							_, err = runner.BatchRun(texts)
						} else {
							_, err = legacyBM25BatchRun(runner, texts)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
		a.Destroy()
	}
}

// A high-cardinality row must not make subsequent one-token rows repeatedly
// scan its retained native TF-map capacity. Both row orders have identical work.
func BenchmarkBM25BatchRunMixedSizes(b *testing.B) {
	b.Logf("GOMAXPROCS=%d analyzer_runner_concurrency=%d", runtime.GOMAXPROCS(0), getAnalyzerRunnerConcurrency())
	a, err := analyzer.NewAnalyzer(`{"tokenizer":"whitespace"}`, "")
	require.NoError(b, err)
	defer a.Destroy()
	runner := &BM25FunctionRunner{tokenizer: a}
	words := make([]string, 200000)
	for i := range words {
		words[i] = fmt.Sprintf("w%d", i)
	}
	long := strings.Join(words, " ")
	for _, nq := range []int{8192, 65536} {
		for _, position := range []string{"first", "last"} {
			texts := make([]string, nq)
			for i := range texts {
				texts[i] = fmt.Sprintf("short%d", i%32)
			}
			if position == "first" {
				texts[0] = long
			} else {
				texts[len(texts)-1] = long
			}
			want, err := legacyBM25BatchRun(runner, texts)
			require.NoError(b, err)
			got, err := runner.BatchRun(texts)
			require.NoError(b, err)
			require.Equal(b, want, got)
			for _, native := range []bool{false, true} {
				b.Run(fmt.Sprintf("nq%d/long_%s/native=%t", nq, position, native), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if native {
							_, err = runner.BatchRun(texts)
						} else {
							_, err = legacyBM25BatchRun(runner, texts)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
