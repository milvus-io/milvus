/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package function

import (
	"encoding/json"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const multiAnalyzerParams = "multi_analyzer_params"

// BM25 Runner with Multi Analyzer
// Input: string string // text, analyzer name
// Output: map[uint32]float32
type MultiAnalyzerBM25FunctionRunner struct {
	mu     sync.RWMutex
	closed bool

	analyzers   map[string]analyzer.Analyzer
	alias       map[string]string // alias -> analyzer name
	schema      *schemapb.FunctionSchema
	outputField *schemapb.FieldSchema
	inputFields []*schemapb.FieldSchema
}

func getMultiAnalyzerParams(field *schemapb.FieldSchema) (string, bool) {
	for _, param := range field.GetTypeParams() {
		if param.Key == multiAnalyzerParams {
			return param.Value, true
		}
	}
	return "", false
}

func NewMultiAnalyzerBM25FunctionRunner(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema, inputField, outputField *schemapb.FieldSchema, params string) (*MultiAnalyzerBM25FunctionRunner, error) {
	runner := &MultiAnalyzerBM25FunctionRunner{
		schema:      schema,
		inputFields: []*schemapb.FieldSchema{inputField},
		outputField: outputField,
		analyzers:   make(map[string]analyzer.Analyzer),
	}
	var m map[string]json.RawMessage
	var mFileName string

	err := json.Unmarshal([]byte(params), &m)
	if err != nil {
		return nil, err
	}

	mfield, ok := m["by_field"]
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function with multi analyzer must have by_field param in multi_analyzer_params")
	}

	err = json.Unmarshal(mfield, &mFileName)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function with multi analyzer by_field must be string but now: %s", mfield)
	}

	for _, field := range coll.GetFields() {
		if field.GetName() == mFileName {
			runner.inputFields = append(runner.inputFields, field)
		}
	}

	if len(runner.inputFields) != 2 {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function with multi analyzer must have two input fields")
	}

	if value, ok := m["alias"]; ok {
		mapping := map[string]string{}
		err = json.Unmarshal(value, &mapping)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("bm25 function with multi analyzer mapping must be string map but now: %s", value)
		}
		runner.alias = mapping
	}

	analyzers, ok := m["analyzers"]
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function with multi analyzer must have analyzers param in multi_analyzer_params")
	}

	var analyzersParam map[string]json.RawMessage
	err = json.Unmarshal(analyzers, &analyzersParam)
	if err != nil {
		return nil, merr.Wrap(err, "bm25 function unmarshal multi_analyzer_params analyzers failed")
	}

	for name, param := range analyzersParam {
		analyzer, err := analyzer.NewAnalyzer(string(param), "")
		if err != nil {
			return nil, merr.Wrapf(err, "bm25 function create analyzer %s failed", name)
		}
		runner.analyzers[name] = analyzer
	}

	return runner, nil
}

func (v *MultiAnalyzerBM25FunctionRunner) resolveAnalyzerName(name string) string {
	if alias, ok := v.alias[name]; ok {
		name = alias
	}
	if _, ok := v.analyzers[name]; ok {
		return name
	}
	return "default"
}

func (v *MultiAnalyzerBM25FunctionRunner) getAnalyzer(name string, analyzers map[string]analyzer.Analyzer) (analyzer.Analyzer, error) {
	name = v.resolveAnalyzerName(name)

	if analyzer, ok := analyzers[name]; ok {
		return analyzer, nil
	}

	var err error
	if analyzer, ok := v.analyzers[name]; ok {
		analyzers[name], err = analyzer.Clone()
		if err != nil {
			return nil, err
		}
		return analyzers[name], nil
	}

	return nil, merr.WrapErrServiceInternalMsg("resolved analyzer %s is not configured", name)
}

func (v *MultiAnalyzerBM25FunctionRunner) run(text []string, analyzerName []string, dst []map[uint32]float32, terms map[string]struct{}) error {
	cloneAnalyzers := map[string]analyzer.Analyzer{}
	defer func() {
		for _, analyzer := range cloneAnalyzers {
			analyzer.Destroy()
		}
	}()

	for i := 0; i < len(text); i++ {
		if len(text[i]) == 0 {
			if dst != nil {
				dst[i] = map[uint32]float32{}
			}
			continue
		}

		if !typeutil.IsUTF8(text[i]) {
			return merr.WrapErrParameterInvalidMsg("string data must be utf8 format: %v", text[i])
		}
		var embeddingMap map[uint32]float32
		if dst != nil {
			embeddingMap = map[uint32]float32{}
		}

		analyzer, err := v.getAnalyzer(analyzerName[i], cloneAnalyzers)
		if err != nil {
			return err
		}

		tokenStream := analyzer.NewTokenStream(text[i])

		for tokenStream.Advance() {
			token := tokenStream.Token()
			// TODO More Hash Option
			if dst != nil {
				hash := typeutil.HashString2LessUint32(token)
				embeddingMap[hash] += 1
			}
			if terms != nil {
				terms[token] = struct{}{}
			}
		}
		tokenStream.Destroy()
		if dst != nil {
			dst[i] = embeddingMap
		}
	}
	return nil
}

func (v *MultiAnalyzerBM25FunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	output, _, err := v.batchRun(true, false, inputs...)
	return output, err
}

func (v *MultiAnalyzerBM25FunctionRunner) BatchRunWithTextTerms(inputs ...any) ([]any, []AnalyzedTextTermBatch, error) {
	return v.batchRun(true, true, inputs...)
}

func (v *MultiAnalyzerBM25FunctionRunner) BatchTextTerms(inputs ...any) ([]AnalyzedTextTermBatch, error) {
	_, terms, err := v.batchRun(false, true, inputs...)
	return terms, err
}

func (v *MultiAnalyzerBM25FunctionRunner) batchRun(buildEmbedding, collectTerms bool, inputs ...any) ([]any, []AnalyzedTextTermBatch, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, nil, merr.WrapErrServiceInternalMsg("analyzer receview request after function closed")
	}

	if len(inputs) != 2 {
		return nil, nil, merr.WrapErrParameterInvalidMsg("BM25 function with multi analyzer must received two input column")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, nil, merr.WrapErrParameterInvalidMsg("BM25 function with multi analyzer text input must be string list")
	}

	analyzer, ok := inputs[1].([]string)
	if !ok {
		return nil, nil, merr.WrapErrParameterInvalidMsg("BM25 function with multi analyzer input analyzer name must be string list")
	}

	if len(text) != len(analyzer) {
		return nil, nil, merr.WrapErrParameterInvalidMsg("BM25 function with multi analyzer input text and analyzer name must have same length")
	}

	rowNum := len(text)
	var embedData []map[uint32]float32
	if buildEmbedding {
		embedData = make([]map[uint32]float32, rowNum)
	}
	wg := sync.WaitGroup{}
	concurrency := getAnalyzerRunnerConcurrency()
	termSets := make([]map[string]struct{}, concurrency)

	errCh := make(chan error, concurrency)
	for i, j := 0, 0; i < concurrency && j < rowNum; i++ {
		chunk := i
		start := j
		end := start + rowNum/concurrency
		if i < rowNum%concurrency {
			end += 1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var terms map[string]struct{}
			if collectTerms {
				terms = make(map[string]struct{})
				termSets[chunk] = terms
			}
			var output []map[uint32]float32
			if buildEmbedding {
				output = embedData[start:end]
			}
			err := v.run(text[start:end], analyzer[start:end], output, terms)
			if err != nil {
				errCh <- err
				return
			}
		}()
		j = end
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, nil, err
		}
	}

	var output []any
	if buildEmbedding {
		output = []any{buildSparseFloatArray(embedData)}
	}
	if !collectTerms {
		return output, nil, nil
	}

	merged := make(map[string]struct{})
	for _, termSet := range termSets {
		for term := range termSet {
			merged[term] = struct{}{}
		}
	}
	return output, []AnalyzedTextTermBatch{{
		InputFieldID: v.inputFields[0].GetFieldID(),
		Terms:        termBytes(merged),
	}}, nil
}

func (v *MultiAnalyzerBM25FunctionRunner) analyze(data []string, analyzerName []string, dst [][]*milvuspb.AnalyzerToken, withDetail bool, withHash bool) error {
	cloneAnalyzers := map[string]analyzer.Analyzer{}
	defer func() {
		for _, analyzer := range cloneAnalyzers {
			analyzer.Destroy()
		}
	}()

	for i := 0; i < len(data); i++ {
		result := []*milvuspb.AnalyzerToken{}
		analyzer, err := v.getAnalyzer(analyzerName[i], cloneAnalyzers)
		if err != nil {
			return err
		}

		tokenStream := analyzer.NewTokenStream(data[i])
		for tokenStream.Advance() {
			var token *milvuspb.AnalyzerToken
			if withDetail {
				token = tokenStream.DetailedToken()
			} else {
				token = &milvuspb.AnalyzerToken{
					Token: tokenStream.Token(),
				}
			}

			if withHash {
				token.Hash = typeutil.HashString2LessUint32(token.GetToken())
			}
			result = append(result, token)
		}
		tokenStream.Destroy()
		dst[i] = result
	}
	return nil
}

func (v *MultiAnalyzerBM25FunctionRunner) BatchAnalyze(withDetail bool, withHash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, merr.WrapErrServiceInternalMsg("analyzer receview request after function closed")
	}

	if len(inputs) != 2 {
		return nil, merr.WrapErrParameterInvalidMsg("multi analyzer must received two input column(text, analyzer_name)")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("multi analyzer text input must be string list")
	}

	analyzer, ok := inputs[1].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("multi analyzer input analyzer name must be string list")
	}

	if len(text) != len(analyzer) {
		return nil, merr.WrapErrParameterInvalidMsg("multi analyzer input text and analyzer name must have same length")
	}

	rowNum := len(text)
	result := make([][]*milvuspb.AnalyzerToken, rowNum)
	pool := getOrCreateAnalyzerPool()
	futures := make([]*conc.Future[struct{}], 0)
	concurrency := getAnalyzerRunnerConcurrency()

	for i, j := 0, 0; i < concurrency && j < rowNum; i++ {
		start := j
		end := start + rowNum/concurrency
		if i < rowNum%concurrency {
			end += 1
		}
		future := pool.Submit(func() (struct{}, error) {
			return struct{}{}, v.analyze(text[start:end], analyzer[start:end], result[start:end], withDetail, withHash)
		})
		futures = append(futures, future)
		j = end
	}

	err := conc.AwaitAll(futures...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (v *MultiAnalyzerBM25FunctionRunner) GetSchema() *schemapb.FunctionSchema {
	return v.schema
}

func (v *MultiAnalyzerBM25FunctionRunner) GetOutputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{v.outputField}
}

func (v *MultiAnalyzerBM25FunctionRunner) GetInputFields() []*schemapb.FieldSchema {
	return v.inputFields
}

func (v *MultiAnalyzerBM25FunctionRunner) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()

	for _, analyzer := range v.analyzers {
		analyzer.Destroy()
	}
	v.closed = true
}
