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
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	concurrency int
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
		concurrency: 8,
	}

	var m map[string]json.RawMessage
	var mFileName string

	err := json.Unmarshal([]byte(params), &m)
	if err != nil {
		return nil, err
	}

	mfield, ok := m["by_field"]
	if !ok {
		return nil, fmt.Errorf("bm25 function with multi analyzer must have by_field param in multi_analyzer_params")
	}

	err = json.Unmarshal(mfield, &mFileName)
	if err != nil {
		return nil, fmt.Errorf("bm25 function with multi analyzer by_field must be string but now: %s", mfield)
	}

	for _, field := range coll.GetFields() {
		if field.GetName() == mFileName {
			runner.inputFields = append(runner.inputFields, field)
		}
	}

	if len(runner.inputFields) != 2 {
		return nil, fmt.Errorf("bm25 function with multi analyzer must have two input fields")
	}

	if value, ok := m["alias"]; ok {
		mapping := map[string]string{}
		err = json.Unmarshal(value, &mapping)
		if err != nil {
			return nil, fmt.Errorf("bm25 function with multi analyzer mapping must be string map but now: %s", value)
		}
		runner.alias = mapping
	}

	analyzers, ok := m["analyzers"]
	if !ok {
		return nil, fmt.Errorf("bm25 function with multi analyzer must have analyzers param in multi_analyzer_params")
	}

	var analyzersParam map[string]json.RawMessage
	err = json.Unmarshal(analyzers, &analyzersParam)
	if err != nil {
		return nil, fmt.Errorf("bm25 function unmarshal multi_analyzer_params analyzers failed : %s", err.Error())
	}

	for name, param := range analyzersParam {
		analyzer, err := analyzer.NewAnalyzer(string(param))
		if err != nil {
			return nil, fmt.Errorf("bm25 function create analyzer %s failed with error: %s", name, err.Error())
		}
		runner.analyzers[name] = analyzer
	}

	return runner, nil
}

func (v *MultiAnalyzerBM25FunctionRunner) getAnalyzer(name string, analyzers map[string]analyzer.Analyzer) (analyzer.Analyzer, error) {
	if alias, ok := v.alias[name]; ok {
		name = alias
	}

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

	return v.getAnalyzer("default", analyzers)
}

func (v *MultiAnalyzerBM25FunctionRunner) run(text []string, analyzerName []string, dst []map[uint32]float32) error {
	cloneAnalyzers := map[string]analyzer.Analyzer{}
	defer func() {
		for _, analyzer := range cloneAnalyzers {
			analyzer.Destroy()
		}
	}()

	for i := 0; i < len(text); i++ {
		if len(text[i]) == 0 {
			dst[i] = map[uint32]float32{}
			continue
		}

		if !typeutil.IsUTF8(text[i]) {
			return merr.WrapErrParameterInvalidMsg("string data must be utf8 format: %v", text[i])
		}
		embeddingMap := map[uint32]float32{}

		analyzer, err := v.getAnalyzer(analyzerName[i], cloneAnalyzers)
		if err != nil {
			return err
		}

		tokenStream := analyzer.NewTokenStream(text[i])
		defer tokenStream.Destroy()

		for tokenStream.Advance() {
			token := tokenStream.Token()
			// TODO More Hash Option
			hash := typeutil.HashString2LessUint32(token)
			embeddingMap[hash] += 1
		}
		dst[i] = embeddingMap
	}
	return nil
}

func (v *MultiAnalyzerBM25FunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, fmt.Errorf("analyzer receview request after function closed")
	}

	if len(inputs) != 2 {
		return nil, fmt.Errorf("BM25 function with multi analyzer must received two input column")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, fmt.Errorf("BM25 function with multi analyzer text input must be string list")
	}

	analyzer, ok := inputs[1].([]string)
	if !ok {
		return nil, fmt.Errorf("BM25 function with multi analyzer input analyzer name must be string list")
	}

	if len(text) != len(analyzer) {
		return nil, fmt.Errorf("BM25 function with multi analyzer input text and analyzer name must have same length")
	}

	rowNum := len(text)
	embedData := make([]map[uint32]float32, rowNum)
	wg := sync.WaitGroup{}

	errCh := make(chan error, v.concurrency)
	for i, j := 0, 0; i < v.concurrency && j < rowNum; i++ {
		start := j
		end := start + rowNum/v.concurrency
		if i < rowNum%v.concurrency {
			end += 1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := v.run(text[start:end], analyzer[start:end], embedData[start:end])
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
			return nil, err
		}
	}

	return []any{buildSparseFloatArray(embedData)}, nil
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
		defer tokenStream.Destroy()
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
		dst[i] = result
	}
	return nil
}

func (v *MultiAnalyzerBM25FunctionRunner) BatchAnalyze(withDetail bool, withHash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, fmt.Errorf("analyzer receview request after function closed")
	}

	if len(inputs) != 2 {
		return nil, fmt.Errorf("multi analyzer must received two input column(text, analyzer_name)")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, fmt.Errorf("multi analyzer text input must be string list")
	}

	analyzer, ok := inputs[1].([]string)
	if !ok {
		return nil, fmt.Errorf("multi analyzer input analyzer name must be string list")
	}

	if len(text) != len(analyzer) {
		return nil, fmt.Errorf("multi analyzer input text and analyzer name must have same length")
	}

	rowNum := len(text)
	result := make([][]*milvuspb.AnalyzerToken, rowNum)
	wg := sync.WaitGroup{}

	errCh := make(chan error, v.concurrency)
	for i, j := 0, 0; i < v.concurrency && j < rowNum; i++ {
		start := j
		end := start + rowNum/v.concurrency
		if i < rowNum%v.concurrency {
			end += 1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := v.analyze(text[start:end], analyzer[start:end], result[start:end], withDetail, withHash)
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
			return nil, err
		}
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
