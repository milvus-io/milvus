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
	"context"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const analyzerParams = "analyzer_params"

var (
	analyzerPool         *conc.Pool[struct{}]
	analyzerPoolInitOnce sync.Once
)

func getAnalyzerPoolSize() int {
	cpuNum := hardware.GetCPUNum()
	poolSize := int(float64(cpuNum) * paramtable.Get().FunctionCfg.AnalyzerConcurrencyPerCPUCore.GetAsFloat())
	if poolSize <= 0 {
		log.Warn("analyzer pool size is not positive, set to cpu num", zap.Int("cpuNum", cpuNum))
		poolSize = cpuNum
	}
	return poolSize
}

func initAnalyzerPool() {
	analyzerPool = conc.NewPool[struct{}](getAnalyzerPoolSize())

	pt := paramtable.Get()
	pt.Watch(pt.FunctionCfg.AnalyzerConcurrencyPerCPUCore.Key, config.NewHandler("function.analyzer.concurrency", ResizeAnalyzerPool))
}

func resizeAnalyzerPool(pool *conc.Pool[struct{}], newSize int) {
	log := log.Ctx(context.Background()).With(zap.Int("newSize", newSize))

	if newSize <= 0 {
		log.Warn("cannot set analyzer pool size to non-positive value")
		return
	}

	if err := pool.Resize(newSize); err != nil {
		log.Warn("failed to resize analyzer pool", zap.Error(err))
		return
	}
	log.Info("analyzer pool resize successfully")
}

func ResizeAnalyzerPool(evt *config.Event) {
	if !evt.HasUpdated {
		return
	}

	resizeAnalyzerPool(getOrCreateAnalyzerPool(), getAnalyzerPoolSize())
}

func getOrCreateAnalyzerPool() *conc.Pool[struct{}] {
	analyzerPoolInitOnce.Do(initAnalyzerPool)
	return analyzerPool
}

func getAnalyzerRunnerConcurrency() int {
	return paramtable.Get().FunctionCfg.GetAnalyzerRunnerConcurrency()
}

type Analyzer interface {
	BatchAnalyze(withDetail bool, withHash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error)
	GetInputFields() []*schemapb.FieldSchema
}

// BM25 Runner
// Input: string
// Output: map[uint32]float32
type BM25FunctionRunner struct {
	mu     sync.RWMutex
	closed bool

	tokenizer   analyzer.Analyzer
	schema      *schemapb.FunctionSchema
	outputField *schemapb.FieldSchema
	inputField  *schemapb.FieldSchema
}

func getAnalyzerParams(field *schemapb.FieldSchema) string {
	for _, param := range field.GetTypeParams() {
		if param.Key == analyzerParams {
			return param.Value
		}
	}
	return "{}"
}

func NewAnalyzerRunner(field *schemapb.FieldSchema) (Analyzer, error) {
	params := getAnalyzerParams(field)
	tokenizer, err := analyzer.NewAnalyzer(params, "")
	if err != nil {
		return nil, err
	}

	return &BM25FunctionRunner{
		inputField: field,
		tokenizer:  tokenizer,
	}, nil
}

func NewBM25FunctionRunner(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (FunctionRunner, error) {
	if len(schema.GetOutputFieldIds()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("bm25 function should only have one output field, but now %d", len(schema.GetOutputFieldIds()))
	}

	var inputField, outputField *schemapb.FieldSchema
	var params string
	for _, field := range coll.GetFields() {
		if field.GetFieldID() == schema.GetOutputFieldIds()[0] {
			outputField = field
		}

		if field.GetFieldID() == schema.GetInputFieldIds()[0] {
			inputField = field
		}
	}

	if outputField == nil {
		return nil, merr.WrapErrParameterInvalidMsg("no output field")
	}

	if params, ok := getMultiAnalyzerParams(inputField); ok {
		return NewMultiAnalyzerBM25FunctionRunner(coll, schema, inputField, outputField, params)
	}

	params = getAnalyzerParams(inputField)
	tokenizer, err := analyzer.NewAnalyzer(params, "")
	if err != nil {
		return nil, err
	}

	return &BM25FunctionRunner{
		schema:      schema,
		inputField:  inputField,
		outputField: outputField,
		tokenizer:   tokenizer,
	}, nil
}

func (v *BM25FunctionRunner) run(data []string, dst []map[uint32]float32) error {
	tokenizer, err := v.tokenizer.Clone()
	if err != nil {
		return err
	}
	defer tokenizer.Destroy()

	for i := 0; i < len(data); i++ {
		if len(data[i]) == 0 {
			dst[i] = map[uint32]float32{}
			continue
		}

		if !typeutil.IsUTF8(data[i]) {
			return merr.WrapErrParameterInvalidMsg("string data must be utf8 format: %v", data[i])
		}
		embeddingMap := map[uint32]float32{}
		tokenStream := tokenizer.NewTokenStream(data[i])
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

func (v *BM25FunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, merr.WrapErrServiceInternalMsg("analyzer receview request after function closed")
	}

	if len(inputs) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("BM25 function received more than one input column")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("BM25 function batch input not string list")
	}

	rowNum := len(text)
	embedData := make([]map[uint32]float32, rowNum)
	wg := sync.WaitGroup{}
	concurrency := getAnalyzerRunnerConcurrency()

	errCh := make(chan error, concurrency)
	for i, j := 0, 0; i < concurrency && j < rowNum; i++ {
		start := j
		end := start + rowNum/concurrency
		if i < rowNum%concurrency {
			end += 1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := v.run(text[start:end], embedData[start:end])
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

func (v *BM25FunctionRunner) analyze(data []string, dst [][]*milvuspb.AnalyzerToken, withDetail bool, withHash bool) error {
	tokenizer, err := v.tokenizer.Clone()
	if err != nil {
		return err
	}
	defer tokenizer.Destroy()

	for i := 0; i < len(data); i++ {
		result := []*milvuspb.AnalyzerToken{}
		tokenStream := tokenizer.NewTokenStream(data[i])
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

func (v *BM25FunctionRunner) BatchAnalyze(withDetail bool, withHash bool, inputs ...any) ([][]*milvuspb.AnalyzerToken, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, merr.WrapErrServiceInternalMsg("analyzer receview request after function closed")
	}

	if len(inputs) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("analyze received should only receive text input column(not set analyzer name)")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("batch input not string list")
	}

	rowNum := len(text)
	result := make([][]*milvuspb.AnalyzerToken, rowNum)
	pool := getOrCreateAnalyzerPool()
	concurrency := getAnalyzerRunnerConcurrency()
	futures := make([]*conc.Future[struct{}], 0, concurrency)

	for i, j := 0, 0; i < concurrency && j < rowNum; i++ {
		start := j
		end := start + rowNum/concurrency
		if i < rowNum%concurrency {
			end += 1
		}
		future := pool.Submit(func() (struct{}, error) {
			return struct{}{}, v.analyze(text[start:end], result[start:end], withDetail, withHash)
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

func (v *BM25FunctionRunner) GetSchema() *schemapb.FunctionSchema {
	return v.schema
}

func (v *BM25FunctionRunner) GetOutputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{v.outputField}
}

func (v *BM25FunctionRunner) GetInputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{v.inputField}
}

func (v *BM25FunctionRunner) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.closed = true
	v.tokenizer.Destroy()
}

func buildSparseFloatArray(mapdata []map[uint32]float32) *schemapb.SparseFloatArray {
	dim := int64(0)
	bytes := lo.Map(mapdata, func(sparseMap map[uint32]float32, _ int) []byte {
		row := typeutil.CreateAndSortSparseFloatRow(sparseMap)
		rowDim := typeutil.SparseFloatRowDim(row)
		if rowDim > dim {
			dim = rowDim
		}
		return row
	})

	return &schemapb.SparseFloatArray{
		Contents: bytes,
		Dim:      dim,
	}
}
