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
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/ctokenizer"
	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const analyzerParams = "analyzer_params"

// BM25 Runner
// Input: string
// Output: map[uint32]float32
type BM25FunctionRunner struct {
	tokenizer   tokenizerapi.Tokenizer
	schema      *schemapb.FunctionSchema
	outputField *schemapb.FieldSchema
	inputField  *schemapb.FieldSchema
	concurrency int
}

func getAnalyzerParams(field *schemapb.FieldSchema) string {
	for _, param := range field.GetTypeParams() {
		if param.Key == analyzerParams {
			return param.Value
		}
	}
	return "{}"
}

func NewBM25FunctionRunner(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (FunctionRunner, error) {
	if len(schema.GetOutputFieldIds()) != 1 {
		return nil, fmt.Errorf("bm25 function should only have one output field, but now %d", len(schema.GetOutputFieldIds()))
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
		return nil, fmt.Errorf("no output field")
	}

	if params, ok := getMultiAnalyzerParams(inputField); ok {
		return NewMultiAnalyzerBM25FunctionRunner(coll, schema, inputField, outputField, params)
	}

	params = getAnalyzerParams(inputField)
	tokenizer, err := ctokenizer.NewTokenizer(params)
	if err != nil {
		return nil, err
	}

	return &BM25FunctionRunner{
		schema:      schema,
		inputField:  inputField,
		outputField: outputField,
		tokenizer:   tokenizer,
		concurrency: 8,
	}, nil
}

func (v *BM25FunctionRunner) run(data []string, dst []map[uint32]float32) error {
	tokenizer, err := v.tokenizer.Clone()
	if err != nil {
		return err
	}
	defer tokenizer.Destroy()

	for i := 0; i < len(data); i++ {
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
	if len(inputs) > 1 {
		return nil, errors.New("BM25 function received more than one input column")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, errors.New("BM25 function batch input not string list")
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
