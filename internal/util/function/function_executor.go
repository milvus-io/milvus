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

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)


type Runner interface {
	GetSchema() *schemapb.FunctionSchema
	GetOutputFields() []*schemapb.FieldSchema

	MaxBatch() int
	ProcessInsert(inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error)	
}


type FunctionExecutor struct {
	runners []Runner
}

func newFunctionExecutor(schema *schemapb.CollectionSchema) (*FunctionExecutor, error) {
	executor := new(FunctionExecutor)
	for _, f_schema := range schema.Functions {
		switch f_schema.GetType() {
		case schemapb.FunctionType_BM25:
		case schemapb.FunctionType_OpenAIEmbedding:
			f, err := NewOpenAIEmbeddingFunction(schema, f_schema)
			if err != nil {
				return nil, err
			}
			executor.runners = append(executor.runners, f)
		default:
			return nil, fmt.Errorf("unknown functionRunner type %s", f_schema.GetType().String())
		}
	}
	return executor, nil
}

func (executor *FunctionExecutor)processSingleFunction(idx int, msg *msgstream.InsertMsg) ([]*schemapb.FieldData, error) {
	runner := executor.runners[idx]
	inputs := make([]*schemapb.FieldData, 0, len(runner.GetSchema().InputFieldIds))
	for _, id := range runner.GetSchema().InputFieldIds {
		for _, field := range msg.FieldsData{
			if field.FieldId == id {
				inputs = append(inputs, field)
			}
		}
	}

	if len(inputs) != len(runner.GetSchema().InputFieldIds) {
		return nil, fmt.Errorf("Input field not found")
	}

	outputs, err := runner.ProcessInsert(inputs)
	if err != nil {
		return nil, err
	}
	return outputs, nil
}

func (executor *FunctionExecutor)ProcessInsert(msg *msgstream.InsertMsg) error {
	numRows := msg.NumRows
	for _, runner := range executor.runners {
		if numRows > uint64(runner.MaxBatch()) {
			return fmt.Errorf("numRows [%d] > function [%s]'s max batch [%d]", numRows, runner.GetSchema().Name, runner.MaxBatch())
		}
	}
	
	outputs := make(chan []*schemapb.FieldData, len(executor.runners))
	errChan := make(chan error, len(executor.runners))
	var wg sync.WaitGroup
	for idx, _ := range executor.runners {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			data, err := executor.processSingleFunction(index, msg)
			if err != nil {
				errChan <- err
			} else {
				outputs <- data
			}

		}(idx)
	}
	wg.Wait()
	close(errChan)
	close(outputs)
	for err := range errChan {
		return err
	}
	for output := range outputs {
		msg.FieldsData = append(msg.FieldsData, output...)
	}
	return nil
}


func  (executor *FunctionExecutor)ProcessSearch(msg *milvuspb.SearchRequest) error {
	return nil
}
