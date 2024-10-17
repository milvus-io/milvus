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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)


type Runner interface {
	GetSchema() *schemapb.FunctionSchema
	GetOutputFields() []*schemapb.FieldSchema

	MaxBatch() int
	ProcessInsert(inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error)
	ProcessSearch(placeholderGroup *commonpb.PlaceholderGroup) (*commonpb.PlaceholderGroup, error)
}

type FunctionExecutor struct {
	runners map[int64]Runner
}

func createFunction(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (Runner, error) {
	switch schema.GetType() {
	case schemapb.FunctionType_BM25: // ignore bm25 function
		return nil, nil
	case schemapb.FunctionType_OpenAIEmbedding:
		f, err := NewOpenAIEmbeddingFunction(coll, schema)
		if err != nil {
			return nil, err
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unknown functionRunner type %s", schema.GetType().String())
	}
}

func NewFunctionExecutor(schema *schemapb.CollectionSchema) (*FunctionExecutor, error) {
	// If the function's outputs exists in outputIDs, then create the function
	// when outputIDs is empty, create all functions
	// Please note that currently we only support the function with one input and one output.
	executor := &FunctionExecutor{
		runners: make(map[int64]Runner),
	}
	for _, f_schema := range schema.Functions {
		if runner, err := createFunction(schema, f_schema); err != nil {
			return nil, err
		} else {
			if runner != nil {
				executor.runners[f_schema.GetOutputFieldIds()[0]] = runner
			}
		}
	}
	return executor, nil
}

func (executor *FunctionExecutor)processSingleFunction(runner Runner, msg *msgstream.InsertMsg) ([]*schemapb.FieldData, error) {
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
	for _, runner := range executor.runners {
		wg.Add(1)
		go func(runner Runner) {
			defer wg.Done()
			data, err := executor.processSingleFunction(runner, msg)
			if err != nil {
				errChan <- err
			} else {
				outputs <- data
			}

		}(runner)
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

func (executor *FunctionExecutor)processSingleSearch(runner Runner, placeholderGroup []byte) ([]byte, error) {
	pb := &commonpb.PlaceholderGroup{}
	proto.Unmarshal(placeholderGroup, pb)
	if len(pb.Placeholders) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("No placeholders founded")
	}
	if pb.Placeholders[0].Type != commonpb.PlaceholderType_VarChar {
		return placeholderGroup, nil
	}
	res, err := runner.ProcessSearch(pb)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(res)
}

func (executor *FunctionExecutor)prcessSearch(req *internalpb.SearchRequest) error {
	runner, exist := executor.runners[req.FieldId]
	if !exist {
		return nil
	}
	if req.Nq > int64(runner.MaxBatch()) {
		return fmt.Errorf("Nq [%d] > function [%s]'s max batch [%d]", req.Nq, runner.GetSchema().Name, runner.MaxBatch())
	}
	if newHolder, err := executor.processSingleSearch(runner, req.GetPlaceholderGroup()); err != nil {
		return err
	} else {
		req.PlaceholderGroup = newHolder
	}
	return nil
}

func (executor *FunctionExecutor)prcessAdvanceSearch(req *internalpb.SearchRequest) error {
	outputs := make(chan map[int64][]byte, len(req.GetSubReqs()))
	errChan := make(chan error, len(req.GetSubReqs()))
	var wg sync.WaitGroup
	for idx, sub := range req.GetSubReqs() {
		if runner, exist := executor.runners[sub.FieldId]; exist {
			if sub.Nq > int64(runner.MaxBatch()) {
				return fmt.Errorf("Nq [%d] > function [%s]'s max batch [%d]", sub.Nq, runner.GetSchema().Name, runner.MaxBatch())
			}
			wg.Add(1)
			go func(runner Runner, idx int64) {
				defer wg.Done()
				if newHolder, err := executor.processSingleSearch(runner, sub.GetPlaceholderGroup()); err != nil {
					errChan <- err
				} else {
					outputs <- map[int64][]byte{idx: newHolder}
				}

			}(runner, int64(idx))
		}
	}
	wg.Wait()
	close(errChan)
	close(outputs)
	for err := range errChan {
		return err
	}

	for output := range outputs {
		for idx, holder := range output {
			req.SubReqs[idx].PlaceholderGroup = holder
		}
	}
	return nil
}

func (executor *FunctionExecutor)ProcessSearch(req *internalpb.SearchRequest) error {
	if !req.IsAdvanced {
		return executor.prcessSearch(req)
	} else {
		return executor.prcessAdvanceSearch(req)
	}
}
