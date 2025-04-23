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
	"fmt"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type Runner interface {
	GetSchema() *schemapb.FunctionSchema
	GetOutputFields() []*schemapb.FieldSchema
	GetCollectionName() string
	GetFunctionTypeName() string
	GetFunctionName() string
	GetFunctionProvider() string
	Check() error

	MaxBatch() int
	ProcessInsert(ctx context.Context, inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error)
	ProcessSearch(ctx context.Context, placeholderGroup *commonpb.PlaceholderGroup) (*commonpb.PlaceholderGroup, error)
	ProcessBulkInsert(inputs []storage.FieldData) (map[storage.FieldID]storage.FieldData, error)
}

type FunctionExecutor struct {
	runners map[int64]Runner
}

func createFunction(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (Runner, error) {
	switch schema.GetType() {
	case schemapb.FunctionType_BM25: // ignore bm25 function
		return nil, nil
	case schemapb.FunctionType_TextEmbedding:
		f, err := NewTextEmbeddingFunction(coll, schema)
		if err != nil {
			return nil, err
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unknown functionRunner type %s", schema.GetType().String())
	}
}

// Since bm25 and embedding are implemented in different ways, the bm25 function is not verified here.
func ValidateFunctions(schema *schemapb.CollectionSchema) error {
	for _, fSchema := range schema.Functions {
		f, err := createFunction(schema, fSchema)
		if err != nil {
			return err
		}

		// ignore bm25 function
		if f == nil {
			continue
		}
		if err := f.Check(); err != nil {
			return fmt.Errorf("Check function [%s:%s] failed, the err is: %v", fSchema.Name, fSchema.GetType().String(), err)
		}
	}
	return nil
}

func NewFunctionExecutor(schema *schemapb.CollectionSchema) (*FunctionExecutor, error) {
	executor := &FunctionExecutor{
		runners: make(map[int64]Runner),
	}
	for _, fSchema := range schema.Functions {
		runner, err := createFunction(schema, fSchema)
		if err != nil {
			return nil, err
		}
		if runner != nil {
			executor.runners[fSchema.GetOutputFieldIds()[0]] = runner
		}
	}
	return executor, nil
}

func (executor *FunctionExecutor) processSingleFunction(ctx context.Context, runner Runner, msg *msgstream.InsertMsg) ([]*schemapb.FieldData, error) {
	inputs := make([]*schemapb.FieldData, 0, len(runner.GetSchema().GetInputFieldNames()))
	for _, name := range runner.GetSchema().GetInputFieldNames() {
		for _, field := range msg.FieldsData {
			if field.GetFieldName() == name {
				inputs = append(inputs, field)
			}
		}
	}
	if len(inputs) != len(runner.GetSchema().InputFieldIds) {
		return nil, errors.New("Input field not found")
	}

	tr := timerecord.NewTimeRecorder("function ProcessInsert")
	outputs, err := runner.ProcessInsert(ctx, inputs)
	if err != nil {
		return nil, err
	}

	metrics.ProxyFunctionlatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), runner.GetCollectionName(), runner.GetFunctionTypeName(), runner.GetFunctionProvider(), runner.GetFunctionName()).Observe(float64(tr.RecordSpan().Milliseconds()))
	tr.CtxElapse(ctx, "function ProcessInsert done")
	return outputs, nil
}

func (executor *FunctionExecutor) ProcessInsert(ctx context.Context, msg *msgstream.InsertMsg) error {
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
			data, err := executor.processSingleFunction(ctx, runner, msg)
			if err != nil {
				errChan <- err
				return
			}
			outputs <- data
		}(runner)
	}
	wg.Wait()
	close(errChan)
	close(outputs)

	// Collect all errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}

	for output := range outputs {
		msg.FieldsData = append(msg.FieldsData, output...)
	}
	return nil
}

func (executor *FunctionExecutor) processSingleSearch(ctx context.Context, runner Runner, placeholderGroup []byte) ([]byte, error) {
	pb := &commonpb.PlaceholderGroup{}
	proto.Unmarshal(placeholderGroup, pb)
	if len(pb.Placeholders) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("No placeholders founded")
	}
	if pb.Placeholders[0].Type != commonpb.PlaceholderType_VarChar {
		return placeholderGroup, nil
	}

	tr := timerecord.NewTimeRecorder("function ProcessSearch")
	res, err := runner.ProcessSearch(ctx, pb)
	if err != nil {
		return nil, err
	}
	metrics.ProxyFunctionlatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), runner.GetCollectionName(), runner.GetFunctionTypeName(), runner.GetFunctionProvider(), runner.GetFunctionName()).Observe(float64(tr.RecordSpan().Milliseconds()))
	tr.CtxElapse(ctx, "function ProcessSearch done")
	return proto.Marshal(res)
}

func (executor *FunctionExecutor) prcessSearch(ctx context.Context, req *internalpb.SearchRequest) error {
	runner, exist := executor.runners[req.FieldId]
	if !exist {
		return fmt.Errorf("Can not found function in field %d", req.FieldId)
	}
	if req.Nq > int64(runner.MaxBatch()) {
		return fmt.Errorf("Nq [%d] > function [%s]'s max batch [%d]", req.Nq, runner.GetSchema().Name, runner.MaxBatch())
	}
	if newHolder, err := executor.processSingleSearch(ctx, runner, req.GetPlaceholderGroup()); err != nil {
		return err
	} else {
		req.PlaceholderGroup = newHolder
	}
	return nil
}

func (executor *FunctionExecutor) prcessAdvanceSearch(ctx context.Context, req *internalpb.SearchRequest) error {
	outputs := make(chan map[int64][]byte, len(req.GetSubReqs()))
	errChan := make(chan error, len(req.GetSubReqs()))
	var wg sync.WaitGroup
	for idx, sub := range req.GetSubReqs() {
		if runner, exist := executor.runners[sub.FieldId]; exist {
			if sub.Nq > int64(runner.MaxBatch()) {
				return fmt.Errorf("Nq [%d] > function [%s]'s max batch [%d]", sub.Nq, runner.GetSchema().Name, runner.MaxBatch())
			}
			wg.Add(1)
			go func(runner Runner, idx int64, placeholderGroup []byte) {
				defer wg.Done()
				if newHolder, err := executor.processSingleSearch(ctx, runner, placeholderGroup); err != nil {
					errChan <- err
				} else {
					outputs <- map[int64][]byte{idx: newHolder}
				}
			}(runner, int64(idx), sub.GetPlaceholderGroup())
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

func (executor *FunctionExecutor) ProcessSearch(ctx context.Context, req *internalpb.SearchRequest) error {
	if !req.IsAdvanced {
		return executor.prcessSearch(ctx, req)
	}
	return executor.prcessAdvanceSearch(ctx, req)
}

func (executor *FunctionExecutor) processSingleBulkInsert(runner Runner, data *storage.InsertData) (map[storage.FieldID]storage.FieldData, error) {
	inputs := make([]storage.FieldData, 0, len(runner.GetSchema().InputFieldIds))
	for idx, id := range runner.GetSchema().InputFieldIds {
		field, exist := data.Data[id]
		if !exist {
			return nil, fmt.Errorf("Can not find input field: [%s]", runner.GetSchema().GetInputFieldNames()[idx])
		}
		inputs = append(inputs, field)
	}

	outputs, err := runner.ProcessBulkInsert(inputs)
	if err != nil {
		return nil, err
	}
	return outputs, nil
}

func (executor *FunctionExecutor) ProcessBulkInsert(data *storage.InsertData) error {
	// Since concurrency has already been used in the outer layer, only a serial logic access model is used here.
	for _, runner := range executor.runners {
		output, err := executor.processSingleBulkInsert(runner, data)
		if err != nil {
			return nil
		}
		for k, v := range output {
			data.Data[k] = v
		}
	}
	return nil
}
