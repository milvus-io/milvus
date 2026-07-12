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

package delegator

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// functionRuntimeState is built once per ready snapshot and immutable after
// construction (RCU semantics): readers access it lock-free.
type functionRuntimeState struct {
	schemaVersion     int32
	functionFieldType map[UniqueID]schemapb.FunctionType
}

func buildFunctionRuntimeState(schema *schemapb.CollectionSchema) (*functionRuntimeState, error) {
	functionFieldType, err := buildFunctionFieldTypes(schema)
	if err != nil {
		return nil, err
	}
	return &functionRuntimeState{
		schemaVersion:     schema.GetVersion(),
		functionFieldType: functionFieldType,
	}, nil
}

func buildFunctionFieldTypes(schema *schemapb.CollectionSchema) (map[UniqueID]schemapb.FunctionType, error) {
	functionFieldType := make(map[UniqueID]schemapb.FunctionType)
	if schema == nil {
		return functionFieldType, nil
	}

	for _, fn := range schema.GetFunctions() {
		if !function.IsEmbeddingFunctionType(fn.GetType()) {
			continue
		}
		outputFieldIDs, err := functionRuntimeOutputFieldIDs(schema, fn)
		if err != nil {
			return nil, err
		}
		for _, outputFieldID := range outputFieldIDs {
			functionFieldType[outputFieldID] = fn.GetType()
		}
	}

	return functionFieldType, nil
}

func functionRuntimeOutputFieldIDs(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) ([]int64, error) {
	if outputIDs := fn.GetOutputFieldIds(); len(outputIDs) > 0 {
		outputFieldIDs := append([]int64(nil), outputIDs...)
		for _, outputFieldID := range outputFieldIDs {
			if typeutil.GetField(schema, outputFieldID) == nil {
				return nil, merr.WrapErrFunctionFailedMsg("function %s output field %d not found", fn.GetName(), outputFieldID)
			}
		}
		return outputFieldIDs, nil
	}

	outputNames := fn.GetOutputFieldNames()
	if len(outputNames) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("function %s output fields not found", fn.GetName())
	}

	outputFieldIDs := make([]int64, 0, len(outputNames))
	for _, outputName := range outputNames {
		field := typeutil.GetFieldByName(schema, outputName)
		if field == nil {
			return nil, merr.WrapErrFunctionFailedMsg("function %s output field %s not found", fn.GetName(), outputName)
		}
		outputFieldIDs = append(outputFieldIDs, field.GetFieldID())
	}
	return outputFieldIDs, nil
}

func (s *functionRuntimeState) hasFunctionType(fieldID UniqueID, functionType schemapb.FunctionType) bool {
	if s == nil {
		return false
	}
	return s.functionFieldType[fieldID] == functionType
}

func (s *functionRuntimeState) withSearchFunction(fieldID UniqueID, run func(schemapb.FunctionType) error) error {
	if s == nil {
		return nil
	}
	functionType := s.functionFieldType[fieldID]
	if functionType == schemapb.FunctionType_Unknown {
		return nil
	}
	return run(functionType)
}
