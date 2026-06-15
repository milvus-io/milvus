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
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type functionRuntimeState struct {
	mu                sync.RWMutex
	functionFieldType map[UniqueID]schemapb.FunctionType
}

func buildFunctionRuntimeState(schema *schemapb.CollectionSchema) (*functionRuntimeState, error) {
	state := &functionRuntimeState{
		functionFieldType: make(map[UniqueID]schemapb.FunctionType),
	}
	if schema == nil {
		return state, nil
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
			state.functionFieldType[outputFieldID] = fn.GetType()
		}
	}

	return state, nil
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

func (s *functionRuntimeState) Close() {
}

func (s *functionRuntimeState) counts() (int, int, int) {
	if s == nil {
		return 0, 0, 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return 0, len(s.functionFieldType), 0
}

func (s *functionRuntimeState) hasFunctionType(fieldID UniqueID, functionType schemapb.FunctionType) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.functionFieldType[fieldID] == functionType
}

func (s *functionRuntimeState) swap(newState *functionRuntimeState) *functionRuntimeState {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldState := &functionRuntimeState{
		functionFieldType: s.functionFieldType,
	}
	s.functionFieldType = newState.functionFieldType
	return oldState
}

func (s *functionRuntimeState) withSearchFunction(fieldID UniqueID, run func(schemapb.FunctionType) error) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	functionType := s.functionFieldType[fieldID]
	if functionType == schemapb.FunctionType_Unknown {
		return nil
	}
	return run(functionType)
}
