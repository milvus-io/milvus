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

// isStale reports whether the runtime state lags behind the given schema
// snapshot's logical schema version.
func (s *functionRuntimeState) isStale(schema *schemapb.CollectionSchema) bool {
	if s == nil || schema == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schemaVersion < schema.GetVersion()
}

// ensureFuncState realigns the runtime state with the given schema snapshot when the
// snapshot carries a newer logical schema version. The collection snapshot can be
// advanced by paths that never run the delegator's UpdateSchema side effects
// (e.g. segment load via collectionManager.PutOrRef), so search-time function
// lookup must not depend on the UpdateSchema event alone. Version-monotonic:
// a stale schema never rolls the state back.
func (s *functionRuntimeState) ensureFuncState(schema *schemapb.CollectionSchema) error {
	if s == nil || schema == nil {
		return nil
	}
	version := schema.GetVersion()
	s.mu.RLock()
	fresh := s.schemaVersion >= version
	s.mu.RUnlock()
	if fresh {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schemaVersion >= version {
		return nil
	}
	functionFieldType, err := buildFunctionFieldTypes(schema)
	if err != nil {
		return err
	}
	s.functionFieldType = functionFieldType
	s.schemaVersion = version
	return nil
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

	// A concurrent search may have lazily refreshed the state from a newer
	// collection snapshot (ensureFuncState); never swap in an older schema version.
	if newState.schemaVersion < s.schemaVersion {
		return newState
	}

	oldState := &functionRuntimeState{
		schemaVersion:     s.schemaVersion,
		functionFieldType: s.functionFieldType,
	}
	s.functionFieldType = newState.functionFieldType
	s.schemaVersion = newState.schemaVersion
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
