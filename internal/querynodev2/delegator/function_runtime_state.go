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
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type functionRuntimeState struct {
	mu                sync.RWMutex
	functionRunners   map[UniqueID]function.FunctionRunner
	functionFieldType map[UniqueID]schemapb.FunctionType
	analyzerRunners   map[UniqueID]function.Analyzer
}

func buildFunctionRuntimeState(schema *schemapb.CollectionSchema) (*functionRuntimeState, error) {
	state := &functionRuntimeState{
		functionRunners:   make(map[UniqueID]function.FunctionRunner),
		functionFieldType: make(map[UniqueID]schemapb.FunctionType),
		analyzerRunners:   make(map[UniqueID]function.Analyzer),
	}

	for _, tf := range schema.GetFunctions() {
		switch tf.GetType() {
		case schemapb.FunctionType_BM25:
			functionRunner, err := function.NewFunctionRunner(schema, tf)
			if err != nil {
				state.Close()
				return nil, err
			}
			outputFieldID := tf.GetOutputFieldIds()[0]
			inputFieldID := tf.GetInputFieldIds()[0]
			state.functionRunners[outputFieldID] = functionRunner
			state.analyzerRunners[inputFieldID] = functionRunner.(function.Analyzer)
			state.functionFieldType[outputFieldID] = schemapb.FunctionType_BM25
		case schemapb.FunctionType_MinHash:
			functionRunner, err := function.NewFunctionRunner(schema, tf)
			if err != nil {
				state.Close()
				return nil, err
			}
			outputFieldID := tf.GetOutputFieldIds()[0]
			state.functionRunners[outputFieldID] = functionRunner
			state.functionFieldType[outputFieldID] = schemapb.FunctionType_MinHash
		}
	}

	for _, field := range schema.GetFields() {
		helper := typeutil.CreateFieldSchemaHelper(field)
		if helper.EnableAnalyzer() && state.analyzerRunners[field.GetFieldID()] == nil {
			analyzerRunner, err := function.NewAnalyzerRunner(field)
			if err != nil {
				state.Close()
				return nil, err
			}
			state.analyzerRunners[field.GetFieldID()] = analyzerRunner
		}
	}

	return state, nil
}

func (s *functionRuntimeState) Close() {
	if s == nil {
		return
	}
	closed := typeutil.NewSet[function.FunctionRunner]()
	for _, runner := range s.functionRunners {
		if runner == nil || closed.Contain(runner) {
			continue
		}
		runner.Close()
		closed.Insert(runner)
	}
	for _, analyzer := range s.analyzerRunners {
		runner, ok := analyzer.(function.FunctionRunner)
		if !ok || runner == nil || closed.Contain(runner) {
			continue
		}
		runner.Close()
		closed.Insert(runner)
	}
}

func (s *functionRuntimeState) counts() (int, int, int) {
	if s == nil {
		return 0, 0, 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.functionRunners), len(s.functionFieldType), len(s.analyzerRunners)
}

func (s *functionRuntimeState) hasFunctionRunner(fieldID UniqueID) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.functionRunners[fieldID]
	return ok
}

func (s *functionRuntimeState) hasAnalyzerRunner(fieldID UniqueID) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.analyzerRunners[fieldID]
	return ok
}

func (s *functionRuntimeState) functionRunnerAliasesAnalyzer(functionFieldID, analyzerFieldID UniqueID) bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	runner, ok := s.functionRunners[functionFieldID]
	if !ok {
		return false
	}
	analyzer, ok := s.analyzerRunners[analyzerFieldID]
	if !ok {
		return false
	}
	runnerAnalyzer, ok := runner.(function.Analyzer)
	return ok && runnerAnalyzer == analyzer
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
		functionRunners:   s.functionRunners,
		functionFieldType: s.functionFieldType,
		analyzerRunners:   s.analyzerRunners,
	}
	s.functionRunners = newState.functionRunners
	s.functionFieldType = newState.functionFieldType
	s.analyzerRunners = newState.analyzerRunners
	return oldState
}

func (s *functionRuntimeState) withSearchFunction(fieldID UniqueID, run func(schemapb.FunctionType, function.FunctionRunner, bool) error) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	functionType := s.functionFieldType[fieldID]
	if functionType == schemapb.FunctionType_Unknown {
		return nil
	}
	runner, ok := s.functionRunners[fieldID]
	return run(functionType, runner, ok)
}

func (s *functionRuntimeState) setFunctionRunnersForTest(functionRunners map[UniqueID]function.FunctionRunner) map[UniqueID]function.FunctionRunner {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.functionRunners
	s.functionRunners = functionRunners
	return old
}

func (s *functionRuntimeState) withFunctionRunner(fieldID UniqueID, run func(function.FunctionRunner) error) (bool, error) {
	if s == nil {
		return false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	runner, ok := s.functionRunners[fieldID]
	if !ok {
		return false, nil
	}
	return true, run(runner)
}

func (s *functionRuntimeState) withRequiredFunctionRunner(fieldID UniqueID, run func(function.FunctionRunner) error) error {
	ok, err := s.withFunctionRunner(fieldID, run)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("functionRunner not found for field: %d", fieldID)
	}
	return nil
}

func (s *functionRuntimeState) withAnalyzerRunner(fieldID UniqueID, run func(function.Analyzer) error) (bool, error) {
	if s == nil {
		return false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	analyzer, ok := s.analyzerRunners[fieldID]
	if !ok {
		return false, nil
	}
	return true, run(analyzer)
}
