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

package function

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// FunctionRunnerLocalStore owns short-lived runners used when a managed runner
// is unavailable. It is intended for consumer-side old insert message handling.
type FunctionRunnerLocalStore struct {
	runners        map[int32][]FunctionRunner
	outputFieldIDs map[int32][]int64
}

func NewFunctionRunnerLocalStore() *FunctionRunnerLocalStore {
	return &FunctionRunnerLocalStore{
		runners:        make(map[int32][]FunctionRunner),
		outputFieldIDs: make(map[int32][]int64),
	}
}

func (s *FunctionRunnerLocalStore) Close() {
	if s == nil {
		return
	}
	for _, runners := range s.runners {
		CloseRunners(runners)
	}
	s.runners = make(map[int32][]FunctionRunner)
	s.outputFieldIDs = make(map[int32][]int64)
}

func (s *FunctionRunnerLocalStore) OutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	if s == nil {
		return EmbeddingOutputFieldIDs(schema)
	}
	schemaVersion := schema.GetVersion()
	if outputFieldIDs, ok := s.outputFieldIDs[schemaVersion]; ok {
		return outputFieldIDs, nil
	}

	if !HasEmbeddingFunctions(schema) {
		s.outputFieldIDs[schemaVersion] = nil
		return nil, nil
	}
	outputFieldIDs, err := EmbeddingOutputFieldIDs(schema)
	if err != nil {
		return nil, err
	}
	s.outputFieldIDs[schemaVersion] = outputFieldIDs
	return outputFieldIDs, nil
}

// FillEmbeddingData is only used to handle old insert messages that were not
// embedded before WAL append.
func (s *FunctionRunnerLocalStore) FillEmbeddingData(collectionID int64, schema *schemapb.CollectionSchema, body *msgpb.InsertRequest) error {
	if !HasEmbeddingFunctions(schema) {
		return nil
	}
	if s == nil {
		runners, err := BuildEmbeddingRunners(schema)
		if err != nil {
			return err
		}
		defer CloseRunners(runners)
		_, err = FillFunctionFields(runners, body)
		return err
	}
	schemaVersion := schema.GetVersion()
	_, ok, err := TryMaterialize(collectionID, schemaVersion, body)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	runners, ok := s.runners[schemaVersion]
	if !ok {
		runners, err = BuildEmbeddingRunners(schema)
		if err != nil {
			return err
		}
		s.runners[schemaVersion] = runners
	}
	_, err = FillFunctionFields(runners, body)
	return err
}
