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

package querycoordv2

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type prewarmTaskRecord struct {
	taskID       string
	state        querypb.PrewarmTaskState
	progress     int32
	errorMessage string
}

type prewarmTaskStore struct {
	mu    sync.RWMutex
	tasks map[string]prewarmTaskRecord
}

func newPrewarmTaskStore() *prewarmTaskStore {
	return &prewarmTaskStore{
		tasks: make(map[string]prewarmTaskRecord),
	}
}

func (s *prewarmTaskStore) create(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[taskID] = prewarmTaskRecord{
		taskID:   taskID,
		state:    querypb.PrewarmTaskState_PrewarmTaskStatePending,
		progress: 0,
	}
}

func (s *prewarmTaskStore) warming(taskID string, progress int32) {
	s.update(taskID, func(record *prewarmTaskRecord) {
		record.state = querypb.PrewarmTaskState_PrewarmTaskStateWarming
		record.progress = progress
		record.errorMessage = ""
	})
}

func (s *prewarmTaskStore) complete(taskID string) {
	s.update(taskID, func(record *prewarmTaskRecord) {
		record.state = querypb.PrewarmTaskState_PrewarmTaskStateCompleted
		record.progress = 100
		record.errorMessage = ""
	})
}

func (s *prewarmTaskStore) fail(taskID string, err error) {
	message := ""
	if err != nil {
		message = err.Error()
	}
	s.update(taskID, func(record *prewarmTaskRecord) {
		record.state = querypb.PrewarmTaskState_PrewarmTaskStateFailed
		record.progress = 100
		record.errorMessage = message
	})
}

func (s *prewarmTaskStore) update(taskID string, updateFn func(*prewarmTaskRecord)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.tasks[taskID]
	if !ok {
		return
	}
	updateFn(&record)
	s.tasks[taskID] = record
}

func (s *prewarmTaskStore) describe(taskID string) (prewarmTaskRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.tasks[taskID]
	return record, ok
}
