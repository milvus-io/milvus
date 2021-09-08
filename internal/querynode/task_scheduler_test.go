// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"errors"
	"testing"
)

type mockTask struct {
	baseTask
	preExecuteError bool
	executeError    bool
	timestamp       Timestamp
}

func (m *mockTask) Timestamp() Timestamp {
	return m.timestamp
}

func (m *mockTask) OnEnqueue() error {
	return nil
}

func (m *mockTask) PreExecute(ctx context.Context) error {
	if m.preExecuteError {
		return errors.New("test error")
	}
	return nil
}

func (m *mockTask) Execute(ctx context.Context) error {
	if m.executeError {
		return errors.New("test error")
	}
	return nil
}

func (m *mockTask) PostExecute(ctx context.Context) error {
	return nil
}

func TestTaskScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := newTaskScheduler(ctx)
	ts.Start()

	task := &mockTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error, 1024),
		},
		preExecuteError: true,
		executeError:    false,
	}
	ts.processTask(task, ts.queue)

	task.preExecuteError = false
	task.executeError = true
	ts.processTask(task, ts.queue)

	ts.Close()
}
