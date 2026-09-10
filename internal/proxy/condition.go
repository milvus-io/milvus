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

package proxy

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Condition defines the interface of variable condition.
type Condition interface {
	WaitToFinish() error
	Notify(err error)
	Ctx() context.Context
	SetOnContextDone(handler func())
}

// make sure interface implementation
var _ Condition = (*TaskCondition)(nil)

// TaskCondition implements Condition interface for tasks
type TaskCondition struct {
	done chan error
	ctx  context.Context

	onContextDone func()
}

// WaitToFinish waits until the TaskCondition is notified or context done or canceled
func (tc *TaskCondition) WaitToFinish() error {
	select {
	case <-tc.ctx.Done():
		if tc.onContextDone != nil {
			tc.onContextDone()
		}
		return errors.Wrap(tc.ctx.Err(), "proxy TaskCondition context Done")
	case err := <-tc.done:
		return err
	}
}

// Notify sends a signal into the done channel
func (tc *TaskCondition) Notify(err error) {
	tc.done <- err
}

// Ctx returns internal context
func (tc *TaskCondition) Ctx() context.Context {
	return tc.ctx
}

// SetOnContextDone registers a handler invoked when WaitToFinish returns because its context is done.
// Registration must complete before WaitToFinish begins, and the handler must not change after waiting starts.
func (tc *TaskCondition) SetOnContextDone(handler func()) {
	tc.onContextDone = handler
}

// NewTaskCondition creates a TaskCondition with provided context
func NewTaskCondition(ctx context.Context) *TaskCondition {
	return &TaskCondition{
		done: make(chan error, 1),
		ctx:  ctx,
	}
}
