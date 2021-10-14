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

package querycoord

import (
	"context"
	"errors"
)

type Condition interface {
	waitToFinish() error
	notify(err error)
	Ctx() context.Context
}

type TaskCondition struct {
	done chan error
	ctx  context.Context
}

func (tc *TaskCondition) waitToFinish() error {
	for {
		select {
		case <-tc.ctx.Done():
			return errors.New("timeout")
		case err := <-tc.done:
			return err
		}
	}
}

func (tc *TaskCondition) notify(err error) {
	tc.done <- err
}

func (tc *TaskCondition) Ctx() context.Context {
	return tc.ctx
}

func NewTaskCondition(ctx context.Context) *TaskCondition {
	return &TaskCondition{
		done: make(chan error, 1),
		ctx:  ctx,
	}
}
