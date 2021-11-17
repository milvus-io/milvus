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

package querycoord

import (
	"context"
	"errors"
)

type condition interface {
	waitToFinish() error
	notify(err error)
	Ctx() context.Context
}

type taskCondition struct {
	done chan error
	ctx  context.Context
}

func (tc *taskCondition) waitToFinish() error {
	for {
		select {
		case <-tc.ctx.Done():
			return errors.New("timeout")
		case err := <-tc.done:
			return err
		}
	}
}

func (tc *taskCondition) notify(err error) {
	tc.done <- err
}

func (tc *taskCondition) Ctx() context.Context {
	return tc.ctx
}

func newTaskCondition(ctx context.Context) *taskCondition {
	return &taskCondition{
		done: make(chan error, 1),
		ctx:  ctx,
	}
}
