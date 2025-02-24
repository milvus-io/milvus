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

package rootcoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

type baseRedoTask struct {
	syncTodoStep  []nestedStep // steps to execute synchronously
	asyncTodoStep []nestedStep // steps to execute asynchronously
	stepExecutor  StepExecutor
}

func newBaseRedoTask(stepExecutor StepExecutor) *baseRedoTask {
	return &baseRedoTask{
		syncTodoStep:  make([]nestedStep, 0),
		asyncTodoStep: make([]nestedStep, 0),
		stepExecutor:  stepExecutor,
	}
}

func (b *baseRedoTask) AddSyncStep(step nestedStep) {
	b.syncTodoStep = append(b.syncTodoStep, step)
}

func (b *baseRedoTask) AddAsyncStep(step nestedStep) {
	b.asyncTodoStep = append(b.asyncTodoStep, step)
}

func (b *baseRedoTask) redoAsyncSteps() {
	l := len(b.asyncTodoStep)
	steps := make([]nestedStep, 0, l)
	for i := l - 1; i >= 0; i-- {
		steps = append(steps, b.asyncTodoStep[i])
	}
	b.asyncTodoStep = nil // make baseRedoTask can be collected.
	b.stepExecutor.AddSteps(&stepStack{steps: steps})
}

func (b *baseRedoTask) Execute(ctx context.Context) error {
	for i := 0; i < len(b.syncTodoStep); i++ {
		todo := b.syncTodoStep[i]
		// no children step in sync steps.
		if _, err := todo.Execute(ctx); err != nil {
			log.Error("failed to execute step", zap.Error(err), zap.String("desc", todo.Desc()))
			return err
		}
	}
	go b.redoAsyncSteps()
	return nil
}
