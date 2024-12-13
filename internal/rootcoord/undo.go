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
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

type baseUndoTask struct {
	todoStep     []nestedStep // steps to execute
	undoStep     []nestedStep // steps to undo
	stepExecutor StepExecutor
}

func newBaseUndoTask(stepExecutor StepExecutor) *baseUndoTask {
	return &baseUndoTask{
		todoStep:     make([]nestedStep, 0),
		undoStep:     make([]nestedStep, 0),
		stepExecutor: stepExecutor,
	}
}

func (b *baseUndoTask) AddStep(todoStep, undoStep nestedStep) {
	b.todoStep = append(b.todoStep, todoStep)
	b.undoStep = append(b.undoStep, undoStep)
}

func (b *baseUndoTask) Execute(ctx context.Context) error {
	if len(b.todoStep) != len(b.undoStep) {
		return fmt.Errorf("todo step and undo step length not equal")
	}
	for i := 0; i < len(b.todoStep); i++ {
		todoStep := b.todoStep[i]
		// no children step in normal case.
		if _, err := todoStep.Execute(ctx); err != nil {
			log.Ctx(ctx).Warn("failed to execute step, trying to undo", zap.Error(err), zap.String("desc", todoStep.Desc()))
			undoSteps := b.undoStep[:i]
			b.undoStep = nil // let baseUndoTask can be collected.
			go b.stepExecutor.AddSteps(&stepStack{steps: undoSteps, stepsCtx: context.WithoutCancel(ctx)})
			return err
		}
	}
	return nil
}
