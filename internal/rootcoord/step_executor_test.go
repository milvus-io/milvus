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
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

type mockChildStep struct{}

func (m *mockChildStep) Execute(ctx context.Context) ([]nestedStep, error) {
	return nil, nil
}

func (m *mockChildStep) Desc() string {
	return "mock child step"
}

func (m *mockChildStep) Weight() stepPriority {
	return stepPriorityLow
}

func newMockChildStep() *mockChildStep {
	return &mockChildStep{}
}

type mockStepWithChild struct{}

func (m *mockStepWithChild) Execute(ctx context.Context) ([]nestedStep, error) {
	return []nestedStep{newMockChildStep()}, nil
}

func (m *mockStepWithChild) Desc() string {
	return "mock step with child"
}

func (m *mockStepWithChild) Weight() stepPriority {
	return stepPriorityLow
}

func newMockStepWithChild() *mockStepWithChild {
	return &mockStepWithChild{}
}

func Test_stepStack_Execute(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		steps := []nestedStep{
			newMockStepWithChild(),
			newMockChildStep(),
		}
		s := &stepStack{steps: steps}
		unfinished := s.Execute(context.Background())
		assert.Nil(t, unfinished)
	})

	t.Run("error case", func(t *testing.T) {
		steps := []nestedStep{
			newMockNormalStep(),
			newMockNormalStep(),
			newMockFailStep(),
			newMockNormalStep(),
		}
		s := &stepStack{steps: steps}
		unfinished := s.Execute(context.Background())
		assert.Equal(t, 3, len(unfinished.steps))
	})

	t.Run("Unrecoverable", func(t *testing.T) {
		failStep := newMockFailStep()
		failStep.err = retry.Unrecoverable(errors.New("error mock Execute"))
		steps := []nestedStep{
			failStep,
		}
		s := &stepStack{steps: steps}
		unfinished := s.Execute(context.Background())
		assert.Nil(t, unfinished)
	})
}

func Test_randomSelect(t *testing.T) {
	s0 := &stepStack{steps: []nestedStep{}}
	s1 := &stepStack{steps: []nestedStep{
		newMockNormalStep(),
	}}
	s2 := &stepStack{steps: []nestedStep{
		newMockNormalStep(),
		newMockNormalStep(),
	}}
	s3 := &stepStack{steps: []nestedStep{
		newMockNormalStep(),
		newMockNormalStep(),
		newMockNormalStep(),
	}}
	s4 := &stepStack{steps: []nestedStep{
		newMockNormalStep(),
		newMockNormalStep(),
		newMockNormalStep(),
		newMockNormalStep(),
	}}
	m := map[*stepStack]struct{}{
		s0: {},
		s1: {},
		s2: {},
		s3: {},
		s4: {},
	}
	selected := randomSelect(0, m)
	assert.Equal(t, defaultBgExecutingParallel, len(selected))
	for _, s := range selected {
		_, ok := m[s]
		assert.True(t, ok)
	}
	selected = randomSelect(2, m)
	assert.Equal(t, 2, len(selected))
	for _, s := range selected {
		_, ok := m[s]
		assert.True(t, ok)
	}
}

func Test_bgStepExecutor_scheduleLoop(t *testing.T) {
	bg := newBgStepExecutor(context.Background(),
		withSelectStepPolicy(randomSelectPolicy(defaultBgExecutingParallel)),
		withBgInterval(time.Millisecond*10))
	bg.Start()
	n := 20
	records := make([]int, 0, n)
	steps := make([]*stepStack, 0, n)
	for i := 0; i < n; i++ {
		var s *stepStack
		r := rand.Int() % 3
		records = append(records, r)
		switch r {
		case 0:
			s = nil
		case 1:
			failStep := newMockFailStep()
			failStep.err = retry.Unrecoverable(errors.New("error mock Execute"))
			s = &stepStack{steps: []nestedStep{
				newMockNormalStep(),
				failStep,
				newMockNormalStep(),
			}}
		case 2:
			s = &stepStack{steps: []nestedStep{
				newMockNormalStep(),
				newMockNormalStep(),
				newMockNormalStep(),
			}}
		default:
		}
		steps = append(steps, s)
		bg.AddSteps(s)
	}
	for i, r := range records {
		switch r {
		case 0:
			assert.Nil(t, steps[i])
		case 1:
			<-steps[i].steps[1].(*mockFailStep).calledChan
			assert.True(t, steps[i].steps[1].(*mockFailStep).called)
		case 2:
		default:
		}
	}
	bg.Stop()
}

func Test_selectByPriorityPolicy(t *testing.T) {
	policy := selectByPriorityPolicy(4)

	t.Run("select all", func(t *testing.T) {
		m := map[*stepStack]struct{}{
			{steps: []nestedStep{}}: {},
			{steps: []nestedStep{}}: {},
		}
		selected := policy(m)
		assert.Equal(t, 2, len(selected))
	})

	t.Run("select by priority", func(t *testing.T) {
		steps := []nestedStep{
			&releaseCollectionStep{},
			&releaseCollectionStep{},
			&releaseCollectionStep{},
			&releaseCollectionStep{},
			&releaseCollectionStep{},
		}
		s1 := &stepStack{steps: steps[0:1]}
		s2 := &stepStack{steps: steps[0:2]}
		s3 := &stepStack{steps: steps[0:3]}
		s4 := &stepStack{steps: steps[0:4]}
		s5 := &stepStack{steps: steps[0:5]}
		m := map[*stepStack]struct{}{
			s1: {},
			s2: {},
			s3: {},
			s4: {},
			s5: {},
		}
		selected := policy(m)
		assert.Equal(t, 4, len(selected))
		for i := 1; i < len(selected); i++ {
			assert.True(t, selected[i].totalPriority() <= selected[i-1].totalPriority())
		}
	})
}
