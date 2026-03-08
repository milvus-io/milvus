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

// package lifetime provides common component lifetime control logic.
package lifetime

import (
	"sync"
)

// Lifetime interface for lifetime control.
type Lifetime[T any] interface {
	SafeChan
	// SetState is the method to change lifetime state.
	SetState(state T)
	// GetState returns current state.
	GetState() T
	// Add records a task is running, returns false if the lifetime is not healthy.
	Add(isHealthy CheckHealth[T]) error
	// Done records a task is done.
	Done()
	// Wait waits until all tasks are done.
	Wait()
}

// CheckHealth function type for lifetime healthy check.
type CheckHealth[T any] func(T) error

var _ Lifetime[any] = (*lifetime[any])(nil)

// NewLifetime returns a new instance of Lifetime with init state and isHealthy logic.
func NewLifetime[T any](initState T) Lifetime[T] {
	return &lifetime[T]{
		safeChan: newSafeChan(),
		state:    initState,
	}
}

// lifetime implementation of Lifetime.
// users shall not care about the internal fields of this struct.
type lifetime[T any] struct {
	*safeChan
	// wg is used for keeping record each running task.
	wg sync.WaitGroup
	// state is the "atomic" value to store component state.
	state T
	// mut is the rwmutex to control each task and state change event.
	mut sync.RWMutex
	// isHealthy is the method to check whether is legal to add a task.
	isHealthy func(int32) error
}

// SetState is the method to change lifetime state.
func (l *lifetime[T]) SetState(state T) {
	l.mut.Lock()
	defer l.mut.Unlock()

	l.state = state
}

// GetState returns current state.
func (l *lifetime[T]) GetState() T {
	l.mut.RLock()
	defer l.mut.RUnlock()

	return l.state
}

// Add records a task is running, returns false if the lifetime is not healthy.
func (l *lifetime[T]) Add(checkHealth CheckHealth[T]) error {
	l.mut.RLock()
	defer l.mut.RUnlock()

	// check lifetime healthy
	if err := checkHealth(l.state); err != nil {
		return err
	}

	l.wg.Add(1)
	return nil
}

// Done records a task is done.
func (l *lifetime[T]) Done() {
	l.wg.Done()
}

// Wait waits until all tasks are done.
func (l *lifetime[T]) Wait() {
	l.wg.Wait()
}
