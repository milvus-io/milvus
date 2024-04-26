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

package conc

import "go.uber.org/atomic"

type future interface {
	wait()
	OK() bool
	Err() error
}

// Future is a result type of async-await style.
// It contains the result (or error) of an async task.
// Trying to obtain the result (or error) blocks until the async task completes.
type Future[T any] struct {
	ch    chan struct{}
	value T
	err   error
	done  *atomic.Bool
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{
		ch:   make(chan struct{}),
		done: atomic.NewBool(false),
	}
}

func (future *Future[T]) wait() {
	<-future.ch
}

// Return the result and error of the async task.
func (future *Future[T]) Await() (T, error) {
	future.wait()
	return future.value, future.err
}

// Return the result of the async task,
// nil if no result or error occurred.
func (future *Future[T]) Value() T {
	<-future.ch

	return future.value
}

// Done indicates if the fn has finished.
func (future *Future[T]) Done() bool {
	return future.done.Load()
}

// False if error occurred,
// true otherwise.
func (future *Future[T]) OK() bool {
	<-future.ch

	return future.err == nil
}

// Return the error of the async task,
// nil if no error.
func (future *Future[T]) Err() error {
	<-future.ch

	return future.err
}

// Return a read-only channel,
// which will be closed if the async task completes.
// Use this if you need to wait the async task in a select statement.
func (future *Future[T]) Inner() <-chan struct{} {
	return future.ch
}

// Go spawns a goroutine to execute fn,
// returns a future that contains the result of fn.
// NOTE: use Pool if you need limited goroutines.
func Go[T any](fn func() (T, error)) *Future[T] {
	future := newFuture[T]()
	go func() {
		future.value, future.err = fn()
		close(future.ch)
		future.done.Store(true)
	}()
	return future
}

// Await for multiple futures,
// Return nil if no future returns error,
// or return the first error in these futures.
func AwaitAll[T future](futures ...T) error {
	for i := range futures {
		if !futures[i].OK() {
			return futures[i].Err()
		}
	}

	return nil
}

// BlockOnAll blocks until all futures complete.
// Return the first error in these futures.
func BlockOnAll[T future](futures ...T) error {
	var err error
	for i := range futures {
		if e := futures[i].Err(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
