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

package concurrency

// Future is a result type of async-await style.
// It contains the result (or error) of an async task.
// Trying to obtain the result (or error) blocks until the async task completes.
type Future struct {
	ch    chan struct{}
	value interface{}
	err   error
}

func newFuture() *Future {
	return &Future{
		ch: make(chan struct{}),
	}
}

// Return the result and error of the async task.
func (future *Future) Await() (interface{}, error) {
	<-future.ch

	return future.value, future.err
}

// Return the result of the async task,
// nil if no result or error occurred.
func (future *Future) Value() interface{} {
	<-future.ch

	return future.value
}

// False if error occurred,
// true otherwise.
func (future *Future) OK() bool {
	<-future.ch

	return future.err == nil
}

// Return the error of the async task,
// nil if no error.
func (future *Future) Err() error {
	<-future.ch

	return future.err
}

// Return a read-only channel,
// which will be closed if the async task completes.
// Use this if you need to wait the async task in a select statement.
func (future *Future) Inner() <-chan struct{} {
	return future.ch
}

// Await for multiple futures,
// Return nil if no future returns error,
// or return the first error in these futures.
func AwaitAll(futures ...*Future) error {
	for i := range futures {
		if !futures[i].OK() {
			return futures[i].err
		}
	}

	return nil
}
