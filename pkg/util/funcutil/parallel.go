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

package funcutil

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// GetFunctionName returns the name of input
func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

type TaskFunc func() error

type ProcessFunc func(idx int) error

type DataProcessFunc func(data interface{}) error

// ProcessFuncParallel processes function in parallel.
//
// ProcessFuncParallel waits for all goroutines done if no errors occur.
// If some goroutines return error, ProcessFuncParallel cancels other goroutines as soon as possible and wait
// for all other goroutines done, and returns the first error occurs.
// Reference: https://stackoverflow.com/questions/40809504/idiomatic-goroutine-termination-and-error-handling
func ProcessFuncParallel(total, maxParallel int, f ProcessFunc, fname string) error {
	if maxParallel <= 0 {
		maxParallel = 1
	}

	t := time.Now()
	defer func() {
		log.Ctx(context.TODO()).Debug(fname, zap.Int("total", total), zap.Any("time cost", time.Since(t)))
	}()

	nPerBatch := (total + maxParallel - 1) / maxParallel

	quit := make(chan bool)
	errc := make(chan error)
	done := make(chan error)
	getMin := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}
	routineNum := 0
	var wg sync.WaitGroup
	for begin := 0; begin < total; begin = begin + nPerBatch {
		j := begin
		wg.Add(1)
		go func(begin int) {
			defer wg.Done()

			select {
			case <-quit:
				return
			default:
			}

			err := error(nil)

			end := getMin(total, begin+nPerBatch)
			for idx := begin; idx < end; idx++ {
				err = f(idx)
				if err != nil {
					log.Error(fname, zap.Error(err), zap.Int("idx", idx))
					break
				}
			}

			ch := done // send to done channel
			if err != nil {
				ch = errc // send to error channel
			}

			select {
			case ch <- err:
				return
			case <-quit:
				return
			}
		}(j)

		routineNum++
	}

	if routineNum <= 0 {
		return nil
	}

	count := 0
	for {
		select {
		case err := <-errc:
			close(quit)
			wg.Wait()
			return err
		case <-done:
			count++
			if count == routineNum {
				wg.Wait()
				return nil
			}
		}
	}
}

// ProcessTaskParallel processes tasks in parallel.
// Similar to ProcessFuncParallel
func ProcessTaskParallel(maxParallel int, fname string, tasks ...TaskFunc) error {
	// option := parallelProcessOption{}
	// for _, opt := range opts {
	// 	opt(&option)
	// }
	log := log.Ctx(context.TODO())

	if maxParallel <= 0 {
		maxParallel = 1
	}

	t := time.Now()
	defer func() {
		log.Debug(fname, zap.Any("time cost", time.Since(t)))
	}()

	total := len(tasks)
	nPerBatch := (total + maxParallel - 1) / maxParallel
	log.Debug(fname, zap.Int("total", total))
	log.Debug(fname, zap.Int("nPerBatch", nPerBatch))

	quit := make(chan bool)
	errc := make(chan error)
	done := make(chan error)
	getMin := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}
	routineNum := 0
	var wg sync.WaitGroup
	for begin := 0; begin < total; begin = begin + nPerBatch {
		j := begin

		// if option.preExecute != nil {
		// 	err := option.preExecute()
		// 	if err != nil {
		// 		close(quit)
		// 		wg.Wait()
		// 		return err
		// 	}
		// }

		wg.Add(1)
		go func(begin int) {
			defer wg.Done()

			select {
			case <-quit:
				return
			default:
			}

			err := error(nil)

			end := getMin(total, begin+nPerBatch)
			for idx := begin; idx < end; idx++ {
				err = tasks[idx]()
				if err != nil {
					log.Error(fname, zap.Error(err), zap.Int("idx", idx))
					break
				}
			}

			ch := done // send to done channel
			if err != nil {
				ch = errc // send to error channel
			}

			select {
			case ch <- err:
				return
			case <-quit:
				return
			}
		}(j)
		// if option.postExecute != nil {
		// 	option.postExecute()
		// }

		routineNum++
	}

	log.Debug(fname, zap.Int("NumOfGoRoutines", routineNum))

	if routineNum <= 0 {
		return nil
	}

	count := 0
	for {
		select {
		case err := <-errc:
			close(quit)
			wg.Wait()
			return err
		case <-done:
			count++
			if count == routineNum {
				wg.Wait()
				return nil
			}
		}
	}
}
