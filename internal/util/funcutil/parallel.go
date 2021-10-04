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

package funcutil

import (
	"reflect"
	"runtime"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

// GetFunctionName returns the name of input
func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

// ProcessFuncParallel process function in parallel.
// Reference: https://stackoverflow.com/questions/40809504/idiomatic-goroutine-termination-and-error-handling
func ProcessFuncParallel(total, maxParallel int, f func(idx int) error, fname string) error {
	if maxParallel <= 0 {
		maxParallel = 1
	}

	t := time.Now()
	defer func() {
		log.Debug(fname, zap.Any("time cost", time.Since(t)))
	}()

	nPerBatch := (total + maxParallel - 1) / maxParallel
	log.Debug(fname, zap.Any("total", total))
	log.Debug(fname, zap.Any("nPerBatch", nPerBatch))

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
	for begin := 0; begin < total; begin = begin + nPerBatch {
		j := begin

		go func(begin int) {
			err := error(nil)

			end := getMin(total, begin+nPerBatch)
			for idx := begin; idx < end; idx++ {
				err = f(idx)
				if err != nil {
					log.Debug(fname, zap.Error(err), zap.Any("idx", idx))
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

	log.Debug(fname, zap.Any("NumOfGoRoutines", routineNum))

	if routineNum <= 0 {
		return nil
	}

	count := 0
	for {
		select {
		case err := <-errc:
			close(quit)
			return err
		case <-done:
			count++
			if count == routineNum {
				return nil
			}
		}
	}
}
