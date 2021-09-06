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

package proxy

import "sync"

type (
	uniqueIntGenerator interface {
		get() int
	}
	naiveUniqueIntGenerator struct {
		now int
		mtx sync.Mutex
	}
)

func (generator *naiveUniqueIntGenerator) get() int {
	generator.mtx.Lock()
	defer func() {
		generator.now++
		generator.mtx.Unlock()
	}()
	return generator.now
}

func newNaiveUniqueIntGenerator() *naiveUniqueIntGenerator {
	return &naiveUniqueIntGenerator{
		now: 0,
	}
}

var uniqueIntGeneratorIns uniqueIntGenerator
var getUniqueIntGeneratorInsOnce sync.Once

func getUniqueIntGeneratorIns() uniqueIntGenerator {
	getUniqueIntGeneratorInsOnce.Do(func() {
		uniqueIntGeneratorIns = newNaiveUniqueIntGenerator()
	})
	return uniqueIntGeneratorIns
}
