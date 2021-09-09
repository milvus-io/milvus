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

package uniquegenerator

import "sync"

type (
	UniqueIntGenerator interface {
		GetInt() int
		GetInts(count int) (int, int)
	}
	NaiveUniqueIntGenerator struct {
		now int
		mtx sync.Mutex
	}
)

func (generator *NaiveUniqueIntGenerator) GetInts(count int) (int, int) {
	generator.mtx.Lock()
	defer func() {
		generator.now += count
		generator.mtx.Unlock()
	}()
	return generator.now, generator.now + count
}

func (generator *NaiveUniqueIntGenerator) GetInt() int {
	begin, _ := generator.GetInts(1)
	return begin
}

func NewNaiveUniqueIntGenerator() *NaiveUniqueIntGenerator {
	return &NaiveUniqueIntGenerator{
		now: 0,
	}
}

var uniqueIntGeneratorIns UniqueIntGenerator
var getUniqueIntGeneratorInsOnce sync.Once

func GetUniqueIntGeneratorIns() UniqueIntGenerator {
	getUniqueIntGeneratorInsOnce.Do(func() {
		uniqueIntGeneratorIns = NewNaiveUniqueIntGenerator()
	})
	return uniqueIntGeneratorIns
}
