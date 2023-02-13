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

package util

import (
	"sync"
	"testing"
)

func BenchmarkAtomic(b *testing.B) {
	var s Value[string]
	s.Store("foo")
	for i := 0; i < b.N; i++ {
		w := sync.WaitGroup{}
		w.Add(10)
		for j := 0; j < 10; j++ {
			go func() {
				s.Load()
				w.Done()
			}()
		}
		w.Wait()
	}
}

func BenchmarkChan(b *testing.B) {
	s := make(chan string)
	close(s)
	for i := 0; i < b.N; i++ {
		w := sync.WaitGroup{}
		w.Add(10)
		for j := 0; j < 10; j++ {
			go func() {
				<-s
				w.Done()
			}()
		}
		w.Wait()
	}
}
