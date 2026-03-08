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

package log

import (
	"fmt"
	"testing"

	"go.uber.org/zap"
)

type foo struct {
	Key   string
	Value string
}

func BenchmarkZapReflect(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		With(zap.Any("payload", payload))
	}
}

func BenchmarkZapWithLazy(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		L().WithLazy(zap.Any("payload", payload))
	}
}

// The following two benchmarks are validations if `WithLazy` has the same performance as `With` in the worst case.
func BenchmarkWithLazyLog(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		log := L().WithLazy(zap.Any("payload", payload))
		log.Info("test")
		log.Warn("test")
	}
}

func BenchmarkWithLog(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		log := L().With(zap.Any("payload", payload))
		log.Info("test")
		log.Warn("test")
	}
}
