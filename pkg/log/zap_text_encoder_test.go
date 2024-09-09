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
	key   string
	value string
}

func BenchmarkZapReflect(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{key: fmt.Sprintf("key%d", i), value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		With(zap.Any("payload", payload))
	}
}

func BenchmarkZapWithLazy(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{key: fmt.Sprintf("key%d", i), value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		WithLazy(zap.Any("payload", payload))
	}
}
