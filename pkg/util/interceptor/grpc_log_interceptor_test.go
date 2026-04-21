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

package interceptor

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/stretchr/testify/assert"
)

// splitFullMethod turns "/service/Method" into ("service", "Method").
func splitFullMethod(fullMethod string) (service, method string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	parts := strings.SplitN(fullMethod, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return fullMethod, ""
}

func callMetaFor(fullMethod string) interceptors.CallMeta {
	service, method := splitFullMethod(fullMethod)
	return interceptors.CallMeta{Service: service, Method: method}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected logging.Level
	}{
		{"debug", logging.LevelDebug},
		{"DEBUG", logging.LevelDebug},
		{"info", logging.LevelInfo},
		{"INFO", logging.LevelInfo},
		{"warn", logging.LevelWarn},
		{"warning", logging.LevelWarn},
		{"error", logging.LevelError},
		{"unknown", logging.LevelInfo},
		{"", logging.LevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, parseLogLevel(tt.input))
		})
	}
}

func TestParseMethodList(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]struct{}
	}{
		{"empty string", "", nil},
		{"whitespace only", "   ", nil},
		{"single method", "/service/Method", map[string]struct{}{"/service/Method": {}}},
		{"multiple methods", "/service/M1,/service/M2", map[string]struct{}{"/service/M1": {}, "/service/M2": {}}},
		{"trimmed whitespace", " /service/M1 , /service/M2 ", map[string]struct{}{"/service/M1": {}, "/service/M2": {}}},
		{"empty parts skipped", "/service/M1,,/service/M2,", map[string]struct{}{"/service/M1": {}, "/service/M2": {}}},
		{"only separators", " , , , ", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parseMethodList(tt.input))
		})
	}
}

func TestDynamicMethodMatcher_Match(t *testing.T) {
	t.Run("nil method set matches nothing", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		var nilSet map[string]struct{}
		m.methodSet.Store(&nilSet)
		assert.False(t, m.Match(context.Background(), callMetaFor("/any/Method")))
	})

	t.Run("empty method set matches nothing", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		empty := make(map[string]struct{})
		m.methodSet.Store(&empty)
		assert.False(t, m.Match(context.Background(), callMetaFor("/any/Method")))
	})

	t.Run("listed methods match, others do not", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		set := map[string]struct{}{"/service/M1": {}, "/service/M2": {}}
		m.methodSet.Store(&set)
		assert.True(t, m.Match(context.Background(), callMetaFor("/service/M1")))
		assert.True(t, m.Match(context.Background(), callMetaFor("/service/M2")))
		assert.False(t, m.Match(context.Background(), callMetaFor("/service/M3")))
	})

	t.Run("concurrent readers do not race", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		set := map[string]struct{}{"/service/M1": {}}
		m.methodSet.Store(&set)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1000; j++ {
					m.Match(context.Background(), callMetaFor("/service/M1"))
				}
			}()
		}
		wg.Wait()
	})
}

func TestDynamicLevelLogger_Log(t *testing.T) {
	t.Run("logging at or above threshold is accepted", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelInfo)
		l.Log(context.Background(), logging.LevelInfo, "info", "k", "v")
		l.Log(context.Background(), logging.LevelWarn, "warn", "k", "v")
		l.Log(context.Background(), logging.LevelError, "error", "k", "v")
	})

	t.Run("logging below threshold is dropped", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelWarn)
		l.Log(context.Background(), logging.LevelDebug, "debug", "k", "v")
		l.Log(context.Background(), logging.LevelInfo, "info", "k", "v")
	})

	t.Run("heterogeneous field types are coerced via zap", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelDebug)
		l.Log(context.Background(), logging.LevelInfo, "msg",
			"string", "s",
			"int", 42,
			"bool", true,
			"any", struct{ Name string }{"n"},
		)
	})

	t.Run("concurrent loggers do not race", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelInfo)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					l.Log(context.Background(), logging.LevelInfo, "msg")
				}
			}()
		}
		wg.Wait()
	})
}

func TestNewLogInterceptors(t *testing.T) {
	t.Run("NewLogUnaryServerInterceptor", func(t *testing.T) {
		assert.NotNil(t, NewLogUnaryServerInterceptor())
	})
	t.Run("NewLogStreamServerInterceptor", func(t *testing.T) {
		assert.NotNil(t, NewLogStreamServerInterceptor())
	})
	t.Run("NewLogClientUnaryInterceptor", func(t *testing.T) {
		assert.NotNil(t, NewLogClientUnaryInterceptor())
	})
	t.Run("NewLogClientStreamInterceptor", func(t *testing.T) {
		assert.NotNil(t, NewLogClientStreamInterceptor())
	})
}

func TestDynamicMethodMatcherUpdate(t *testing.T) {
	m := &dynamicMethodMatcher{}
	empty := make(map[string]struct{})
	m.methodSet.Store(&empty)
	assert.False(t, m.Match(context.Background(), callMetaFor("/service/M1")))

	next := map[string]struct{}{"/service/M1": {}}
	m.methodSet.Store(&next)
	assert.True(t, m.Match(context.Background(), callMetaFor("/service/M1")))
}

func TestDynamicLevelLoggerUpdate(t *testing.T) {
	l := &dynamicLevelLogger{}
	l.level.Store(logging.LevelWarn)
	l.Log(context.Background(), logging.LevelInfo, "info msg")
	l.level.Store(logging.LevelDebug)
	l.Log(context.Background(), logging.LevelInfo, "info msg")
}

func TestNewDynamicMethodMatcher(t *testing.T) {
	t.Run("seeds matcher from initial value", func(t *testing.T) {
		m := newDynamicMethodMatcher("test.key.a", "/service/M1,/service/M2")
		assert.True(t, m.Match(context.Background(), callMetaFor("/service/M1")))
		assert.True(t, m.Match(context.Background(), callMetaFor("/service/M2")))
		assert.False(t, m.Match(context.Background(), callMetaFor("/service/M3")))
	})

	t.Run("empty initial value matches nothing", func(t *testing.T) {
		m := newDynamicMethodMatcher("test.key.b", "")
		assert.False(t, m.Match(context.Background(), callMetaFor("/service/M1")))
	})
}

func TestNewDynamicLevelLogger(t *testing.T) {
	t.Run("seeds level from initial value (debug)", func(t *testing.T) {
		l := newDynamicLevelLogger("test.key.c", "debug")
		l.Log(context.Background(), logging.LevelDebug, "debug msg")
	})

	t.Run("seeds level from initial value (error)", func(t *testing.T) {
		l := newDynamicLevelLogger("test.key.d", "error")
		l.Log(context.Background(), logging.LevelDebug, "dropped")
		l.Log(context.Background(), logging.LevelInfo, "dropped")
		l.Log(context.Background(), logging.LevelError, "passed")
	})

	t.Run("unknown seed falls back to info", func(t *testing.T) {
		l := newDynamicLevelLogger("test.key.e", "unknown")
		l.Log(context.Background(), logging.LevelDebug, "dropped")
		l.Log(context.Background(), logging.LevelInfo, "passed")
	})
}
