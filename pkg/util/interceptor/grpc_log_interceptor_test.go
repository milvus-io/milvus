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

// splitFullMethod splits a full method like "/service/Method" into service and method parts
func splitFullMethod(fullMethod string) (service, method string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	parts := strings.SplitN(fullMethod, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return fullMethod, ""
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
		{"WARN", logging.LevelWarn},
		{"warning", logging.LevelWarn},
		{"WARNING", logging.LevelWarn},
		{"error", logging.LevelError},
		{"ERROR", logging.LevelError},
		{"unknown", logging.LevelInfo}, // default to info
		{"", logging.LevelInfo},        // default to info
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseLogLevel(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseMethodList(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]struct{}
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: nil,
		},
		{
			name:  "single method",
			input: "/service/Method",
			expected: map[string]struct{}{
				"/service/Method": {},
			},
		},
		{
			name:  "multiple methods",
			input: "/service/Method1,/service/Method2",
			expected: map[string]struct{}{
				"/service/Method1": {},
				"/service/Method2": {},
			},
		},
		{
			name:  "methods with spaces",
			input: " /service/Method1 , /service/Method2 ",
			expected: map[string]struct{}{
				"/service/Method1": {},
				"/service/Method2": {},
			},
		},
		{
			name:  "methods with empty parts",
			input: "/service/Method1,,/service/Method2,",
			expected: map[string]struct{}{
				"/service/Method1": {},
				"/service/Method2": {},
			},
		},
		{
			name:     "only commas and whitespace",
			input:    " , , , ",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseMethodList(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDynamicMethodMatcher_Match(t *testing.T) {
	t.Run("nil method set matches none", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		// Store nil
		var nilSet map[string]struct{}
		m.methodSet.Store(&nilSet)

		service, method := splitFullMethod("/any/Method")
		callMeta := interceptors.CallMeta{Service: service, Method: method}
		assert.False(t, m.Match(context.Background(), callMeta))
	})

	t.Run("empty method set matches none", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		emptySet := make(map[string]struct{})
		m.methodSet.Store(&emptySet)

		service, method := splitFullMethod("/any/Method")
		callMeta := interceptors.CallMeta{Service: service, Method: method}
		assert.False(t, m.Match(context.Background(), callMeta))
	})

	t.Run("method in set matches", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		methodSet := map[string]struct{}{
			"/service/Method1": {},
			"/service/Method2": {},
		}
		m.methodSet.Store(&methodSet)

		service1, method1 := splitFullMethod("/service/Method1")
		callMeta1 := interceptors.CallMeta{Service: service1, Method: method1}
		assert.True(t, m.Match(context.Background(), callMeta1))

		service2, method2 := splitFullMethod("/service/Method2")
		callMeta2 := interceptors.CallMeta{Service: service2, Method: method2}
		assert.True(t, m.Match(context.Background(), callMeta2))
	})

	t.Run("method not in set does not match", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		methodSet := map[string]struct{}{
			"/service/Method1": {},
			"/service/Method2": {},
		}
		m.methodSet.Store(&methodSet)

		service, method := splitFullMethod("/service/Method3")
		callMeta := interceptors.CallMeta{Service: service, Method: method}
		assert.False(t, m.Match(context.Background(), callMeta))
	})

	t.Run("concurrent read access", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		methodSet := map[string]struct{}{
			"/service/Method1": {},
		}
		m.methodSet.Store(&methodSet)

		var wg sync.WaitGroup
		iterations := 1000

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					service, method := splitFullMethod("/service/Method1")
					callMeta := interceptors.CallMeta{Service: service, Method: method}
					m.Match(context.Background(), callMeta)
				}
			}()
		}

		wg.Wait()
	})
}

func TestDynamicLevelLogger_Log(t *testing.T) {
	t.Run("log at info level with info threshold", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelInfo)

		// Should not panic when logging at or above threshold
		l.Log(context.Background(), logging.LevelInfo, "test message", "key", "value")
		l.Log(context.Background(), logging.LevelWarn, "test warn", "key", "value")
		l.Log(context.Background(), logging.LevelError, "test error", "key", "value")
	})

	t.Run("log below threshold is skipped", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelWarn)

		// Debug and Info should be skipped (no panic, no output)
		l.Log(context.Background(), logging.LevelDebug, "debug msg", "key", "value")
		l.Log(context.Background(), logging.LevelInfo, "info msg", "key", "value")
	})

	t.Run("log with different value types", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelDebug)

		l.Log(context.Background(), logging.LevelInfo, "test",
			"string", "value",
			"int", 42,
			"bool", true,
			"any", struct{ Name string }{"test"},
		)
	})

	t.Run("concurrent read access", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelInfo)

		var wg sync.WaitGroup
		iterations := 100

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					l.Log(context.Background(), logging.LevelInfo, "test")
				}
			}()
		}

		wg.Wait()
	})
}

func TestNewLogInterceptors(t *testing.T) {
	// These tests verify that the interceptors can be created without panic
	t.Run("NewLogUnaryServerInterceptor", func(t *testing.T) {
		interceptor := NewLogUnaryServerInterceptor()
		assert.NotNil(t, interceptor)
	})

	t.Run("NewLogStreamServerInterceptor", func(t *testing.T) {
		interceptor := NewLogStreamServerInterceptor()
		assert.NotNil(t, interceptor)
	})

	t.Run("NewLogClientUnaryInterceptor", func(t *testing.T) {
		interceptor := NewLogClientUnaryInterceptor()
		assert.NotNil(t, interceptor)
	})

	t.Run("NewLogClientStreamInterceptor", func(t *testing.T) {
		interceptor := NewLogClientStreamInterceptor()
		assert.NotNil(t, interceptor)
	})
}

func TestDynamicLevelLoggerAllLevels(t *testing.T) {
	t.Run("log at debug level", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelDebug)
		// Should not panic
		l.Log(context.Background(), logging.LevelDebug, "debug msg", "key", "value")
	})

	t.Run("log at error level", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelDebug)
		// Should not panic
		l.Log(context.Background(), logging.LevelError, "error msg", "key", "value")
	})

	t.Run("log with warn level", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelDebug)
		// Should not panic
		l.Log(context.Background(), logging.LevelWarn, "warn msg", "key", "value")
	})
}

func TestDynamicMethodMatcherUpdate(t *testing.T) {
	t.Run("update method set", func(t *testing.T) {
		m := &dynamicMethodMatcher{}
		// Initial empty set
		emptySet := make(map[string]struct{})
		m.methodSet.Store(&emptySet)

		service, method := splitFullMethod("/service/Method1")
		callMeta := interceptors.CallMeta{Service: service, Method: method}
		assert.False(t, m.Match(context.Background(), callMeta))

		// Update method set
		newSet := map[string]struct{}{
			"/service/Method1": {},
		}
		m.methodSet.Store(&newSet)
		assert.True(t, m.Match(context.Background(), callMeta))
	})
}

func TestDynamicLevelLoggerUpdate(t *testing.T) {
	t.Run("update log level", func(t *testing.T) {
		l := &dynamicLevelLogger{}
		l.level.Store(logging.LevelWarn)

		// Info should be skipped at warn level
		// (no panic, returns early)
		l.Log(context.Background(), logging.LevelInfo, "info msg")

		// Update to debug level
		l.level.Store(logging.LevelDebug)

		// Now info should be logged (no panic)
		l.Log(context.Background(), logging.LevelInfo, "info msg")
	})
}

func TestNewDynamicMethodMatcher(t *testing.T) {
	t.Run("initializes with method list", func(t *testing.T) {
		m := newDynamicMethodMatcher("test.key", "/service/Method1,/service/Method2")

		service1, method1 := splitFullMethod("/service/Method1")
		callMeta1 := interceptors.CallMeta{Service: service1, Method: method1}
		assert.True(t, m.Match(context.Background(), callMeta1))

		service2, method2 := splitFullMethod("/service/Method2")
		callMeta2 := interceptors.CallMeta{Service: service2, Method: method2}
		assert.True(t, m.Match(context.Background(), callMeta2))

		service3, method3 := splitFullMethod("/service/Method3")
		callMeta3 := interceptors.CallMeta{Service: service3, Method: method3}
		assert.False(t, m.Match(context.Background(), callMeta3))
	})

	t.Run("initializes with empty method list", func(t *testing.T) {
		m := newDynamicMethodMatcher("test.key", "")

		service, method := splitFullMethod("/service/Method1")
		callMeta := interceptors.CallMeta{Service: service, Method: method}
		assert.False(t, m.Match(context.Background(), callMeta))
	})
}

func TestNewDynamicLevelLogger(t *testing.T) {
	t.Run("initializes with debug level", func(t *testing.T) {
		l := newDynamicLevelLogger("test.key", "debug")
		// Should be able to log at debug level
		l.Log(context.Background(), logging.LevelDebug, "test debug")
	})

	t.Run("initializes with error level", func(t *testing.T) {
		l := newDynamicLevelLogger("test.key", "error")
		// Debug and Info should be skipped
		l.Log(context.Background(), logging.LevelDebug, "test debug")
		l.Log(context.Background(), logging.LevelInfo, "test info")
		// Error should pass
		l.Log(context.Background(), logging.LevelError, "test error")
	})

	t.Run("initializes with unknown level defaults to info", func(t *testing.T) {
		l := newDynamicLevelLogger("test.key", "unknown")
		// Debug should be skipped at info level
		l.Log(context.Background(), logging.LevelDebug, "test debug")
		// Info and above should pass
		l.Log(context.Background(), logging.LevelInfo, "test info")
	})
}
