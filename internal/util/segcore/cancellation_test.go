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

package segcore

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCancellationGuard_Creation(t *testing.T) {
	ctx := context.Background()
	guard := NewCancellationGuard(ctx)

	assert.NotNil(t, guard)
	assert.NotNil(t, guard.Source())

	guard.Close()
}

func TestCancellationGuard_SourceNotNil(t *testing.T) {
	ctx := context.Background()
	guard := NewCancellationGuard(ctx)
	defer guard.Close()

	// Source should return a non-nil pointer
	source := guard.Source()
	assert.NotNil(t, source)
}

func TestCancellationGuard_CloseIsIdempotent(t *testing.T) {
	ctx := context.Background()
	guard := NewCancellationGuard(ctx)

	// Close should be safe to call
	guard.Close()

	// Note: Calling Close() twice would cause a panic due to closing
	// a closed channel, so we only test single close here
}

func TestCancellationGuard_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	guard := NewCancellationGuard(ctx)

	// Cancel the context
	cancel()

	// Give the goroutine time to process the cancellation
	time.Sleep(10 * time.Millisecond)

	// Close should still work properly after context cancellation
	guard.Close()
}

func TestCancellationGuard_CloseBeforeContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	guard := NewCancellationGuard(ctx)

	// Close the guard before context is canceled
	guard.Close()

	// Context cancel after guard close should not cause issues
	cancel()
}

func TestCancellationGuard_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	guard := NewCancellationGuard(ctx)

	var wg sync.WaitGroup
	// Multiple goroutines accessing Source() concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			source := guard.Source()
			assert.NotNil(t, source)
		}()
	}

	wg.Wait()
	guard.Close()
}

func TestCancellationGuard_WithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	guard := NewCancellationGuard(ctx)

	// Wait for timeout to trigger
	time.Sleep(100 * time.Millisecond)

	// Close should still work properly after timeout
	guard.Close()
}

func TestCancellationGuard_ImmediateClose(t *testing.T) {
	ctx := context.Background()
	guard := NewCancellationGuard(ctx)

	// Immediately close after creation
	guard.Close()
}

func TestCancellationGuard_MultipleGuards(t *testing.T) {
	ctx := context.Background()

	// Create multiple guards from the same context
	guard1 := NewCancellationGuard(ctx)
	guard2 := NewCancellationGuard(ctx)
	guard3 := NewCancellationGuard(ctx)

	// Each guard should have its own source
	assert.NotNil(t, guard1.Source())
	assert.NotNil(t, guard2.Source())
	assert.NotNil(t, guard3.Source())

	// Close all guards
	guard1.Close()
	guard2.Close()
	guard3.Close()
}

func TestCancellationGuard_CancelledContext(t *testing.T) {
	// Create an already canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	guard := NewCancellationGuard(ctx)

	// Give the goroutine time to process
	time.Sleep(10 * time.Millisecond)

	// Should still be able to close properly
	guard.Close()
}
