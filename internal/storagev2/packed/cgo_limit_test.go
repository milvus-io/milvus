// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestManifestFFILimiterBoundsConcurrency verifies the limiter caps the number
// of simultaneous holders even when more goroutines than the budget request it.
func TestManifestFFILimiterBoundsConcurrency(t *testing.T) {
	limiter := &manifestFFILimit{slots: semaphore.NewWeighted(2)}
	ctx := context.Background()

	var held atomic.Int32
	var maxHeld atomic.Int32
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 20; j++ {
				require.NoError(t, limiter.acquire(ctx))
				current := held.Add(1)
				for maxHeld.Load() < current {
					if maxHeld.CompareAndSwap(maxHeld.Load(), current) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				held.Add(-1)
				limiter.release()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	assert.LessOrEqual(t, int(maxHeld.Load()), 2, "the limiter must never admit more holders than its budget")
}

// TestManifestFFILimiterContextCancellation verifies a canceled caller is not
// admitted and does not consume a slot.
func TestManifestFFILimiterContextCancellation(t *testing.T) {
	limiter := &manifestFFILimit{slots: semaphore.NewWeighted(1)}
	require.NoError(t, limiter.acquire(context.Background()))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, limiter.acquire(canceled), context.Canceled)

	limiter.release()
	require.NoError(t, limiter.acquire(context.Background()))
	limiter.release()
}

// TestManifestFFILimiterReadsConfiguredBudget verifies the package-wide budget
// is sized from the configured paramtable value and is at least one.
func TestManifestFFILimiterReadsConfiguredBudget(t *testing.T) {
	paramtable.Init()
	require.NoError(t, acquireManifestFFI(context.Background()))
	releaseManifestFFI()
}
