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

package retry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMaxAttemptsFromContext(t *testing.T) {
	// No value set — returns 0, false
	ctx := context.Background()
	v, ok := MaxAttemptsFromContext(ctx)
	assert.False(t, ok)
	assert.Equal(t, uint(0), v)

	// Value set — returns it
	ctx = WithMaxAttemptsContext(ctx, 3)
	v, ok = MaxAttemptsFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, uint(3), v)

	// Zero is a valid explicit value
	ctx = WithMaxAttemptsContext(context.Background(), 0)
	v, ok = MaxAttemptsFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, uint(0), v)
}

func TestMaxAttemptsFromContextOrDefault(t *testing.T) {
	// No value in ctx — returns default
	ctx := context.Background()
	assert.Equal(t, uint(10), MaxAttemptsFromContextOrDefault(ctx, 10))

	// Value in ctx — returns ctx value, ignoring default
	ctx = WithMaxAttemptsContext(ctx, 3)
	assert.Equal(t, uint(3), MaxAttemptsFromContextOrDefault(ctx, 10))

	// Zero in ctx — returns 0, not default
	ctx = WithMaxAttemptsContext(context.Background(), 0)
	assert.Equal(t, uint(0), MaxAttemptsFromContextOrDefault(ctx, 10))
}

// TestSleepAndMaxSleepTimeOrder pins the order-sensitive interaction between Sleep
// and MaxSleepTime that the import write-retry path (internal/datanode/importv2)
// depends on: it applies Sleep(initial) before MaxSleepTime(max).
func TestSleepAndMaxSleepTimeOrder(t *testing.T) {
	effective := func(initial, max time.Duration) *config {
		c := newDefaultConfig()
		for _, opt := range []Option{Attempts(0), Sleep(initial), MaxSleepTime(max)} {
			opt(c)
		}
		return c
	}

	// The shipped import defaults produce exactly the advertised 1s -> 60s schedule.
	c := effective(time.Second, 60*time.Second)
	assert.Equal(t, time.Second, c.sleep)
	assert.Equal(t, 60*time.Second, c.maxSleepTime)

	// A zero initial interval is never clamped by either option, so it would yield a
	// zero-delay retry loop -- callers must reject it before building the options.
	c = effective(0, 60*time.Second)
	assert.Equal(t, time.Duration(0), c.sleep)

	// Sleep runs first and raises maxSleepTime to 2*initial, so a configured max
	// below that floor is not a hard cap.
	c = effective(60*time.Second, 60*time.Second)
	assert.Equal(t, 120*time.Second, c.maxSleepTime)
}
