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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestHandleRetryImmediately(t *testing.T) {
	t.Run("deadline and attempt budget", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		calls, refreshes := 0, 0
		err := Handle(ctx, func() (bool, error) {
			calls++
			return true, merr.ErrServiceNotReady
		}, Attempts(3), Sleep(2*time.Second), RetryImmediately(func(error) bool { refreshes++; return true }))
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.Equal(t, 3, calls)
		require.Equal(t, 2, refreshes)
	})
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		err := Handle(ctx, func() (bool, error) {
			calls++
			return true, merr.ErrServiceNotReady
		}, AttemptAlways(), RetryImmediately(func(error) bool { cancel(); return true }))
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.Equal(t, 1, calls)
	})
	t.Run("retry predicate", func(t *testing.T) {
		err := Handle(context.Background(), func() (bool, error) {
			return true, merr.ErrServiceNotReady
		}, RetryErr(func(error) bool { return false }), RetryImmediately(func(error) bool {
			t.Fatal("refresh must not bypass retry eligibility")
			return true
		}))
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	})
	t.Run("unchanged backoff after one immediate retry", func(t *testing.T) {
		var calls []time.Time
		err := Handle(context.Background(), func() (bool, error) {
			calls = append(calls, time.Now())
			return true, merr.ErrServiceNotReady
		}, Attempts(3), Sleep(50*time.Millisecond), RetryImmediately(func(error) bool { return len(calls) == 1 }))
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.Len(t, calls, 3)
		require.Less(t, calls[1].Sub(calls[0]), 50*time.Millisecond)
		require.GreaterOrEqual(t, calls[2].Sub(calls[1]), 50*time.Millisecond)
	})
}
