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

package taskmodel

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestTaskCondition_OnWaitError(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		condition := NewTaskCondition(context.Background())
		var calls atomic.Int32
		condition.SetOnWaitError(func() {
			calls.Add(1)
		})

		condition.Notify(nil)
		assert.NoError(t, condition.WaitToFinish())
		assert.Zero(t, calls.Load())
	})

	t.Run("task error", func(t *testing.T) {
		condition := NewTaskCondition(context.Background())
		var calls atomic.Int32
		condition.SetOnWaitError(func() {
			calls.Add(1)
		})

		condition.Notify(errors.New("task failed"))
		assert.Error(t, condition.WaitToFinish())
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		condition := NewTaskCondition(ctx)
		var calls atomic.Int32
		condition.SetOnWaitError(func() {
			calls.Add(1)
		})

		cancel()
		assert.Error(t, condition.WaitToFinish())
		assert.Equal(t, int32(1), calls.Load())
	})
}
