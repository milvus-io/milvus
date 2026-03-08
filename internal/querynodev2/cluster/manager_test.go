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

package cluster

import (
	context "context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	ctx := context.Background()
	t.Run("normal_get", func(t *testing.T) {
		worker := &MockWorker{}
		worker.EXPECT().IsHealthy().Return(true)
		var buildErr error
		var called int
		builder := func(_ context.Context, nodeID int64) (Worker, error) {
			called++
			return worker, buildErr
		}
		manager := NewWorkerManager(builder)

		w, err := manager.GetWorker(ctx, 0)
		assert.Equal(t, worker, w)
		assert.NoError(t, err)
		assert.Equal(t, 1, called)

		w, err = manager.GetWorker(ctx, 0)
		assert.Equal(t, worker, w)
		assert.NoError(t, err)
		assert.Equal(t, 1, called)
	})

	t.Run("builder_return_error", func(t *testing.T) {
		worker := &MockWorker{}
		worker.EXPECT().IsHealthy().Return(true)
		var buildErr error
		var called int
		buildErr = errors.New("mocked error")
		builder := func(_ context.Context, nodeID int64) (Worker, error) {
			called++
			return worker, buildErr
		}
		manager := NewWorkerManager(builder)

		_, err := manager.GetWorker(ctx, 0)
		assert.Error(t, err)
		assert.Equal(t, 1, called)
	})

	t.Run("worker_not_healthy", func(t *testing.T) {
		worker := &MockWorker{}
		worker.EXPECT().IsHealthy().Return(false)
		var buildErr error
		var called int
		builder := func(_ context.Context, nodeID int64) (Worker, error) {
			called++
			return worker, buildErr
		}
		manager := NewWorkerManager(builder)

		_, err := manager.GetWorker(ctx, 0)
		assert.Error(t, err)
		assert.Equal(t, 1, called)
	})
}
