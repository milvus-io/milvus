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

package schemaevolution

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGateManagerCloseDrainsExistingLease(t *testing.T) {
	manager := NewGateManager()
	release, err := manager.Acquire(context.Background(), 100)
	require.NoError(t, err)

	manager.Close(100)
	err = manager.Check(100)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)
	assert.True(t, merr.Status(err).GetRetriable())

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = manager.WaitIdle(waitCtx, 100)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	release()
	require.NoError(t, manager.WaitIdle(context.Background(), 100))
	assert.True(t, manager.IsClosed(100))

	manager.Open(100)
	release, err = manager.Acquire(context.Background(), 100)
	require.NoError(t, err)
	release()
}

func TestGateManagerAcquireOrCloseIsAtomic(t *testing.T) {
	for i := 0; i < 100; i++ {
		manager := NewGateManager()
		start := make(chan struct{})
		result := make(chan error, 1)
		releases := make(chan func(), 1)

		go func() {
			<-start
			release, err := manager.Acquire(context.Background(), 100)
			if err == nil {
				releases <- release
			}
			result <- err
		}()
		close(start)
		manager.Close(100)

		err := <-result
		if err == nil {
			release := <-releases
			assert.Equal(t, 1, manager.Active(100))
			release()
		} else {
			assert.ErrorIs(t, err, merr.ErrServiceNotReady)
			assert.Equal(t, 0, manager.Active(100))
		}
		require.NoError(t, manager.WaitIdle(context.Background(), 100))
	}
}

func TestAdmissionBypassDoesNotCreateLease(t *testing.T) {
	manager := NewGateManager()
	manager.Close(100)

	release, err := manager.Acquire(WithAdmissionBypass(context.Background()), 100)
	require.NoError(t, err)
	release()
	assert.Equal(t, 0, manager.Active(100))
	assert.True(t, manager.IsClosed(100))
}
