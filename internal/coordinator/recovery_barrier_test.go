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

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoveryBarrier(t *testing.T) {
	barrier := newRecoveryBarrier()
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- barrier.Wait(context.Background())
	}()

	select {
	case <-waitDone:
		require.Fail(t, "recovery barrier became ready before recovery completed")
	case <-time.After(10 * time.Millisecond):
	}

	barrier.Ready()
	barrier.Ready()
	require.NoError(t, <-waitDone)
	require.NoError(t, barrier.Wait(context.Background()))
}

func TestRecoveryBarrierWaitCancellation(t *testing.T) {
	barrier := newRecoveryBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, barrier.Wait(ctx), context.Canceled)
}
