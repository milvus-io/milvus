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

package rootcoord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBeginTransferProtectedCollectionOperationRejectsTransferredCollection(t *testing.T) {
	core := newTestCore()
	require.NoError(t, core.transferGate.Freeze(100, "transfer-1", 10))
	require.NoError(t, core.transferGate.Deactivate(100, "transfer-1", 10))

	done, err := core.beginTransferProtectedCollectionOperation(100)
	require.Nil(t, done)
	require.ErrorIs(t, err, errCollectionTransferredOut)
}

func TestBeginTransferProtectedCollectionOperationDrainsPrepare(t *testing.T) {
	core := newTestCore()
	done, err := core.beginTransferProtectedCollectionOperation(100)
	require.NoError(t, err)
	require.NotNil(t, done)

	prepared := make(chan error, 1)
	go func() {
		prepared <- core.transferGate.FreezeWithDrain(100, "transfer-1", 10, time.Second)
	}()

	select {
	case err := <-prepared:
		require.Failf(t, "prepare returned before protected operation completed", "err: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	done()
	require.NoError(t, <-prepared)

	nextDone, err := core.beginTransferProtectedCollectionOperation(100)
	require.Nil(t, nextDone)
	require.ErrorIs(t, err, errCollectionTransferring)
}

func TestWithTransferProtectedCollectionOperationRunsDoneOnCallbackError(t *testing.T) {
	core := newTestCore()

	err := core.withTransferProtectedCollectionOperation(100, func() error {
		return assertTransferGuardErr("callback failed")
	})
	require.ErrorContains(t, err, "callback failed")

	require.NoError(t, core.transferGate.FreezeWithDrain(100, "transfer-1", 10, time.Millisecond))
}

func TestWithTransferProtectedCollectionOperationSkipsInvalidCollectionID(t *testing.T) {
	core := newTestCore()

	err := core.withTransferProtectedCollectionOperation(0, func() error {
		return nil
	})
	require.NoError(t, err)
}

type assertTransferGuardErr string

func (e assertTransferGuardErr) Error() string { return string(e) }
