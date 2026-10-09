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

package walsummary

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestTerminalFailureNotifiesOwnerAndStopsObservation(t *testing.T) {
	for _, source := range []string{"chunk", "manifest", "same term", "generation"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			m, _ := newTestManagerWithStore(t)
			var reported []error
			m.cfg.OnFatal = func(err error) {
				// The owner may inspect the manager, but must not close it here.
				require.True(t, m.mu.TryLock())
				m.mu.Unlock()
				require.True(t, m.publishMu.TryLock())
				m.publishMu.Unlock()
				reported = append(reported, err)
			}
			m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1))
			before, err := m.ReadTransform(ctx, "v1", 0, 100, ReadLimits{})
			require.NoError(t, err)
			switch source {
			case "chunk":
				patch := mockey.Mock((*Store).WriteChunk).Return(nil, uint64(0), storeCorruptedf("chunk conflict")).Build()
				defer patch.UnPatch()
				m.RequestFlushThrough(100)
				require.ErrorIs(t, m.pendingSealed[0].task.Execute(ctx), ErrStoreCorrupted)
			case "manifest":
				m.RequestFlushThrough(100)
				require.NoError(t, m.pendingSealed[0].task.Execute(ctx))
				patch := mockey.Mock((*Store).WriteManifest).Return(storeCorruptedf("invalid manifest")).Build()
				defer patch.UnPatch()
				require.ErrorIs(t, m.manifestTask.Execute(ctx), ErrStoreCorrupted)
			case "same term":
				m.reopenedTerm = true
				m.RequestFlushThrough(100)
			case "generation":
				m.generationExhausted = true
				m.RequestFlushThrough(100)
			}
			require.Len(t, reported, 1)
			require.ErrorIs(t, reported[0], ErrStoreCorrupted)
			select {
			case <-before.Changed:
			default:
				t.Fatal("terminal failure must wake readers")
			}
			pending, bytes := len(m.pending), m.pendingBytes
			m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 200, 10, 2))
			m.ObserveMessage(ctx, newTestBarrierMessage(t, "v1", 300))
			m.RequestFlushThrough(300)
			require.Len(t, m.pending, pending)
			require.Equal(t, bytes, m.pendingBytes)
			require.Equal(t, uint64(100), m.lastObserved)
			after, err := m.ReadTransform(ctx, "v1", 100, 300, ReadLimits{})
			require.ErrorIs(t, err, ErrStoreCorrupted)
			require.Equal(t, before.ReadableThrough, after.ReadableThrough)
			require.Zero(t, m.LastAcked(), "failed persistence cannot confirm the staged data")
			require.ErrorIs(t, m.taskError(ctx, storeCorruptedf("another failed task")), ErrStoreCorrupted)
			require.Len(t, reported, 1, "only the first terminal error notifies the owner")
			require.Same(t, reported[0], m.terminalErr)
		})
	}
}

func TestTransientFailureAndCancellationDoNotNotifyOwner(t *testing.T) {
	m, _ := newTestManagerWithStore(t)
	m.cfg.OnFatal = func(error) { t.Fatal("transient errors and shutdown are not fatal") }
	requireSummaryError(t, m.taskError(context.Background(), context.DeadlineExceeded), nodescheduler.ErrDelay)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, m.taskError(ctx, storeCorruptedf("error during close")), context.Canceled)
	require.Nil(t, m.terminalErr)
}
