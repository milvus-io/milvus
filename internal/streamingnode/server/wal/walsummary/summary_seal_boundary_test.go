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

	"github.com/stretchr/testify/require"
)

// TestLastAckedIsHeldByAStagedRecordWhateverStagedIt pins the discriminator
// behind the shard split stall (e2e F-r5-1): what decides whether a pchannel's
// published checkpoint keeps advancing is not the KIND of traffic, it is
// whether anything is staged in the pending span at all.
//
// refreshLastAckedLocked advances lastAcked to lastObserved only while
// `len(pending) == 0 && len(pendingSealed) == 0`. A message that stages no
// record therefore moves the frontier on every observation, while one staged
// record pins it until something seals the span -- and the only unprompted seal
// is the FlushMaxBytes high water mark.
//
// That is why the e2e "clean" shape escaped and "fork-1" did not: the clean
// shape wrote unkeyed inserts, which stage nothing. It is NOT about deletes --
// an insert appended with a client idempotency key pins the frontier exactly
// the same way, and that is what makes the fence's seal request unconditional
// rather than delete-specific.
func TestLastAckedIsHeldByAStagedRecordWhateverStagedIt(t *testing.T) {
	ctx := context.Background()

	t.Run("an unkeyed insert stages nothing, so the frontier tracks the pchannel", func(t *testing.T) {
		m, _ := newTestManagerWithStore(t)
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "", []int64{1}, nil))
		require.Equal(t, uint64(100), m.LastAcked())
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 200, "", []int64{2}, nil))
		require.Equal(t, uint64(200), m.LastAcked(), "nothing is staged, so nothing holds the frontier back")
	})

	t.Run("a keyed insert stages a record, and only a flush request releases it", func(t *testing.T) {
		m, _ := newTestManagerWithStore(t)
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "key-1", []int64{1}, []uint32{0}))
		require.Zero(t, m.LastAcked(), "a staged record pins the frontier below its own tick")

		// The pchannel keeps being observed and the frontier still does not
		// move: the span is far under the high water mark, so nothing seals it.
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 200, "", []int64{2}, nil))
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 300, "", []int64{3}, nil))
		require.Zero(t, m.LastAcked(), "an unsealed span never releases the frontier on its own")

		// An explicit flush request is the only thing that releases it on a
		// vchannel that will never take another message of its own.
		m.RequestFlushThrough(300)
		require.NoError(t, drainSummary(ctx, m))
		require.GreaterOrEqual(t, m.LastAcked(), uint64(300),
			"an explicit flush request must release the frontier past the staged span")
	})
}
