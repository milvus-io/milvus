// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A dispatched hedge spends the extra-attempt slot even if only one attempt
// is overloaded. Process each error first in turn while its sibling is pending.
func TestHedgeMixedFailuresEndRequest(t *testing.T) {
	for _, first := range []string{"primary", "hedge"} {
		t.Run(first+"_fails_first", func(t *testing.T) {
			h := newHedgeHarness(t, 1, 2, 3)
			synctest.Test(t, func(t *testing.T) {
				h.warm(1, 2, 3)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				entered := make(chan int32, 2)
				release := []chan struct{}{make(chan struct{}), make(chan struct{})}
				done := make(chan error, 1)
				go func() {
					_, err := h.runCtx(ctx, func(call int32, _ int64) (time.Duration, error) {
						if call > 2 {
							return 0, nil // A healthy third replica must remain untried.
						}
						entered <- call
						select {
						case <-release[call-1]:
						case <-ctx.Done():
							return 0, ctx.Err()
						}
						if call == 1 {
							return 0, merr.ErrServiceUnavailable
						}
						return 0, overloadErr()
					})
					done <- err
				}()
				require.Equal(t, int32(1), <-entered)
				require.Equal(t, int32(2), <-entered)
				firstIndex := 0
				if first == "hedge" {
					firstIndex = 1
				}
				close(release[firstIndex])
				// Wait until the first result has been consumed, not merely
				// until its RPC callback has returned.
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("request ended while sibling was pending: %v", err)
				default:
				}
				close(release[1-firstIndex])
				require.ErrorIs(t, <-done, merr.ErrServiceTooManyRequests)
				nodes := h.nodes()
				require.Len(t, nodes, 2)
				require.NotEqual(t, nodes[0], nodes[1])
				require.Zero(t, h.uncached.Load(), "no coordinator refresh after overload")
				require.Zero(t, h.publishedCount())
				require.Zero(t, h.inflight())
			})
		})
	}
}
