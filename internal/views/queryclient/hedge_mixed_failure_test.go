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
package queryclient

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAdaptiveHedgeMixedFailuresEndRequest(t *testing.T) {
	paramtable.Init()
	for _, first := range []string{"primary", "hedge"} {
		t.Run(first+"_fails_first", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				entered := make(chan int, 2)
				release := []chan struct{}{make(chan struct{}), make(chan struct{})}
				var mu sync.Mutex
				var nodes []int64
				client := overloadTestClient(t, true, true, []int64{1, 2, 3}, func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
					mu.Lock()
					nodes = append(nodes, req.GetShardId().GetReplicaId())
					call := len(nodes)
					mu.Unlock()
					if call > 2 {
						// A healthy third replica must remain untried.
						return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
					}
					entered <- call
					select {
					case <-release[call-1]:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if call == 1 {
						return nil, status.Error(codes.Unavailable, "worker disconnected")
					}
					return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
				})
				done := make(chan error, 1)
				go func() {
					_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
					done <- err
				}()
				require.Equal(t, 1, <-entered)
				require.Equal(t, 2, <-entered)
				firstIndex := 0
				if first == "hedge" {
					firstIndex = 1
				}
				close(release[firstIndex])
				// Drain the first result through routing before releasing the
				// sibling, so both completion orders are deterministic.
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("request ended while sibling was pending: %v", err)
				default:
				}
				close(release[1-firstIndex])
				require.ErrorIs(t, <-done, merr.ErrServiceTooManyRequests)
				synctest.Wait()
				require.Len(t, nodes, 2)
				require.NotEqual(t, nodes[0], nodes[1])
			})
		})
	}
}
