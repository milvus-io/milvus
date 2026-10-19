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

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type collectionReleaseClient struct {
	milvuspb.MilvusServiceClient
	release func(context.Context, *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error)
}

func (c collectionReleaseClient) ReleaseCollection(ctx context.Context, req *milvuspb.ReleaseCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return c.release(ctx, req)
}

func TestReleaseLoadedCollectionsRetainsFailures(t *testing.T) {
	rpcErr := merr.WrapErrServiceUnavailableMsg("injected transport failure")
	statusErr := merr.WrapErrCollectionNotFound("status_error")
	collections := &milvuspb.ShowCollectionsResponse{
		CollectionNames:       []string{"loaded", "available", "unloaded", "rpc_error", "status_error", "rpc_error_with_status", "after_errors"},
		CollectionIds:         []int64{1, 2, 3, 4, 5, 6, 7},
		InMemoryPercentages:   []int64{100, 50, 0, 100, 100, 100, 100},
		QueryServiceAvailable: []bool{false, true, false, true, true, true, true},
	}
	var mu sync.Mutex
	var called []string
	client := collectionReleaseClient{release: func(_ context.Context, req *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
		mu.Lock()
		called = append(called, req.GetCollectionName())
		mu.Unlock()
		switch req.GetCollectionName() {
		case "rpc_error":
			return nil, rpcErr
		case "status_error":
			return merr.Status(statusErr), nil
		case "rpc_error_with_status":
			return merr.Status(statusErr), rpcErr
		default:
			return merr.Success(), nil
		}
	}}
	results := releaseLoadedCollections(context.Background(), client, collections)
	require.Len(t, results, 6)
	expectedNames := []string{"loaded", "available", "rpc_error", "status_error", "rpc_error_with_status", "after_errors"}
	var names []string
	var ids []int64
	for _, result := range results {
		names = append(names, result.name)
		ids = append(ids, result.id)
	}
	assert.Equal(t, expectedNames, names)
	assert.ElementsMatch(t, expectedNames, called)
	assert.Equal(t, []int64{1, 2, 4, 5, 6, 7}, ids)
	assert.NoError(t, results[0].err)
	assert.NoError(t, results[1].err)
	assert.ErrorIs(t, results[2].err, rpcErr)
	assert.ErrorIs(t, results[3].err, merr.ErrCollectionNotFound)
	assert.ErrorIs(t, results[4].err, rpcErr)
	assert.NoError(t, results[5].err)
}

func TestReleaseLoadedCollectionsBoundedConcurrency(t *testing.T) {
	const collectionCount = 9
	collections := &milvuspb.ShowCollectionsResponse{}
	for i := 0; i < collectionCount; i++ {
		collections.CollectionNames = append(collections.CollectionNames, fmt.Sprint(i))
		collections.CollectionIds = append(collections.CollectionIds, int64(i))
		collections.InMemoryPercentages = append(collections.InMemoryPercentages, 100)
	}
	started := make(chan struct{}, collectionCount)
	unblock := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var active, peak atomic.Int32
	client := collectionReleaseClient{release: func(ctx context.Context, _ *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-unblock:
			return merr.Success(), nil
		}
	}}
	done := make(chan struct{})
	var results []collectionReleaseResult
	go func() {
		defer close(done)
		results = releaseLoadedCollections(ctx, client, collections)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("collection releases did not finish after cancellation")
		}
	})
	// Block all requests until four have started. This verifies overlap
	// without making a wall-clock performance assertion.
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d concurrent releases started, want 4", i)
		}
	}
	close(unblock)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("collection releases did not finish")
	}
	assert.Equal(t, int32(4), peak.Load())
	require.Len(t, results, collectionCount)
	for i, result := range results {
		assert.Equal(t, int64(i), result.id)
		assert.NoError(t, result.err)
	}
}

func TestReleaseLoadedCollectionsEmpty(t *testing.T) {
	client := collectionReleaseClient{release: func(context.Context, *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
		t.Error("unexpected release for an empty collection list")
		return merr.Success(), nil
	}}
	assert.Empty(t, releaseLoadedCollections(context.Background(), client, &milvuspb.ShowCollectionsResponse{}))
}
