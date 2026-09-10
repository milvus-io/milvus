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

package balance

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

type blockedLoadClient struct {
	milvuspb.MilvusServiceClient
	started chan struct{}
	release chan struct{}
}

func (c *blockedLoadClient) LoadCollection(ctx context.Context, _ *milvuspb.LoadCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	close(c.started)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, context.Canceled
	}
}

func TestLoadCollectionUsesTestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &blockedLoadClient{started: make(chan struct{}), release: make(chan struct{})}
	s := &LoadTestSuite{ctx: ctx}
	s.SetT(t)
	s.Cluster = &cluster.MiniClusterV3{MilvusClient: client}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The suite helper panics on RPC errors. Check cancellation rather than
		// allowing the expected cancellation error to fail this regression test.
		defer func() { _ = recover() }()
		s.loadCollection("blocked_collection", "default", 1, nil)
	}()
	defer func() { close(client.release); <-done }()
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("load request did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("load request ignored the test context cancellation")
	}
}
