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

package controller

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestController_StartAndStop_WithEvents(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()

	// Create test data
	replicateMeta := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "by-dev-test-source-channel",
		TargetChannelName: "by-dev-test-target-channel",
	}
	metaBytes, _ := proto.Marshal(replicateMeta)

	// Create mock events
	putEvent := &clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:   []byte(streamingcoord.BuildReplicatePChannelMetaKey(replicateMeta)),
			Value: metaBytes,
		},
	}

	deleteEvent := &clientv3.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Key: []byte(streamingcoord.BuildReplicatePChannelMetaKey(replicateMeta)),
		},
		PrevKv: &mvccpb.KeyValue{
			Value: metaBytes,
		},
	}

	eventCh := make(chan clientv3.WatchResponse, 2)
	// Send events
	go func() {
		eventCh <- clientv3.WatchResponse{
			Events: []*clientv3.Event{putEvent},
		}
		eventCh <- clientv3.WatchResponse{
			Events: []*clientv3.Event{deleteEvent},
		}
	}()

	notifyCh := make(chan struct{}, 2)
	mockReplicateManagerClient.EXPECT().CreateReplicator(mock.Anything).RunAndReturn(func(replicate *meta.ReplicateChannel) {
		notifyCh <- struct{}{}
	})
	mockReplicateManagerClient.EXPECT().RemoveReplicator(mock.Anything, mock.Anything).RunAndReturn(func(key string, modRevision int64) {
		notifyCh <- struct{}{}
	})

	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
	)

	ctrl := NewController()
	go ctrl.watchLoop(eventCh)

	// Wait for put event to be processed
	select {
	case <-notifyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for put event")
	}
	// Wait for delete event to be processed
	select {
	case <-notifyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for delete event")
	}
	ctrl.Stop()
}

// failingRangeServer fails the first failures Range calls with err.
type failingRangeServer struct {
	etcdserverpb.UnimplementedKVServer
	failures int32
	err      error
	calls    atomic.Int32
}

func (s *failingRangeServer) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if s.calls.Add(1) <= s.failures {
		return nil, s.err
	}
	return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 10}}, nil
}

func newBufconnEtcdClient(t *testing.T, kv etcdserverpb.KVServer) *clientv3.Client {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, kv)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://etcd-controller"},
		DialTimeout: time.Second,
		// Surface server errors to the controller instead of retrying them
		// inside clientv3.
		MaxUnaryRetries: 1,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		})},
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return cli
}

func TestController_ListRetriesTransientEtcdError(t *testing.T) {
	// The leader change outlasts clientv3's own retry and reaches the controller.
	kv := &failingRangeServer{failures: 3, err: rpctypes.ErrGRPCLeaderChanged}
	resource.InitForTest(t, resource.OptETCD(newBufconnEtcdClient(t, kv)))

	ctrl := NewController()
	defer ctrl.cancel()
	m, err := ctrl.listReplicatePChannels()
	require.NoError(t, err)
	require.EqualValues(t, 10, m.Revision)
	require.EqualValues(t, 4, kv.calls.Load())
}

func TestController_ListAbortsOnNonTransientError(t *testing.T) {
	kv := &failingRangeServer{failures: 1, err: rpctypes.ErrGRPCPermissionDenied}
	resource.InitForTest(t, resource.OptETCD(newBufconnEtcdClient(t, kv)))

	ctrl := NewController()
	defer ctrl.cancel()
	_, err := ctrl.listReplicatePChannels()
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	require.EqualValues(t, 1, kv.calls.Load())
}

func TestController_ListWithCanceledContext(t *testing.T) {
	kv := &failingRangeServer{}
	manager := replication.NewMockReplicateManagerClient(t)
	manager.EXPECT().Close().Return().Once()
	resource.InitForTest(t,
		resource.OptETCD(newBufconnEtcdClient(t, kv)),
		resource.OptReplicateManagerClient(manager),
	)

	ctrl := NewController()
	ctrl.cancel()
	require.NoError(t, ctrl.Start())
	ctrl.Stop()
	require.Zero(t, kv.calls.Load())
}

func TestController_WatchLoopCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan clientv3.WatchResponse)
	ctrl := &controller{ctx: ctx}
	// clientv3 closes the watch channel after cancellation. Both select cases
	// can be ready before the controller resumes.
	cancel()
	close(events)
	for range 128 {
		require.NotPanics(t, func() {
			require.NoError(t, ctrl.watchLoop(events))
		})
	}
}

func TestController_WatchLoopUnexpectedClose(t *testing.T) {
	ctrl := &controller{ctx: context.Background()}
	events := make(chan clientv3.WatchResponse)
	close(events)
	require.PanicsWithValue(t, "etcd event channel closed", func() {
		ctrl.watchLoop(events)
	})
}
