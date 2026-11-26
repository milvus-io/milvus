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

package sessionutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveringSessionEtcd struct {
	clientv3.KV
	clientv3.Watcher
	getErrors []error
	getCalls  int
	revision  int64
	events    chan clientv3.WatchResponse
}

func (c *recoveringSessionEtcd) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	c.getCalls++
	if c.getCalls <= len(c.getErrors) {
		return nil, c.getErrors[c.getCalls-1]
	}
	return &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 20},
		Kvs:    []*mvccpb.KeyValue{{Key: []byte("test-200"), Value: []byte(`{"ServerID":200}`)}},
	}, nil
}

func (c *recoveringSessionEtcd) Watch(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	c.revision = clientv3.OpGet(key, opts...).Rev()
	return c.events
}

func (c *recoveringSessionEtcd) Close() error { return nil }

func TestSessionWatcher_LeaderChangedRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stub := &recoveringSessionEtcd{
		getErrors: []error{rpctypes.ErrLeaderChanged, status.Error(codes.DeadlineExceeded, "snapshot timed out")},
		events:    make(chan clientv3.WatchResponse),
	}
	cli := clientv3.NewCtxClient(ctx)
	cli.KV, cli.Watcher = stub, stub
	defer cli.Close()

	rewatched := false
	w := &sessionWatcher{
		s:       &Session{ctx: ctx, etcdCli: cli},
		ctx:     ctx,
		prefix:  "test",
		eventCh: make(chan *SessionEvent),
		rewatch: func(sessions map[string]*Session) error {
			require.Equal(t, int64(200), sessions["test-200"].ServerID)
			rewatched = true
			return nil
		},
	}
	require.NoError(t, w.handleWatchErr(rpctypes.ErrLeaderChanged))
	require.Equal(t, 3, stub.getCalls)
	require.True(t, rewatched)
	require.EqualValues(t, 20, stub.revision)
	require.Equal(t, clientv3.WatchChan(stub.events), w.rch)
	select {
	case <-w.eventCh:
		t.Fatal("successful recovery closed the session event channel")
	default:
	}

	require.ErrorIs(t, w.handleWatchErr(rpctypes.ErrPermissionDenied), rpctypes.ErrPermissionDenied)
	require.Equal(t, 3, stub.getCalls, "permission denied must not retry")
	_, open := <-w.eventCh
	require.False(t, open)
}

func TestSessionWatcher_DoesNotRetryRewatchCallbackDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stub := &recoveringSessionEtcd{}
	cli := clientv3.NewCtxClient(ctx)
	cli.KV = stub
	defer cli.Close()
	calls := 0
	w := &sessionWatcher{
		s: &Session{ctx: ctx, etcdCli: cli}, ctx: ctx,
		eventCh: make(chan *SessionEvent),
		rewatch: func(map[string]*Session) error {
			calls++
			return context.DeadlineExceeded
		},
	}
	require.ErrorIs(t, w.handleWatchErr(rpctypes.ErrCompacted), context.DeadlineExceeded)
	require.NoError(t, ctx.Err())
	require.Equal(t, 1, calls)
}
