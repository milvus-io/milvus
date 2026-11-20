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

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type recoveringSessionEtcd struct {
	clientv3.KV
	clientv3.Watcher
	getCalls  int
	revision  int64
	events    chan clientv3.WatchResponse
	watchCtxs []context.Context
}

func (c *recoveringSessionEtcd) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	c.getCalls++
	if c.getCalls == 1 {
		return nil, rpctypes.ErrLeaderChanged
	}
	return &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 20},
		Kvs:    []*mvccpb.KeyValue{{Key: []byte("test-200"), Value: []byte(`{"ServerID":200}`)}},
	}, nil
}

func (c *recoveringSessionEtcd) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	c.watchCtxs = append(c.watchCtxs, ctx)
	c.revision = clientv3.OpGet(key, opts...).Rev()
	return c.events
}

func (c *recoveringSessionEtcd) Close() error { return nil }

func newRecoveringWatcher(ctx context.Context, t *testing.T, responses ...clientv3.WatchResponse) (*sessionWatcher, *recoveringSessionEtcd) {
	stub := &recoveringSessionEtcd{events: make(chan clientv3.WatchResponse, len(responses))}
	for _, response := range responses {
		stub.events <- response
	}
	cli := clientv3.NewCtxClient(ctx)
	cli.KV, cli.Watcher = stub, stub
	t.Cleanup(func() { cli.Close() })
	return &sessionWatcher{
		s:       &Session{ctx: ctx, etcdCli: cli},
		ctx:     ctx,
		prefix:  "test",
		eventCh: make(chan *SessionEvent),
	}, stub
}

func TestSessionWatcher_LeaderChangedRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, stub := newRecoveringWatcher(ctx, t, clientv3.WatchResponse{Created: true})

	rewatched := false
	w.rewatch = func(sessions map[string]*Session) error {
		require.Equal(t, int64(200), sessions["test-200"].ServerID)
		rewatched = true
		return nil
	}
	require.NoError(t, w.handleWatchErr(rpctypes.ErrLeaderChanged))
	require.Equal(t, 2, stub.getCalls)
	require.True(t, rewatched)
	require.EqualValues(t, 20, stub.revision)
	require.Equal(t, clientv3.WatchChan(stub.events), w.rch)
	select {
	case <-w.eventCh:
		t.Fatal("successful recovery closed the session event channel")
	default:
	}

	require.ErrorIs(t, w.handleWatchErr(rpctypes.ErrPermissionDenied), rpctypes.ErrPermissionDenied)
	require.Equal(t, 2, stub.getCalls, "permission denied must not retry")
	_, open := <-w.eventCh
	require.False(t, open)
}

func TestSessionWatcher_RejectedWatchRetriesWithoutReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rejected := clientv3.WatchResponse{Canceled: true, CancelReason: rpctypes.ErrorDesc(rpctypes.ErrGRPCNotSupportedForLearner)}
	w, stub := newRecoveringWatcher(ctx, t, rejected, clientv3.WatchResponse{Created: true})
	replays := 0
	w.rewatch = func(map[string]*Session) error {
		replays++
		return nil
	}

	require.NoError(t, w.handleWatchErr(rejected.Err()))
	require.Equal(t, 3, stub.getCalls, "the leader change and the rejection each fail one attempt")
	require.Equal(t, 1, replays, "only the created watch replays the sessions")
}

// A replay that fails with a retriable error runs the attempt again, so it must
// stop the watch it created. Only the installed watch stays open, until the next
// re-watch replaces it.
func TestSessionWatcher_FailedReplayCancelsItsWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	created := clientv3.WatchResponse{Created: true}
	w, stub := newRecoveringWatcher(ctx, t, created, created, created)
	replays := 0
	w.rewatch = func(map[string]*Session) error {
		replays++
		if replays == 1 {
			return rpctypes.ErrLeaderChanged
		}
		return nil
	}

	require.NoError(t, w.handleWatchErr(rpctypes.ErrLeaderChanged))
	require.Equal(t, 2, replays)
	require.Len(t, stub.watchCtxs, 2)
	require.ErrorIs(t, stub.watchCtxs[0].Err(), context.Canceled, "the failed attempt's watch must stop")
	require.NoError(t, stub.watchCtxs[1].Err(), "the installed watch must stay open")

	require.NoError(t, w.handleWatchErr(rpctypes.ErrLeaderChanged))
	require.Len(t, stub.watchCtxs, 3)
	require.ErrorIs(t, stub.watchCtxs[1].Err(), context.Canceled, "a re-watch replaces the installed watch")
	require.NoError(t, stub.watchCtxs[2].Err())
}
