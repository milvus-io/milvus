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
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// appliedRegistrationKV applies the real registration write set before losing
// the first transaction response. Subsequent transactions evaluate the version
// comparisons, so recovery must use the Else branch without rewriting the keys.
type appliedRegistrationKV struct {
	clientv3.KV
	kvs      map[string]*mvccpb.KeyValue
	leaseID  int64
	txnCalls int
}

func (k *appliedRegistrationKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resp := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 20}}
	op := clientv3.OpGet(key, opts...)
	for stored, kv := range k.kvs {
		if stored == key || (len(op.RangeBytes()) > 0 && strings.HasPrefix(stored, key)) {
			resp.Kvs = append(resp.Kvs, kv)
			resp.Count++
		}
	}
	return resp, nil
}

func (k *appliedRegistrationKV) Txn(ctx context.Context) clientv3.Txn {
	return &appliedRegistrationTxn{kv: k, ctx: ctx}
}

type appliedRegistrationTxn struct {
	kv      *appliedRegistrationKV
	ctx     context.Context
	cmps    []clientv3.Cmp
	thenOps []clientv3.Op
	elseOps []clientv3.Op
}

func (t *appliedRegistrationTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	t.cmps = cmps
	return t
}

func (t *appliedRegistrationTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.thenOps = ops
	return t
}

func (t *appliedRegistrationTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.elseOps = ops
	return t
}

func (t *appliedRegistrationTxn) Commit() (*clientv3.TxnResponse, error) {
	if err := t.ctx.Err(); err != nil {
		return nil, err
	}
	t.kv.txnCalls++
	for _, cmp := range t.cmps {
		version := int64(0)
		if kv := t.kv.kvs[string(cmp.Key)]; kv != nil {
			version = kv.Version
		}
		if version != (*etcdserverpb.Compare)(&cmp).GetVersion() {
			resp := &clientv3.TxnResponse{Header: &etcdserverpb.ResponseHeader{Revision: 20}}
			for _, op := range t.elseOps {
				rangeResp, err := t.kv.Get(t.ctx, string(op.KeyBytes()))
				if err != nil {
					return nil, err
				}
				resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
					Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: (*etcdserverpb.RangeResponse)(rangeResp)},
				})
			}
			return resp, nil
		}
	}
	for _, op := range t.thenOps {
		t.kv.kvs[string(op.KeyBytes())] = &mvccpb.KeyValue{
			Key: op.KeyBytes(), Value: op.ValueBytes(), Lease: t.kv.leaseID, ModRevision: 20, Version: 1,
		}
	}
	return nil, rpctypes.ErrTimeout
}

func TestSessionProcessActiveStandBy_RecoversCommittedMixCoordTxn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv := &appliedRegistrationKV{kvs: make(map[string]*mvccpb.KeyValue), leaseID: 7}
	cli := clientv3.NewCtxClient(ctx)
	cli.KV = kv
	defer cli.Close()
	leaseID := clientv3.LeaseID(kv.leaseID)
	s := &Session{
		ctx: ctx, etcdCli: cli, metaRoot: "review", versionKey: "review/session/version",
		SessionRaw: SessionRaw{ServerName: typeutil.MixCoordRole, ServerID: 42, LeaseID: &leaseID},
	}
	defer s.updateStandby(false)
	activated := false
	err := s.ProcessActiveStandBy(func() error {
		activated = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, activated)
	require.Equal(t, 2, kv.txnCalls)
	require.Len(t, kv.kvs, 5, "MixCoord activation includes all three legacy session keys")
	require.EqualValues(t, 20, s.GetRegisteredRevision())
	for _, role := range []string{typeutil.RootCoordRole, typeutil.DataCoordRole, typeutil.QueryCoordRole} {
		stored := &Session{}
		require.NoError(t, json.Unmarshal(kv.kvs[path.Join("review/session", role)].Value, stored))
		require.Equal(t, s.ServerID, stored.ServerID)
		require.Equal(t, s.LeaseID, stored.LeaseID)
	}
}

func TestSessionProcessActiveStandBy_PreservesForeignLegacySession(t *testing.T) {
	for _, key := range []string{typeutil.RootCoordRole, typeutil.RootCoordRole + "-99"} {
		t.Run(key, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			leaseID, foreignLeaseID := clientv3.LeaseID(7), clientv3.LeaseID(8)
			foreignJSON, err := json.Marshal(SessionRaw{ServerName: typeutil.RootCoordRole, ServerID: 99, LeaseID: &foreignLeaseID})
			require.NoError(t, err)
			foreignKey := path.Join("review/session", key)
			ownJSON, err := json.Marshal(SessionRaw{ServerName: typeutil.RootCoordRole, ServerID: 42, LeaseID: &leaseID})
			require.NoError(t, err)
			kv := &appliedRegistrationKV{
				kvs:     map[string]*mvccpb.KeyValue{foreignKey: {Key: []byte(foreignKey), Value: foreignJSON, Lease: int64(foreignLeaseID), Version: 1}},
				leaseID: int64(leaseID),
			}
			activeKey := path.Join("review/session", typeutil.MixCoordRole)
			kv.kvs[activeKey] = &mvccpb.KeyValue{Key: []byte(activeKey), Value: ownJSON, Lease: int64(leaseID), Version: 1}
			if key != typeutil.RootCoordRole {
				ownKey := path.Join("review/session", typeutil.RootCoordRole)
				kv.kvs[ownKey] = &mvccpb.KeyValue{Key: []byte(ownKey), Value: ownJSON, Lease: int64(leaseID), Version: 1}
			}
			cli := clientv3.NewCtxClient(ctx)
			cli.KV = kv
			defer cli.Close()
			s := &Session{
				ctx: ctx, etcdCli: cli, metaRoot: "review", versionKey: "review/session/version",
				SessionRaw: SessionRaw{ServerName: typeutil.MixCoordRole, ServerID: 42, LeaseID: &leaseID},
			}
			defer s.updateStandby(false)
			activated := false
			err = s.ProcessActiveStandBy(func() error { activated = true; return nil })
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.False(t, activated)
			require.Zero(t, kv.txnCalls, "a foreign legacy owner must block activation")
			require.Equal(t, foreignJSON, kv.kvs[foreignKey].Value)
		})
	}
}

type lifecycleSessionEtcd struct {
	clientv3.KV
	clientv3.Watcher
	get      func(context.Context) (*clientv3.GetResponse, error)
	watchCtx context.Context
	events   chan clientv3.WatchResponse
}

func (c *lifecycleSessionEtcd) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return c.get(ctx)
}

func (c *lifecycleSessionEtcd) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	c.watchCtx = ctx
	return c.events
}

func (c *lifecycleSessionEtcd) Close() error { return nil }

func TestSessionWatcher_StopDuringRecovery(t *testing.T) {
	for _, blockRequest := range []bool{false, true} {
		name := "retry_backoff"
		if blockRequest {
			name = "snapshot_request"
		}
		t.Run(name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan context.Context, 1)
			stub := &lifecycleSessionEtcd{events: make(chan clientv3.WatchResponse, 1)}
			stub.get = func(ctx context.Context) (*clientv3.GetResponse, error) {
				select {
				case entered <- ctx:
				default:
				}
				if blockRequest {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, rpctypes.ErrLeaderChanged
			}
			cli := clientv3.NewCtxClient(parent)
			cli.KV, cli.Watcher = stub, stub
			defer cli.Close()
			s := &Session{ctx: parent, etcdCli: cli}
			w := s.WatchServices("querynode", 1, nil)
			stub.events <- clientv3.WatchResponse{CompactRevision: 1}
			var requestCtx context.Context
			select {
			case requestCtx = <-entered:
			case <-time.After(time.Second):
				t.Fatal("watch recovery never started")
			}
			stopped := make(chan struct{})
			go func() { w.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				cancel()
				<-stopped
				t.Fatal("watcher Stop waited for parent session cancellation")
			}
			require.NoError(t, parent.Err())
			require.ErrorIs(t, stub.watchCtx.Err(), context.Canceled)
			require.ErrorIs(t, requestCtx.Err(), context.Canceled)
			select {
			case _, open := <-w.EventChannel():
				require.True(t, open, "intentional Stop must not report a session failure")
			default:
			}
		})
	}
}

func TestSessionWatcher_StopWithBlockedEventConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan clientv3.WatchResponse, 1)
	delivering := make(chan struct{})
	w := &sessionWatcher{
		ctx: ctx, cancel: cancel, rch: events, eventCh: make(chan *SessionEvent),
		validate: func(*Session) bool { close(delivering); return true },
	}
	w.start()
	events <- clientv3.WatchResponse{Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Value: []byte(`{"ServerID": 1}`)}}}}
	select {
	case <-delivering:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("watcher did not begin event delivery")
	}
	stopped := make(chan struct{})
	go func() { w.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("watcher Stop blocked on event delivery")
	}
}

type stoppingSessionKV struct {
	clientv3.KV
	put      func(context.Context, string) error
	getCalls int
	putCalls int
}

func (k *stoppingSessionKV) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	k.getCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &clientv3.GetResponse{Count: 1}, nil
}

func (k *stoppingSessionKV) Put(ctx context.Context, _, value string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	k.putCalls++
	if err := k.put(ctx, value); err != nil {
		return nil, err
	}
	return &clientv3.PutResponse{}, nil
}

func newStoppingSession(ctx context.Context, kv clientv3.KV) (*Session, *clientv3.Client) {
	cli := clientv3.NewCtxClient(ctx)
	cli.KV = kv
	leaseID := clientv3.LeaseID(7)
	return &Session{ctx: ctx, etcdCli: cli, SessionRaw: SessionRaw{ServerName: typeutil.QueryNodeRole, LeaseID: &leaseID}}, cli
}

func TestSessionGoingStop_RetryTransientWrite(t *testing.T) {
	paramtable.Init()
	kv := &stoppingSessionKV{}
	var values []string
	kv.put = func(_ context.Context, value string) error {
		values = append(values, value)
		if kv.putCalls == 1 {
			// The write was applied, but its response was lost.
			return rpctypes.ErrTimeout
		}
		return nil
	}
	s, cli := newStoppingSession(context.Background(), kv)
	defer cli.Close()
	require.NoError(t, s.GoingStop())
	require.Equal(t, 2, kv.putCalls)
	require.Equal(t, values[0], values[1], "a lost successful response must be safe to retry")
	stored := &Session{}
	require.NoError(t, json.Unmarshal([]byte(values[1]), stored))
	require.True(t, stored.Stopping)
}

func TestSessionGoingStop_PermanentError(t *testing.T) {
	paramtable.Init()
	kv := &stoppingSessionKV{put: func(context.Context, string) error { return rpctypes.ErrPermissionDenied }}
	s, cli := newStoppingSession(context.Background(), kv)
	defer cli.Close()
	require.ErrorIs(t, s.GoingStop(), rpctypes.ErrPermissionDenied)
	require.Equal(t, 1, kv.putCalls)
}

func TestSessionGoingStop_ConfirmedWriteWinsCancellationRace(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv := &stoppingSessionKV{put: func(context.Context, string) error {
		cancel()
		return nil
	}}
	s, cli := newStoppingSession(ctx, kv)
	defer cli.Close()
	require.NoError(t, s.GoingStop(), "a confirmed stopping write must still permit graceful drain")
	require.Equal(t, 1, kv.putCalls)
}

func TestSessionGoingStop_DeadlineAndCancellation(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().EtcdCfg.RequestTimeout.Key
	oldValue := paramtable.Get().EtcdCfg.RequestTimeout.GetValue()
	require.NoError(t, paramtable.Get().Save(key, "100"))
	defer paramtable.Get().Save(key, oldValue)
	for _, cancelParent := range []bool{false, true} {
		name := "operation_timeout"
		if cancelParent {
			name = "parent_canceled"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			kv := &stoppingSessionKV{put: func(requestCtx context.Context, _ string) error {
				if cancelParent {
					cancel()
				}
				<-requestCtx.Done()
				return requestCtx.Err()
			}}
			s, cli := newStoppingSession(ctx, kv)
			defer cli.Close()
			started := time.Now()
			err := s.GoingStop()
			if cancelParent {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.NoError(t, ctx.Err(), "operation deadline must not cancel the session")
			}
			require.Less(t, time.Since(started), time.Second)
			require.Equal(t, 1, kv.putCalls)
		})
	}
}
