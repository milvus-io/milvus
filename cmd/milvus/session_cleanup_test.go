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

package milvus

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func newSessionCleanupEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	endpoint := func() url.URL {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		return url.URL{Scheme: "http", Host: address}
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{endpoint()}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{endpoint()}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}
	client := v3client.New(server.Server)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func sessionCleanupJSON(serverID int64) string {
	return fmt.Sprintf(`{"ServerID":%d}`, serverID)
}

func TestSessionCleanupSelection(t *testing.T) {
	client := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("own node and active keys", func(t *testing.T) {
		prefix := "cleanup/own/session"
		suffixes := []string{"mixcoord-7", "querycoord-8", "datacoord-9", "querynode-10", "proxy-11", "streamingnode-12"}
		keys := map[string]int64{
			"mixcoord-7": 7, "mixcoord": 7,
			"querycoord-8": 8, "querycoord": 8,
			"datacoord-9": 9, "datacoord": 9,
			"querynode-10": 10, "proxy-11": 11, "streamingnode-12": 12,
		}
		for key, owner := range keys {
			_, err := client.Put(ctx, path.Join(prefix, key), sessionCleanupJSON(owner))
			require.NoError(t, err)
		}
		targets, err := collectSessionCleanupTargets(ctx, client, prefix, suffixes)
		require.NoError(t, err)
		require.Len(t, targets, len(keys))
		require.NoError(t, deleteSessionCleanupTargets(ctx, client, targets))
		response, err := client.Get(ctx, prefix+"/", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Empty(t, response.Kvs)
	})

	t.Run("foreign active coordinator survives", func(t *testing.T) {
		prefix := "cleanup/foreign-active/session"
		_, err := client.Put(ctx, path.Join(prefix, "mixcoord-7"), sessionCleanupJSON(7))
		require.NoError(t, err)
		_, err = client.Put(ctx, path.Join(prefix, "mixcoord"), sessionCleanupJSON(8))
		require.NoError(t, err)
		targets, err := collectSessionCleanupTargets(ctx, client, prefix, []string{"mixcoord-7"})
		require.NoError(t, err)
		require.Len(t, targets, 1)
		require.NoError(t, deleteSessionCleanupTargets(ctx, client, targets))
		response, err := client.Get(ctx, path.Join(prefix, "mixcoord"))
		require.NoError(t, err)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, sessionCleanupJSON(8), string(response.Kvs[0].Value))
	})

	for name, body := range map[string]string{
		"foreign owner":    sessionCleanupJSON(8),
		"invalid json":     "{",
		"null session":     "null",
		"missing owner":    `{}`,
		"null owner":       `{"ServerID":null}`,
		"noninteger owner": `{"ServerID":"7"}`,
	} {
		t.Run(name, func(t *testing.T) {
			prefix := path.Join("cleanup/invalid", strings.ReplaceAll(name, " ", "-"), "session")
			key := path.Join(prefix, "querynode-7")
			_, err := client.Put(ctx, key, body)
			require.NoError(t, err)
			targets, err := collectSessionCleanupTargets(ctx, client, prefix, []string{"querynode-7"})
			require.NoError(t, err)
			require.Empty(t, targets)
			require.NoError(t, deleteSessionCleanupTargets(ctx, client, targets))
			response, err := client.Get(ctx, key)
			require.NoError(t, err)
			require.Len(t, response.Kvs, 1)
			require.Equal(t, body, string(response.Kvs[0].Value))
		})
	}

	t.Run("missing and malformed suffixes", func(t *testing.T) {
		targets, err := collectSessionCleanupTargets(ctx, client, "cleanup/missing/session", []string{
			"querynode-7", "", "mixcoord", "-7", "mixcoord-not-an-id", "querynode-7-extra",
		})
		require.NoError(t, err)
		require.Empty(t, targets)
	})
}

func TestSessionCleanupPreservesTakeover(t *testing.T) {
	client := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := "cleanup/takeover/session"
	activeKey := path.Join(prefix, "mixcoord")
	leaseA, err := client.Grant(ctx, 20)
	require.NoError(t, err)
	for _, suffix := range []string{"mixcoord-7", "mixcoord"} {
		_, err = client.Put(ctx, path.Join(prefix, suffix), sessionCleanupJSON(7), clientv3.WithLease(leaseA.ID))
		require.NoError(t, err)
	}
	targets, err := collectSessionCleanupTargets(ctx, client, prefix, []string{"mixcoord-7"})
	require.NoError(t, err)
	require.Len(t, targets, 2)

	// Stop exactly between the ownership reads and stale cleanup. B acquires
	// A's active key under a new lease before the old supervisor resumes.
	_, err = client.Revoke(ctx, leaseA.ID)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 20)
	require.NoError(t, err)
	for _, suffix := range []string{"mixcoord-8", "mixcoord"} {
		_, err = client.Put(ctx, path.Join(prefix, suffix), sessionCleanupJSON(8), clientv3.WithLease(leaseB.ID))
		require.NoError(t, err)
	}
	require.NoError(t, deleteSessionCleanupTargets(ctx, client, targets))
	for _, suffix := range []string{"mixcoord-8", "mixcoord"} {
		response, err := client.Get(ctx, path.Join(prefix, suffix))
		require.NoError(t, err)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, sessionCleanupJSON(8), string(response.Kvs[0].Value))
		require.Equal(t, int64(leaseB.ID), response.Kvs[0].Lease)
	}
	// C uses the active-key absence condition of coordinator registration.
	// Keeping B's key must prevent a second active coordinator.
	acquireC, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(activeKey), "=", 0)).
		Then(clientv3.OpPut(activeKey, sessionCleanupJSON(9))).Commit()
	require.NoError(t, err)
	require.False(t, acquireC.Succeeded)
}

func TestSessionCleanupPreservesChangedRevision(t *testing.T) {
	client := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, suffix := range []string{"mixcoord", "mixcoord-7"} {
		for _, mutation := range []string{"same owner rewrite", "delete recreate same value"} {
			t.Run(suffix+"/"+mutation, func(t *testing.T) {
				prefix := path.Join("cleanup/changed", strings.ReplaceAll(t.Name(), " ", "-"), "session")
				for _, initial := range []string{"mixcoord", "mixcoord-7"} {
					_, err := client.Put(ctx, path.Join(prefix, initial), sessionCleanupJSON(7))
					require.NoError(t, err)
				}
				targets, err := collectSessionCleanupTargets(ctx, client, prefix, []string{"mixcoord-7"})
				require.NoError(t, err)
				require.Len(t, targets, 2)
				key := path.Join(prefix, suffix)
				body := sessionCleanupJSON(7)
				if mutation == "delete recreate same value" {
					_, err = client.Delete(ctx, key)
					require.NoError(t, err)
				} else {
					body = `{"ServerID":7,"Address":"new-process"}`
				}
				_, err = client.Put(ctx, key, body)
				require.NoError(t, err)
				require.NoError(t, deleteSessionCleanupTargets(ctx, client, targets))
				response, err := client.Get(ctx, key)
				require.NoError(t, err)
				require.Len(t, response.Kvs, 1)
				require.Equal(t, body, string(response.Kvs[0].Value))
			})
		}
	}
}

type sessionCleanupInterceptKV struct {
	clientv3.KV
	get    func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	txnErr error
}

func (kv *sessionCleanupInterceptKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if kv.get != nil {
		return kv.get(ctx, key, opts...)
	}
	return kv.KV.Get(ctx, key, opts...)
}

func (kv *sessionCleanupInterceptKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	panic("session cleanup must not fall back to unconditional Delete")
}

func (kv *sessionCleanupInterceptKV) Txn(ctx context.Context) clientv3.Txn {
	return &sessionCleanupFailTxn{Txn: kv.KV.Txn(ctx), err: kv.txnErr}
}

type sessionCleanupFailTxn struct {
	clientv3.Txn
	err error
}

func (txn *sessionCleanupFailTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	txn.Txn = txn.Txn.If(cmps...)
	return txn
}

func (txn *sessionCleanupFailTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Then(ops...)
	return txn
}

func (txn *sessionCleanupFailTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Else(ops...)
	return txn
}

func (txn *sessionCleanupFailTxn) Commit() (*clientv3.TxnResponse, error) {
	if txn.err != nil {
		return nil, txn.err
	}
	return txn.Txn.Commit()
}

func TestSessionCleanupKeepsFirstObservation(t *testing.T) {
	client := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "cleanup/duplicate/session"
	key := path.Join(prefix, "querynode-7")
	_, err := client.Put(ctx, key, sessionCleanupJSON(7))
	require.NoError(t, err)
	var firstRevision int64
	kv := &sessionCleanupInterceptKV{KV: client.KV}
	kv.get = func(ctx context.Context, requested string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		response, err := client.Get(ctx, requested, opts...)
		if err == nil && requested == key && firstRevision == 0 {
			require.Len(t, response.Kvs, 1)
			firstRevision = response.Kvs[0].ModRevision
			// A duplicate suffix must not refresh the original ownership read.
			_, putErr := client.Put(ctx, key, sessionCleanupJSON(7))
			require.NoError(t, putErr)
		}
		return response, err
	}
	targets, err := collectSessionCleanupTargets(ctx, kv, prefix, []string{"querynode-7", "querynode-7"})
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.Equal(t, firstRevision, targets[0].modRevision)
	require.NoError(t, deleteSessionCleanupTargets(ctx, kv, targets))
	response, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	require.Greater(t, response.Kvs[0].ModRevision, firstRevision)
}

func TestSessionCleanupErrors(t *testing.T) {
	client := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := "cleanup/errors/session"
	key := path.Join(prefix, "querynode-7")
	_, err := client.Put(ctx, key, sessionCleanupJSON(7))
	require.NoError(t, err)

	t.Run("read failure aborts selection", func(t *testing.T) {
		readErr := errors.New("injected Get failure")
		kv := &sessionCleanupInterceptKV{KV: client.KV}
		kv.get = func(ctx context.Context, requested string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			if requested == path.Join(prefix, "proxy-8") {
				return nil, readErr
			}
			return client.Get(ctx, requested, opts...)
		}
		targets, err := collectSessionCleanupTargets(ctx, kv, prefix, []string{"querynode-7", "proxy-8"})
		require.ErrorIs(t, err, readErr)
		require.Empty(t, targets)
	})

	targets, err := collectSessionCleanupTargets(ctx, client, prefix, []string{"querynode-7"})
	require.NoError(t, err)
	require.Len(t, targets, 1)
	t.Run("transaction failure has no unconditional fallback", func(t *testing.T) {
		txnErr := errors.New("injected Txn failure")
		kv := &sessionCleanupInterceptKV{KV: client.KV, txnErr: txnErr}
		require.ErrorIs(t, deleteSessionCleanupTargets(ctx, kv, targets), txnErr)
		response, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, sessionCleanupJSON(7), string(response.Kvs[0].Value))
	})
	t.Run("real canceled transaction", func(t *testing.T) {
		// Cancellation does not prove that a transaction was never applied.
		// Even if etcd applies the stale transaction, it must preserve a successor.
		_, err := client.Put(ctx, key, sessionCleanupJSON(8))
		require.NoError(t, err)
		canceled, stop := context.WithCancel(ctx)
		stop()
		require.ErrorIs(t, deleteSessionCleanupTargets(canceled, client, targets), context.Canceled)
	})
	response, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, sessionCleanupJSON(8), string(response.Kvs[0].Value))
}
