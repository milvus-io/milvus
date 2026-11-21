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
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMixCoordLegacySessionsBlockTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-ctx.Done():
		t.Fatal("embedded etcd did not become ready")
	}
	cli := v3client.New(server.Server)
	t.Cleanup(func() { _ = cli.Close() })

	metaRoot := "mixed-coordinator"
	prefix := path.Join(metaRoot, DefaultServiceRoot)
	activeKey := path.Join(prefix, typeutil.MixCoordRole)
	newMixCoord := func(sessionCtx context.Context, id int64) *Session {
		lease, err := cli.Grant(ctx, 30)
		require.NoError(t, err)
		s := &Session{
			ctx: sessionCtx, etcdCli: cli, metaRoot: metaRoot, versionKey: path.Join(prefix, serverVersionKey),
			SessionRaw: SessionRaw{ServerName: typeutil.MixCoordRole, ServerID: id, LeaseID: &lease.ID},
			Version:    common.Version,
		}
		t.Cleanup(func() { s.updateStandby(false) })
		return s
	}

	old := newMixCoord(ctx, 1)
	require.NoError(t, old.ProcessActiveStandBy(nil))
	// The old supervisor records the active key before the old lease expires.
	observed, err := cli.Get(ctx, activeKey)
	require.NoError(t, err)
	require.Len(t, observed.Kvs, 1)
	require.EqualValues(t, *old.LeaseID, observed.Kvs[0].Lease)
	_, err = cli.Revoke(ctx, *old.LeaseID)
	require.NoError(t, err)

	active := newMixCoord(ctx, 2)
	require.NoError(t, active.ProcessActiveStandBy(nil))
	for _, role := range []string{typeutil.MixCoordRole, typeutil.RootCoordRole, typeutil.DataCoordRole, typeutil.QueryCoordRole} {
		resp, err := cli.Get(ctx, path.Join(prefix, role))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1, role)
		kv := resp.Kvs[0]
		require.EqualValues(t, *active.LeaseID, kv.Lease, role)
		require.Equal(t, active.GetRegisteredRevision(), kv.ModRevision, role)
		var registered SessionRaw
		require.NoError(t, json.Unmarshal(kv.Value, &registered))
		require.Equal(t, role, registered.ServerName)
		require.Equal(t, active.ServerID, registered.ServerID)
	}

	// The delayed, unconditional cleanup removes the new active key. The
	// surviving legacy keys must still stop a third coordinator from activating.
	deleted, err := cli.Delete(ctx, string(observed.Kvs[0].Key))
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted.Deleted)
	blockedCtx, cancelBlocked := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancelBlocked()
	blocked := newMixCoord(blockedCtx, 3)
	require.ErrorIs(t, blocked.ProcessActiveStandBy(nil), context.DeadlineExceeded)
	alive, err := cli.TimeToLive(ctx, *active.LeaseID)
	require.NoError(t, err)
	require.Positive(t, alive.TTL)

	_, err = cli.Revoke(ctx, *active.LeaseID)
	require.NoError(t, err)
	successor := newMixCoord(ctx, 4)
	require.NoError(t, successor.ProcessActiveStandBy(nil))
}
