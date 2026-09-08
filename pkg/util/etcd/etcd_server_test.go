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

package etcd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func embeddedEtcdTestConfig(t *testing.T) *embed.Config {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", freePort(t))}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", freePort(t))}}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	return cfg
}

func TestStartEmbeddedEtcdReady(t *testing.T) {
	cfg := embeddedEtcdTestConfig(t)
	e, err := startEmbeddedEtcd(cfg, 10*time.Second)
	require.NoError(t, err)
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	default:
		t.Fatal("embedded etcd returned before becoming ready")
	}

	// Exercise the in-process client used by Standalone, which bypasses
	// the readiness wait in etcd's network serving path.
	client := v3client.New(e.Server)
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "session/id", "1")
	require.NoError(t, err)
	resp, err := client.Get(ctx, "session/id")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "1", string(resp.Kvs[0].Value))
}

func TestStartEmbeddedEtcdTimeout(t *testing.T) {
	cfg := embeddedEtcdTestConfig(t)
	// Only start one member of a two-member cluster: without a quorum,
	// it cannot publish its member information and become ready.
	cfg.InitialCluster += fmt.Sprintf(",missing=http://127.0.0.1:%d", freePort(t))
	e, err := startEmbeddedEtcd(cfg, 100*time.Millisecond)
	if e != nil {
		t.Cleanup(e.Close)
	}
	require.ErrorContains(t, err, "did not become ready")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, e)

	// A failed startup must release both listeners before returning.
	for _, endpoint := range []url.URL{cfg.ListenClientUrls[0], cfg.ListenPeerUrls[0]} {
		listener, err := net.Listen("tcp", endpoint.Host)
		require.NoError(t, err)
		require.NoError(t, listener.Close())
	}

	// Reopening the same data directory also checks that the WAL and
	// backend locks were released by the timeout cleanup.
	restarted, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	restarted.Server.Stop()
	restarted.Close()
}

func TestStartEmbeddedEtcdInvalidConfig(t *testing.T) {
	cfg := embeddedEtcdTestConfig(t)
	cfg.InitialCluster = "other=http://127.0.0.1:2380"
	e, err := startEmbeddedEtcd(cfg, 10*time.Second)
	if e != nil {
		t.Cleanup(e.Close)
	}
	require.Error(t, err)
	require.Nil(t, e)
}

func TestInitEtcdServerInvalidConfig(t *testing.T) {
	const childEnv = "MILVUS_TEST_ETCD_INIT_FAILURE"
	if os.Getenv(childEnv) != "1" {
		// Isolate the singleton from TestEtcd without resetting sync.Once.
		cmd := exec.Command(os.Args[0], "-test.run=^TestInitEtcdServerInvalidConfig$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	dir := t.TempDir()
	configPath := filepath.Join(dir, "missing.yaml")
	err := InitEtcdServer(true, configPath, dir, "stdout", "error")
	require.Error(t, err)
	require.False(t, HasServer())
	for i := 0; i < 2; i++ {
		nextErr := InitEtcdServer(true, configPath, dir, "stdout", "error")
		require.ErrorIs(t, nextErr, err)
		require.False(t, HasServer())
	}
}
