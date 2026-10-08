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

package config

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
)

type EtcdSourceSuite struct {
	suite.Suite

	endpoints []string
}

func (s *EtcdSourceSuite) SetupSuite() {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	s.endpoints = strings.Split(endpoints, ",")
}

func (s *EtcdSourceSuite) TearDownSuite() {
}

func (s *EtcdSourceSuite) TestNewSource() {
	etcdCli, err := newEtcdClient(&EtcdInfo{Endpoints: s.endpoints, DialTimeout: 5 * time.Second})
	s.Require().NoError(err)
	defer etcdCli.Close()
	source, err := NewEtcdSource(etcdCli, &EtcdInfo{
		Endpoints:       s.endpoints,
		KeyPrefix:       "by-dev",
		DialTimeout:     5 * time.Second,
		RefreshInterval: time.Second,
	})
	s.NoError(err)
	s.NotNil(source)
	source.Close()
}

func (s *EtcdSourceSuite) TestUpdateOptions() {
	etcdCli, err := newEtcdClient(&EtcdInfo{Endpoints: s.endpoints, DialTimeout: 5 * time.Second})
	s.Require().NoError(err)
	defer etcdCli.Close()
	source, err := NewEtcdSource(etcdCli, &EtcdInfo{
		Endpoints:       s.endpoints,
		KeyPrefix:       "test_update_options_1",
		RefreshInterval: time.Second,
	})
	s.Require().NoError(err)
	s.Require().NotNil(source)
	defer source.Close()

	called := atomic.NewBool(false)

	handler := NewHandler("test_update_options", func(evt *Event) {
		called.Store(true)
	})

	source.SetEventHandler(handler)

	source.UpdateOptions(Options{
		EtcdInfo: &EtcdInfo{
			Endpoints:       s.endpoints,
			KeyPrefix:       "test_update_options_2",
			RefreshInterval: time.Millisecond * 100,
		},
	})

	client, err := etcd.GetRemoteEtcdClient(s.endpoints)
	s.Require().NoError(err)
	client.Put(context.Background(), "test_update_options_2/config/abc", "def")

	s.Eventually(func() bool {
		return called.Load()
	}, time.Second*2, time.Millisecond*100)
}

// TestRefreshLinearizableSeesWriteBeforeNextPoll pins the read-your-own-write property
// that a caller relies on when it must act on a value the moment it is written, rather
// than whenever the periodic poll next happens to run -- the woodpecker WAL opener does
// exactly that, because the storage mode it resolves once decides the on-disk format of
// every segment the process will ever write.
func (s *EtcdSourceSuite) TestRefreshLinearizableSeesWriteBeforeNextPoll() {
	prefix := fmt.Sprintf("test-linearizable-%d", time.Now().UnixNano())
	key := "woodpeckerstoragetype"

	etcdCli, err := newEtcdClient(&EtcdInfo{Endpoints: s.endpoints, DialTimeout: 5 * time.Second})
	s.Require().NoError(err)
	defer etcdCli.Close()

	// A refresh interval far beyond the lifetime of this test: if the assertion below
	// passed because the periodic poll happened to fire, the test would prove nothing.
	source, err := NewEtcdSource(etcdCli, &EtcdInfo{
		Endpoints:       s.endpoints,
		KeyPrefix:       prefix,
		DialTimeout:     5 * time.Second,
		RefreshInterval: time.Hour,
	})
	s.Require().NoError(err)
	defer source.Close()

	_, err = etcdCli.Put(context.Background(), prefix+"/config/"+key, "service")
	s.Require().NoError(err)
	defer etcdCli.Delete(context.Background(), prefix+"/config/"+key)

	// The state a node is in when an action that depends on this key arrives before the
	// poll that would have delivered it.
	_, err = source.GetConfigurationByKey(key)
	s.Require().ErrorIs(err, ErrKeyNotFound)

	s.Require().NoError(source.RefreshConfigurationsLinearizable())

	value, err := source.GetConfigurationByKey(key)
	s.Require().NoError(err)
	s.Equal("service", value)
}

// TestStaleSnapshotDoesNotRollBackNewerRefresh replays two refreshes finishing out of order.
// The etcd read happens before the source serializes publication, so a poll that read etcd
// before a write can still publish after a linearizable refresh has published that write. The
// older snapshot must not roll the configuration back.
func (s *EtcdSourceSuite) TestStaleSnapshotDoesNotRollBackNewerRefresh() {
	ctx := context.Background()
	prefix := fmt.Sprintf("test-monotonic-%d", time.Now().UnixNano())
	key := "woodpeckerstoragetype"
	fullKey := prefix + "/config/" + key

	etcdCli, err := newEtcdClient(&EtcdInfo{Endpoints: s.endpoints, DialTimeout: 5 * time.Second})
	s.Require().NoError(err)
	defer etcdCli.Close()
	defer etcdCli.Delete(ctx, prefix, clientv3.WithPrefix())

	source, err := NewEtcdSource(etcdCli, &EtcdInfo{
		Endpoints:       s.endpoints,
		KeyPrefix:       prefix,
		DialTimeout:     5 * time.Second,
		RefreshInterval: time.Hour,
	})
	s.Require().NoError(err)
	defer source.Close()

	// A slow poll reads etcd before the new value is written ...
	_, err = etcdCli.Put(ctx, fullKey, "minio")
	s.Require().NoError(err)
	stale, err := etcdCli.Get(ctx, prefix+"/config", clientv3.WithPrefix())
	s.Require().NoError(err)

	// ... the value is written and a linearizable refresh publishes it ...
	_, err = etcdCli.Put(ctx, fullKey, "service")
	s.Require().NoError(err)
	s.Require().NoError(source.RefreshConfigurationsLinearizable())

	// ... and only then does the slow poll publish what it read.
	s.Require().NoError(source.update(map[string]string{key: string(stale.Kvs[0].Value)}, stale.Header.Revision))

	value, err := source.GetConfigurationByKey(key)
	s.Require().NoError(err)
	s.Equal("service", value)
}

// headerlessKV is the shape a hand-written clientv3.KV fake naturally takes: it fills in the
// key-values it wants to serve and leaves the rest of GetResponse zero, so Header is nil.
type headerlessKV struct {
	clientv3.KV
	kvs []*mvccpb.KeyValue
}

func (kv *headerlessKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{Kvs: kv.kvs}, nil
}

// TestSnapshotWithoutResponseHeaderIsPublished pins that a response carrying no revision is
// still published. Revision tracking must not assume every clientv3.KV fills in Header, and a
// snapshot that cannot be ordered has to be published rather than dropped.
func TestSnapshotWithoutResponseHeaderIsPublished(t *testing.T) {
	client := clientv3.NewCtxClient(context.Background())
	client.KV = &headerlessKV{kvs: []*mvccpb.KeyValue{{
		Key:   []byte("no-header/config/woodpeckerstoragetype"),
		Value: []byte("service"),
	}}}

	source, err := NewEtcdSource(client, &EtcdInfo{KeyPrefix: "no-header"})
	require.NoError(t, err)
	defer source.Close()

	configs, err := source.GetConfigurations()
	require.NoError(t, err)
	assert.Equal(t, "service", configs["woodpeckerstoragetype"])
}

func TestEtcdSource(t *testing.T) {
	suite.Run(t, new(EtcdSourceSuite))
}
