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
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func TestConfigFromEnv(t *testing.T) {
	mgr, _ := Init()
	_, _, err := mgr.GetConfig("test.env")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	t.Setenv("TEST_ENV", "value")
	mgr, _ = Init(WithEnvSource(formatKey))

	_, v, err := mgr.GetConfig("test.env")
	assert.NoError(t, err)
	assert.Equal(t, "value", v)

	_, v, err = mgr.GetConfig("TEST_ENV")
	assert.NoError(t, err)
	assert.Equal(t, "value", v)
}

func TestConfigFromRemote(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = "/tmp/milvus/test"
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()
	defer os.RemoveAll(cfg.Dir)

	client := v3client.New(e.Server)

	t.Setenv("TMP_KEY", "1")
	t.Setenv("log.level", "info")
	mgr, _ := Init(WithEnvSource(formatKey),
		WithFilesSource(&FileInfo{[]string{"../../configs/milvus.yaml"}, -1}),
		WithEtcdSource(&EtcdInfo{
			Endpoints:       []string{cfg.ACUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	ctx := context.Background()

	t.Run("origin is empty", func(t *testing.T) {
		_, _, err = mgr.GetConfig("test.etcd")
		assert.ErrorIs(t, err, ErrKeyNotFound)

		client.KV.Put(ctx, "test/config/test/etcd", "value")

		time.Sleep(100 * time.Millisecond)

		_, v, err := mgr.GetConfig("test.etcd")
		assert.NoError(t, err)
		assert.Equal(t, "value", v)
		_, v, err = mgr.GetConfig("TEST_ETCD")
		assert.NoError(t, err)
		assert.Equal(t, "value", v)

		client.KV.Delete(ctx, "test/config/test/etcd")
		time.Sleep(100 * time.Millisecond)

		_, _, err = mgr.GetConfig("TEST_ETCD")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("override origin value", func(t *testing.T) {
		_, v, _ := mgr.GetConfig("tmp.key")
		assert.Equal(t, "1", v)
		client.KV.Put(ctx, "test/config/tmp/key", "2")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "2", v)

		client.KV.Put(ctx, "test/config/tmp/key", "3")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "3", v)

		client.KV.Delete(ctx, "test/config/tmp/key")
		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "1", v)
	})

	t.Run("multi priority", func(t *testing.T) {
		_, v, _ := mgr.GetConfig("log.level")
		assert.Equal(t, "info", v)
		client.KV.Put(ctx, "test/config/log/level", "error")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("log.level")
		assert.Equal(t, "error", v)

		client.KV.Delete(ctx, "test/config/log/level")
		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("log.level")
		assert.Equal(t, "info", v)
	})

	t.Run("close manager", func(t *testing.T) {
		mgr.Close()

		client.KV.Put(ctx, "test/config/test/etcd", "value2")
		assert.Eventually(t, func() bool {
			_, _, err = mgr.GetConfig("test.etcd")
			return err != nil && errors.Is(err, ErrKeyNotFound)
		}, 300*time.Millisecond, 10*time.Millisecond)
	})
}
