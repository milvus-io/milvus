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
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"golang.org/x/sync/errgroup"
)

func TestAllConfigFromManager(t *testing.T) {
	mgr, _ := Init()
	all := mgr.GetConfigs()
	assert.Equal(t, 0, len(all))

	mgr, _ = Init(WithEnvSource(formatKey))
	all = mgr.GetConfigs()
	assert.Less(t, 0, len(all))
}

func TestConfigChangeEvent(t *testing.T) {
	dir, _ := os.MkdirTemp("", "milvus")
	os.WriteFile(path.Join(dir, "milvus.yaml"), []byte("a.b: 1\nc.d: 2"), 0o600)
	os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 3"), 0o600)

	fs := NewFileSource(&FileInfo{[]string{path.Join(dir, "milvus.yaml"), path.Join(dir, "user.yaml")}, 1})
	mgr, _ := Init()
	err := mgr.AddSource(fs)
	assert.NoError(t, err)
	_, res, err := mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, res, "3")
	os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 6"), 0o600)
	time.Sleep(3 * time.Second)
	_, res, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, res, "6")
}

func TestAllDupliateSource(t *testing.T) {
	mgr, _ := Init()
	err := mgr.AddSource(NewEnvSource(formatKey))
	assert.NoError(t, err)
	err = mgr.AddSource(NewEnvSource(formatKey))
	assert.Error(t, err)

	err = mgr.AddSource(ErrSource{})
	assert.Error(t, err, "error")

	err = mgr.pullSourceConfigs("ErrSource")
	assert.Error(t, err, "invalid source or source not added")
}

func TestBasic(t *testing.T) {
	mgr, _ := Init()

	// test set config
	mgr.SetConfig("a.b", "aaa")
	_, value, err := mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, value, "aaa")
	_, _, err = mgr.GetConfig("a.a")
	assert.Error(t, err)

	// test delete config
	mgr.SetConfig("a.b", "aaa")
	mgr.DeleteConfig("a.b")
	assert.Error(t, err)

	// test reset config
	mgr.ResetConfig("a.b")
	assert.Error(t, err)

	// test forbid config
	envSource := NewEnvSource(formatKey)
	err = mgr.AddSource(envSource)
	assert.NoError(t, err)

	envSource.configs.Insert("ab", "aaa")
	mgr.OnEvent(&Event{
		EventSource: envSource.GetSourceName(),
		EventType:   CreateType,
		Key:         "ab",
		Value:       "aaa",
	})
	_, value, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, value, "aaa")

	mgr.ForbidUpdate("a.b")
	mgr.OnEvent(&Event{
		EventSource: envSource.GetSourceName(),
		EventType:   UpdateType,
		Key:         "a.b",
		Value:       "bbb",
	})
	_, value, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, value, "aaa")

	configs := mgr.FileConfigs()
	assert.Len(t, configs, 0)
}

func TestOnEvent(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = t.TempDir()
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()

	client := v3client.New(e.Server)

	dir := t.TempDir()
	yamlFile := path.Join(dir, "milvus.yaml")
	os.WriteFile(yamlFile, []byte("a.b: \"\""), 0o600)
	mgr, _ := Init(WithEnvSource(formatKey),
		WithFilesSource(&FileInfo{
			Files:           []string{yamlFile},
			RefreshInterval: 10 * time.Millisecond,
		}),
		WithEtcdSource(&EtcdInfo{
			Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	os.WriteFile(yamlFile, []byte("a.b: aaa"), 0o600)
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "aaa"
	}, time.Second*5, time.Second)

	ctx := context.Background()
	client.Put(ctx, "test/config/a/b", "bbb")

	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "bbb"
	}, time.Second*5, time.Second)

	client.Put(ctx, "test/config/a/b", "ccc")
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "ccc"
	}, time.Second*5, time.Second)

	os.WriteFile(yamlFile, []byte("a.b: ddd"), 0o600)
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "ccc"
	}, time.Second*5, time.Second)

	client.Delete(ctx, "test/config/a/b")
	assert.Eventually(t, func() bool {
		_, value, err := mgr.GetConfig("a.b")
		return err == nil && value == "ddd"
	}, time.Second*5, time.Second)
}

func TestGetConfigAndSource(t *testing.T) {
	mgr, _ := Init()
	envSource := NewEnvSource(formatKey)
	err := mgr.AddSource(envSource)
	assert.NoError(t, err)

	envSource.configs.Insert("ab-key", "ab-value")
	mgr.OnEvent(&Event{
		EventSource: envSource.GetSourceName(),
		EventType:   CreateType,
		Key:         "ab-key",
	})

	mgr.SetConfig("ac-key", "ac-value")
	_, value, err := mgr.GetConfig("ac-key")
	assert.NoError(t, err)
	assert.Equal(t, value, "ac-value")

	// test get all configs
	configs := mgr.GetConfigsView()
	v, ok := configs["ab-key"]
	assert.True(t, ok)
	assert.Contains(t, v, "EnvironmentSource")

	v, ok = configs["ac-key"]
	assert.True(t, ok)
	assert.Contains(t, v, RuntimeSource)
}

func TestDeadlock(t *testing.T) {
	mgr, _ := Init()

	// test concurrent lock and recursive rlock
	wg, _ := errgroup.WithContext(context.Background())
	wg.Go(func() error {
		for i := 0; i < 100; i++ {
			mgr.GetBy(WithPrefix("rootcoord."))
		}
		return nil
	})

	wg.Go(func() error {
		for i := 0; i < 100; i++ {
			mgr.SetConfig("rootcoord.xxx", "111")
		}
		return nil
	})

	wg.Wait()
}

func TestCachedConfig(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = t.TempDir()
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()

	dir := t.TempDir()
	yamlFile := path.Join(dir, "milvus.yaml")
	os.WriteFile(yamlFile, []byte("a.b: aaa"), 0o600)
	mgr, _ := Init(WithEnvSource(formatKey),
		WithFilesSource(&FileInfo{
			Files:           []string{yamlFile},
			RefreshInterval: 10 * time.Millisecond,
		}),
		WithEtcdSource(&EtcdInfo{
			Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	// test get cached value from file
	{
		time.Sleep(time.Second)
		_, exist := mgr.GetCachedValue("a.b")
		assert.False(t, exist)
		ok := mgr.CASCachedValue("a.b", "aaa", "aaa")
		require.True(t, ok)
		val, exist := mgr.GetCachedValue("a.b")
		require.True(t, exist)
		assert.Equal(t, "aaa", val.(string))

		// after refresh, the cached value should be reset
		os.WriteFile(yamlFile, []byte("a.b: xxx"), 0o600)
		assert.Eventually(t, func() bool {
			_, exist = mgr.GetCachedValue("a.b")
			return !exist
		}, 5*time.Second, 100*time.Millisecond)
	}
	client := v3client.New(e.Server)
	{
		_, exist := mgr.GetCachedValue("c.d")
		assert.False(t, exist)
		ok := mgr.CASCachedValue("cd", "", "xxx")
		require.True(t, ok)
		_, exist = mgr.GetCachedValue("cd")
		assert.True(t, exist)

		// after refresh, the cached value should be reset
		ctx := context.Background()
		client.KV.Put(ctx, "test/config/c/d", "www")
		assert.Eventually(t, func() bool {
			_, exist = mgr.GetCachedValue("cd")
			return !exist
		}, 5*time.Second, 100*time.Millisecond)
	}
}

type ErrSource struct{}

func (e ErrSource) Close() {
}

func (e ErrSource) GetConfigurationByKey(string) (string, error) {
	return "", errors.New("error")
}

// GetConfigurations implements Source
func (ErrSource) GetConfigurations() (map[string]string, error) {
	return nil, errors.New("error")
}

// GetPriority implements Source
func (ErrSource) GetPriority() int {
	return 2
}

func (ErrSource) SetManager(m ConfigManager) {
}

// GetSourceName implements Source
func (ErrSource) GetSourceName() string {
	return "ErrSource"
}

func (e ErrSource) SetEventHandler(eh EventHandler) {
}

func (e ErrSource) UpdateOptions(opt Options) {
}

func TestAlterConfigsInEtcd(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = "/tmp/milvus/test_alter_configs"
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()
	defer os.RemoveAll(cfg.Dir)

	mgr, _ := Init(WithEtcdSource(&EtcdInfo{
		Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
		KeyPrefix:       "test",
		RefreshInterval: 10 * time.Millisecond,
	}))

	etcdSource, ok := mgr.GetEtcdSource()
	assert.True(t, ok, "should get etcd source")

	t.Run("update multiple configs atomically", func(t *testing.T) {
		configs := map[string]string{
			"config.key1": "value1",
			"config.key2": "value2",
			"config.key3": "value3",
		}

		err := mgr.AlterConfigsInEtcd(etcdSource, configs, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			for key, expectedValue := range configs {
				_, actualValue, err := mgr.GetConfig(key)
				if err != nil || actualValue != expectedValue {
					return false
				}
			}
			return true
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("update single config via helper", func(t *testing.T) {
		err := mgr.UpdateConfigInEtcd(etcdSource, "single.key", "single.value")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("single.key")
			return err == nil && value == "single.value"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("empty updates and deletes should fail", func(t *testing.T) {
		err := mgr.AlterConfigsInEtcd(etcdSource, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no configs to alter")
	})

	t.Run("nil etcd source should fail", func(t *testing.T) {
		err := mgr.AlterConfigsInEtcd(nil, map[string]string{"key": "value"}, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "etcd client is not available")
	})

	t.Run("overwrite existing config", func(t *testing.T) {
		err := mgr.UpdateConfigInEtcd(etcdSource, "overwrite.key", "initial.value")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("overwrite.key")
			return err == nil && value == "initial.value"
		}, time.Second*5, 100*time.Millisecond)

		err = mgr.UpdateConfigInEtcd(etcdSource, "overwrite.key", "updated.value")
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("overwrite.key")
			return err == nil && value == "updated.value"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("batch update with key normalization", func(t *testing.T) {
		configs := map[string]string{
			"config/key/with/slashes": "value1",
			"config.key.with.dots":    "value2",
		}

		err := mgr.AlterConfigsInEtcd(etcdSource, configs, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, value1, err1 := mgr.GetConfig("config.key.with.slashes")
			_, value2, err2 := mgr.GetConfig("config.key.with.dots")
			return err1 == nil && value1 == "value1" && err2 == nil && value2 == "value2"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("delete configs from etcd", func(t *testing.T) {
		// First write some configs
		err := mgr.AlterConfigsInEtcd(etcdSource, map[string]string{
			"delete.key1": "value1",
			"delete.key2": "value2",
		}, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("delete.key1")
			_, v2, err2 := mgr.GetConfig("delete.key2")
			return err1 == nil && v1 == "value1" && err2 == nil && v2 == "value2"
		}, time.Second*5, 100*time.Millisecond)

		// Delete them
		err = mgr.AlterConfigsInEtcd(etcdSource, nil, []string{"delete.key1", "delete.key2"})
		assert.NoError(t, err)
	})

	t.Run("mixed update and delete in one transaction", func(t *testing.T) {
		// Setup: write two configs
		err := mgr.AlterConfigsInEtcd(etcdSource, map[string]string{
			"mixed.keep":   "old_value",
			"mixed.remove": "to_be_deleted",
		}, nil)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("mixed.keep")
			_, v2, err2 := mgr.GetConfig("mixed.remove")
			return err1 == nil && v1 == "old_value" && err2 == nil && v2 == "to_be_deleted"
		}, time.Second*5, 100*time.Millisecond)

		// Atomically: update one, delete the other
		err = mgr.AlterConfigsInEtcd(etcdSource,
			map[string]string{"mixed.keep": "new_value"},
			[]string{"mixed.remove"},
		)
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			_, v, err := mgr.GetConfig("mixed.keep")
			return err == nil && v == "new_value"
		}, time.Second*5, 100*time.Millisecond)
	})
}
