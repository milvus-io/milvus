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
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
			Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	ctx := context.Background()

	t.Run("origin is empty", func(t *testing.T) {
		_, _, err = mgr.GetConfig("test.etcd")
		assert.ErrorIs(t, err, ErrKeyNotFound)

		client.Put(ctx, "test/config/test/etcd", "value")

		time.Sleep(100 * time.Millisecond)

		_, v, err := mgr.GetConfig("test.etcd")
		assert.NoError(t, err)
		assert.Equal(t, "value", v)
		_, v, err = mgr.GetConfig("TEST_ETCD")
		assert.NoError(t, err)
		assert.Equal(t, "value", v)

		client.Delete(ctx, "test/config/test/etcd")
		time.Sleep(100 * time.Millisecond)

		_, _, err = mgr.GetConfig("TEST_ETCD")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("override origin value", func(t *testing.T) {
		_, v, _ := mgr.GetConfig("tmp.key")
		assert.Equal(t, "1", v)
		client.Put(ctx, "test/config/tmp/key", "2")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "2", v)

		client.Put(ctx, "test/config/tmp/key", "3")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "3", v)

		client.Delete(ctx, "test/config/tmp/key")
		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "1", v)
	})

	t.Run("multi priority", func(t *testing.T) {
		_, v, _ := mgr.GetConfig("log.level")
		assert.Equal(t, "info", v)
		client.Put(ctx, "test/config/log/level", "error")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("log.level")
		assert.Equal(t, "error", v)

		client.Delete(ctx, "test/config/log/level")
		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("log.level")
		assert.Equal(t, "info", v)
	})

	t.Run("close manager", func(t *testing.T) {
		mgr.Close()

		client.Put(ctx, "test/config/test/etcd", "value2")
		assert.Eventually(t, func() bool {
			_, _, err = mgr.GetConfig("test.etcd")
			return err != nil && errors.Is(err, ErrKeyNotFound)
		}, 300*time.Millisecond, 10*time.Millisecond)
	})
}

// FormatKey is what a guard on a specific config key has to compare against.
// Separators are stripped rather than translated, so a guard that lowercases
// and swaps "/" for "." -- the obvious hand-rolled version -- lets the
// underscore and separator-free spellings through to the same stored key.
func TestFormatKeyCollapsesEverySpellingOfAKey(t *testing.T) {
	identity := FormatKey("mq.type")
	assert.Equal(t, "mqtype", identity)

	for _, spelling := range []string{
		"mq.type",
		"mq_type",
		"mq/type",
		"MQ.TYPE",
		"mqtype",
		"mq_Type",
	} {
		assert.Equal(t, identity, FormatKey(spelling), spelling)
	}

	assert.NotEqual(t, identity, FormatKey("common.security.authorizationEnabled"))
}

// /management/config/get and /management/config/alter normalize caller-supplied
// keys, so the memo cannot grow with whatever a caller sends. Past the bound the
// answer has to stay correct -- it is only the caching that stops.
func TestFormatKeyMemoIsBounded(t *testing.T) {
	saved := formattedKeys
	formattedKeys = typeutil.NewConcurrentMap[string, string]()
	t.Cleanup(func() { formattedKeys = saved })

	for i := 0; i < maxFormattedKeys*2; i++ {
		key := fmt.Sprintf("caller.supplied.key_%d", i)
		assert.Equal(t, normalizeKey(key), FormatKey(key), key)
	}
	assert.LessOrEqual(t, formattedKeys.Len(), maxFormattedKeys,
		"an anonymous caller must not be able to grow the normalization memo without limit")

	// A real key still normalizes correctly with the memo full.
	assert.Equal(t, "mqtype",
		FormatKey("mq.type"))
}

func TestFormatKeyMemoIsStrictlyBoundedUnderConcurrency(t *testing.T) {
	saved := formattedKeys
	formattedKeys = typeutil.NewConcurrentMap[string, string]()
	t.Cleanup(func() { formattedKeys = saved })

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < maxFormattedKeys*2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			FormatKey(fmt.Sprintf("concurrent.caller.key_%d", i))
		}(i)
	}
	close(start)
	wg.Wait()

	assert.LessOrEqual(t, formattedKeys.Len(), maxFormattedKeys)
}

// A count bound alone still retains megabytes per entry when HTTP callers send
// large unknown keys. Normalization must work without retaining those strings.
func TestFormatKeyMemoRejectsOversizedKeys(t *testing.T) {
	saved := formattedKeys
	formattedKeys = typeutil.NewConcurrentMap[string, string]()
	t.Cleanup(func() { formattedKeys = saved })

	for i := 0; i < 16; i++ {
		suffix := fmt.Sprint(i)
		key := strings.Repeat("A._/É", 1024) + suffix
		assert.Equal(t, strings.Repeat("aé", 1024)+suffix, FormatKey(key))
	}
	assert.Zero(t, formattedKeys.Len(), "oversized keys must not enter the memo")

	// Unicode case folding can grow UTF-8 output past the input's byte length.
	key := strings.Repeat("Ⱥ", 512)
	assert.Equal(t, strings.Repeat("ⱥ", 512), FormatKey(key))
	assert.Zero(t, formattedKeys.Len(), "oversized normalized values must not enter the memo")

	special := NotFormatPrefix + strings.Repeat("A._/", 1024)
	assert.Equal(t, special, FormatKey(special), "knowhere keys retain their existing identity")
	assert.Zero(t, formattedKeys.Len())
	assert.Equal(t, "mqtype", FormatKey("mq.type"))
	assert.Equal(t, 1, formattedKeys.Len(), "ordinary config keys still use the memo")
}

// URL.Query can return a short key as a substring of an otherwise huge request.
// Retaining that substring keeps the entire request allocation alive even when
// the memo checks len(key). Both memo strings must own only their small bytes.
func TestFormatKeyMemoDoesNotRetainRequestBackingString(t *testing.T) {
	saved := formattedKeys
	formattedKeys = typeutil.NewConcurrentMap[string, string]()
	t.Cleanup(func() { formattedKeys = saved })

	for _, name := range []string{"shortkey", "SHORTKEY"} {
		requestURL := &url.URL{RawQuery: "keys=" + name + "&padding=" + strings.Repeat("padding", 1<<15)}
		key := requestURL.Query().Get("keys")
		assert.Equal(t, "shortkey", FormatKey(key))
		found := false
		formattedKeys.Range(func(cachedKey, cachedValue string) bool {
			if cachedKey == key {
				found = true
				assert.False(t, unsafe.StringData(key) == unsafe.StringData(cachedKey),
					"the cached key must not retain a caller's large backing string")
				assert.False(t, unsafe.StringData(key) == unsafe.StringData(cachedValue),
					"the cached value must not retain a caller's large backing string")
			}
			return true
		})
		assert.True(t, found, "ordinary short keys must remain memoized")
	}
}
