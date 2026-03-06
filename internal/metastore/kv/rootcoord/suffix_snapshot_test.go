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

package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var snapshotPrefix = "snapshots"

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

func Test_NewSuffixSnapshot_NilKV(t *testing.T) {
	_, err := NewSuffixSnapshot(nil, "_ts", "root", snapshotPrefix)
	assert.Error(t, err)
}

func Test_NewSuffixSnapshot_OK(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	// The cleanup goroutine calls WalkWithPrefix immediately on startup
	kv.EXPECT().
		WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	ss, err := NewSuffixSnapshot(kv, "_ts", "root/", snapshotPrefix)
	require.NoError(t, err)
	require.NotNil(t, ss)
	defer ss.Close()
}

func Test_SuffixSnapshotSaveAndLoad(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	require.NotNil(t, ss)
	defer ss.Close()

	ctx := context.TODO()

	// Save writes to plain key, ts is ignored
	err = ss.Save(ctx, "key1", "value-1", 100)
	assert.NoError(t, err)

	// Load reads the plain key, ts is ignored
	val, err := ss.Load(ctx, "key1", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value-1", val)

	// Overwrite
	err = ss.Save(ctx, "key1", "value-2", 200)
	assert.NoError(t, err)

	// Always reads the latest value regardless of ts
	val, err = ss.Load(ctx, "key1", 100)
	assert.NoError(t, err)
	assert.Equal(t, "value-2", val)

	val, err = ss.Load(ctx, "key1", typeutil.MaxTimestamp)
	assert.NoError(t, err)
	assert.Equal(t, "value-2", val)

	// Load non-existent key
	_, err = ss.Load(ctx, "non-existent", 0)
	assert.Error(t, err)

	// Save does not create snapshot keys
	keys := make([]string, 0)
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		keys = append(keys, string(k))
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(keys), "Save should not create snapshot keys")

	// Cleanup
	ss.RemoveWithPrefix(ctx, "")
}

func Test_SuffixSnapshotLoadTombstone(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	ctx := context.TODO()

	// Simulate legacy tombstone value in a plain key
	err = etcdKV.Save(ctx, "tombstone-key", string(SuffixSnapshotTombstone))
	assert.NoError(t, err)

	// Load should return error for tombstone values
	_, err = ss.Load(ctx, "tombstone-key", 0)
	assert.Error(t, err)

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

func Test_SuffixSnapshotMultiSave(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	ctx := context.TODO()

	// MultiSave writes plain keys, ts ignored
	saves := map[string]string{"k1": "v1", "k2": "v2"}
	err = ss.MultiSave(ctx, saves, 100)
	assert.NoError(t, err)

	val, err := ss.Load(ctx, "k1", 0)
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	val, err = ss.Load(ctx, "k2", 0)
	assert.NoError(t, err)
	assert.Equal(t, "v2", val)

	// Cleanup
	ss.RemoveWithPrefix(ctx, "")
}

func Test_SuffixSnapshotLoadWithPrefix(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	ctx := context.TODO()

	// Save some keys
	err = ss.Save(ctx, "prefix/a", "va", 100)
	assert.NoError(t, err)
	err = ss.Save(ctx, "prefix/b", "vb", 200)
	assert.NoError(t, err)
	// Save a tombstone value to test filtering
	err = etcdKV.Save(ctx, "prefix/c", string(SuffixSnapshotTombstone))
	assert.NoError(t, err)

	keys, vals, err := ss.LoadWithPrefix(ctx, "prefix/", 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, 2, len(vals))
	// Tombstone key should be filtered out
	for _, v := range vals {
		assert.NotEqual(t, string(SuffixSnapshotTombstone), v)
	}

	// Cleanup
	ss.RemoveWithPrefix(ctx, "")
}

func Test_SuffixSnapshotLoadWithPrefix_WalkError(t *testing.T) {
	sep := "_ts"
	rootPath := "root/"
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().
		WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("walk error"))

	ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	require.NotNil(t, ss)
	defer ss.Close()

	keys, values, err := ss.LoadWithPrefix(context.TODO(), "prefix", 100)
	assert.Error(t, err)
	assert.Equal(t, 0, len(keys))
	assert.Equal(t, 0, len(values))
}

func Test_SuffixSnapshotMultiSaveAndRemove(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	ctx := context.TODO()

	// Save some keys first
	err = ss.Save(ctx, "keep", "keep-val", 100)
	assert.NoError(t, err)
	err = ss.Save(ctx, "remove-me", "remove-val", 100)
	assert.NoError(t, err)

	// MultiSaveAndRemove: save new key and physically remove old key
	saves := map[string]string{"new-key": "new-val"}
	removals := []string{"remove-me"}
	err = ss.MultiSaveAndRemove(ctx, saves, removals, 200)
	assert.NoError(t, err)

	// Verify removed key is physically deleted
	_, err = ss.Load(ctx, "remove-me", 0)
	assert.Error(t, err)

	// Verify saved keys exist
	val, err := ss.Load(ctx, "keep", 0)
	assert.NoError(t, err)
	assert.Equal(t, "keep-val", val)

	val, err = ss.Load(ctx, "new-key", 0)
	assert.NoError(t, err)
	assert.Equal(t, "new-val", val)

	// Cleanup
	ss.MultiSaveAndRemoveWithPrefix(ctx, map[string]string{}, []string{""}, 0)
}

func Test_SuffixSnapshotMultiSaveAndRemoveWithPrefix(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	ctx := context.TODO()

	// Save keys with a common prefix
	for i := 0; i < 5; i++ {
		err = ss.Save(ctx, fmt.Sprintf("group/item-%d", i), fmt.Sprintf("val-%d", i), 100)
		assert.NoError(t, err)
	}
	err = ss.Save(ctx, "other-key", "other-val", 100)
	assert.NoError(t, err)

	// Remove by prefix
	saves := map[string]string{"saved-key": "saved-val"}
	removals := []string{"group/"}
	err = ss.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, 200)
	assert.NoError(t, err)

	// All group/ keys should be physically deleted
	keys, _, err := ss.LoadWithPrefix(ctx, "group/", 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(keys))

	// other-key should still exist
	val, err := ss.Load(ctx, "other-key", 0)
	assert.NoError(t, err)
	assert.Equal(t, "other-val", val)

	// Cleanup
	ss.MultiSaveAndRemoveWithPrefix(ctx, map[string]string{}, []string{""}, 0)
}

func Test_SuffixSnapshotLegacyCleanup(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/legacy-cleanup-%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdKV.Close()

	ss, err := NewSuffixSnapshot(etcdKV, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	ctx := context.TODO()

	// Pre-write legacy snapshot keys (simulating old version data)
	legacyKeys := 50
	for i := 0; i < legacyKeys; i++ {
		key := fmt.Sprintf("%s/root-coord/fields/%d/%d_ts%d", snapshotPrefix, 100+i%5, 1000+i, 438497159122780160+int64(i))
		err = etcdKV.Save(ctx, key, fmt.Sprintf("legacy-value-%d", i))
		assert.NoError(t, err)
	}

	// Verify legacy keys exist
	count := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		count++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, legacyKeys, count)

	// Run cleanup
	cleaned, err := ss.cleanLegacySnapshots(ctx)
	assert.NoError(t, err)
	assert.Equal(t, legacyKeys, cleaned)

	// Verify all legacy keys are cleaned
	count = 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		count++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// Second cleanup should return 0 (nothing left)
	cleaned, err = ss.cleanLegacySnapshots(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, cleaned)

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

func Test_SuffixSnapshotLegacyCleanup_WalkError(t *testing.T) {
	sep := "_ts"
	rootPath := "root/"
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().
		WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("walk error"))

	ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	cleaned, err := ss.cleanLegacySnapshots(context.TODO())
	assert.Error(t, err)
	assert.Equal(t, 0, cleaned)
}

func Test_IsTombstone(t *testing.T) {
	assert.True(t, IsTombstone(string(SuffixSnapshotTombstone)))
	assert.False(t, IsTombstone("normal-value"))
	assert.False(t, IsTombstone(""))
}

func Test_ComposeSnapshotKey(t *testing.T) {
	result := ComposeSnapshotKey("snapshots/", "key1", "_ts", 12345)
	assert.Equal(t, "snapshots/key1_ts12345", result)
}
