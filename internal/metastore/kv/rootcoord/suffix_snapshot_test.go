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
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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
	// The GC goroutine calls WalkWithPrefixFrom on startup
	kv.EXPECT().
		WalkWithPrefixFrom(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	kv.EXPECT().
		WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	ss, err := NewSuffixSnapshot(kv, "_ts", "root/", snapshotPrefix)
	require.NoError(t, err)
	require.NotNil(t, ss)
	defer ss.Close()
}

func Test_SuffixSnapshotSaveAndLoad(t *testing.T) {
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

	// Save with ts=100 creates both plain key and snapshot key
	err = ss.Save(ctx, "key1", "value-1", 100)
	assert.NoError(t, err)

	// Load latest
	val, err := ss.Load(ctx, "key1", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value-1", val)

	// Overwrite with ts=200
	err = ss.Save(ctx, "key1", "value-2", 200)
	assert.NoError(t, err)

	// Load latest returns value-2
	val, err = ss.Load(ctx, "key1", typeutil.MaxTimestamp)
	assert.NoError(t, err)
	assert.Equal(t, "value-2", val)

	// Time-travel: load at ts=150 should return value-1 (the version at ts=100)
	val, err = ss.Load(ctx, "key1", 150)
	assert.NoError(t, err)
	assert.Equal(t, "value-1", val)

	// Time-travel: load at ts=100 should return value-1
	val, err = ss.Load(ctx, "key1", 100)
	assert.NoError(t, err)
	assert.Equal(t, "value-1", val)

	// Time-travel: load at ts=200 should return value-2
	val, err = ss.Load(ctx, "key1", 200)
	assert.NoError(t, err)
	assert.Equal(t, "value-2", val)

	// Load non-existent key
	_, err = ss.Load(ctx, "non-existent", 0)
	assert.Error(t, err)

	// Save with ts creates snapshot keys
	keys := make([]string, 0)
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		keys = append(keys, string(k))
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, len(keys) >= 2, "Save with ts should create snapshot keys, got %d", len(keys))

	// Cleanup
	ss.RemoveWithPrefix(ctx, "")
}

func Test_SuffixSnapshotLoadTombstone(t *testing.T) {
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

	// MultiSave with ts creates snapshot keys
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
	kv.EXPECT().
		WalkWithPrefixFrom(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	require.NotNil(t, ss)
	defer ss.Close()

	keys, values, err := ss.LoadWithPrefix(context.TODO(), "prefix", 0)
	assert.Error(t, err)
	assert.Equal(t, 0, len(keys))
	assert.Equal(t, 0, len(values))
}

func Test_SuffixSnapshotMultiSaveAndRemove(t *testing.T) {
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

	// MultiSaveAndRemove: save new key and tombstone old key
	saves := map[string]string{"new-key": "new-val"}
	removals := []string{"remove-me"}
	err = ss.MultiSaveAndRemove(ctx, saves, removals, 200)
	assert.NoError(t, err)

	// Verify removed key returns tombstone error
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

	// Remove by prefix — writes tombstones
	saves := map[string]string{"saved-key": "saved-val"}
	removals := []string{"group/"}
	err = ss.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, 200)
	assert.NoError(t, err)

	// All group/ keys should return tombstone errors
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

func Test_GCOneBatch_BasicExpiry(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-basic-%d", randVal)
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

	// Create 20 expired snapshot keys across 4 key groups (5 versions each)
	// Use very old timestamps to ensure they're expired
	for group := 0; group < 4; group++ {
		for ver := 0; ver < 5; ver++ {
			ts := uint64(438497159122780160 + group*5 + ver) // old timestamp, definitely expired
			key := fmt.Sprintf("%s/root-coord/fields/%d/%d%s%d",
				snapshotPrefix, 100+group, 1000+group, sep, ts)
			err = etcdKV.Save(ctx, key, fmt.Sprintf("value-%d-%d", group, ver))
			require.NoError(t, err)
		}
	}

	// Verify keys exist
	count := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 20, count)

	// Run gcOneBatch
	deleted, scanned, err := ss.gcOneBatch(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 20, scanned)
	// Should delete all but the latest per group (4 groups * 4 deleted = 16)
	assert.Equal(t, 16, deleted)

	// Verify only 4 keys remain (latest per group)
	remaining := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		remaining++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, remaining)

	// Cursor should be reset (scanned < batchSize)
	assert.Equal(t, "", ss.gcCursor)

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

func Test_GCOneBatch_CursorProgress(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-cursor-%d", randVal)
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

	// Create 30 snapshot keys
	for i := 0; i < 30; i++ {
		ts := uint64(438497159122780160 + i)
		key := fmt.Sprintf("%s/root-coord/fields/%d/%d%s%d",
			snapshotPrefix, 100+i, 1000+i, sep, ts)
		err = etcdKV.Save(ctx, key, fmt.Sprintf("value-%d", i))
		require.NoError(t, err)
	}

	// Run with batchSize=10 — should process first 10 keys
	deleted1, scanned1, err := ss.gcOneBatch(ctx, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, scanned1)
	assert.True(t, ss.gcCursor != "", "cursor should advance after partial scan")

	// Run again — should process next 10 keys
	_, scanned2, err := ss.gcOneBatch(ctx, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, scanned2)
	assert.True(t, ss.gcCursor != "", "cursor should still be set")

	// Run again — should process remaining 10 keys and wrap around
	_, scanned3, err := ss.gcOneBatch(ctx, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, scanned3)

	// Each key is a unique group with 1 version, so it's the "latest" and protected.
	// The important assertions are about cursor mechanics:
	// - deleted1 should be 0 (all are latest-per-group, single version each)
	assert.Equal(t, 0, deleted1, "single-version keys are latest-per-group, should not be deleted")

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

func Test_GCOneBatch_PreservesLatestPerGroup(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-preserve-%d", randVal)
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

	// Create 5 versions of same key, all expired
	originalKey := "root-coord/fields/100/1000"
	for ver := 0; ver < 5; ver++ {
		ts := uint64(438497159122780160 + ver)
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, originalKey, sep, ts)
		err = etcdKV.Save(ctx, key, fmt.Sprintf("version-%d", ver))
		require.NoError(t, err)
	}

	// Run gcOneBatch
	deleted, scanned, err := ss.gcOneBatch(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 5, scanned)
	// Should delete 4, preserve latest (version-4)
	assert.Equal(t, 4, deleted)

	// Verify only latest version remains
	remaining := make([]string, 0)
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix, 100, func(k []byte, v []byte) error {
		remaining = append(remaining, string(v))
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remaining))
	assert.Equal(t, "version-4", remaining[0])

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

func Test_GCOneBatch_TombstonePlainKeyCleanup(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-tombstone-%d", randVal)
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

	// Simulate a dropped collection: plain key has tombstone, snapshot keys have tombstone values
	originalKey := "root-coord/fields/999/1000"
	plainKeyPath := originalKey

	// Write tombstone to plain key (as MultiSaveAndRemove would do)
	err = etcdKV.Save(ctx, plainKeyPath, string(SuffixSnapshotTombstone))
	require.NoError(t, err)

	// Write 3 expired tombstone snapshot keys
	for ver := 0; ver < 3; ver++ {
		ts := uint64(438497159122780160 + ver) // very old timestamps
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, originalKey, sep, ts)
		err = etcdKV.Save(ctx, key, string(SuffixSnapshotTombstone))
		require.NoError(t, err)
	}

	// Also create a non-tombstone key group (should NOT have plain key deleted)
	liveOriginalKey := "root-coord/fields/888/1000"
	err = etcdKV.Save(ctx, liveOriginalKey, "live-value")
	require.NoError(t, err)
	for ver := 0; ver < 2; ver++ {
		ts := uint64(438497159122780160 + ver)
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, liveOriginalKey, sep, ts)
		err = etcdKV.Save(ctx, key, fmt.Sprintf("version-%d", ver))
		require.NoError(t, err)
	}

	// Run gcOneBatch — should clean up tombstone group completely
	deleted, scanned, err := ss.gcOneBatch(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 5, scanned) // 3 tombstone + 2 live snapshot keys

	// Tombstone group: all 3 snapshot keys + 1 plain key = 4 deleted
	// Live group: 1 deleted (oldest), 1 kept (latest)
	assert.Equal(t, 5, deleted) // 4 tombstone + 1 live oldest

	// Verify tombstone plain key is gone
	_, err = etcdKV.Load(ctx, plainKeyPath)
	assert.Error(t, err, "tombstone plain key should be deleted by GC")

	// Verify tombstone snapshot keys are all gone
	tombstoneRemaining := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix+"/"+originalKey, 100, func(k []byte, v []byte) error {
		tombstoneRemaining++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, tombstoneRemaining, "all tombstone snapshot keys should be deleted")

	// Verify live plain key still exists
	val, err := etcdKV.Load(ctx, liveOriginalKey)
	assert.NoError(t, err)
	assert.Equal(t, "live-value", val)

	// Verify live group's latest snapshot key still exists
	liveRemaining := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix+"/"+liveOriginalKey, 100, func(k []byte, v []byte) error {
		liveRemaining++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, liveRemaining, "latest live snapshot key should be preserved")

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

func Test_GCOneBatch_WalkError(t *testing.T) {
	sep := "_ts"
	rootPath := "root/"
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().
		WalkWithPrefixFrom(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("walk error"))
	kv.EXPECT().
		WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
	require.NoError(t, err)
	defer ss.Close()

	deleted, scanned, err := ss.gcOneBatch(context.TODO(), 100)
	assert.Error(t, err)
	assert.Equal(t, 0, deleted)
	assert.Equal(t, 0, scanned)
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

func Test_BinarySearchRecords(t *testing.T) {
	records := []tsv{
		{value: "v1", ts: 100},
		{value: "v2", ts: 200},
		{value: "v3", ts: 300},
	}

	// Exact match
	val, found := binarySearchRecords(records, 200)
	assert.True(t, found)
	assert.Equal(t, "v2", val)

	// Between versions
	val, found = binarySearchRecords(records, 150)
	assert.True(t, found)
	assert.Equal(t, "v1", val)

	// After all versions
	val, found = binarySearchRecords(records, 500)
	assert.True(t, found)
	assert.Equal(t, "v3", val)

	// Before all versions
	_, found = binarySearchRecords(records, 50)
	assert.False(t, found)

	// Empty records
	_, found = binarySearchRecords(nil, 100)
	assert.False(t, found)
}

// Test_GCOneBatch_DualTTLSemantics verifies that non-tombstone keys between
// reserveTime (1h) and ttlTime (24h) are NOT deleted, while tombstone keys
// of the same age ARE deleted. This is the core dual-TTL semantic.
func Test_GCOneBatch_DualTTLSemantics(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-dual-ttl-%d", randVal)
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

	// Create timestamps at 2 hours ago — between reserveTime (1h) and ttlTime (24h)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)

	// Non-tombstone group: 3 versions at ~2h ago
	liveKey := "root-coord/fields/100/2000"
	for ver := 0; ver < 3; ver++ {
		ts := tsoutil.ComposeTSByTime(twoHoursAgo.Add(time.Duration(ver)*time.Second), 0)
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, liveKey, sep, ts)
		err = etcdKV.Save(ctx, key, fmt.Sprintf("live-v%d", ver))
		require.NoError(t, err)
	}

	// Tombstone group: 3 versions at ~2h ago, all tombstone values
	tombKey := "root-coord/fields/200/3000"
	err = etcdKV.Save(ctx, tombKey, string(SuffixSnapshotTombstone))
	require.NoError(t, err)
	for ver := 0; ver < 3; ver++ {
		ts := tsoutil.ComposeTSByTime(twoHoursAgo.Add(time.Duration(ver)*time.Second), 0)
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, tombKey, sep, ts)
		err = etcdKV.Save(ctx, key, string(SuffixSnapshotTombstone))
		require.NoError(t, err)
	}

	// Run gcOneBatch
	deleted, scanned, err := ss.gcOneBatch(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 6, scanned)

	// Tombstone group (fully expired by reserveTime): all 3 snapshot keys + plain key = 4 deleted
	// Non-tombstone group: age=2h > reserveTime(1h) but < ttlTime(24h) → NOT deleted
	// Latest per non-tombstone group is also protected regardless.
	assert.Equal(t, 4, deleted)

	// Verify: non-tombstone snapshot keys all still exist
	liveRemaining := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix+"/"+liveKey, 100, func(k []byte, v []byte) error {
		liveRemaining++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, liveRemaining, "non-tombstone keys between reserveTime and ttlTime should be preserved")

	// Verify: tombstone snapshot keys all gone
	tombRemaining := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix+"/"+tombKey, 100, func(k []byte, v []byte) error {
		tombRemaining++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, tombRemaining, "tombstone snapshot keys should be deleted")

	// Verify: tombstone plain key also gone
	_, err = etcdKV.Load(ctx, tombKey)
	assert.Error(t, err, "tombstone plain key should be deleted")

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

// Test_GCOneBatch_PartiallyExpiredTombstoneGroup verifies that when a tombstone
// group has some expired and some non-expired snapshot keys, the latest is
// preserved and the plain key is NOT deleted.
func Test_GCOneBatch_PartiallyExpiredTombstoneGroup(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-partial-tomb-%d", randVal)
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

	// Tombstone group: 2 old expired versions + 1 recent (not expired) version
	originalKey := "root-coord/fields/300/4000"
	err = etcdKV.Save(ctx, originalKey, string(SuffixSnapshotTombstone))
	require.NoError(t, err)

	// 2 versions from 2 hours ago (expired by reserveTime)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	for ver := 0; ver < 2; ver++ {
		ts := tsoutil.ComposeTSByTime(twoHoursAgo.Add(time.Duration(ver)*time.Second), 0)
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, originalKey, sep, ts)
		err = etcdKV.Save(ctx, key, string(SuffixSnapshotTombstone))
		require.NoError(t, err)
	}

	// 1 version from 10 seconds ago (NOT expired by reserveTime)
	recentTime := time.Now().Add(-10 * time.Second)
	recentTS := tsoutil.ComposeTSByTime(recentTime, 0)
	recentKey := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, originalKey, sep, recentTS)
	err = etcdKV.Save(ctx, recentKey, string(SuffixSnapshotTombstone))
	require.NoError(t, err)

	// Run gcOneBatch
	deleted, scanned, err := ss.gcOneBatch(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 3, scanned)

	// Only 2 old versions are expired. The recent one is not expired and is the latest.
	// Group is NOT fully expired → plain key is NOT deleted, latest is protected.
	assert.Equal(t, 2, deleted)

	// Verify: plain key still exists
	val, err := etcdKV.Load(ctx, originalKey)
	assert.NoError(t, err)
	assert.Equal(t, string(SuffixSnapshotTombstone), val, "plain key should still exist for partially expired group")

	// Verify: latest snapshot key still exists
	latestRemaining := 0
	err = etcdKV.WalkWithPrefix(ctx, snapshotPrefix+"/"+originalKey, 100, func(k []byte, v []byte) error {
		latestRemaining++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, latestRemaining, "latest snapshot key should be preserved")

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

// Test_GCOneBatch_BoundaryGroupProtection verifies that when a tombstone group
// is split across batch boundaries, the plain key is NOT prematurely deleted.
func Test_GCOneBatch_BoundaryGroupProtection(t *testing.T) {
	randVal := rand.Int()

	rootPath := fmt.Sprintf("/test/meta/gc-boundary-%d", randVal)
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

	// Create a tombstone group with 5 expired snapshot keys.
	// Use batchSize=3 so the group is split: batch 1 sees 3 keys, batch 2 sees 2 keys.
	originalKey := "root-coord/fields/500/5000"
	err = etcdKV.Save(ctx, originalKey, string(SuffixSnapshotTombstone))
	require.NoError(t, err)

	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	for ver := 0; ver < 5; ver++ {
		ts := tsoutil.ComposeTSByTime(twoHoursAgo.Add(time.Duration(ver)*time.Second), 0)
		key := fmt.Sprintf("%s/%s%s%d", snapshotPrefix, originalKey, sep, ts)
		err = etcdKV.Save(ctx, key, string(SuffixSnapshotTombstone))
		require.NoError(t, err)
	}

	// Batch 1: batchSize=3, sees first 3 keys of the group.
	// The group is the lastGroup in the batch and should be protected.
	deleted1, scanned1, err := ss.gcOneBatch(ctx, 3)
	assert.NoError(t, err)
	assert.Equal(t, 3, scanned1)
	// Last group is protected from full cleanup, but 2 non-latest keys can still be deleted
	assert.Equal(t, 2, deleted1)

	// Plain key must still exist after batch 1
	val, err := etcdKV.Load(ctx, originalKey)
	assert.NoError(t, err)
	assert.Equal(t, string(SuffixSnapshotTombstone), val, "plain key must survive batch boundary")

	// Batch 2: sees remaining 2 keys. The group is the firstGroup (cursor continuing)
	// and should also be protected from full cleanup.
	deleted2, scanned2, err := ss.gcOneBatch(ctx, 3)
	assert.NoError(t, err)
	assert.Equal(t, 2, scanned2)
	// First group is protected, 1 non-latest key deleted, latest preserved
	assert.Equal(t, 1, deleted2)

	// Plain key must still exist after batch 2
	val, err = etcdKV.Load(ctx, originalKey)
	assert.NoError(t, err)
	assert.Equal(t, string(SuffixSnapshotTombstone), val, "plain key must survive batch boundary")

	// Batch 3: cursor wraps around (scanned < batchSize in batch 2).
	// Remaining: snap_ts2 (kept as latest in batch 1) + snap_ts4 (kept as latest in batch 2).
	// Both appear fully within this batch, group is fully expired → full cleanup.
	deleted3, scanned3, err := ss.gcOneBatch(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, scanned3)
	// 2 snapshot keys + 1 plain key = 3 deleted
	assert.Equal(t, 3, deleted3)

	// Plain key should now be gone
	_, err = etcdKV.Load(ctx, originalKey)
	assert.Error(t, err, "plain key should be deleted after full cleanup")

	// Cleanup
	etcdKV.RemoveWithPrefix(ctx, "")
}

// Test_Close_DoubleCall verifies that calling Close() twice does not panic.
func Test_Close_DoubleCall(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().
		WalkWithPrefixFrom(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	kv.EXPECT().
		WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	ss, err := NewSuffixSnapshot(kv, "_ts", "root/", snapshotPrefix)
	require.NoError(t, err)
	require.NotNil(t, ss)

	// First close should work normally
	ss.Close()

	// Second close should not panic
	assert.NotPanics(t, func() {
		ss.Close()
	})
}
