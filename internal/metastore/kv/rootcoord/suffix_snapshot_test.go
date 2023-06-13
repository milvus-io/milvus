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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	snapshotPrefix = "snapshots"
)

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	Params.Init()
	code := m.Run()
	os.Exit(code)
}

func Test_binarySearchRecords(t *testing.T) {
	type testcase struct {
		records     []tsv
		ts          typeutil.Timestamp
		expected    string
		shouldFound bool
	}

	cases := []testcase{
		{
			records:     []tsv{},
			ts:          0,
			expected:    "",
			shouldFound: false,
		},
		{
			records: []tsv{
				{
					ts:    101,
					value: "abc",
				},
			},
			ts:          100,
			expected:    "",
			shouldFound: false,
		},
		{
			records: []tsv{
				{
					ts:    100,
					value: "a",
				},
			},
			ts:          100,
			expected:    "a",
			shouldFound: true,
		},
		{
			records: []tsv{
				{
					ts:    100,
					value: "a",
				},
				{
					ts:    200,
					value: "b",
				},
			},
			ts:          100,
			expected:    "a",
			shouldFound: true,
		},
		{
			records: []tsv{
				{
					ts:    100,
					value: "a",
				},
				{
					ts:    200,
					value: "b",
				},
				{
					ts:    300,
					value: "c",
				},
			},
			ts:          150,
			expected:    "a",
			shouldFound: true,
		},
		{
			records: []tsv{
				{
					ts:    100,
					value: "a",
				},
				{
					ts:    200,
					value: "b",
				},
				{
					ts:    300,
					value: "c",
				},
			},
			ts:          300,
			expected:    "c",
			shouldFound: true,
		},
		{
			records: []tsv{
				{
					ts:    100,
					value: "a",
				},
				{
					ts:    200,
					value: "b",
				},
				{
					ts:    300,
					value: "c",
				},
			},
			ts:          201,
			expected:    "b",
			shouldFound: true,
		},
		{
			records: []tsv{
				{
					ts:    100,
					value: "a",
				},
				{
					ts:    200,
					value: "b",
				},
				{
					ts:    300,
					value: "c",
				},
			},
			ts:          301,
			expected:    "c",
			shouldFound: true,
		},
	}
	for _, c := range cases {
		result, found := binarySearchRecords(c.records, c.ts)
		assert.Equal(t, c.expected, result)
		assert.Equal(t, c.shouldFound, found)
	}
}

func Test_ComposeIsTsKey(t *testing.T) {
	sep := "_ts"
	ss, err := NewSuffixSnapshot(etcdkv.NewEtcdKV(nil, ""), sep, "", snapshotPrefix)
	require.Nil(t, err)
	defer ss.Close()

	type testcase struct {
		key         string
		expected    uint64
		shouldFound bool
	}
	testCases := []testcase{
		{
			key:         ss.composeTSKey("key", 100),
			expected:    100,
			shouldFound: true,
		},
		{
			key:         ss.composeTSKey("other-key", 65536),
			expected:    65536,
			shouldFound: true,
		},
		{
			key:         "snapshots/test/1000",
			expected:    0,
			shouldFound: false,
		},
		{
			key:         "snapshots",
			expected:    0,
			shouldFound: false,
		},
	}
	for _, c := range testCases {
		ts, found := ss.isTSKey(c.key)
		assert.EqualValues(t, c.expected, ts)
		assert.Equal(t, c.shouldFound, found)
	}
}

func Test_SuffixSnaphotIsTSOfKey(t *testing.T) {
	sep := "_ts"
	ss, err := NewSuffixSnapshot(etcdkv.NewEtcdKV(nil, ""), sep, "", snapshotPrefix)
	require.Nil(t, err)
	defer ss.Close()

	type testcase struct {
		key         string
		target      string
		expected    uint64
		shouldFound bool
	}
	testCases := []testcase{
		{
			key:         ss.composeTSKey("key", 100),
			target:      "key",
			expected:    100,
			shouldFound: true,
		},
		{
			key:         ss.composeTSKey("other-key", 65536),
			target:      "other-key",
			expected:    65536,
			shouldFound: true,
		},
		{
			key:         ss.composeTSKey("other-key", 65536),
			target:      "key",
			expected:    0,
			shouldFound: false,
		},
		{
			key:         "snapshots/test/1000",
			expected:    0,
			shouldFound: false,
		},
		{
			key:         "snapshots",
			expected:    0,
			shouldFound: false,
		},
	}
	for _, c := range testCases {
		ts, found := ss.isTSOfKey(c.key, c.target)
		assert.EqualValues(t, c.expected, ts)
		assert.Equal(t, c.shouldFound, found)
	}

}

func Test_SuffixSnapshotLoad(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
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
	require.Nil(t, err)
	defer etcdCli.Close()
	etcdkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdkv.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ss, err := NewSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.NoError(t, err)
	assert.NotNil(t, ss)
	defer ss.Close()

	for i := 0; i < 20; i++ {
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.Save("key", fmt.Sprintf("value-%d", i), ts)
		assert.NoError(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		val, err := ss.Load("key", typeutil.Timestamp(100+i*5+2))
		t.Log("ts:", typeutil.Timestamp(100+i*5+2), i, val)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
	}
	val, err := ss.Load("key", 0)
	assert.NoError(t, err)
	assert.Equal(t, "value-19", val)

	for i := 0; i < 20; i++ {
		val, err := ss.Load("key", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, val, fmt.Sprintf("value-%d", i))
	}

	ss.RemoveWithPrefix("")
}

func Test_SuffixSnapshotMultiSave(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
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
	require.Nil(t, err)
	defer etcdCli.Close()
	etcdkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdkv.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ss, err := NewSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.NoError(t, err)
	assert.NotNil(t, ss)
	defer ss.Close()

	for i := 0; i < 20; i++ {
		saves := map[string]string{"k1": fmt.Sprintf("v1-%d", i), "k2": fmt.Sprintf("v2-%d", i)}
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.MultiSave(saves, ts)
		assert.NoError(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		keys, vals, err := ss.LoadWithPrefix("k", typeutil.Timestamp(100+i*5+2))
		t.Log(i, keys, vals)
		assert.NoError(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, len(keys), 2)
		assert.Equal(t, keys[0], "k1")
		assert.Equal(t, keys[1], "k2")
		assert.Equal(t, vals[0], fmt.Sprintf("v1-%d", i))
		assert.Equal(t, vals[1], fmt.Sprintf("v2-%d", i))
	}
	keys, vals, err := ss.LoadWithPrefix("k", 0)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, keys[0], "k1")
	assert.Equal(t, keys[1], "k2")
	assert.Equal(t, vals[0], "v1-19")
	assert.Equal(t, vals[1], "v2-19")

	for i := 0; i < 20; i++ {
		keys, vals, err := ss.LoadWithPrefix("k", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, len(keys), 2)
		assert.ElementsMatch(t, keys, []string{"k1", "k2"})
		assert.ElementsMatch(t, vals, []string{fmt.Sprintf("v1-%d", i), fmt.Sprintf("v2-%d", i)})
	}
	// mix non ts k-v
	err = ss.Save("kextra", "extra-value", 0)
	assert.NoError(t, err)
	keys, vals, err = ss.LoadWithPrefix("k", typeutil.Timestamp(300))
	assert.NoError(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)
	assert.ElementsMatch(t, keys, []string{"k1", "k2"})
	assert.ElementsMatch(t, vals, []string{"v1-19", "v2-19"})

	// clean up
	ss.RemoveWithPrefix("")
}

func Test_SuffixSnapshotRemoveExpiredKvs(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/remove-expired-test-%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	assert.NoError(t, err)
	defer etcdkv.Close()

	ss, err := NewSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.NoError(t, err)
	assert.NotNil(t, ss)
	defer ss.Close()

	saveFn := func(key, value string, ts typeutil.Timestamp) {
		err = ss.Save(key, value, ts)
		assert.NoError(t, err)
	}

	multiSaveFn := func(kvs map[string]string, ts typeutil.Timestamp) {
		err = ss.MultiSave(kvs, ts)
		assert.NoError(t, err)
	}

	now := time.Now()
	ftso := func(ts int) typeutil.Timestamp {
		return tsoutil.ComposeTS(now.Add(-1*time.Duration(ts)*time.Millisecond).UnixMilli(), 0)
	}

	getKey := func(prefix string, id int) string {
		return fmt.Sprintf("%s-%d", prefix, id)
	}

	generateTestData := func(prefix string, kCnt int, kVersion int, expiredKeyCnt int) {
		var value string
		cnt := 0
		for i := 0; i < kVersion; i++ {
			kvs := make(map[string]string)
			ts := ftso((i + 1) * 100)
			for v := 0; v < kCnt; v++ {
				if i == 0 && v%2 == 0 && cnt < expiredKeyCnt {
					value = string(SuffixSnapshotTombstone)
					cnt++
				} else {
					value = "v"
				}

				kvs[getKey(prefix, v)] = value
				if v%25 == 0 {
					multiSaveFn(kvs, ts)
					kvs = make(map[string]string)
				}
			}
			multiSaveFn(kvs, ts)
		}
	}

	countPrefix := func(prefix string) int {
		cnt := 0
		err := etcdkv.WalkWithPrefix("", 10, func(key []byte, value []byte) error {
			cnt++
			return nil
		})
		assert.NoError(t, err)
		return cnt
	}

	t.Run("Mixed test ", func(t *testing.T) {
		prefix := fmt.Sprintf("prefix%d", rand.Int())
		keyCnt := 500
		keyVersion := 3
		expiredKCnt := 100
		generateTestData(prefix, keyCnt, keyVersion, expiredKCnt)

		cnt := countPrefix(prefix)
		assert.Equal(t, keyCnt*keyVersion+keyCnt, cnt)

		err = ss.removeExpiredKvs(now, time.Duration(50)*time.Millisecond)
		assert.NoError(t, err)

		cnt = countPrefix(prefix)
		assert.Equal(t, keyCnt*keyVersion+keyCnt-(expiredKCnt*keyVersion+expiredKCnt), cnt)

		// clean all data
		err := etcdkv.RemoveWithPrefix("")
		assert.NoError(t, err)
	})

	t.Run("partial expired and all expired", func(t *testing.T) {
		prefix := fmt.Sprintf("prefix%d", rand.Int())
		value := "v"
		ts := ftso(100)
		saveFn(getKey(prefix, 0), value, ts)
		ts = ftso(200)
		saveFn(getKey(prefix, 0), value, ts)
		ts = ftso(300)
		saveFn(getKey(prefix, 0), value, ts)

		// insert partial expired kv
		ts = ftso(25)
		saveFn(getKey(prefix, 1), string(SuffixSnapshotTombstone), ts)
		ts = ftso(50)
		saveFn(getKey(prefix, 1), value, ts)
		ts = ftso(70)
		saveFn(getKey(prefix, 1), value, ts)

		// insert all expired kv
		ts = ftso(100)
		saveFn(getKey(prefix, 2), string(SuffixSnapshotTombstone), ts)
		ts = ftso(200)
		saveFn(getKey(prefix, 2), value, ts)
		ts = ftso(300)
		saveFn(getKey(prefix, 2), value, ts)

		cnt := countPrefix(prefix)
		assert.Equal(t, 12, cnt)

		err = ss.removeExpiredKvs(now, time.Duration(50)*time.Millisecond)
		assert.NoError(t, err)

		cnt = countPrefix(prefix)
		assert.Equal(t, 6, cnt)

		// clean all data
		err := etcdkv.RemoveWithPrefix("")
		assert.NoError(t, err)
	})

	t.Run("parse ts fail", func(t *testing.T) {
		prefix := fmt.Sprintf("prefix%d", rand.Int())
		key := fmt.Sprintf("%s-%s", prefix, "ts_error-ts")
		err = etcdkv.Save(ss.composeSnapshotPrefix(key), "")
		assert.NoError(t, err)

		err = ss.removeExpiredKvs(now, time.Duration(50)*time.Millisecond)
		assert.NoError(t, err)

		cnt := countPrefix(prefix)
		assert.Equal(t, 1, cnt)

		// clean all data
		err := etcdkv.RemoveWithPrefix("")
		assert.NoError(t, err)
	})

	t.Run("test walk kv data fail", func(t *testing.T) {
		sep := "_ts"
		rootPath := "root/"
		kv := mocks.NewMetaKv(t)
		kv.EXPECT().
			WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("error"))

		ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
		assert.NotNil(t, ss)
		assert.NoError(t, err)

		err = ss.removeExpiredKvs(time.Now(), time.Duration(100))
		assert.Error(t, err)
	})
}

func Test_SuffixSnapshotMultiSaveAndRemoveWithPrefix(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
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
	require.Nil(t, err)
	defer etcdCli.Close()
	etcdkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	require.Nil(t, err)
	defer etcdkv.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ss, err := NewSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.NoError(t, err)
	assert.NotNil(t, ss)
	defer ss.Close()

	for i := 0; i < 20; i++ {
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.Save(fmt.Sprintf("kd-%04d", i), fmt.Sprintf("value-%d", i), ts)
		assert.NoError(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 20; i < 40; i++ {
		sm := map[string]string{"ks": fmt.Sprintf("value-%d", i)}
		dm := []string{fmt.Sprintf("kd-%04d", i-20)}
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.MultiSaveAndRemoveWithPrefix(sm, dm, ts)
		assert.NoError(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		val, err := ss.Load(fmt.Sprintf("kd-%04d", i), typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, i+1, len(vals))
	}
	for i := 20; i < 40; i++ {
		val, err := ss.Load("ks", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, 39-i, len(vals))
	}

	for i := 0; i < 20; i++ {
		val, err := ss.Load(fmt.Sprintf("kd-%04d", i), typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, i+1, len(vals))
	}
	for i := 20; i < 40; i++ {
		val, err := ss.Load("ks", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.NoError(t, err)
		assert.Equal(t, 39-i, len(vals))
	}
	// try to load
	_, err = ss.Load("kd-0000", 500)
	assert.Error(t, err)
	_, err = ss.Load("kd-0000", 0)
	assert.Error(t, err)
	_, err = ss.Load("kd-0000", 1)
	assert.Error(t, err)

	// cleanup
	ss.MultiSaveAndRemoveWithPrefix(map[string]string{}, []string{""}, 0)
}

func TestSuffixSnapshot_LoadWithPrefix(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/loadWithPrefix-test-%d", randVal)
	sep := "_ts"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	assert.NoError(t, err)
	defer etcdkv.Close()

	ss, err := NewSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.NoError(t, err)
	assert.NotNil(t, ss)
	defer ss.Close()

	t.Run("parse ts fail", func(t *testing.T) {
		prefix := fmt.Sprintf("prefix%d", rand.Int())
		key := fmt.Sprintf("%s-%s", prefix, "ts_error-ts")
		err = etcdkv.Save(ss.composeSnapshotPrefix(key), "")
		assert.NoError(t, err)

		keys, values, err := ss.LoadWithPrefix(prefix, 100)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(keys))
		assert.Equal(t, 0, len(values))

		// clean all data
		err = etcdkv.RemoveWithPrefix("")
		assert.NoError(t, err)
	})

	t.Run("test walk kv data fail", func(t *testing.T) {
		sep := "_ts"
		rootPath := "root/"
		kv := mocks.NewMetaKv(t)
		kv.EXPECT().
			WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("error"))

		ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
		assert.NotNil(t, ss)
		assert.NoError(t, err)

		keys, values, err := ss.LoadWithPrefix("t", 100)
		assert.Error(t, err)
		assert.Nil(t, keys)
		assert.Nil(t, values)
	})
}

func Test_getOriginalKey(t *testing.T) {
	sep := "_ts"
	rootPath := "root/"
	kv := mocks.NewMetaKv(t)
	ss, err := NewSuffixSnapshot(kv, sep, rootPath, snapshotPrefix)
	assert.NotNil(t, ss)
	assert.NoError(t, err)

	t.Run("match prefix fail", func(t *testing.T) {
		ret, err := ss.getOriginalKey("non-snapshots/k1")
		assert.Equal(t, "", ret)
		assert.Error(t, err)
	})

	t.Run("find separator fail", func(t *testing.T) {
		ret, err := ss.getOriginalKey("snapshots/k1")
		assert.Equal(t, "", ret)
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		ret, err := ss.getOriginalKey("snapshots/prefix-1_ts438497159122780160")
		assert.Equal(t, "prefix-1", ret)
		assert.NoError(t, err)
	})
}
