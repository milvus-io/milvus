package rootcoord

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	snapshotPrefix = "snapshots"
)

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
	ss, err := newSuffixSnapshot((*etcdkv.EtcdKV)(nil), sep, "", snapshotPrefix)
	require.Nil(t, err)
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
	ss, err := newSuffixSnapshot((*etcdkv.EtcdKV)(nil), sep, "", snapshotPrefix)
	require.Nil(t, err)
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

	etcdkv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, rootPath)
	require.Nil(t, err)
	defer etcdkv.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ss, err := newSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, ss)

	for i := 0; i < 20; i++ {
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.Save("key", fmt.Sprintf("value-%d", i), ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		val, err := ss.Load("key", typeutil.Timestamp(100+i*5+2))
		t.Log("ts:", typeutil.Timestamp(100+i*5+2), i, val)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
	}
	val, err := ss.Load("key", 0)
	assert.Nil(t, err)
	assert.Equal(t, "value-19", val)

	ss, err = newSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, ss)

	for i := 0; i < 20; i++ {
		val, err := ss.Load("key", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
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
	etcdkv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, rootPath)
	require.Nil(t, err)
	defer etcdkv.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ss, err := newSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, ss)

	for i := 0; i < 20; i++ {
		saves := map[string]string{"k1": fmt.Sprintf("v1-%d", i), "k2": fmt.Sprintf("v2-%d", i)}
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.MultiSave(saves, ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		keys, vals, err := ss.LoadWithPrefix("k", typeutil.Timestamp(100+i*5+2))
		t.Log(i, keys, vals)
		assert.Nil(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, len(keys), 2)
		assert.Equal(t, keys[0], "k1")
		assert.Equal(t, keys[1], "k2")
		assert.Equal(t, vals[0], fmt.Sprintf("v1-%d", i))
		assert.Equal(t, vals[1], fmt.Sprintf("v2-%d", i))
	}
	keys, vals, err := ss.LoadWithPrefix("k", 0)
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, keys[0], "k1")
	assert.Equal(t, keys[1], "k2")
	assert.Equal(t, vals[0], "v1-19")
	assert.Equal(t, vals[1], "v2-19")

	ss, err = newSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, ss)
	for i := 0; i < 20; i++ {
		keys, vals, err := ss.LoadWithPrefix("k", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, len(keys), 2)
		assert.ElementsMatch(t, keys, []string{"k1", "k2"})
		assert.ElementsMatch(t, vals, []string{fmt.Sprintf("v1-%d", i), fmt.Sprintf("v2-%d", i)})
	}
	// mix non ts k-v
	err = ss.Save("kextra", "extra-value", 0)
	assert.Nil(t, err)
	keys, vals, err = ss.LoadWithPrefix("k", typeutil.Timestamp(300))
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 3)
	assert.ElementsMatch(t, keys, []string{"k1", "k2", "kextra"})
	assert.ElementsMatch(t, vals, []string{"v1-19", "v2-19", "extra-value"})

	// clean up
	ss.RemoveWithPrefix("")
}

func Test_SuffixSnapshotMultiSaveAndRemoveWithPrefix(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	sep := "_ts"

	etcdkv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, rootPath)
	require.Nil(t, err)
	defer etcdkv.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ss, err := newSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, ss)

	for i := 0; i < 20; i++ {
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.Save(fmt.Sprintf("kd-%04d", i), fmt.Sprintf("value-%d", i), ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 20; i < 40; i++ {
		sm := map[string]string{"ks": fmt.Sprintf("value-%d", i)}
		dm := []string{fmt.Sprintf("kd-%04d", i-20)}
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ss.MultiSaveAndRemoveWithPrefix(sm, dm, ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		val, err := ss.Load(fmt.Sprintf("kd-%04d", i), typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, i+1, len(vals))
	}
	for i := 20; i < 40; i++ {
		val, err := ss.Load("ks", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, 39-i, len(vals))
	}

	ss, err = newSuffixSnapshot(etcdkv, sep, rootPath, snapshotPrefix)
	assert.Nil(t, err)
	assert.NotNil(t, ss)

	for i := 0; i < 20; i++ {
		val, err := ss.Load(fmt.Sprintf("kd-%04d", i), typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, i+1, len(vals))
	}
	for i := 20; i < 40; i++ {
		val, err := ss.Load("ks", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ss.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, 39-i, len(vals))
	}
	// try to load
	_, err = ss.Load("kd-0000", 500)
	assert.NotNil(t, err)
	_, err = ss.Load("kd-0000", 0)
	assert.NotNil(t, err)
	_, err = ss.Load("kd-0000", 1)
	assert.NotNil(t, err)

	// cleanup
	ss.MultiSaveAndRemoveWithPrefix(map[string]string{}, []string{""}, 0)
}
