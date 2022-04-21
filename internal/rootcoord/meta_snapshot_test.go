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
	"path"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestMetaSnapshot(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ms, err := newMetaSnapshot(etcdCli, rootPath, tsKey, 4)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 8; i++ {
		vtso = typeutil.Timestamp(100 + i)
		ts := ftso()
		err = ms.Save("abc", fmt.Sprintf("value-%d", i), ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
		_, err = etcdCli.Put(context.Background(), "other", fmt.Sprintf("other-%d", i))
		assert.Nil(t, err)
	}

	ms, err = newMetaSnapshot(etcdCli, rootPath, tsKey, 4)
	assert.Nil(t, err)
	assert.NotNil(t, ms)
}

func TestSearchOnCache(t *testing.T) {
	ms := &metaSnapshot{}
	for i := 0; i < 8; i++ {
		ms.ts2Rev = append(ms.ts2Rev,
			rtPair{
				rev: int64(i * 2),
				ts:  typeutil.Timestamp(i * 2),
			})
	}
	rev := ms.searchOnCache(9, 0, 8)
	assert.Equal(t, int64(8), rev)
	rev = ms.searchOnCache(1, 0, 2)
	assert.Equal(t, int64(0), rev)
	rev = ms.searchOnCache(1, 0, 8)
	assert.Equal(t, int64(0), rev)
	rev = ms.searchOnCache(14, 0, 8)
	assert.Equal(t, int64(14), rev)
	rev = ms.searchOnCache(0, 0, 8)
	assert.Equal(t, int64(0), rev)
}

func TestGetRevOnCache(t *testing.T) {
	ms := &metaSnapshot{}
	ms.ts2Rev = make([]rtPair, 7)
	ms.initTs(7, 16)
	ms.initTs(6, 14)
	ms.initTs(5, 12)
	ms.initTs(4, 10)

	var rev int64
	rev = ms.getRevOnCache(17)
	assert.Equal(t, int64(7), rev)
	rev = ms.getRevOnCache(9)
	assert.Equal(t, int64(0), rev)
	rev = ms.getRevOnCache(10)
	assert.Equal(t, int64(4), rev)
	rev = ms.getRevOnCache(16)
	assert.Equal(t, int64(7), rev)
	rev = ms.getRevOnCache(15)
	assert.Equal(t, int64(6), rev)
	rev = ms.getRevOnCache(12)
	assert.Equal(t, int64(5), rev)

	ms.initTs(3, 8)
	ms.initTs(2, 6)
	assert.Equal(t, ms.maxPos, 6)
	assert.Equal(t, ms.minPos, 1)

	rev = ms.getRevOnCache(17)
	assert.Equal(t, int64(7), rev)
	rev = ms.getRevOnCache(9)
	assert.Equal(t, int64(3), rev)
	rev = ms.getRevOnCache(10)
	assert.Equal(t, int64(4), rev)
	rev = ms.getRevOnCache(16)
	assert.Equal(t, int64(7), rev)
	rev = ms.getRevOnCache(15)
	assert.Equal(t, int64(6), rev)
	rev = ms.getRevOnCache(12)
	assert.Equal(t, int64(5), rev)
	rev = ms.getRevOnCache(5)
	assert.Equal(t, int64(0), rev)

	ms.putTs(8, 18)
	assert.Equal(t, ms.maxPos, 0)
	assert.Equal(t, ms.minPos, 1)
	for rev = 2; rev <= 7; rev++ {
		ts := ms.getRevOnCache(typeutil.Timestamp(rev*2 + 3))
		assert.Equal(t, rev, ts)
	}
	ms.putTs(9, 20)
	assert.Equal(t, ms.maxPos, 1)
	assert.Equal(t, ms.minPos, 2)
	assert.Equal(t, ms.numTs, 7)

	curMax := ms.maxPos
	curMin := ms.minPos
	for i := 10; i < 20; i++ {
		ms.putTs(int64(i), typeutil.Timestamp(i*2+2))
		curMax++
		curMin++
		if curMax == len(ms.ts2Rev) {
			curMax = 0
		}
		if curMin == len(ms.ts2Rev) {
			curMin = 0
		}
		assert.Equal(t, curMax, ms.maxPos)
		assert.Equal(t, curMin, ms.minPos)
	}

	for i := 13; i < 20; i++ {
		rev = ms.getRevOnCache(typeutil.Timestamp(i*2 + 2))
		assert.Equal(t, int64(i), rev)
		rev = ms.getRevOnCache(typeutil.Timestamp(i*2 + 3))
		assert.Equal(t, int64(i), rev)
	}
	rev = ms.getRevOnCache(27)
	assert.Zero(t, rev)
}

func TestGetRevOnEtcd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), etcdkv.RequestTimeout)
	defer cancel()
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"
	key := path.Join(rootPath, tsKey)

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	ms := metaSnapshot{
		cli:   etcdCli,
		root:  rootPath,
		tsKey: tsKey,
	}

	resp, err := etcdCli.Put(ctx, key, "100")
	assert.NoError(t, err)
	revList := []int64{}
	tsList := []typeutil.Timestamp{}
	revList = append(revList, resp.Header.Revision)
	tsList = append(tsList, 100)
	for i := 110; i < 200; i += 10 {
		resp, err = etcdCli.Put(ctx, key, fmt.Sprintf("%d", i))
		assert.Nil(t, err)
		revList = append(revList, resp.Header.Revision)
		tsList = append(tsList, typeutil.Timestamp(i))
	}
	lastRev := revList[len(revList)-1] + 1
	for i, ts := range tsList {
		rev := ms.getRevOnEtcd(ts, lastRev)
		assert.Equal(t, revList[i], rev)
	}
	for i := 0; i < len(tsList); i++ {
		rev := ms.getRevOnEtcd(tsList[i]+5, lastRev)
		assert.Equal(t, revList[i], rev)
	}
	rev := ms.getRevOnEtcd(200, lastRev)
	assert.Equal(t, lastRev-1, rev)
	rev = ms.getRevOnEtcd(99, lastRev)
	assert.Zero(t, rev)
}

func TestLoad(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ms, err := newMetaSnapshot(etcdCli, rootPath, tsKey, 7)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 20; i++ {
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ms.Save("key", fmt.Sprintf("value-%d", i), ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		val, err := ms.Load("key", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, val, fmt.Sprintf("value-%d", i))
	}
	val, err := ms.Load("key", 0)
	assert.Nil(t, err)
	assert.Equal(t, "value-19", val)

	ms, err = newMetaSnapshot(etcdCli, rootPath, tsKey, 11)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 20; i++ {
		val, err := ms.Load("key", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, val, fmt.Sprintf("value-%d", i))
	}
}

func TestMultiSave(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}

	ms, err := newMetaSnapshot(etcdCli, rootPath, tsKey, 7)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 20; i++ {
		saves := map[string]string{"k1": fmt.Sprintf("v1-%d", i), "k2": fmt.Sprintf("v2-%d", i)}
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ms.MultiSave(saves, ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 0; i < 20; i++ {
		keys, vals, err := ms.LoadWithPrefix("k", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, len(keys), 2)
		assert.Equal(t, keys[0], "k1")
		assert.Equal(t, keys[1], "k2")
		assert.Equal(t, vals[0], fmt.Sprintf("v1-%d", i))
		assert.Equal(t, vals[1], fmt.Sprintf("v2-%d", i))
	}
	keys, vals, err := ms.LoadWithPrefix("k", 0)
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, keys[0], "k1")
	assert.Equal(t, keys[1], "k2")
	assert.Equal(t, vals[0], "v1-19")
	assert.Equal(t, vals[1], "v2-19")

	ms, err = newMetaSnapshot(etcdCli, rootPath, tsKey, 11)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 20; i++ {
		keys, vals, err := ms.LoadWithPrefix("k", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, len(keys), 2)
		assert.Equal(t, keys[0], "k1")
		assert.Equal(t, keys[1], "k2")
		assert.Equal(t, vals[0], fmt.Sprintf("v1-%d", i))
		assert.Equal(t, vals[1], fmt.Sprintf("v2-%d", i))
	}
}

func TestMultiSaveAndRemoveWithPrefix(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		return vtso
	}
	defer etcdCli.Close()

	ms, err := newMetaSnapshot(etcdCli, rootPath, tsKey, 7)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 20; i++ {
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ms.Save(fmt.Sprintf("kd-%04d", i), fmt.Sprintf("value-%d", i), ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}
	for i := 20; i < 40; i++ {
		sm := map[string]string{"ks": fmt.Sprintf("value-%d", i)}
		dm := []string{fmt.Sprintf("kd-%04d", i-20)}
		vtso = typeutil.Timestamp(100 + i*5)
		ts := ftso()
		err = ms.MultiSaveAndRemoveWithPrefix(sm, dm, ts)
		assert.Nil(t, err)
		assert.Equal(t, vtso, ts)
	}

	for i := 0; i < 20; i++ {
		val, err := ms.Load(fmt.Sprintf("kd-%04d", i), typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ms.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, i+1, len(vals))
	}
	for i := 20; i < 40; i++ {
		val, err := ms.Load("ks", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ms.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, 39-i, len(vals))
	}

	ms, err = newMetaSnapshot(etcdCli, rootPath, tsKey, 11)
	assert.Nil(t, err)
	assert.NotNil(t, ms)

	for i := 0; i < 20; i++ {
		val, err := ms.Load(fmt.Sprintf("kd-%04d", i), typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ms.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, i+1, len(vals))
	}
	for i := 20; i < 40; i++ {
		val, err := ms.Load("ks", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), val)
		_, vals, err := ms.LoadWithPrefix("kd-", typeutil.Timestamp(100+i*5+2))
		assert.Nil(t, err)
		assert.Equal(t, 39-i, len(vals))
	}
}

func TestTsBackward(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	kv, err := newMetaSnapshot(etcdCli, rootPath, tsKey, 1024)
	assert.Nil(t, err)

	err = kv.loadTs()
	assert.Nil(t, err)

	kv.Save("a", "b", 100)
	kv.Save("a", "c", 99) // backward
	kv.Save("a", "d", 200)

	kv, err = newMetaSnapshot(etcdCli, rootPath, tsKey, 1024)
	assert.Error(t, err)

}

func TestFix7150(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()

	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	tsKey := "timestamp"

	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()

	kv, err := newMetaSnapshot(etcdCli, rootPath, tsKey, 1024)
	assert.Nil(t, err)

	err = kv.loadTs()
	assert.Nil(t, err)

	kv.Save("a", "b", 100)
	kv.Save("a", "c", 0) // bug introduced
	kv.Save("a", "d", 200)

	kv, err = newMetaSnapshot(etcdCli, rootPath, tsKey, 1024)
	assert.Nil(t, err)
	err = kv.loadTs()
	assert.Nil(t, err)
}
