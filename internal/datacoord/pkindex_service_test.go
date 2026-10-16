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

package datacoord

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	pkindexservice "github.com/milvus-io/milvus/internal/datacoord/pkindex/service"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPKIndexMetaKVStore(t *testing.T) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: Params.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	root := "pkindex-meta-store-" + uuid.NewString() + "/meta"
	kv := etcdkv.NewEtcdKV(client, root)
	defer func() { _ = kv.RemoveWithPrefix(ctx, "") }()
	p := metaKVStore{kv: kv}
	key := manifest.PointerKey(1, "ch_v0")

	_, found, err := p.Load(ctx, key)
	require.NoError(t, err)
	assert.False(t, found)
	var values []string
	var founds []bool

	require.NoError(t, p.Save(ctx, map[string]string{key: "a"}, predicates.KeyNotExist(key)))
	assert.Error(t, p.Save(ctx, map[string]string{key: "b"}, predicates.KeyNotExist(key)))
	v, found, err := p.Load(ctx, key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "a", v)

	assert.Error(t, p.Save(ctx, map[string]string{key: "c"}, predicates.ValueEqual(key, "x")))
	require.NoError(t, p.Save(ctx, map[string]string{key: "c"}, predicates.ValueEqual(key, "a")))
	v, _, err = p.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "c", v)

	// a write under a predicate on a second key that it does not write: it
	// fails when that key does not hold the value, and leaves that key as is
	pchannelMeta := streamingcoord.BuildPChannelInfoPath("ch")
	require.NoError(t, p.Save(ctx, map[string]string{pchannelMeta: "t1"}, predicates.KeyNotExist(pchannelMeta)))
	assert.Error(t, p.Save(ctx, map[string]string{key: "d"}, predicates.ValueEqual(key, "c"), predicates.ValueEqual(pchannelMeta, "t2")))
	v, _, err = p.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "c", v)
	require.NoError(t, p.Save(ctx, map[string]string{key: "d"}, predicates.ValueEqual(key, "c"), predicates.ValueEqual(pchannelMeta, "t1")))
	v, _, err = p.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "d", v)
	v, _, err = p.Load(ctx, pchannelMeta)
	require.NoError(t, err)
	assert.Equal(t, "t1", v)

	values, founds, err = p.MultiLoad(ctx, []string{key, manifest.PointerKey(1, "missing_v0"), pchannelMeta})
	require.NoError(t, err)
	assert.Equal(t, []string{"d", "", "t1"}, values)
	assert.Equal(t, []bool{true, false, true}, founds)

	// keys above the txn limit are read in several calls, in order
	txnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key
	paramtable.Get().Save(txnNum, "2")
	defer paramtable.Get().Reset(txnNum)
	values, founds, err = p.MultiLoad(ctx, []string{manifest.PointerKey(1, "missing_v0"), key, pchannelMeta, manifest.PointerKey(1, "missing_v1"), key})
	require.NoError(t, err)
	assert.Equal(t, []string{"", "d", "t1", "", "d"}, values)
	assert.Equal(t, []bool{false, true, true, false, true}, founds)

	keys, values, err := p.List(ctx, manifest.PointerPrefix)
	require.NoError(t, err)
	assert.Equal(t, []string{key}, keys)
	assert.Equal(t, []string{"d"}, values, "the pchannel meta is not under the pointer prefix")
	collectionID, vchannel, ok := manifest.ParsePointerKey(keys[0])
	assert.True(t, ok)
	assert.Equal(t, int64(1), collectionID)
	assert.Equal(t, "ch_v0", vchannel)
}

func TestPKIndexChunkManagerStore(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	s := chunkManagerStore{cm: cm}
	root := cm.RootPath()

	_, err := s.Read(ctx, pkindex.ManifestBodyKey(pkindex.ManifestDir(root, 1, "ch_v0"), 1, "n"))
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))

	written := []string{
		pkindex.ManifestBodyKey(pkindex.ManifestDir(root, 1, "ch_v0"), 1, "n1"),
		pkindex.ManifestBodyKey(pkindex.ManifestDir(root, 1, "ch_v0"), 2, "n2"),
		pkindex.SSTKey(pkindex.SSTBasePath(root, 1, "ch_v0"), 7),
	}
	for _, key := range written {
		require.NoError(t, s.Write(ctx, key, []byte(key)))
	}
	b, err := s.Read(ctx, written[0])
	require.NoError(t, err)
	assert.Equal(t, []byte(written[0]), b)

	var walked []string
	require.NoError(t, s.Walk(ctx, pkindex.ManifestDir(root, 1, "ch_v0")+"/", func(key string, _ time.Time) bool {
		walked = append(walked, key)
		return true
	}))
	sort.Strings(walked)
	assert.Equal(t, written[:2], walked)

	walked = nil
	require.NoError(t, s.Walk(ctx, pkindex.SSTBasePath(root, 1, "ch_v0")+"/", func(key string, _ time.Time) bool {
		walked = append(walked, key)
		return true
	}))
	assert.Equal(t, written[2:], walked)

	require.NoError(t, s.MultiRemove(ctx, written))
	_, err = s.Read(ctx, written[0])
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
}

func TestInitPKIndexInvalidGCConfig(t *testing.T) {
	key := Params.DataCoordCfg.PKIndexGCGrace.Key
	// one second below the minimum grace of the primary key index gc
	paramtable.Get().Save(key, "599")
	defer paramtable.Get().Reset(key)

	client, err := clientv3.New(clientv3.Config{
		Endpoints: Params.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()
	kv := etcdkv.NewEtcdKV(client, "pkindex-init-"+uuid.NewString()+"/meta")

	s := &Server{ctx: context.Background(), kv: kv, pkindexService: pkindexservice.New()}
	s.initPKIndex(storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())))
	assert.Nil(t, s.pkindexGC)
	require.NotNil(t, s.pkindexCompaction)

	resp, err := s.pkindexService.GetRecoveryManifests(context.Background(), &pkindexpb.GetRecoveryManifestsRequest{
		Targets: []*pkindexpb.ManifestTarget{{CollectionId: 1, Vchannel: "v"}},
	})
	require.NoError(t, err)
	assert.NotEqual(t, merr.Code(merr.ErrServiceNotReady), resp.GetStatus().GetCode())
}

// A flush committed through the service reaches the inspector, which proves
// that initPKIndex made the inspector the observer of the manager.
func TestInitPKIndexWiresCompaction(t *testing.T) {
	ctx := context.Background()
	const pchannel, vchannel = "by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_0_1v0"
	pchannelMeta, err := proto.Marshal(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: pchannel, Term: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
	})
	require.NoError(t, err)
	var mu sync.Mutex
	stored := map[string]string{streamingcoord.BuildPChannelInfoPath(pchannel): string(pchannelMeta)}
	metaKV := mocks.NewMetaKv(t)
	metaKV.EXPECT().Load(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, key string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if v, ok := stored[key]; ok {
			return v, nil
		}
		return "", merr.WrapErrIoKeyNotFound(key)
	}).Maybe()
	save := func(_ context.Context, saves map[string]string, _ []string, _ ...predicates.Predicate) error {
		mu.Lock()
		defer mu.Unlock()
		for k, v := range saves {
			stored[k] = v
		}
		return nil
	}
	// a flush writes the pointer on two predicates: the pointer and the pchannel meta
	metaKV.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(save).Maybe()

	s := &Server{ctx: ctx, kv: metaKV, pkindexService: pkindexservice.New()}
	s.initPKIndex(storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())))
	require.NotNil(t, s.pkindexGC)
	require.NotNil(t, s.pkindexCompaction)

	assert.False(t, s.pkindexCompaction.Known(1, vchannel))
	flush, err := s.pkindexService.CommitFlush(ctx, &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: vchannel, PchannelTerm: 1, CheckpointTimeTick: 10,
		AddSsts: []*pkindexpb.SSTRef{{Id: 1}},
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(flush.GetStatus()))

	assert.True(t, s.pkindexCompaction.Known(1, vchannel))
	// the inspector has launched no task yet
	ids, err := s.pkindexCompaction.InFlightOutputs(ctx, 1, vchannel)
	require.NoError(t, err)
	assert.Nil(t, ids)
}
