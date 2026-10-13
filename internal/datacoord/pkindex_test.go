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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	pkindexservice "github.com/milvus-io/milvus/internal/pkindex/coordinator/service"
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPKIndexMetaKVPointers(t *testing.T) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: Params.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	root := "pkindex-pointers-" + uuid.NewString() + "/meta"
	kv := etcdkv.NewEtcdKV(client, root)
	defer func() { _ = kv.RemoveWithPrefix(ctx, "") }()
	p := metaKVPointers{kv: kv}
	key := manifest.PointerKey(1, "ch_v0")

	_, found, err := p.Load(ctx, key)
	require.NoError(t, err)
	assert.False(t, found)
	var values []string
	var founds []bool

	require.NoError(t, p.Save(ctx, map[string]string{key: "a"}, manifest.KeyAbsent(key)))
	assert.Error(t, p.Save(ctx, map[string]string{key: "b"}, manifest.KeyAbsent(key)))
	v, found, err := p.Load(ctx, key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "a", v)

	assert.Error(t, p.Save(ctx, map[string]string{key: "c"}, manifest.ValueIs(key, "x")))
	require.NoError(t, p.Save(ctx, map[string]string{key: "c"}, manifest.ValueIs(key, "a")))
	v, _, err = p.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "c", v)

	// a write under a condition on a second key: it fails when that key does
	// not hold the value, and writes both keys in one transaction otherwise
	claim := manifest.ClaimKey("ch")
	require.NoError(t, p.Save(ctx, map[string]string{claim: "t1"}, manifest.KeyAbsent(claim)))
	assert.Error(t, p.Save(ctx, map[string]string{key: "d", claim: "t2"}, manifest.ValueIs(key, "c"), manifest.ValueIs(claim, "t2")))
	v, _, err = p.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "c", v)
	require.NoError(t, p.Save(ctx, map[string]string{key: "d", claim: "t1"}, manifest.ValueIs(key, "c"), manifest.ValueIs(claim, "t1")))
	v, _, err = p.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "d", v)

	values, founds, err = p.MultiLoad(ctx, []string{key, manifest.PointerKey(1, "missing_v0"), claim})
	require.NoError(t, err)
	assert.Equal(t, []string{"d", "", "t1"}, values)
	assert.Equal(t, []bool{true, false, true}, founds)

	keys, values, err := p.List(ctx, manifest.PointerPrefix)
	require.NoError(t, err)
	assert.Equal(t, []string{key}, keys)
	assert.Equal(t, []string{"d"}, values, "the claim record is not under the pointer prefix")
	collectionID, vchannel, ok := manifest.ParsePointerKey(keys[0])
	assert.True(t, ok)
	assert.Equal(t, int64(1), collectionID)
	assert.Equal(t, "ch_v0", vchannel)
}

func TestPKIndexChunkManagerStore(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	s := chunkManagerStore{cm: cm}
	layout := manifest.Layout{RootPath: cm.RootPath()}

	_, err := s.Read(ctx, layout.ManifestBodyKey(1, "ch_v0", 1, "n"))
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))

	written := []string{
		layout.ManifestBodyKey(1, "ch_v0", 1, "n1"),
		layout.ManifestBodyKey(1, "ch_v0", 2, "n2"),
		manifest.SSTKey(layout.SSTBasePath(1, "ch_v0"), 7),
	}
	for _, key := range written {
		require.NoError(t, s.Write(ctx, key, []byte(key)))
	}
	b, err := s.Read(ctx, written[0])
	require.NoError(t, err)
	assert.Equal(t, []byte(written[0]), b)

	var walked []string
	require.NoError(t, s.Walk(ctx, layout.ManifestDir(1, "ch_v0")+"/", func(key string, _ time.Time) bool {
		walked = append(walked, key)
		return true
	}))
	sort.Strings(walked)
	assert.Equal(t, written[:2], walked)

	walked = nil
	require.NoError(t, s.Walk(ctx, layout.SSTBasePath(1, "ch_v0")+"/", func(key string, _ time.Time) bool {
		walked = append(walked, key)
		return true
	}))
	assert.Equal(t, written[2:], walked)

	require.NoError(t, s.MultiRemove(ctx, written))
	_, err = s.Read(ctx, written[0])
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
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

	resp, err := s.pkindexService.GetRecoveryManifests(context.Background(), &pkindexpb.GetRecoveryManifestsRequest{
		Targets: []*pkindexpb.ManifestTarget{{CollectionId: 1, Vchannel: "v"}},
	})
	require.NoError(t, err)
	assert.NotEqual(t, merr.Code(merr.ErrServiceNotReady), resp.GetStatus().GetCode())
}
