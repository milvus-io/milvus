// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRLSPrincipalNamesAndLegacyCountUseKeysOnly(t *testing.T) {
	ctx := context.Background()
	kv := memkv.NewMemoryKV()
	catalog := NewCatalog(kv)
	names := []string{"alice", "team/a user", "user%2F", "用户"}
	for _, name := range names {
		// Enumeration and legacy counting must not decode or materialize tag values.
		require.NoError(t, kv.Save(ctx, buildRLSPrincipalKey(20, name), "not a protobuf"))
	}
	require.NoError(t, kv.Save(ctx, buildRLSPrincipalKey(201, "other collection"), "not a protobuf"))
	params := paramtable.Get()
	for _, key := range []string{params.ProxyCfg.RLSMaxPrincipalCacheEntries.Key, params.ProxyCfg.RLSMaxPrincipalCacheBytes.Key} {
		require.NoError(t, params.Save(key, "1"))
		t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
	}

	actual, err := catalog.ListRLSPrincipalNames(ctx, 20)
	require.NoError(t, err)
	require.Equal(t, names, actual)
	count, err := catalog.GetRLSPrincipalCount(ctx, 20)
	require.NoError(t, err)
	require.Equal(t, int64(len(names)), count)
	stored, err := kv.Load(ctx, BuildRLSPrincipalCountKey(20))
	require.NoError(t, err)
	require.Equal(t, "4", stored)

	// An existing counter is authoritative; subsequent reads must not scan keys.
	require.NoError(t, kv.Save(ctx, BuildRLSPrincipalCountKey(20), "5000000000"))
	count, err = catalog.GetRLSPrincipalCount(ctx, 20)
	require.NoError(t, err)
	require.Equal(t, int64(5000000000), count)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	actual, err = catalog.ListRLSPrincipalNames(canceled, 20)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, actual)
	require.NoError(t, kv.Save(ctx, BuildRLSPrincipalPrefix(20)+"bad%escape", ""))
	actual, err = catalog.ListRLSPrincipalNames(ctx, 20)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, actual)
}

// Simulate both a rejected transaction and a lost response after it committed.
type principalCountFaultKV struct {
	*memkv.MemoryKV
	failBeforeCommit bool
	failAfterCommit  bool
}

func (kv *principalCountFaultKV) MultiSave(ctx context.Context, saves map[string]string) error {
	return kv.MultiSaveAndRemove(ctx, saves, nil)
}

func (kv *principalCountFaultKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if kv.failBeforeCommit {
		return merr.ErrServiceUnavailable
	}
	if err := kv.MemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...); err != nil {
		return err
	}
	if kv.failAfterCommit {
		return context.DeadlineExceeded
	}
	return nil
}

func TestRLSPrincipalCountMutationRetries(t *testing.T) {
	ctx := context.Background()
	kv := &principalCountFaultKV{MemoryKV: memkv.NewMemoryKV()}
	catalog := NewCatalog(kv)
	principal := &model.RLSPrincipal{
		CollectionID: 20, PrincipalName: "alice",
		Tags: map[string]rlsutil.TagValue{"tenant": rlsutil.NewInt64TagValue(1)},
	}

	kv.failBeforeCommit = true
	_, err := catalog.SaveRLSPrincipal(ctx, principal)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	count, err := catalog.GetRLSPrincipalCount(ctx, 20)
	require.NoError(t, err)
	require.Zero(t, count)
	_, err = catalog.GetRLSPrincipal(ctx, 20, "alice")
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	kv.failBeforeCommit = false
	kv.failAfterCommit = true
	_, err = catalog.SaveRLSPrincipal(ctx, principal)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	kv.failAfterCommit = false
	count, err = catalog.SaveRLSPrincipal(ctx, principal)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	principal.Tags["tenant"] = rlsutil.NewInt64TagValue(2)
	count, err = catalog.SaveRLSPrincipal(ctx, principal)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	kv.failBeforeCommit = true
	_, err = catalog.DropRLSPrincipal(ctx, 20, "alice")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	count, err = catalog.GetRLSPrincipalCount(ctx, 20)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	_, err = catalog.GetRLSPrincipal(ctx, 20, "alice")
	require.NoError(t, err)

	kv.failBeforeCommit = false
	kv.failAfterCommit = true
	_, err = catalog.DropRLSPrincipal(ctx, 20, "alice")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	kv.failAfterCommit = false
	count, err = catalog.DropRLSPrincipal(ctx, 20, "alice")
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = catalog.SaveRLSPrincipal(ctx, principal)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Dropping one collection must remove its counter, not adjacent IDs' counters.
	require.NoError(t, kv.Save(ctx, BuildRLSPrincipalCountKey(201), "5"))
	require.NoError(t, catalog.DropCollection(ctx, &model.Collection{CollectionID: 20}, 0))
	_, err = kv.Load(ctx, BuildRLSPrincipalCountKey(20))
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	_, err = catalog.GetRLSPrincipal(ctx, 20, "alice")
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	count, err = catalog.GetRLSPrincipalCount(ctx, 201)
	require.NoError(t, err)
	require.Equal(t, int64(5), count)
}

func TestRLSPrincipalCountRejectsCorruptCounter(t *testing.T) {
	ctx := context.Background()
	kv := memkv.NewMemoryKV()
	catalog := NewCatalog(kv)
	principal := &model.RLSPrincipal{CollectionID: 20, PrincipalName: "alice", Tags: map[string]rlsutil.TagValue{"tenant": rlsutil.NewInt64TagValue(1)}}
	for _, value := range []string{"invalid", "-1", "9223372036854775808"} {
		require.NoError(t, kv.Save(ctx, BuildRLSPrincipalCountKey(20), value))
		_, err := catalog.GetRLSPrincipalCount(ctx, 20)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
	require.NoError(t, kv.Save(ctx, BuildRLSPrincipalCountKey(20), "9223372036854775807"))
	_, err := catalog.SaveRLSPrincipal(ctx, principal)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, kv.Save(ctx, buildRLSPrincipalKey(20, "alice"), "stored tag record"))
	require.NoError(t, kv.Save(ctx, BuildRLSPrincipalCountKey(20), "0"))
	_, err = catalog.DropRLSPrincipal(ctx, 20, "alice")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	_, err = kv.Load(ctx, buildRLSPrincipalKey(20, "alice"))
	require.NoError(t, err)
}
