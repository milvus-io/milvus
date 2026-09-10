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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// TestDDLCallbacksAlterAliasV2AckCallback_OldTargetRouting locks in how the
// AlterAlias ack callback routes the OLD target eviction by the header's
// OldCollectionId, which shares the message type with CreateAlias. The zero
// value is the compat-safe scan path so a replayed old-rootcoord AlterAlias
// (which predates old_collection_id and leaves it 0) still closes its ghost:
//   - > 0  : old target known -> evict it by id (no scan).
//   - == 0 : old target UNKNOWN (new AlterAlias could not resolve it, OR an old
//     rootcoord that never set the field) -> emit a CollectionID==0 alias entry
//     so the proxy holder-scans (graceful degrade, never fails the alter).
//   - < 0  : CreateAlias sentinel (provably no old target) -> NO CollectionID==0
//     entry, so a normal create never triggers the O(N) holder scan.
func TestDDLCallbacksAlterAliasV2AckCallback_OldTargetRouting(t *testing.T) {
	ctx := context.Background()
	const newID = int64(20)
	invalidate := func(t *testing.T, oldCollectionID int64) []*proxypb.InvalidateCollMetaCacheRequest {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().AlterAlias(mock.Anything, mock.Anything).Return(nil)

		var got []*proxypb.InvalidateCollMetaCacheRequest
		pcm := proxyutil.NewMockProxyClientManager(t)
		pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest, opts ...proxyutil.ExpireCacheOpt) error {
				got = append(got, req)
				return nil
			}).Maybe()

		c := newTestCore(withMeta(meta), withTsoAllocator(newMockTsoAllocator()))
		c.proxyClientManager = pcm
		cb := &DDLCallback{Core: c}

		raw := message.NewAlterAliasMessageBuilderV2().
			WithHeader(&message.AlterAliasMessageHeader{
				DbName:          "db",
				Alias:           "a",
				CollectionId:    newID,
				OldCollectionId: oldCollectionID,
			}).
			WithBody(&message.AlterAliasMessageBody{}).
			WithBroadcast([]string{funcutil.GetControlChannel("test")}).
			MustBuildBroadcast()
		msg := message.MustAsBroadcastAlterAliasMessageV2(raw)
		require.NoError(t, cb.alterAliasV2AckCallback(ctx, message.BroadcastResultAlterAliasMessageV2{
			Message: msg,
			Results: map[string]*message.AppendResult{},
		}))
		return got
	}

	// helpers over the captured requests
	hasIDOnly := func(reqs []*proxypb.InvalidateCollMetaCacheRequest, id int64) bool {
		for _, r := range reqs {
			if r.GetCollectionID() == id && r.GetCollectionName() == "" {
				return true
			}
		}
		return false
	}
	hasScanTrigger := func(reqs []*proxypb.InvalidateCollMetaCacheRequest) bool {
		// the proxy holder-scan is gated on CollectionID==0 + the alias name
		for _, r := range reqs {
			if r.GetCollectionID() == 0 && r.GetCollectionName() == "a" {
				return true
			}
		}
		return false
	}
	// hasID matches any request carrying the id, regardless of an accompanying
	// name (the new-target eviction rides on the first request, which also carries
	// the alias name, so hasIDOnly deliberately does NOT match it).
	hasID := func(reqs []*proxypb.InvalidateCollMetaCacheRequest, id int64) bool {
		for _, r := range reqs {
			if r.GetCollectionID() == id {
				return true
			}
		}
		return false
	}

	t.Run("old target known -> evict by id, no scan trigger", func(t *testing.T) {
		reqs := invalidate(t, 10)
		require.True(t, hasIDOnly(reqs, 10), "old target 10 must be evicted by id")
		require.False(t, hasScanTrigger(reqs), "a known old target must not trigger the holder scan")
	})

	t.Run("old target unknown (0) -> scan trigger, no stray id eviction", func(t *testing.T) {
		// zero covers both a new AlterAlias that could not resolve and a replayed
		// old-rootcoord AlterAlias that never set old_collection_id.
		reqs := invalidate(t, 0)
		require.True(t, hasScanTrigger(reqs), "an unknown old target (0) must emit a CollectionID==0 scan trigger")
		require.False(t, hasIDOnly(reqs, 0), "zero must never be sent as a real id eviction")
	})

	t.Run("create alias sentinel (-1) -> no scan trigger", func(t *testing.T) {
		reqs := invalidate(t, aliasNoOldTarget)
		require.False(t, hasScanTrigger(reqs), "CreateAlias (aliasNoOldTarget) must not trigger an O(N) holder scan")
	})

	t.Run("new target is always evicted by id", func(t *testing.T) {
		// the new target's canonical entry must be expired regardless of the old
		// target's fate; a regression dropping the unconditional first eviction
		// would leave an id-only Describe of the new target serving a stale
		// Aliases list.
		for _, oldID := range []int64{10, 0, aliasNoOldTarget} {
			reqs := invalidate(t, oldID)
			require.True(t, hasID(reqs, newID), "new target %d must be evicted (oldID=%d)", newID, oldID)
		}
	})

	t.Run("old target == new target -> deduped, no redundant eviction, no scan", func(t *testing.T) {
		// the guard is OldCollectionId > 0 && OldCollectionId != CollectionId; when
		// they coincide the old-target branch must be skipped so we do not emit a
		// second, redundant eviction for the same id, and never fall into the scan.
		reqs := invalidate(t, newID)
		require.True(t, hasID(reqs, newID), "the new target must still be evicted")
		require.False(t, hasIDOnly(reqs, newID), "no separate id-only old-target eviction when old==new")
		require.False(t, hasScanTrigger(reqs), "a resolved (equal) old target must not trigger the scan")
	})
}
