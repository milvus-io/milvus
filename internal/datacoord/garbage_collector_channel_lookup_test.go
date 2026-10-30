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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Exercise real etcd metadata and local object deletion through the GC sweep.
// Faults affect only the RootCoord collection lookup; segment reads and
// deletion stay healthy.
func TestGarbageCollector_ChannelLookup(t *testing.T) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: Params.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	loaded := mockey.Mock((*ServerHandler).ListLoadedSegments).Return([]int64{}, nil).Build()
	defer loaded.UnPatch()

	for _, tc := range []struct {
		name              string
		collectionDropped bool
		checkpoint        uint64
		lookupErr         error
		persistentError   bool
		wantRetained      bool
	}{
		{name: "transient lookup failure", checkpoint: 100, lookupErr: context.DeadlineExceeded, wantRetained: true},
		{name: "persistent lookup failure", checkpoint: 100, lookupErr: merr.ErrServiceUnavailable, persistentError: true, wantRetained: true},
		{name: "checkpoint behind", checkpoint: 100, wantRetained: true},
		{name: "checkpoint equal", checkpoint: 120},
		{name: "checkpoint passed", checkpoint: 130},
		{name: "checkpoint equal with persistent failure", checkpoint: 120, lookupErr: context.DeadlineExceeded, persistentError: true},
		{name: "checkpoint passed with persistent failure", checkpoint: 130, lookupErr: merr.ErrServiceUnavailable, persistentError: true},
		{name: "dropped checkpoint with persistent failure", checkpoint: funcutil.DroppedChannelCheckpointTimestamp, lookupErr: merr.ErrServiceUnavailable, persistentError: true},
		{name: "dropped collection", collectionDropped: true, checkpoint: 100},
		{name: "dropped collection after lookup recovers", collectionDropped: true, checkpoint: 100, lookupErr: context.DeadlineExceeded, wantRetained: true},
		{name: "canceled lookup", checkpoint: 100, lookupErr: context.Canceled, wantRetained: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := "gc-channel-lookup-" + uuid.NewString()
			kv := etcdkv.NewEtcdKV(client, root)
			defer func() { require.NoError(t, kv.RemoveWithPrefix(ctx, "")) }()
			objectRoot := t.TempDir()
			cli := storage.NewLocalChunkManager(objectstorage.RootPath(objectRoot))
			rootPatch := mockey.Mock(binlog.GetRootPath).Return(objectRoot).Build()
			defer rootPatch.UnPatch()
			catalog := kvdatacoord.NewCatalog(kv, objectRoot, root)
			store := metacache.NewMetaStore(catalog)
			m := &meta{ctx: ctx, catalog: catalog, segments: NewSegmentsInfo(store), metaStore: store, channelSync: newChannelSync()}
			handler := &ServerHandler{s: &Server{ctx: ctx, meta: m}}

			lookups, injected := 0, 0
			b := broker2.NewMockBroker(t)
			b.EXPECT().HasCollection(mock.Anything, int64(1)).RunAndReturn(func(context.Context, int64) (bool, error) {
				lookups++
				if tc.lookupErr != nil && (tc.persistentError || injected == 0) {
					injected++
					return false, tc.lookupErr
				}
				return !tc.collectionDropped, nil
			}).Maybe()
			gc := newGarbageCollector(m, handler, GcOption{cli: cli, dropTolerance: 0, broker: b})
			defer gc.close()

			const channel = "gc-channel-lookup_1v0"
			store.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{
				channel: {ChannelName: channel, Timestamp: tc.checkpoint, MsgID: []byte{1}},
			})
			segment := NewSegmentInfo(&datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2, InsertChannel: channel,
				State: commonpb.SegmentState_Dropped, Level: datapb.SegmentLevel_L1,
				DroppedAt:   uint64(time.Now().Add(-time.Hour).UnixNano()),
				DmlPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 120, MsgID: []byte{2}},
				Binlogs:     []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 10, EntriesNum: 1}}}},
			})
			require.NoError(t, m.AddSegment(ctx, segment))
			objectPath, err := binlog.BuildLogPath(storage.InsertBinlog, 1, 2, 3, 100, 10)
			require.NoError(t, err)
			require.NoError(t, cli.Write(ctx, objectPath, []byte("segment data")))

			assertState := func(retained bool) {
				t.Helper()
				exists, err := cli.Exist(ctx, objectPath)
				require.NoError(t, err)
				require.Equal(t, retained, exists, "segment file retention")
				require.Equal(t, retained, m.GetSegment(ctx, 3) != nil, "in-memory segment retention")
				persisted, err := catalog.ListSegments(ctx, 1)
				require.NoError(t, err)
				if retained {
					require.Len(t, persisted, 1, "persisted segment and binlogs must survive")
					require.Equal(t, uint64(120), persisted[0].GetDmlPosition().GetTimestamp())
					require.Len(t, persisted[0].GetBinlogs(), 1)
					// Recover the segment cache from persisted metadata, rather than
					// relying only on the pre-GC in-memory copy. A fresh store is
					// what makes that real: reusing the old one would still hold
					// the pre-GC segment. The channel checkpoint is re-seeded
					// because GetDataVChanPositions resolves through it.
					recovered := metacache.NewMetaStore(catalog)
					recovered.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{
						channel: {ChannelName: channel, Timestamp: tc.checkpoint, MsgID: []byte{1}},
					})
					m.segments = NewSegmentsInfo(recovered)
					m.metaStore = recovered
					m.segments.SetSegment(3, NewSegmentInfo(persisted[0]))
					recovery := handler.GetDataVChanPositions(&channelMeta{Name: channel, CollectionID: 1}, allPartitionID)
					require.Equal(t, []int64{3}, recovery.GetDroppedSegmentIds())
				} else {
					require.Empty(t, persisted)
					_, err = catalog.LoadFromSegmentPath(ctx, 1, 2, 3)
					require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
				}
			}
			gc.recycleDroppedSegments(ctx, nil)
			if tc.checkpoint >= 120 {
				require.Zero(t, lookups, "a sufficient checkpoint must not depend on the collection lookup")
			} else {
				require.Equal(t, 1, lookups)
				if tc.lookupErr != nil {
					require.Equal(t, 1, injected, "fault must occur at the collection lookup")
				}
			}
			assertState(tc.wantRetained)
			if tc.wantRetained {
				// Retry without clearing a persistent fault.
				gc.recycleDroppedSegments(ctx, nil)
				stillRetained := tc.persistentError || !tc.collectionDropped
				assertState(stillRetained)
				require.Equal(t, 2, lookups)
				if tc.lookupErr != nil {
					if tc.persistentError {
						require.Equal(t, 2, injected)
					} else {
						require.Equal(t, 1, injected, "transient failure must not affect the next sweep")
					}
				}
				if stillRetained {
					// Advance the checkpoint past the segment's DML position.
					// Goes through m.metaStore because assertState may have
					// swapped in the recovered store above.
					m.metaStore.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{
						channel: {ChannelName: channel, Timestamp: 130, MsgID: []byte{1}},
					})
					gc.recycleDroppedSegments(ctx, nil)
					assertState(false)
					require.Equal(t, 2, lookups, "checkpoint progress must unblock GC even if lookups still fail")
				}
			}
		})
	}
}
