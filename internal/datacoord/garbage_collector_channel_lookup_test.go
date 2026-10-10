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
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Exercise real etcd metadata and local object deletion through the GC sweep.
// Faults affect only channel-marker Load; segment reads and deletion stay healthy.
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
		name            string
		marker          string
		checkpoint      uint64
		lookupErr       error
		persistentError bool
		wantRetained    bool
	}{
		{name: "transient lookup failure", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 100, lookupErr: context.DeadlineExceeded, wantRetained: true},
		{name: "persistent lookup failure", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 100, lookupErr: rpctypes.ErrPermissionDenied, persistentError: true, wantRetained: true},
		{name: "checkpoint behind", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 100, wantRetained: true},
		{name: "checkpoint equal", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 120},
		{name: "checkpoint passed", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 130},
		{name: "checkpoint equal with persistent failure", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 120, lookupErr: context.DeadlineExceeded, persistentError: true},
		{name: "checkpoint passed with persistent failure", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 130, lookupErr: rpctypes.ErrPermissionDenied, persistentError: true},
		{name: "dropped checkpoint with persistent failure", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: funcutil.DroppedChannelCheckpointTimestamp, lookupErr: rpctypes.ErrPermissionDenied, persistentError: true},
		{name: "removed channel", marker: kvdatacoord.RemoveFlagTomestone, checkpoint: 100},
		{name: "missing marker", checkpoint: 100},
		{name: "removed channel after lookup recovers", marker: kvdatacoord.RemoveFlagTomestone, checkpoint: 100, lookupErr: context.DeadlineExceeded, wantRetained: true},
		{name: "missing marker after lookup recovers", checkpoint: 100, lookupErr: context.DeadlineExceeded, wantRetained: true},
		{name: "permission denied", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 100, lookupErr: rpctypes.ErrPermissionDenied, wantRetained: true},
		{name: "canceled lookup", marker: kvdatacoord.NonRemoveFlagTomestone, checkpoint: 100, lookupErr: context.Canceled, wantRetained: true},
		{name: "unknown marker preserves legacy behavior", marker: "invalid", checkpoint: 100},
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
			m := &meta{ctx: ctx, catalog: catalog, segments: NewSegmentsInfo(), channelCPs: newChannelCps()}
			handler := &ServerHandler{s: &Server{ctx: ctx, meta: m}}
			gc := newGarbageCollector(m, handler, GcOption{cli: cli, dropTolerance: 0})
			defer gc.close()

			const channel = "gc-channel-lookup_1v0"
			markerKey := path.Join(kvdatacoord.ChannelRemovePrefix, channel)
			if tc.marker != "" {
				require.NoError(t, kv.Save(ctx, markerKey, tc.marker))
			}
			m.channelCPs.checkpoints[channel] = &msgpb.MsgPosition{ChannelName: channel, Timestamp: tc.checkpoint, MsgID: []byte{1}}
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

			markerReads, injected := 0, 0
			patch := mockey.Mock(mockey.GetMethod(kv, "Load")).When(func(_ context.Context, key string) bool {
				if key != markerKey {
					return false
				}
				markerReads++
				if tc.lookupErr != nil && (tc.persistentError || injected == 0) {
					injected++
					return true
				}
				return false
			}).Return("", tc.lookupErr).Build()
			defer patch.UnPatch()

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
					// relying only on the pre-GC in-memory copy.
					m.segments = NewSegmentsInfo()
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
				require.Zero(t, markerReads, "a sufficient checkpoint must not depend on marker availability")
			} else {
				require.Equal(t, 1, markerReads)
				if tc.lookupErr != nil {
					require.Equal(t, 1, injected, "fault must occur at the channel marker lookup")
				}
			}
			assertState(tc.wantRetained)
			if tc.wantRetained {
				// Retry without rewriting the marker or clearing the injected fault.
				gc.recycleDroppedSegments(ctx, nil)
				stillRetained := tc.persistentError || tc.marker == kvdatacoord.NonRemoveFlagTomestone
				assertState(stillRetained)
				require.Equal(t, 2, markerReads)
				if tc.lookupErr != nil {
					if tc.persistentError {
						require.Equal(t, 2, injected)
					} else {
						require.Equal(t, 1, injected, "transient failure must not affect the next sweep")
					}
				}
				if stillRetained {
					m.channelCPs.checkpoints[channel].Timestamp = 130
					gc.recycleDroppedSegments(ctx, nil)
					assertState(false)
					require.Equal(t, 2, markerReads, "checkpoint progress must unblock GC even if marker reads still fail")
				}
			}
		})
	}
}
