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

package gc

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Unrelated GC tests assume the WAL has durably consumed their segments.

// Use real catalog reads, etcd metadata and local files, so a wrong watermark
// source or a fail-open decision is observable as data deletion.
func TestGarbageCollector_PhysicalCheckpoint(t *testing.T) {
	ctx := context.Background()
	client, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	loaded := mockey.Mock((*meta.MockHandler).ListLoadedSegments).Return([]int64{}, nil).Build()
	defer loaded.UnPatch()
	for _, tc := range []struct {
		name                                 string
		tick                                 uint64
		missing, corrupt, readError, removed bool
		zeroDML                              bool
		commitTick                           uint64
		wantRetained                         bool
	}{
		{name: "behind despite newer control and historical checkpoints", tick: 119, wantRetained: true},
		{name: "equal", tick: 120},
		{name: "ahead", tick: 121},
		{name: "missing", missing: true, wantRetained: true},
		{name: "missing with zero DML position", missing: true, zeroDML: true, wantRetained: true},
		{name: "corrupt", corrupt: true, wantRetained: true},
		{name: "read failure", readError: true, wantRetained: true},
		{name: "removed channel still waits for durable replay", tick: 119, removed: true, wantRetained: true},
		{name: "removed channel with missing checkpoint", missing: true, removed: true, wantRetained: true},
		{name: "removed channel covered", tick: 120, removed: true},
		{name: "import commit is not covered", tick: 120, commitTick: 130, wantRetained: true},
		{name: "import commit is covered", tick: 130, commitTick: 130},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := "gc-physical-checkpoint-" + uuid.NewString()
			kv := etcdkv.NewEtcdKV(client, root)
			defer func() { require.NoError(t, kv.RemoveWithPrefix(ctx, "")) }()
			objectRoot := t.TempDir()
			cli := storage.NewLocalChunkManager(objectstorage.RootPath(objectRoot))
			rootPatch := mockey.Mock(binlog.GetRootPath).Return(objectRoot).Build()
			defer rootPatch.UnPatch()
			catalog := kvdatacoord.NewCatalog(kv, objectRoot, root)
			store := metacache.NewMetaStore(catalog)
			m := &meta.Meta{Ctx: ctx, Catalog: catalog, Segments: meta.NewSegmentsInfo(store), MetaStore: store, ChannelSync: meta.NewChannelSync()}
			gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{Cli: cli, StreamingNodeCatalog: kvstreamingnode.NewCataLog(kv)})
			defer gc.Close()
			const pchannel = "gc-physical"
			const checkpointKey = "streamingnode-meta/wal/" + pchannel + "/consume-checkpoint"
			const healthyCheckpointKey = "streamingnode-meta/wal/healthy/consume-checkpoint"
			saveCheckpoint := func(tick uint64) {
				data, err := proto.Marshal(&streamingpb.WALCheckpoint{TimeTick: tick, ControlCheckpointTimeTick: 1000})
				require.NoError(t, err)
				require.NoError(t, kv.Save(ctx, checkpointKey, string(data)))
			}
			if !tc.missing {
				saveCheckpoint(tc.tick)
			}
			if tc.corrupt {
				require.NoError(t, kv.Save(ctx, checkpointKey, "invalid protobuf"))
			}
			healthyData, err := proto.Marshal(&streamingpb.WALCheckpoint{TimeTick: 130})
			require.NoError(t, err)
			require.NoError(t, kv.Save(ctx, healthyCheckpointKey, string(healthyData)))
			// Different VChannels on the same PChannel must share a single read.
			// A failed read on that PChannel must not block another healthy one.
			channels := []string{pchannel + "_1v0", pchannel + "_2v0", pchannel + "_1v0", "healthy_3v0"}
			paths := make([]string, len(channels))
			for i, channel := range channels {
				marker := kvdatacoord.NonRemoveFlagTomestone
				if tc.removed {
					marker = kvdatacoord.RemoveFlagTomestone
				}
				require.NoError(t, kv.Save(ctx, path.Join(kvdatacoord.ChannelRemovePrefix, channel), marker))
				// A stale or even dropped historical checkpoint must have no influence.
				store.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{channel: {Timestamp: ^uint64(0)}})
				id := int64(i + 1)
				seg := meta.NewSegmentInfo(&datapb.SegmentInfo{
					ID: id, CollectionID: 1, PartitionID: 2, InsertChannel: channel,
					State: commonpb.SegmentState_Dropped, Level: datapb.SegmentLevel_L1,
					DroppedAt: uint64(time.Now().Add(-time.Hour).UnixNano()), CommitTimestamp: tc.commitTick,
					DmlPosition: &msgpb.MsgPosition{Timestamp: 120},
					Binlogs:     []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 10, EntriesNum: 1}}}},
				})
				if tc.zeroDML {
					seg.DmlPosition = nil
				}
				require.NoError(t, m.AddSegment(ctx, seg))
				paths[i], err = binlog.BuildLogPath(storage.InsertBinlog, 1, 2, id, 100, 10)
				require.NoError(t, err)
				require.NoError(t, cli.Write(ctx, paths[i], []byte("segment data")))
			}
			reads := make(map[string]int)
			readPatch := mockey.Mock(mockey.GetMethod(kv, "Load")).When(func(_ context.Context, key string) bool {
				if key != checkpointKey && key != healthyCheckpointKey {
					return false
				}
				reads[key]++
				return key == checkpointKey && tc.readError
			}).Return("", merr.ErrServiceUnavailable).Build()
			defer readPatch.UnPatch()
			gc.RecycleDroppedSegments(ctx, nil)
			require.Equal(t, map[string]int{checkpointKey: 1, healthyCheckpointKey: 1}, reads, "one read per PChannel, even on failure")
			for i, file := range paths {
				wantRetained := tc.wantRetained && i < 3
				require.Equal(t, wantRetained, m.GetSegment(ctx, int64(i+1)) != nil)
				exists, err := cli.Exist(ctx, file)
				require.NoError(t, err)
				require.Equal(t, wantRetained, exists)
			}
			if tc.wantRetained {
				// A failed/old snapshot is never reused by the next sweep.
				tc.readError = false
				saveCheckpoint(130)
				gc.RecycleDroppedSegments(ctx, nil)
				require.Equal(t, map[string]int{checkpointKey: 2, healthyCheckpointKey: 1}, reads)
				for i, file := range paths {
					require.Nil(t, m.GetSegment(ctx, int64(i+1)))
					exists, err := cli.Exist(ctx, file)
					require.NoError(t, err)
					require.False(t, exists)
				}
			}
		})
	}
}
