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

package l0materializer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type capturingDeltaRegistration struct {
	syncmgr.MetaWriter
	calls  int
	failAt int
	files  []*datapb.Binlog
}

func (r *capturingDeltaRegistration) UpdateSync(ctx context.Context, t *syncmgr.SyncTask) error {
	r.calls++
	if r.calls == r.failAt {
		return context.DeadlineExceeded
	}
	_, _, delta, _ := t.Binlogs()
	r.files = append(r.files, delta.GetBinlogs()...)
	return nil
}

type typedWALFixture struct {
	dir      string
	cm       storage.ChunkManager
	registry capturingDeltaRegistration
	task     nodescheduler.Task
	notified []uint64
	m        *WALMaterializer
	tracker  *messageack.Tracker
}

func newTypedWALFixture(t *testing.T) *typedWALFixture {
	t.Helper()
	paramtable.Init()
	f := &typedWALFixture{dir: t.TempDir(), tracker: messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)}
	pt := paramtable.Get()
	oldType := pt.CommonCfg.StorageType.SwapTempValue("local")
	t.Cleanup(func() { pt.CommonCfg.StorageType.SwapTempValue(oldType) })
	oldPath := pt.LocalStorageCfg.Path.SwapTempValue(f.dir)
	t.Cleanup(func() { pt.LocalStorageCfg.Path.SwapTempValue(oldPath) })
	f.cm = storage.NewLocalChunkManager(objectstorage.RootPath(f.dir))
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task, _ ...nodescheduler.Option) nodescheduler.TaskHandle {
		f.task = task
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	schema := materializeSchema(schemapb.DataType_Int64)
	schema.Version = 7
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 105, Name: "age", DataType: schemapb.DataType_Int64})
	f.m = NewWALMaterializer(WALConfig{
		VChannel: "p_1v0", Runtime: moduleapi.Runtime{Scheduler: scheduler}, MaterializeMaxBytes: 1, MaterializeMaxRows: 1,
		Materializer:   NewSyncMaterializer(f.cm, allocator.NewLocalAllocator(1, 100), &f.registry),
		SchemaAt:       func(uint64) *schemapb.CollectionSchema { return schema },
		OnMaterialized: func(tt uint64) { f.notified = append(f.notified, tt) },
	})
	return f
}

func walUpdate(tt uint64, scope, val int64) message.ImmutableMessage {
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}
	return message.NewUpdateMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 1, SchemaVersion: proto.Int32(7)}).
		WithBody(&messagespb.UpdateMessageBody{PartitionId: scope, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{
			{FieldId: 105, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: val}}}}},
		}}).MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
}

func TestWALMaterializerMixedTxnNativeRegistrationRetry(t *testing.T) {
	f := newTypedWALFixture(t)
	f.registry.failAt = 2
	txn := walTxn(t, walUpdate(100, -1, 11), walDelete(101), walUpdate(102, 10, 22), walUpdate(103, -1, 33))
	walObserve(f.m, f.tracker, txn)
	require.NotNil(t, f.task)
	ctx := context.Background()
	// A test-owned regular file at the output directory injects a real native
	// write failure before the coordinator sees a registration.
	obstacle := filepath.Join(f.dir, common.SegmentDeltaLogPath)
	require.NoError(t, os.WriteFile(obstacle, []byte("block writer"), 0o600))
	require.Error(t, f.task.Execute(ctx))
	require.Zero(t, f.registry.calls)
	require.Empty(t, f.notified)
	require.Zero(t, f.m.MaterializedTimeTick())
	require.Zero(t, f.tracker.CompletedPoint().TimeTick)
	require.NoError(t, os.Remove(obstacle))
	require.Error(t, f.task.Execute(ctx))
	require.Zero(t, f.m.MaterializedTimeTick())
	require.Zero(t, f.tracker.CompletedPoint().TimeTick)
	require.Empty(t, f.notified)
	require.Len(t, f.registry.files, 1)
	require.NoError(t, f.task.Execute(ctx))
	require.Equal(t, []uint64{300}, f.notified)
	require.Equal(t, uint64(300), f.tracker.CompletedPoint().TimeTick)
	require.Len(t, f.registry.files, 3)
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: f.dir}
	seen := map[uint32][]int64{}
	duplicates := 0
	for _, bin := range f.registry.files {
		require.NotNil(t, bin.GetMutationCounts())
		err := storage.ReadMutationDelta(ctx, f.cm, bin, storage.StorageV2, schemapb.DataType_Int64, nil, nil, cfg, nil, func(l deltacodec.Layout, rs []deltacodec.Row) error {
			for _, r := range rs {
				require.Equal(t, uint64(300), r.TimeTick)
				if r.Operation == deltacodec.Delete {
					require.Equal(t, uint32(1), r.SourceOrdinal)
					require.Equal(t, int64(1), bin.DeleteKeyCount())
					continue
				}
				value := r.Assignments[0].GetLiteral().GetScalarValue().GetLongData()
				if existing, ok := seen[r.SourceOrdinal]; ok {
					duplicates++
					require.Equal(t, []int64{l.PartitionID, value}, existing)
				}
				seen[r.SourceOrdinal] = []int64{l.PartitionID, value}
				require.True(t, slices.Equal([]int64{1, 2}, r.PrimaryKeys.GetIntId().GetData()))
			}
			return nil
		})
		require.NoError(t, err)
	}
	require.Positive(t, duplicates)
	require.Equal(t, map[uint32][]int64{0: {-1, 11}, 2: {10, 22}, 3: {-1, 33}}, seen)
}

func TestWALMaterializerStandaloneNativeUpdate(t *testing.T) {
	f := newTypedWALFixture(t)
	ctx := context.Background()
	walObserve(f.m, f.tracker, walUpdate(140, 10, 77))
	require.NotNil(t, f.task)
	require.NoError(t, f.task.Execute(ctx))
	require.Equal(t, []uint64{140}, f.notified)
	require.Equal(t, uint64(140), f.m.MaterializedTimeTick())
	require.Equal(t, uint64(140), f.tracker.CompletedPoint().TimeTick)
	require.Len(t, f.registry.files, 1)
	bin := f.registry.files[0]
	require.Equal(t, int64(2), bin.GetMutationCounts().GetUpdateKeys())
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: f.dir}
	var decoded []deltacodec.Row
	err := storage.ReadMutationDelta(ctx, f.cm, bin, storage.StorageV2, schemapb.DataType_Int64, nil, nil, cfg, nil, func(l deltacodec.Layout, rows []deltacodec.Row) error {
		require.Equal(t, int64(10), l.PartitionID)
		require.Equal(t, "p_1v0", l.VChannel)
		decoded = append(decoded, rows...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	row := decoded[0]
	require.Equal(t, deltacodec.Update, row.Operation)
	require.Equal(t, uint64(140), row.TimeTick)
	require.Zero(t, row.SourceOrdinal)
	require.Equal(t, []int64{1, 2}, row.PrimaryKeys.GetIntId().GetData())
	require.Len(t, row.Assignments, 1)
	require.Equal(t, int64(105), row.Assignments[0].GetFieldId())
	require.Equal(t, int64(77), row.Assignments[0].GetLiteral().GetScalarValue().GetLongData())
}

func (f *typedWALFixture) requireUnpublished(t *testing.T) {
	t.Helper()
	require.Zero(t, f.registry.calls)
	require.Empty(t, f.notified)
	require.Zero(t, f.m.MaterializedTimeTick())
	require.Zero(t, f.tracker.CompletedPoint().TimeTick)
}

func TestWALMaterializerInvalidScope(t *testing.T) {
	f := newTypedWALFixture(t)
	// Two ticks in scope 10 force a closed valid group before the invalid group.
	f.m.maxBytes = 0
	walObserve(f.m, f.tracker, walUpdate(80, 10, 1))
	walObserve(f.m, f.tracker, walUpdate(90, 10, 2))
	walObserve(f.m, f.tracker, walUpdate(100, -2, 3))
	f.m.RequestPersistThrough(100)
	require.NotNil(t, f.task)
	require.ErrorContains(t, f.task.Execute(context.Background()), "partition scope")
	f.requireUnpublished(t)
	require.NoDirExists(t, filepath.Join(f.dir, common.SegmentDeltaLogPath))
}

func TestWALMaterializerDroppedUpdatePayload(t *testing.T) {
	for _, name := range []string{"standalone-empty", "txn-delete-arm", "txn-dropped-update-block"} {
		t.Run(name, func(t *testing.T) {
			f := newTypedWALFixture(t)
			msg := walUpdate(100, 10, 11)
			if name != "standalone-empty" {
				msg = walTxn(t, walDelete(99), msg)
			}
			walObserve(f.m, f.tracker, msg)
			require.NotNil(t, f.task)
			entry := messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{})
			switch name {
			case "standalone-empty":
				entry.Entry = nil
			case "txn-delete-arm":
				block := entry.GetMutations().GetBlocks()[0].GetDelete()
				entry.Entry = &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{Blocks: []*streamingpb.TransformDeleteBlock{block}}}
			case "txn-dropped-update-block":
				entry.GetMutations().Blocks = entry.GetMutations().Blocks[:1]
			}
			patch := mockey.Mock(messageutil.BuildTransformLogEntry).Return(entry).Build()
			defer patch.UnPatch()
			require.Error(t, f.task.Execute(context.Background()))
			f.requireUnpublished(t)
		})
	}
}

func TestMixedTxnDeleteOnlyScopeKeepsOrdinal(t *testing.T) {
	f := newTypedWALFixture(t)
	walObserve(f.m, f.tracker, walTxn(t, walUpdate(100, 10, 55), walScopedDelete(101, common.AllPartitionsID)))
	require.NoError(t, f.task.Execute(t.Context()))
	require.Len(t, f.registry.files, 2)
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: f.dir}
	scopes := map[int64]uint32{}
	for _, file := range f.registry.files {
		require.NotNil(t, file.MutationCounts)
		require.NoError(t, storage.ReadMutationDelta(t.Context(), f.cm, file, storage.StorageV2, schemapb.DataType_Int64, nil, nil, cfg, nil, func(layout deltacodec.Layout, rows []deltacodec.Row) error {
			require.Len(t, rows, 1)
			require.EqualValues(t, 300, rows[0].TimeTick)
			scopes[layout.PartitionID] = rows[0].SourceOrdinal
			if layout.PartitionID == common.AllPartitionsID {
				require.Equal(t, deltacodec.Delete, rows[0].Operation)
			}
			return nil
		}))
	}
	require.Equal(t, map[int64]uint32{10: 0, common.AllPartitionsID: 1}, scopes)
}
