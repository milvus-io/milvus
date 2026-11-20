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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type capturingDeltaRegistration struct {
	syncmgr.MetaWriter
	calls int
	files []*datapb.Binlog
}

func (r *capturingDeltaRegistration) UpdateSync(ctx context.Context, t *syncmgr.SyncTask) error {
	r.calls++
	if r.calls == 2 {
		return context.DeadlineExceeded
	}
	_, _, delta, _ := t.Binlogs()
	r.files = append(r.files, delta.GetBinlogs()...)
	return nil
}

func TestWALMaterializerMixedTxnNativeRegistrationRetry(t *testing.T) {
	paramtable.Init()
	dir := t.TempDir()
	pt := paramtable.Get()
	oldType := pt.CommonCfg.StorageType.SwapTempValue("local")
	defer pt.CommonCfg.StorageType.SwapTempValue(oldType)
	oldPath := pt.LocalStorageCfg.Path.SwapTempValue(dir)
	defer pt.LocalStorageCfg.Path.SwapTempValue(oldPath)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(dir))
	registry := &capturingDeltaRegistration{}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	var task nodescheduler.Task
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(t nodescheduler.Task, _ ...nodescheduler.Option) nodescheduler.TaskHandle { task = t; return nil }).Build()
	defer patch.UnPatch()
	schema := materializeSchema(schemapb.DataType_Int64)
	schema.Version = 7
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 105, Name: "age", DataType: schemapb.DataType_Int64})
	var notified []uint64
	m := NewWALMaterializer(WALConfig{VChannel: "p_1v0", Runtime: moduleapi.Runtime{Scheduler: scheduler}, MaterializeMaxBytes: 1, MaterializeMaxRows: 1, Materializer: NewSyncMaterializer(cm, allocator.NewLocalAllocator(1, 100), registry), SchemaAt: func(uint64) *schemapb.CollectionSchema { return schema }, OnMaterialized: func(tt uint64) { notified = append(notified, tt) }})
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}
	update := func(tt uint64, scope, val int64) message.ImmutableMessage {
		return message.NewUpdateMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 1, SchemaVersion: proto.Int32(7)}).WithBody(&messagespb.UpdateMessageBody{PartitionId: scope, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{{FieldId: 105, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: val}}}}}}}).MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
	}
	txn := walTxn(t, update(100, -1, 11), walDelete(101), update(102, 10, 22), update(103, -1, 33))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, txn)
	require.NotNil(t, task)
	ctx := context.Background()
	// A test-owned regular file at the output directory injects a real native
	// write failure before the coordinator sees a registration.
	obstacle := filepath.Join(dir, common.SegmentDeltaLogPath)
	require.NoError(t, os.WriteFile(obstacle, []byte("block writer"), 0o600))
	require.Error(t, task.Execute(ctx))
	require.Zero(t, registry.calls)
	require.Empty(t, notified)
	require.Zero(t, m.MaterializedTimeTick())
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	require.NoError(t, os.Remove(obstacle))
	require.Error(t, task.Execute(ctx))
	require.Zero(t, m.MaterializedTimeTick())
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	require.Empty(t, notified)
	require.Len(t, registry.files, 1)
	require.NoError(t, task.Execute(ctx))
	require.Equal(t, []uint64{300}, notified)
	require.Equal(t, uint64(300), tracker.CompletedPoint().TimeTick)
	require.Len(t, registry.files, 3)
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	seen := map[uint32][]int64{}
	duplicates := 0
	for _, bin := range registry.files {
		require.NotNil(t, bin.GetMutationCounts())
		err := storage.ReadMutationDelta(ctx, cm, bin, storage.StorageV2, schemapb.DataType_Int64, nil, cfg, nil, func(l deltacodec.Layout, rs []deltacodec.Row) error {
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
