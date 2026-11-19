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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type pinFlushScheduler struct{ tasks []nodescheduler.Task }

func (s *pinFlushScheduler) Submit(task nodescheduler.Task, _ ...nodescheduler.Option) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return nil
}

type pinFlushWAL struct {
	streaming.WALAccesser
	appendFn func(context.Context, message.MutableMessage) (*types.AppendResult, error)
}

func (w *pinFlushWAL) RawAppend(ctx context.Context, msg message.MutableMessage, _ ...streaming.AppendOption) (*types.AppendResult, error) {
	return w.appendFn(ctx, msg)
}

func observePinFlushModule(t *testing.T, module *vchannel.VChannelRecoveryModule, msg message.ImmutableMessage) {
	t.Helper()
	owner := message.NewOwnedImmutableMessage(msg, nil)
	dispatch := owner.Clone()
	require.True(t, module.ObserveMessage(t.Context(), dispatch))
	dispatch.Release()
	owner.Release()
}

func pinFlushImmutable(msg message.MutableMessage, tick uint64) message.ImmutableMessage {
	return msg.WithTimeTick(tick).WithLastConfirmed(rmq.NewRmqID(int64(tick - 1))).IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
}

func persistPinFlushModule(t *testing.T, catalog metastore.StreamingNodeCataLog, module *vchannel.VChannelRecoveryModule) {
	t.Helper()
	for _, snapshot := range module.ConsumeDirtySnapshots() {
		if snapshot.ModuleName() != moduleapi.ModuleNameVChannel {
			continue
		}
		entry := snapshot.Payload().(*streamingpb.VChannelMeta)
		payload := &metastore.WALRecoverySnapshot{}
		if snapshot.Op() == moduleapi.SnapshotOpUpsertBase {
			payload.VChannelBaseMetas = map[string]*streamingpb.VChannelMeta{entry.GetVchannel(): entry}
		} else {
			payload.VChannels = map[string]*streamingpb.VChannelMeta{entry.GetVchannel(): entry}
		}
		require.NoError(t, catalog.SaveRecoverySnapshot(t.Context(), snapshot.Key().PChannel, payload))
		snapshot.MarkPersisted()
	}
}

// Exercise the real input producer of M: target genesis -> WALMaterializer ->
// module.markL0Materialized -> dirty snapshot -> recovery catalog -> DataCoord.
// No test injects a positive materialization timestamp.
func TestSplitNoDeletePinReleaseRequiresRealFlushedCertificate(t *testing.T) {
	ctx := t.Context()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	store, taskCatalog := newSplitTestStore(t)
	_, err = store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: splitTestTarget1, Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, TransformStartAfterTimetick: 100, NumOfRows: 1})))
	mt.StreamingNodeCatalog = kvstreamingnode.NewCataLog(meta.NewMetaMemoryKV())
	scheduler := &pinFlushScheduler{}
	modules := map[string]*vchannel.VChannelRecoveryModule{}
	for index, target := range []string{splitTestTarget1, splitTestTarget2} {
		module, err := vchannel.NewModule(vchannel.ModuleConfig{PChannel: funcutil.ToPhysicalChannel(target), VChannel: target, Runtime: moduleapi.Runtime{Scheduler: scheduler}})
		require.NoError(t, err)
		genesis := message.NewSplitShardMessageBuilderV2().WithVChannel(target).
			WithHeader(&message.SplitShardMessageHeader{CollectionId: 100, SplitTaskId: 7, SourceVchannel: splitTestSource, TargetVchannels: []string{splitTestTarget1, splitTestTarget2}}).
			WithBody(&message.SplitShardMessageBody{GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: 100, PartitionIds: []int64{1}}, Genesis: &message.CreateCollectionRequest{CollectionID: 100, CollectionSchema: &schemapb.CollectionSchema{Name: "split", Version: 1}}, Routing: &message.SplitShardRouting{RoutingModulus: 2}}).MustBuildMutable()
		observePinFlushModule(t, module, pinFlushImmutable(genesis, uint64(110+index)))
		persistPinFlushModule(t, mt.StreamingNodeCatalog, module)
		base, err := mt.StreamingNodeCatalog.GetVChannelBase(ctx, funcutil.ToPhysicalChannel(target), target)
		require.NoError(t, err)
		require.Zero(t, base.GetTransformMaterializedTimeTick(), "genesis does not create an L0 certificate")
		modules[target] = module
	}
	require.Empty(t, scheduler.tasks, "no-delete target schedules no materialization by itself")
	appends := 0
	accesser := &pinFlushWAL{appendFn: func(_ context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
		appends++
		require.Equal(t, message.MessageTypeManualFlush, msg.MessageType())
		require.True(t, msg.IsUnreplicable())
		require.Nil(t, msg.BroadcastHeader(), "local boundary must not take the collection broadcast key")
		observePinFlushModule(t, modules[msg.VChannel()], pinFlushImmutable(msg, uint64(200+appends)))
		return &types.AppendResult{TimeTick: uint64(200 + appends)}, nil
	}}
	previous := streaming.WAL()
	streaming.SetWALForTest(accesser)
	t.Cleanup(func() { streaming.SetWALForTest(previous) })
	server := &Server{meta: mt, splitStore: store}
	require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
	require.Equal(t, 2, appends)
	require.Len(t, scheduler.tasks, 2)
	// Restarting DataCoord after the requests were appended does not issue a
	// flush on every Release poll or Resume retry.
	server.splitStore, err = split.NewStore(ctx, taskCatalog)
	require.NoError(t, err)
	require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
	require.Equal(t, 2, appends)
	done, err := server.ReleaseShardSplitPins(ctx, 7)
	require.NoError(t, err)
	require.False(t, done, "append success is not materialization proof")
	for _, task := range scheduler.tasks {
		require.NoError(t, task.Execute(ctx))
	}
	done, err = server.ReleaseShardSplitPins(ctx, 7)
	require.NoError(t, err)
	require.False(t, done, "dirty in-memory M must not be treated as persisted")
	for _, module := range modules {
		persistPinFlushModule(t, mt.StreamingNodeCatalog, module)
	}
	// Advance real persisted M beyond F with a later local flush. Closure must
	// keep the original F, rather than chase this moving frontier.
	later := message.NewManualFlushMessageBuilderV2().WithUnreplicable().WithVChannel(splitTestTarget1).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 100}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
	observePinFlushModule(t, modules[splitTestTarget1], pinFlushImmutable(later, 500))
	require.NoError(t, scheduler.tasks[len(scheduler.tasks)-1].Execute(ctx))
	persistPinFlushModule(t, mt.StreamingNodeCatalog, modules[splitTestTarget1])
	// A straddling L0 remains a hole even when its end exceeds F. In contrast,
	// continuous new L0 after F must not extend the split's finite obligation.
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 20, CollectionID: 100, PartitionID: 1, InsertChannel: splitTestTarget1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, StartPosition: &msgpb.MsgPosition{Timestamp: 200}, DmlPosition: &msgpb.MsgPosition{Timestamp: 300}})))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 21, CollectionID: 100, PartitionID: 1, InsertChannel: splitTestTarget1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, StartPosition: &msgpb.MsgPosition{Timestamp: 300}, DmlPosition: &msgpb.MsgPosition{Timestamp: 400}})))
	done, err = server.ReleaseShardSplitPins(ctx, 7)
	require.NoError(t, err)
	require.False(t, done, "straddling L0 still needs folding")
	require.EqualValues(t, 100, mt.GetSegment(ctx, 10).GetTransformStartAfterTimetick())
	require.NoError(t, mt.DropSegment(ctx, 20))
	done, err = server.ReleaseShardSplitPins(ctx, 7)
	require.NoError(t, err)
	require.True(t, done, "newer L0 must not block finite history closure")
	require.Empty(t, server.splitStore.TransformPins(100))
	require.EqualValues(t, 201, mt.GetSegment(ctx, 10).GetTransformStartAfterTimetick(), "closure uses fixed F even after real M reaches 500")
}

func TestSplitPinFlushFailureDoesNotPersistResume(t *testing.T) {
	ctx := t.Context()
	store, _ := newSplitTestStore(t)
	_, err := store.Record(ctx, splitTestRecord(7))
	require.NoError(t, err)
	require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
	require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	failure := merr.WrapErrServiceUnavailableMsg("injected target flush append failure")
	appends := 0
	accesser := &pinFlushWAL{appendFn: func(context.Context, message.MutableMessage) (*types.AppendResult, error) {
		appends++
		if appends == 2 {
			return nil, failure
		}
		return &types.AppendResult{TimeTick: 200}, nil
	}}
	previous := streaming.WAL()
	streaming.SetWALForTest(accesser)
	t.Cleanup(func() { streaming.SetWALForTest(previous) })
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	server := &Server{splitStore: store, meta: mt}
	require.ErrorIs(t, server.ResumeShardSplitL0(ctx, 7), failure)
	require.False(t, store.Get(7).GetL0Resumed())
	require.NotEmpty(t, store.TransformPins(100))
	require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
	require.True(t, store.Get(7).GetL0Resumed())
	require.Equal(t, 3, appends, "partial attempt reuses its durable first target boundary")
	require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
	require.Equal(t, 3, appends)
}

// A lost boundary write must retain the pin and suspension. Only the lost
// target is flushed again; established boundaries survive Store recovery.
type failingSplitHistoryCatalog struct {
	metastore.DataCoordCatalog
	splitHistoryCatalog
	failure            error
	persistBeforeError bool
}

func (c *failingSplitHistoryCatalog) SaveSplitHistoryBoundary(ctx context.Context, task *datapb.SplitShardTask, target *datapb.SplitShardTaskTarget, through uint64) error {
	if c.failure != nil {
		err := c.failure
		c.failure = nil
		if c.persistBeforeError {
			if saveErr := c.splitHistoryCatalog.SaveSplitHistoryBoundary(ctx, task, target, through); saveErr != nil {
				return saveErr
			}
		}
		return err
	}
	return c.splitHistoryCatalog.SaveSplitHistoryBoundary(ctx, task, target, through)
}

func TestSplitFiniteBoundaryLostWriteAndLegacyResume(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			ctx := t.Context()
			mt, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			store, taskCatalog := newSplitTestStore(t)
			_, err = store.Record(ctx, splitTestRecord(7))
			require.NoError(t, err)
			require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
			require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
			if legacy {
				require.NoError(t, store.ResumeL0(ctx, 7))
			}
			failure := merr.WrapErrServiceUnavailableMsg("injected boundary catalog write loss")
			mt.Catalog = &failingSplitHistoryCatalog{DataCoordCatalog: mt.Catalog, splitHistoryCatalog: mt.Catalog.(splitHistoryCatalog), failure: failure}
			appends := 0
			previous := streaming.WAL()
			streaming.SetWALForTest(&pinFlushWAL{appendFn: func(context.Context, message.MutableMessage) (*types.AppendResult, error) {
				appends++
				return &types.AppendResult{TimeTick: uint64(200 + appends)}, nil
			}})
			t.Cleanup(func() { streaming.SetWALForTest(previous) })
			server := &Server{meta: mt, splitStore: store}
			require.ErrorIs(t, server.ResumeShardSplitL0(ctx, 7), failure)
			require.Equal(t, legacy, store.Get(7).GetL0Resumed())
			require.NotEmpty(t, store.TransformPins(100))
			if legacy {
				done, releaseErr := server.ReleaseShardSplitPins(ctx, 7)
				require.False(t, done)
				require.ErrorIs(t, releaseErr, merr.ErrServiceNotReady)
				require.Equal(t, 1, appends, "polling outside the lifecycle guard must not create a missing F")
			}
			require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
			require.Equal(t, 3, appends, "append with lost durable F is safely reissued")
			server.splitStore, err = split.NewStore(ctx, taskCatalog)
			require.NoError(t, err)
			require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
			require.Equal(t, 3, appends)
		})
	}
}

func TestSplitFiniteBoundaryAmbiguousWriteAndLegacyResume(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			ctx := t.Context()
			mt, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			store, taskCatalog := newSplitTestStore(t)
			_, err = store.Record(ctx, splitTestRecord(7))
			require.NoError(t, err)
			require.NoError(t, store.MarkSourceAcked(ctx, 7, 100, splitTestSource, nil))
			require.NoError(t, store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
			if legacy {
				require.NoError(t, store.ResumeL0(ctx, 7))
			}
			failure := merr.WrapErrServiceUnavailableMsg("injected boundary catalog write loss")
			mt.Catalog = &failingSplitHistoryCatalog{DataCoordCatalog: mt.Catalog, splitHistoryCatalog: mt.Catalog.(splitHistoryCatalog), failure: failure, persistBeforeError: true}
			appends := 0
			previous := streaming.WAL()
			streaming.SetWALForTest(&pinFlushWAL{appendFn: func(context.Context, message.MutableMessage) (*types.AppendResult, error) {
				appends++
				return &types.AppendResult{TimeTick: uint64(200 + appends)}, nil
			}})
			t.Cleanup(func() { streaming.SetWALForTest(previous) })
			server := &Server{meta: mt, splitStore: store}
			require.ErrorIs(t, server.ResumeShardSplitL0(ctx, 7), failure)
			require.Equal(t, legacy, store.Get(7).GetL0Resumed())
			require.NotEmpty(t, store.TransformPins(100))
			if legacy {
				done, releaseErr := server.ReleaseShardSplitPins(ctx, 7)
				require.False(t, done)
				require.ErrorIs(t, releaseErr, merr.ErrServiceNotReady)
				require.Equal(t, 1, appends, "polling outside the lifecycle guard must not create a missing F")
			}
			require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
			require.Equal(t, 2, appends, "ambiguous successful write reuses durable F")
			server.splitStore, err = split.NewStore(ctx, taskCatalog)
			require.NoError(t, err)
			require.NoError(t, server.ResumeShardSplitL0(ctx, 7))
			require.Equal(t, 2, appends)
		})
	}
}
