package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type recordingVChannelScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingVChannelScheduler) Submit(task nodescheduler.Task, opts ...nodescheduler.Option) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingVChannelTaskHandle{}
}

type recordingVChannelTaskHandle struct{}

func (recordingVChannelTaskHandle) Cancel() {}

func (recordingVChannelTaskHandle) Wait(context.Context) error { return nil }

// newTestSummaryManager uses a local summary store.
func newTestSummaryManager(t *testing.T, scheduler *recordingVChannelScheduler) *walsummary.Manager {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := walsummary.NewStore(cm, "p1", 1)
	return walsummary.NewManager(walsummary.ManagerConfig{
		Runtime:           moduleapi.Runtime{Scheduler: scheduler},
		PChannel:          "p1",
		Term:              1,
		Store:             store,
		RetentionMaxBytes: 1 << 30,
	})
}

func TestSummaryManagerPersistsThroughPChannelLevel(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	manager := newTestSummaryManager(t, scheduler)
	manager.RequestFlushThrough(10)
	require.Empty(t, scheduler.tasks)
	var finalized bool
	observeSummaryDelete(t, manager, "v1", 10, &finalized)
	require.True(t, finalized)
	require.Empty(t, manager.Manifest().GetChunks())
	manager.RequestFlushThrough(10)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.Len(t, manager.Manifest().GetChunks(), 1)
	require.Equal(t, uint64(0), manager.LastAcked(), "first publication gates confirmation")
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	require.Equal(t, uint64(10), manager.LastAcked())
	manager.RequestFlushThrough(10)
	require.Len(t, scheduler.tasks, 2)
}

// observeSummaryDelete observes one delete message through the summary
// manager's pchannel-level entry point and releases the owner.
func observeSummaryDelete(t *testing.T, manager *walsummary.Manager, vchannel string, timetick uint64, finalized *bool) {
	t.Helper()
	mutable := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1}},
			}},
			Timestamps: []uint64{timetick},
		}).
		MustBuildMutable()
	raw := mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(raw, func() { *finalized = true })
	retained := owner.Clone()
	manager.ObserveMessage(context.Background(), retained.Message())
	retained.Release()
	owner.Release()
}

func observeVChannelDelete(t *testing.T, module *VChannelRecoveryModule, vchannel string, timetick uint64, summaries ...*walsummary.Manager) {
	t.Helper()
	mutable := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1}},
			}},
			Timestamps: []uint64{timetick},
		}).
		MustBuildMutable()
	raw := mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	for _, summary := range summaries {
		summary.ObserveMessage(context.Background(), raw)
	}
	require.True(t, module.ObserveMessage(context.Background(), retained))
	retained.Release()
	owner.Release()
}

func TestPressureRequestRoutesToSelectedVChannel(t *testing.T) {
	view := &segment.SegmentView{}
	l0 := &l0materializer.WALMaterializer{}
	module := &VChannelRecoveryModule{segments: map[int64]*segment.SegmentView{1: view}, l0Materializer: l0}
	m := &PChannelRecoveryManager{modules: typeutil.NewConcurrentMap[string, *VChannelRecoveryModule]()}
	m.modules.Insert("v1", module)
	var segments, deletes int
	p1 := mockey.Mock((*segment.SegmentView).RequestPersistThroughUnderPressure).To(func(s *segment.SegmentView, tt uint64) bool {
		require.Same(t, view, s)
		require.Equal(t, uint64(100), tt)
		segments++
		return true
	}).Build()
	defer p1.UnPatch()
	p2 := mockey.Mock((*l0materializer.WALMaterializer).RequestPersistThroughUnderPressure).To(func(s *l0materializer.WALMaterializer, tt uint64) {
		require.Same(t, l0, s)
		require.Equal(t, uint64(100), tt)
		deletes++
	}).Build()
	defer p2.UnPatch()
	m.RequestPersistThrough("v1", 100)
	module.RequestPersistThrough(100)
	m.RequestPersistThroughUnderPressure("v1", 100)
	require.Equal(t, 1, segments)
	require.Equal(t, 1, deletes)
	m.RequestPersistThroughUnderPressure("missing", 100)
	m.RequestPersistThroughUnderPressure("", 100)
	(*PChannelRecoveryManager)(nil).RequestPersistThroughUnderPressure("v1", 100)
	(*VChannelRecoveryModule)(nil).RequestPersistThroughUnderPressure(100)
	module.removed = true
	m.RequestPersistThroughUnderPressure("v1", 100)
	require.Equal(t, 1, segments)
	require.Equal(t, 1, deletes)
}
