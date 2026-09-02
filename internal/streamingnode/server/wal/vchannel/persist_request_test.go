package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type recordingVChannelScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingVChannelScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingVChannelTaskHandle{}
}

type recordingVChannelTaskHandle struct{}

func (recordingVChannelTaskHandle) Cancel() {}

func (recordingVChannelTaskHandle) Wait(context.Context) error { return nil }

// newTestSummaryManager uses a local store with the transform consumer enabled.
func newTestSummaryManager(t *testing.T) *walsummary.Manager {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := walsummary.NewStore(cm, "p1", 1)
	return walsummary.NewManager(walsummary.ManagerConfig{
		PChannel:          "p1",
		Term:              1,
		Store:             store,
		EnableTransform:   true,
		RetentionMaxBytes: 1 << 30,
	})
}

func TestSummaryManagerPersistsThroughPChannelLevel(t *testing.T) {
	ctx := context.Background()
	manager := newTestSummaryManager(t)
	require.NoError(t, manager.Persist(ctx))
	require.Empty(t, manager.Manifest().GetChunks())
	var finalized bool
	observeSummaryDelete(t, manager, "v1", 10, &finalized)
	require.True(t, finalized)
	require.Empty(t, manager.Manifest().GetChunks())
	require.NoError(t, manager.Persist(ctx))
	require.Len(t, manager.Manifest().GetChunks(), 1)
	require.Equal(t, uint64(10), manager.DurableTimeTick("v1"))
	require.NoError(t, manager.Persist(ctx))
	require.Len(t, manager.Manifest().GetChunks(), 1)
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
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(raw, func() { *finalized = true })
	retained := owner.Clone()
	manager.ObserveMessage(context.Background(), retained.Message())
	retained.Release()
	owner.Release()
}

func observeVChannelDelete(t *testing.T, module *VChannelRecoveryModule, vchannel string, timetick uint64) {
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
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	require.True(t, module.ObserveMessage(context.Background(), retained))
	retained.Release()
	owner.Release()
}
