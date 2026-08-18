package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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

type discardTransformLogStore struct{}

func (discardTransformLogStore) WriteTransformLogChunk(
	context.Context,
	string,
	*streamingpb.TransformLogChunk,
) error {
	return nil
}

func (discardTransformLogStore) ReadTransformLogChunk(
	context.Context,
	string,
	uint64,
) (*streamingpb.TransformLogChunk, error) {
	return nil, nil
}

func TestPChannelRecoveryManagerRequestsOnlyNamedVChannel(t *testing.T) {
	scheduler := &recordingVChannelScheduler{}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1",
		VChannelMetas: map[string]*streamingpb.VChannelMeta{
			"v1": {Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
			"v2": {Vchannel: "v2", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		},
		Runtime:             moduleapi.Runtime{Scheduler: scheduler},
		TransformLogStore:   discardTransformLogStore{},
		TransformLogMaxRows: 100,
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	manager.SwitchIntoMetaAndData()

	observeVChannelDelete(t, manager.Module("v1"), "v1", 10)
	observeVChannelDelete(t, manager.Module("v2"), "v2", 20)
	require.Empty(t, scheduler.tasks)

	manager.RequestPersistThrough("v1", 10)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))

	// The request for v1 must not schedule buffered v2 data.
	require.Len(t, scheduler.tasks, 1)
	manager.RequestPersistThrough("v2", 20)
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
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

var _ transformlog.Store = discardTransformLogStore{}
