package broadcaster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchemaTargetFollowsOrderedCallback(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	newTask := func(id, tick uint64, version int32) *broadcastTask {
		msg := message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 100, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Version: version, Name: "original"}}}).
			WithBroadcast([]string{"p" + funcutil.ControlChannelSuffix, "v1", "v2"}).MustBuildBroadcast().WithBroadcastID(id)
		pb := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{1, 1, 1})
		for i, cp := range pb.AckedCheckpoints {
			cp.TimeTick = tick + uint64(i)
		}
		return newBroadcastTaskFromProto(pb, metrics, nil)
	}
	// WAL order is authoritative even if recovered IDs have a different order.
	first, second := newTask(200, 10, 1), newTask(100, 20, 2)
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: map[uint64]*broadcastTask{100: second, 200: first}}
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "recovery alone cannot authorize a target")
	second.schemaTargetEligible = true
	_, err = bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "must not skip a predecessor")
	first.schemaTargetEligible = true
	got, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 200, got.BroadcastID)
	require.EqualValues(t, 1, got.Schema.Version)
	require.Equal(t, map[string]uint64{"v1": 11, "v2": 12}, got.TimeTicks)
	got.Schema.Name = "changed"
	got.TimeTicks["v1"] = 900
	again, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Equal(t, "original", again.Schema.Name)
	require.EqualValues(t, 11, again.TimeTicks["v1"])
	first.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	got, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 2, got.Schema.Version)
	second.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	got, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Nil(t, got, "published metadata takes over after completion")
	got, err = bm.GetPendingSchemaTarget(999)
	require.NoError(t, err)
	require.Nil(t, got)
}
