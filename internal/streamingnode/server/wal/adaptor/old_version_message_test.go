package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func TestNewOldVersionImmutableMessage(t *testing.T) {
	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:               merr.Success(),
		CollectionID:         1,
		PhysicalChannelNames: []string{"test1", "test2"},
		VirtualChannelNames:  []string{"test1-v0", "test2-v0"},
	}, nil)
	rcf := syncutil.NewFuture[types.RootCoordClient]()
	rcf.Set(rc)
	resource.InitForTest(t, resource.OptRootCoordClient(rcf))

	ctx := context.Background()
	pchannel := "test1"
	lastConfirmedMessageID := walimplstest.NewTestMessageID(1)
	messageID := walimplstest.NewTestMessageID(2)
	tt := uint64(10086)

	// createCollectionMsg
	createCollectionMsgV0 := msgpb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			Timestamp: tt,
		},
		CollectionID:         1,
		PhysicalChannelNames: []string{"test1", "test2"},
		VirtualChannelNames:  []string{"test1-v0", "test2-v0"},
		PartitionIDs:         []int64{1},
	}
	payload, _ := proto.Marshal(&createCollectionMsgV0)

	msg, err := newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.NotNil(t, msg.LastConfirmedMessageID())
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	createCollectionMsgV1, err := message.AsImmutableCreateCollectionMessageV1(msg)
	assert.NoError(t, err)
	assert.Equal(t, createCollectionMsgV1.Header().CollectionId, int64(1))
	assert.Equal(t, createCollectionMsgV1.Header().PartitionIds, []int64{1})

	// dropCollectionMsg
	dropCollectionMsgV0 := msgpb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			Timestamp: tt,
		},
		CollectionID: 1,
	}
	payload, _ = proto.Marshal(&dropCollectionMsgV0)
	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	dropCollectionMsgV1, err := message.AsImmutableDropCollectionMessageV1(msg)
	assert.NoError(t, err)
	assert.Equal(t, dropCollectionMsgV1.Header().CollectionId, int64(1))

	// insertMsg
	insertMsgV0 := msgpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			Timestamp: tt,
		},
		Timestamps:   []uint64{10086},
		CollectionID: 1,
		PartitionID:  2,
		NumRows:      102,
		SegmentID:    100,
		ShardName:    "test1-v0",
	}
	payload, _ = proto.Marshal(&insertMsgV0)
	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	insertMsgV1, err := message.AsImmutableInsertMessageV1(msg)
	assert.NoError(t, err)
	assert.Equal(t, insertMsgV1.Header().CollectionId, int64(1))
	assert.Equal(t, insertMsgV1.Header().Partitions[0].PartitionId, int64(2))
	assert.Equal(t, insertMsgV1.Header().Partitions[0].SegmentAssignment.SegmentId, int64(100))
	assert.NotZero(t, insertMsgV1.Header().Partitions[0].BinarySize)
	assert.Equal(t, insertMsgV1.Header().Partitions[0].Rows, uint64(102))

	// deleteMsg
	deleteMsgV0 := msgpb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Delete,
			Timestamp: tt,
		},
		Timestamps:   []uint64{10086},
		CollectionID: 1,
		PartitionID:  2,
		NumRows:      102,
		ShardName:    "test1-v0",
	}
	payload, _ = proto.Marshal(&deleteMsgV0)
	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	deleteMsgV1, err := message.AsImmutableDeleteMessageV1(msg)
	assert.NoError(t, err)
	assert.Equal(t, deleteMsgV1.Header().CollectionId, int64(1))

	// timetickSyncMsg
	timetickSyncMsgV0 := msgpb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			Timestamp: tt,
		},
		Timestamps:   []uint64{10086},
		CollectionID: 1,
		PartitionID:  2,
		NumRows:      102,
		ShardName:    "test1-v0",
	}
	payload, _ = proto.Marshal(&timetickSyncMsgV0)
	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "")
	assert.Equal(t, msg.TimeTick(), tt)
	_, err = message.AsImmutableTimeTickMessageV1(msg)
	assert.NoError(t, err)

	// createPartitionMsg
	createPartitionMsgV0 := msgpb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			Timestamp: tt,
		},
		CollectionID: 1,
		PartitionID:  2,
	}
	payload, _ = proto.Marshal(&createPartitionMsgV0)

	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	createPartitionMsgV1, err := message.AsImmutableCreatePartitionMessageV1(msg)
	assert.NoError(t, err)
	assert.Equal(t, createPartitionMsgV1.Header().CollectionId, int64(1))
	assert.Equal(t, createPartitionMsgV1.Header().PartitionId, int64(2))

	// dropPartitionMsg
	dropPartitionMsgV0 := msgpb.DropPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropPartition,
			Timestamp: tt,
		},
		CollectionID: 1,
		PartitionID:  2,
	}
	payload, _ = proto.Marshal(&dropPartitionMsgV0)
	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	dropPartitionMsgV1, err := message.AsImmutableDropPartitionMessageV1(msg)
	assert.NoError(t, err)
	assert.Equal(t, createPartitionMsgV1.Header().CollectionId, int64(1))
	assert.Equal(t, dropPartitionMsgV1.Header().PartitionId, int64(2))

	// ImportMsg
	ImportMsgV0 := msgpb.ImportMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Import,
			Timestamp: tt,
		},
		CollectionID: 1,
	}
	payload, _ = proto.Marshal(&ImportMsgV0)
	msg, err = newOldVersionImmutableMessage(ctx, pchannel, lastConfirmedMessageID, message.NewImmutableMesasge(messageID, payload, map[string]string{}))
	assert.NoError(t, err)
	assert.True(t, msg.MessageID().EQ(messageID))
	assert.True(t, msg.LastConfirmedMessageID().EQ(lastConfirmedMessageID))
	assert.Equal(t, msg.VChannel(), "test1-v0")
	assert.Equal(t, msg.TimeTick(), tt)
	ImportMsgV1, err := message.AsImmutableImportMessageV1(msg)
	assert.NoError(t, err)
	assert.NotNil(t, ImportMsgV1)
}
