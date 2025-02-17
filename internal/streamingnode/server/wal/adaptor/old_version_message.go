package adaptor

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
)

// newOldVersionImmutableMessage creates a new immutable message from the old version message.
// Because some old version message didn't have vchannel, so we need to recognize it from the pchnnel and some data field.
func newOldVersionImmutableMessage(
	ctx context.Context,
	pchannel string,
	lastConfirmedMessageID message.MessageID,
	msg message.ImmutableMessage,
) (message.ImmutableMessage, error) {
	if msg.Version() != message.VersionOld {
		panic("invalid message version")
	}
	msgType, err := common.GetMsgTypeFromRaw(msg.Payload(), msg.Properties().ToRawMap())
	if err != nil {
		panic(fmt.Sprintf("failed to get message type: %v", err))
	}
	tsMsg, err := adaptor.UnmashalerDispatcher.Unmarshal(msg.Payload(), msgType)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal message: %v", err))
	}

	// We will transfer it from v0 into v1 here to make it can be consumed by streaming service.
	// It will lose some performance, but there should always a little amount of old version message, so it should be ok.
	var mutableMessage message.MutableMessage
	switch underlyingMsg := tsMsg.(type) {
	case *msgstream.CreateCollectionMsg:
		mutableMessage = newV1CreateCollectionMsgFromV0(pchannel, underlyingMsg)
	case *msgstream.DropCollectionMsg:
		mutableMessage, err = newV1DropCollectionMsgFromV0(ctx, pchannel, underlyingMsg)
	case *msgstream.InsertMsg:
		mutableMessage = newV1InsertMsgFromV0(underlyingMsg, uint64(len(msg.Payload())))
	case *msgstream.DeleteMsg:
		mutableMessage = newV1DeleteMsgFromV0(underlyingMsg)
	case *msgstream.TimeTickMsg:
		mutableMessage = newV1TimeTickMsgFromV0(underlyingMsg)
	case *msgstream.CreatePartitionMsg:
		mutableMessage, err = newV1CreatePartitionMessageV0(ctx, pchannel, underlyingMsg)
	case *msgstream.DropPartitionMsg:
		mutableMessage, err = newV1DropPartitionMessageV0(ctx, pchannel, underlyingMsg)
	case *msgstream.ImportMsg:
		mutableMessage, err = newV1ImportMsgFromV0(ctx, pchannel, underlyingMsg)
	default:
		panic("unsupported message type")
	}
	if err != nil {
		return nil, err
	}
	return mutableMessage.WithLastConfirmed(lastConfirmedMessageID).IntoImmutableMessage(msg.MessageID()), nil
}

// newV1CreateCollectionMsgFromV0 creates a new create collection message from the old version create collection message.
func newV1CreateCollectionMsgFromV0(pchannel string, msg *msgstream.CreateCollectionMsg) message.MutableMessage {
	var vchannel string
	for idx, v := range msg.PhysicalChannelNames {
		if v == pchannel {
			vchannel = msg.VirtualChannelNames[idx]
			break
		}
	}
	if vchannel == "" {
		panic(fmt.Sprintf("vchannel not found at create collection message, collection id: %d, pchannel: %s", msg.CollectionID, pchannel))
	}

	mutableMessage, err := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: msg.CollectionID,
			PartitionIds: msg.PartitionIDs,
		}).
		WithBody(msg.CreateCollectionRequest).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs())
}

// newV1DropCollectionMsgFromV0 creates a new drop collection message from the old version drop collection message.
func newV1DropCollectionMsgFromV0(ctx context.Context, pchannel string, msg *msgstream.DropCollectionMsg) (message.MutableMessage, error) {
	vchannel, err := resource.Resource().VChannelTempStorage().GetVChannelByPChannelOfCollection(ctx, msg.CollectionID, pchannel)
	if err != nil {
		return nil, err
	}

	mutableMessage, err := message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: msg.CollectionID,
		}).
		WithBody(msg.DropCollectionRequest).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs()), nil
}

// newV1InsertMsgFromV0 creates a new insert message from the old version insert message.
func newV1InsertMsgFromV0(msg *msgstream.InsertMsg, binarySize uint64) message.MutableMessage {
	mutableMessage, err := message.NewInsertMessageBuilderV1().
		WithVChannel(msg.ShardName).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: msg.CollectionID,
			Partitions: []*message.PartitionSegmentAssignment{{
				PartitionId: msg.PartitionID,
				Rows:        msg.NumRows,
				BinarySize:  binarySize,
				SegmentAssignment: &message.SegmentAssignment{
					SegmentId: msg.SegmentID,
				},
			}},
		}).
		WithBody(msg.InsertRequest).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs())
}

// newV1DeleteMsgFromV0 creates a new delete message from the old version delete message.
func newV1DeleteMsgFromV0(msg *msgstream.DeleteMsg) message.MutableMessage {
	mutableMessage, err := message.NewDeleteMessageBuilderV1().
		WithVChannel(msg.ShardName).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: msg.CollectionID,
		}).
		WithBody(msg.DeleteRequest).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs())
}

// newV1TimeTickMsgFromV0 creates a new time tick message from the old version time tick message.
func newV1TimeTickMsgFromV0(msg *msgstream.TimeTickMsg) message.MutableMessage {
	mutableMessage, err := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(msg.TimeTickMsg).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs())
}

// newV1CreatePartitionMessageV0 creates a new create partition message from the old version create partition message.
func newV1CreatePartitionMessageV0(ctx context.Context, pchannel string, msg *msgstream.CreatePartitionMsg) (message.MutableMessage, error) {
	vchannel, err := resource.Resource().VChannelTempStorage().GetVChannelByPChannelOfCollection(ctx, msg.CollectionID, pchannel)
	if err != nil {
		return nil, err
	}

	mutableMessage, err := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: msg.CollectionID,
			PartitionId:  msg.PartitionID,
		}).
		WithBody(msg.CreatePartitionRequest).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs()), nil
}

// newV1DropPartitionMessageV0 creates a new drop partition message from the old version drop partition message.
func newV1DropPartitionMessageV0(ctx context.Context, pchannel string, msg *msgstream.DropPartitionMsg) (message.MutableMessage, error) {
	vchannel, err := resource.Resource().VChannelTempStorage().GetVChannelByPChannelOfCollection(ctx, msg.CollectionID, pchannel)
	if err != nil {
		return nil, err
	}
	mutableMessage, err := message.NewDropPartitionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: msg.CollectionID,
			PartitionId:  msg.PartitionID,
		}).
		WithBody(msg.DropPartitionRequest).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs()), nil
}

// newV1ImportMsgFromV0 creates a new import message from the old version import message.
func newV1ImportMsgFromV0(ctx context.Context, pchannel string, msg *msgstream.ImportMsg) (message.MutableMessage, error) {
	vchannel, err := resource.Resource().VChannelTempStorage().GetVChannelByPChannelOfCollection(ctx, msg.CollectionID, pchannel)
	if err != nil {
		return nil, err
	}
	mutableMessage, err := message.NewImportMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(msg.ImportMsg).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return mutableMessage.WithTimeTick(msg.BeginTs()), nil
}
