package recovery

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestRecoveryStorage(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALRecoveryPersistInterval.Key, "1ms")
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALRecoveryGracefulCloseTimeout.Key, "10ms")

	vchannelMetas := make(map[string]*streamingpb.VChannelMeta)
	segmentMetas := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	cp := &streamingpb.WALCheckpoint{
		MessageId: &messagespb.MessageID{
			Id: rmq.NewRmqID(1).Marshal(),
		},
		TimeTick:      1,
		RecoveryMagic: 0,
	}

	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, channel string) ([]*streamingpb.SegmentAssignmentMeta, error) {
		return lo.Values(segmentMetas), nil
	})
	snCatalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, channel string) ([]*streamingpb.VChannelMeta, error) {
		return lo.Values(vchannelMetas), nil
	})
	segmentSaveFailure := true
	snCatalog.EXPECT().SaveSegmentAssignments(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, m map[int64]*streamingpb.SegmentAssignmentMeta) error {
		if segmentSaveFailure {
			segmentSaveFailure = false
			return errors.New("save failed")
		}
		for _, v := range m {
			if v.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
				segmentMetas[v.SegmentId] = v
			} else {
				delete(segmentMetas, v.SegmentId)
			}
		}
		return nil
	})
	vchannelSaveFailure := true
	snCatalog.EXPECT().SaveVChannels(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, m map[string]*streamingpb.VChannelMeta) error {
		if vchannelSaveFailure {
			vchannelSaveFailure = false
			return errors.New("save failed")
		}
		for _, v := range m {
			if v.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
				vchannelMetas[v.Vchannel] = v
			} else {
				delete(vchannelMetas, v.Vchannel)
			}
		}
		return nil
	})
	snCatalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(cp, nil)
	checkpointSaveFailure := true
	snCatalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannelName string, checkpoint *streamingpb.WALCheckpoint) error {
		if checkpointSaveFailure {
			checkpointSaveFailure = false
			return errors.New("save failed")
		}
		cp = checkpoint
		return nil
	})

	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))
	b := &streamBuilder{
		channel:                types.PChannelInfo{Name: "test_channel"},
		lastConfirmedMessageID: 1,
		messageID:              1,
		timetick:               1,
		collectionIDs:          make(map[int64]map[int64]map[int64]struct{}),
		vchannels:              make(map[int64]string),
		idAlloc:                1,
	}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))
	b.generateStreamMessage()

	for i := 0; i < 3; i++ {
		if i == 2 {
			// make sure the checkpoint is saved.
			paramtable.Get().Save(paramtable.Get().StreamingCfg.WALRecoveryGracefulCloseTimeout.Key, "1000s")
		}
		rs, snapshot, err := RecoverRecoveryStorage(context.Background(), b, msg)
		assert.NoError(t, err)
		assert.NotNil(t, rs)
		assert.NotNil(t, snapshot)

		msgs := b.generateStreamMessage()
		for _, msg := range msgs {
			rs.ObserveMessage(msg)
		}
		rs.Close()
		var partitionNum int
		var collectionNum int
		var segmentNum int
		for _, v := range rs.vchannels {
			if v.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
				collectionNum += 1
				partitionNum += len(v.meta.CollectionInfo.Partitions)
			}
		}
		for _, v := range rs.segments {
			if v.meta.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
				segmentNum += 1
			}
		}
		assert.Equal(t, partitionNum, b.partitionNum())
		assert.Equal(t, collectionNum, b.collectionNum())
		assert.Equal(t, segmentNum, b.segmentNum())
	}
	assert.Equal(t, b.collectionNum(), len(vchannelMetas))
	partitionNum := 0
	for _, v := range vchannelMetas {
		partitionNum += len(v.CollectionInfo.Partitions)
	}
	assert.Equal(t, b.partitionNum(), partitionNum)
	assert.Equal(t, b.segmentNum(), len(segmentMetas))
}

type streamBuilder struct {
	channel                types.PChannelInfo
	lastConfirmedMessageID int64
	messageID              int64
	timetick               uint64
	collectionIDs          map[int64]map[int64]map[int64]struct{}
	vchannels              map[int64]string
	idAlloc                int64
	histories              []message.ImmutableMessage
}

func (b *streamBuilder) collectionNum() int {
	return len(b.collectionIDs)
}

func (b *streamBuilder) partitionNum() int {
	partitionNum := 0
	for _, partitions := range b.collectionIDs {
		partitionNum += len(partitions)
	}
	return partitionNum
}

func (b *streamBuilder) segmentNum() int {
	segmentNum := 0
	for _, partitions := range b.collectionIDs {
		for _, segments := range partitions {
			segmentNum += len(segments)
		}
	}
	return segmentNum
}

type testRecoveryStream struct {
	ch chan message.ImmutableMessage
}

func (ts *testRecoveryStream) Chan() <-chan message.ImmutableMessage {
	return ts.ch
}

func (ts *testRecoveryStream) Error() error {
	return nil
}

func (ts *testRecoveryStream) TxnBuffer() *utility.TxnBuffer {
	return nil
}

func (ts *testRecoveryStream) Close() error {
	return nil
}

func (b *streamBuilder) WALName() string {
	return "rocksmq"
}

func (b *streamBuilder) Channel() types.PChannelInfo {
	return b.channel
}

func (b *streamBuilder) Build(param BuildRecoveryStreamParam) RecoveryStream {
	rs := &testRecoveryStream{
		ch: make(chan message.ImmutableMessage, len(b.histories)),
	}
	cp := param.StartCheckpoint
	for _, msg := range b.histories {
		if cp.LTE(msg.MessageID()) {
			rs.ch <- msg
		}
	}
	close(rs.ch)
	return rs
}

func (b *streamBuilder) generateStreamMessage() []message.ImmutableMessage {
	ops := []func() message.ImmutableMessage{
		b.createCollection,
		b.createPartition,
		b.createSegment,
		b.createSegment,
		b.dropCollection,
		b.dropPartition,
		b.flushSegment,
		b.flushSegment,
		b.createInsert,
		b.createInsert,
		b.createInsert,
		b.createDelete,
		b.createDelete,
		b.createDelete,
		b.createTxn,
		b.createTxn,
		b.createManualFlush,
	}
	msgs := make([]message.ImmutableMessage, 0)
	for i := 0; i < int(rand.Int63n(1000)+1000); i++ {
		op := rand.Int31n(int32(len(ops)))
		if msg := ops[op](); msg != nil {
			msgs = append(msgs, msg)
		}
	}
	b.histories = append(b.histories, msgs...)
	return msgs
}

// createCollection creates a collection message with the given vchannel.
func (b *streamBuilder) createCollection() message.ImmutableMessage {
	vchannel := fmt.Sprintf("vchannel_%d", b.allocID())
	collectionID := b.allocID()
	partitions := rand.Int31n(1023) + 1
	partitionIDs := make(map[int64]map[int64]struct{}, partitions)
	for i := int32(0); i < partitions; i++ {
		partitionIDs[b.allocID()] = make(map[int64]struct{})
	}
	b.nextMessage()
	b.collectionIDs[collectionID] = partitionIDs
	b.vchannels[collectionID] = vchannel
	return message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: collectionID,
			PartitionIds: lo.Keys(partitionIDs),
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(b.timetick).
		WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
		IntoImmutableMessage(rmq.NewRmqID(b.messageID))
}

func (b *streamBuilder) createPartition() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		partitionID := b.allocID()
		collection[partitionID] = make(map[int64]struct{})
		b.nextMessage()
		return message.NewCreatePartitionMessageBuilderV1().
			WithVChannel(b.vchannels[collectionID]).
			WithHeader(&message.CreatePartitionMessageHeader{
				CollectionId: collectionID,
				PartitionId:  partitionID,
			}).
			WithBody(&msgpb.CreatePartitionRequest{}).
			MustBuildMutable().
			WithTimeTick(b.timetick).
			WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
			IntoImmutableMessage(rmq.NewRmqID(b.messageID))
	}
	return nil
}

func (b *streamBuilder) createSegment() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		for partitionID, partition := range collection {
			if rand.Int31n(3) < 1 {
				continue
			}
			segmentID := b.allocID()
			partition[segmentID] = struct{}{}
			b.nextMessage()
			return message.NewCreateSegmentMessageBuilderV2().
				WithVChannel(b.vchannels[collectionID]).
				WithHeader(&message.CreateSegmentMessageHeader{
					CollectionId: collectionID,
					SegmentIds:   []int64{segmentID},
				}).
				WithBody(&message.CreateSegmentMessageBody{
					CollectionId: collectionID,
					Segments: []*messagespb.CreateSegmentInfo{
						{
							PartitionId:    partitionID,
							SegmentId:      segmentID,
							StorageVersion: 1,
							MaxSegmentSize: 1024,
						},
					},
				}).
				MustBuildMutable().
				WithTimeTick(b.timetick).
				WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
				IntoImmutableMessage(rmq.NewRmqID(b.messageID))
		}
	}
	return nil
}

func (b *streamBuilder) dropCollection() message.ImmutableMessage {
	for collectionID := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		b.nextMessage()
		delete(b.collectionIDs, collectionID)
		return message.NewDropCollectionMessageBuilderV1().
			WithVChannel(b.vchannels[collectionID]).
			WithHeader(&message.DropCollectionMessageHeader{
				CollectionId: collectionID,
			}).
			WithBody(&msgpb.DropCollectionRequest{}).
			MustBuildMutable().
			WithTimeTick(b.timetick).
			WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
			IntoImmutableMessage(rmq.NewRmqID(b.messageID))
	}
	return nil
}

func (b *streamBuilder) dropPartition() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		for partitionID := range collection {
			if rand.Int31n(3) < 1 {
				continue
			}
			b.nextMessage()
			delete(collection, partitionID)
			return message.NewDropPartitionMessageBuilderV1().
				WithVChannel(b.vchannels[collectionID]).
				WithHeader(&message.DropPartitionMessageHeader{
					CollectionId: collectionID,
					PartitionId:  partitionID,
				}).
				WithBody(&msgpb.DropPartitionRequest{}).
				MustBuildMutable().
				WithTimeTick(b.timetick).
				WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
				IntoImmutableMessage(rmq.NewRmqID(b.messageID))
		}
	}
	return nil
}

func (b *streamBuilder) flushSegment() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		for partitionID := range collection {
			if rand.Int31n(3) < 1 {
				continue
			}
			segmentIDs := make([]int64, 0, len(collection[partitionID]))
			for segmentID := range collection[partitionID] {
				if rand.Int31n(4) < 1 {
					continue
				}
				delete(collection[partitionID], segmentID)
				segmentIDs = append(segmentIDs, segmentID)
			}
			if len(segmentIDs) > 0 {
				b.nextMessage()
				return message.NewFlushMessageBuilderV2().
					WithVChannel(b.vchannels[collectionID]).
					WithHeader(&message.FlushMessageHeader{
						CollectionId: collectionID,
						SegmentIds:   segmentIDs,
					}).
					WithBody(&message.FlushMessageBody{}).
					MustBuildMutable().
					WithTimeTick(b.timetick).
					WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
					IntoImmutableMessage(rmq.NewRmqID(b.messageID))
			}
		}
	}
	return nil
}

func (b *streamBuilder) createTxn() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		b.nextMessage()
		txnSession := &message.TxnContext{
			TxnID: message.TxnID(b.allocID()),
		}
		begin := message.NewBeginTxnMessageBuilderV2().
			WithVChannel(b.vchannels[collectionID]).
			WithHeader(&message.BeginTxnMessageHeader{}).
			WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().
			WithTimeTick(b.timetick).
			WithTxnContext(*txnSession).
			WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
			IntoImmutableMessage(rmq.NewRmqID(b.messageID))

		builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
		for partitionID := range collection {
			for segmentID := range collection[partitionID] {
				b.nextMessage()
				builder.Add(message.NewInsertMessageBuilderV1().
					WithVChannel(b.vchannels[collectionID]).
					WithHeader(&message.InsertMessageHeader{
						CollectionId: collectionID,
						Partitions: []*messagespb.PartitionSegmentAssignment{
							{
								PartitionId:       partitionID,
								Rows:              uint64(rand.Int31n(100)),
								BinarySize:        uint64(rand.Int31n(100)),
								SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: segmentID},
							},
						},
					}).
					WithBody(&msgpb.InsertRequest{}).
					MustBuildMutable().
					WithTimeTick(b.timetick).
					WithTxnContext(*txnSession).
					WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
					IntoImmutableMessage(rmq.NewRmqID(b.messageID)))
			}
		}

		b.nextMessage()
		commit := message.NewCommitTxnMessageBuilderV2().
			WithVChannel(b.vchannels[collectionID]).
			WithHeader(&message.CommitTxnMessageHeader{}).
			WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable().
			WithTimeTick(b.timetick).
			WithTxnContext(*txnSession).
			WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
			IntoImmutableMessage(rmq.NewRmqID(b.messageID))
		txnMsg, _ := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
		return txnMsg
	}
	return nil
}

func (b *streamBuilder) createDelete() message.ImmutableMessage {
	for collectionID := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		b.nextMessage()
		return message.NewDeleteMessageBuilderV1().
			WithVChannel(b.vchannels[collectionID]).
			WithHeader(&message.DeleteMessageHeader{
				CollectionId: collectionID,
			}).
			WithBody(&msgpb.DeleteRequest{}).
			MustBuildMutable().
			WithTimeTick(b.timetick).
			WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
			IntoImmutableMessage(rmq.NewRmqID(b.messageID))
	}
	return nil
}

func (b *streamBuilder) createManualFlush() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		segmentIDs := make([]int64, 0)
		for partitionID := range collection {
			if rand.Int31n(3) < 1 {
				continue
			}
			for segmentID := range collection[partitionID] {
				if rand.Int31n(4) < 2 {
					continue
				}
				segmentIDs = append(segmentIDs, segmentID)
				delete(collection[partitionID], segmentID)
			}
		}
		if len(segmentIDs) == 0 {
			continue
		}
		b.nextMessage()
		return message.NewManualFlushMessageBuilderV2().
			WithVChannel(b.vchannels[collectionID]).
			WithHeader(&message.ManualFlushMessageHeader{
				CollectionId: collectionID,
				SegmentIds:   segmentIDs,
			}).
			WithBody(&message.ManualFlushMessageBody{}).
			MustBuildMutable().
			WithTimeTick(b.timetick).
			WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
			IntoImmutableMessage(rmq.NewRmqID(b.messageID))
	}
	return nil
}

func (b *streamBuilder) createInsert() message.ImmutableMessage {
	for collectionID, collection := range b.collectionIDs {
		if rand.Int31n(3) < 1 {
			continue
		}
		for partitionID := range collection {
			if rand.Int31n(3) < 1 {
				continue
			}
			for segmentID := range collection[partitionID] {
				if rand.Int31n(4) < 2 {
					continue
				}
				b.nextMessage()
				return message.NewInsertMessageBuilderV1().
					WithVChannel(b.vchannels[collectionID]).
					WithHeader(&message.InsertMessageHeader{
						CollectionId: collectionID,
						Partitions: []*messagespb.PartitionSegmentAssignment{
							{
								PartitionId:       partitionID,
								Rows:              uint64(rand.Int31n(100)),
								BinarySize:        uint64(rand.Int31n(100)),
								SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: segmentID},
							},
						},
					}).
					WithBody(&msgpb.InsertRequest{}).
					MustBuildMutable().
					WithTimeTick(b.timetick).
					WithLastConfirmed(rmq.NewRmqID(b.lastConfirmedMessageID)).
					IntoImmutableMessage(rmq.NewRmqID(b.messageID))
			}
		}
	}
	return nil
}

func (b *streamBuilder) nextMessage() {
	b.messageID++
	if rand.Int31n(3) < 2 {
		b.lastConfirmedMessageID = b.messageID + 1
	}
	b.timetick++
}

func (b *streamBuilder) allocID() int64 {
	b.idAlloc++
	return b.idAlloc
}
