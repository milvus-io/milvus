package transformlog

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type memoryStore struct {
	chunks map[string]map[uint64]*streamingpb.TransformLogChunk
	reads  map[string]map[uint64]int
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		chunks: make(map[string]map[uint64]*streamingpb.TransformLogChunk),
		reads:  make(map[string]map[uint64]int),
	}
}

func (s *memoryStore) WriteTransformLogChunk(_ context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error {
	if s.chunks[vchannel] == nil {
		s.chunks[vchannel] = make(map[uint64]*streamingpb.TransformLogChunk)
	}
	s.chunks[vchannel][chunk.GetChunkId()] = proto.Clone(chunk).(*streamingpb.TransformLogChunk)
	return nil
}

func (s *memoryStore) ReadTransformLogChunk(_ context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	if s.reads[vchannel] == nil {
		s.reads[vchannel] = make(map[uint64]int)
	}
	s.reads[vchannel][chunkID]++
	return proto.Clone(s.chunks[vchannel][chunkID]).(*streamingpb.TransformLogChunk), nil
}

func (s *memoryStore) resetReadCount() {
	s.reads = make(map[string]map[uint64]int)
}

func (s *memoryStore) readCount(vchannel string, chunkID uint64) int {
	return s.reads[vchannel][chunkID]
}

func newTransformLogTestDeleteMessage(t *testing.T, timetick uint64) message.ImmutableDeleteMessageV1 {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func newTransformLogTestManualFlushMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}
