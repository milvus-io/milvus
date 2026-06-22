package transformlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestModuleDirtySnapshotAdvancesDataBarrierAfterMarkPersisted(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	msg := newModuleTestDeleteMessage(t, 10)
	result := module.ObserveMessage(ctx, msg)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	log := module.getLog("v1")
	require.NotNil(t, log)
	flushResult, err := log.log.Flush(ctx, FlushOption{TargetTimeTick: 10})
	require.NoError(t, err)
	assert.True(t, flushResult.Started)

	snapshots := module.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	assert.Equal(t, moduleapi.ModuleNameTransformLog, snapshots[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpUpsert, snapshots[0].Op())
	assert.Equal(t, uint64(10), snapshots[0].DataTimeTick())
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	snapshots[0].MarkPersisted()
	assert.Equal(t, uint64(10), result.Data.TimeTick())
}

func TestLatestTransformTimeTickIncludesUnflushedBuffer(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	assert.Equal(t, uint64(0), module.LatestTransformTimeTick("v1"))

	module.ObserveMessage(ctx, newModuleTestDeleteMessage(t, 10))

	assert.Equal(t, uint64(10), module.LatestTransformTimeTick("v1"))
}

type memoryStore struct {
	chunks map[string]map[uint64]*streamingpb.TransformLogChunk
}

func newMemoryStore() *memoryStore {
	return &memoryStore{chunks: make(map[string]map[uint64]*streamingpb.TransformLogChunk)}
}

func (s *memoryStore) WriteTransformLogChunk(_ context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error {
	if s.chunks[vchannel] == nil {
		s.chunks[vchannel] = make(map[uint64]*streamingpb.TransformLogChunk)
	}
	s.chunks[vchannel][chunk.GetChunkId()] = proto.Clone(chunk).(*streamingpb.TransformLogChunk)
	return nil
}

func (s *memoryStore) ReadTransformLogChunk(_ context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	return proto.Clone(s.chunks[vchannel][chunkID]).(*streamingpb.TransformLogChunk), nil
}

func newModuleTestDeleteMessage(t *testing.T, timetick uint64) message.ImmutableDeleteMessageV1 {
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
