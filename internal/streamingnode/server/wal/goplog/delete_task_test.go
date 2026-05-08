package goplog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestDeleteChunkTask_FieldIDMatchesPrimaryKey(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockCM.EXPECT().Write(mock.Anything, "files/delta_log/1/100/2001/10", mock.Anything).Return(nil).Once()

	chunk := &DeleteChunk{}
	chunk.Push(newMockDeleteMessage(100, 5))
	var result *DeleteChunkTaskResult
	task := NewDeleteChunkTask(
		chunk,
		1,
		100,
		2001,
		newTestSchema(),
		mockCM,
		allocator.NewLocalAllocator(10, 11),
		&indexpb.StorageConfig{RootPath: "files"},
		func(r *DeleteChunkTaskResult, err error) {
			assert.NoError(t, err)
			result = r
		},
	)

	assert.Equal(t, syncutil.ErrContinue, task.Poll(context.Background()))
	assert.Equal(t, syncutil.ErrContinue, task.Poll(context.Background()))
	assert.NoError(t, task.Poll(context.Background()))
	assert.NotNil(t, result)
	assert.Equal(t, int64(100), result.Binlog.GetFieldID())
	assert.Len(t, result.Binlog.GetBinlogs(), 1)
}

func newTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
		},
	}
}

type mockDeleteMessage struct {
	header   *messagespb.DeleteMessageHeader
	body     *message.DeleteRequest
	vchannel string
	timetick uint64
}

func newMockDeleteMessage(partitionID int64, rows uint64) message.ImmutableDeleteMessageV1 {
	return &mockDeleteMessage{
		header: &messagespb.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         rows,
		},
		body: &message.DeleteRequest{
			PartitionID: partitionID,
			PrimaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
				},
			},
		},
		vchannel: "test-channel-v0",
		timetick: 1500,
	}
}

func (m *mockDeleteMessage) Header() *messagespb.DeleteMessageHeader {
	return m.header
}

func (m *mockDeleteMessage) VChannel() string {
	return m.vchannel
}

func (m *mockDeleteMessage) PChannel() string {
	return m.vchannel
}

func (m *mockDeleteMessage) TimeTick() uint64 {
	return m.timetick
}

func (m *mockDeleteMessage) MessageType() message.MessageType {
	return message.MessageTypeDelete
}

func (m *mockDeleteMessage) MessageID() message.MessageID {
	return nil
}

func (m *mockDeleteMessage) EstimateSize() int {
	return 500
}

func (m *mockDeleteMessage) Properties() message.RProperties {
	return nil
}

func (m *mockDeleteMessage) Version() message.Version {
	return message.VersionV1
}

func (m *mockDeleteMessage) WALName() message.WALName {
	return message.WALName(0)
}

func (m *mockDeleteMessage) RawHeader() any {
	return m.header
}

func (m *mockDeleteMessage) RawBody() any {
	return m.body
}

func (m *mockDeleteMessage) Unmarshal(any) error {
	return nil
}

func (m *mockDeleteMessage) BarrierTimeTick() uint64 {
	return m.timetick
}

func (m *mockDeleteMessage) Body() (*message.DeleteRequest, error) {
	return m.body, nil
}

func (m *mockDeleteMessage) MustBody() *message.DeleteRequest {
	return m.body
}

func (m *mockDeleteMessage) LastConfirmedMessageID() message.MessageID {
	return nil
}

func (m *mockDeleteMessage) IntoImmutableMessageProto() *commonpb.ImmutableMessage {
	return nil
}

func (m *mockDeleteMessage) IntoBroadcastMutableMessage() message.BroadcastMutableMessage {
	return nil
}

func (m *mockDeleteMessage) BroadcastHeader() *message.BroadcastHeader {
	return nil
}

func (m *mockDeleteMessage) IntoMessageProto() *messagespb.Message {
	return nil
}

func (m *mockDeleteMessage) IsPersisted() bool {
	return false
}

func (m *mockDeleteMessage) IsPChannelLevel() bool {
	return false
}

func (m *mockDeleteMessage) MarshalLogObject(zapcore.ObjectEncoder) error {
	return nil
}

func (m *mockDeleteMessage) MessageTypeWithVersion() message.MessageTypeWithVersion {
	return message.MessageTypeWithVersion{}
}

func (m *mockDeleteMessage) Payload() []byte {
	return nil
}

func (m *mockDeleteMessage) TxnContext() *message.TxnContext {
	return nil
}

func (m *mockDeleteMessage) ReplicateHeader() *message.ReplicateHeader {
	return nil
}
