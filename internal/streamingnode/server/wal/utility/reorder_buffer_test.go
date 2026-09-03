package utility

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestReOrderByTimeTickBuffer(t *testing.T) {
	buf := NewReOrderBuffer(true)
	timeticks := rand.Perm(25)
	for i, timetick := range timeticks {
		msg := newReorderBufferTestMessage(t, int64(i), uint64(timetick+1), message.MessageTypeInsert)
		buf.Push(msg)
		assert.Equal(t, i+1, buf.Len())
	}

	result := buf.PopUtilTimeTick(0)
	assert.Len(t, result, 0)
	result = buf.PopUtilTimeTick(1)
	assert.Len(t, result, 1)
	for _, msg := range result {
		assert.LessOrEqual(t, msg.TimeTick(), uint64(1))
	}

	result = buf.PopUtilTimeTick(10)
	assert.Len(t, result, 9)
	for _, msg := range result {
		assert.LessOrEqual(t, msg.TimeTick(), uint64(10))
		assert.Greater(t, msg.TimeTick(), uint64(1))
	}

	result = buf.PopUtilTimeTick(25)
	assert.Len(t, result, 15)
	for _, msg := range result {
		assert.Greater(t, msg.TimeTick(), uint64(10))
		assert.LessOrEqual(t, msg.TimeTick(), uint64(25))
	}
}

func TestReOrderByTimeTickBufferDeduplicatesNonTimeTick(t *testing.T) {
	buf := NewReOrderBuffer(true)

	msg1 := newReorderBufferTestMessage(t, 1, 10, message.MessageTypeInsert)
	msg2 := newReorderBufferTestMessage(t, 2, 10, message.MessageTypeInsert)

	pushResult, err := buf.Push(msg1)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	pushResult, err = buf.Push(msg2)
	require.NoError(t, err)
	require.True(t, pushResult.Dropped)
	require.Equal(t, ReOrderByTimeTickBufferDropReasonDuplicateTimeTick, pushResult.DropReason)
	assert.Equal(t, 1, buf.Len())

	result := buf.PopUtilTimeTick(10)
	require.Len(t, result, 1)
	assert.Equal(t, newReorderBufferTestMessageID(1).Marshal(), result[0].MessageID().Marshal())
}

// With physical dedup disabled (idempotency off), the timetick-based drop rule
// must not apply at all — the feature flag is a real kill switch restoring the
// pre-idempotency scanner behavior.
func TestReOrderByTimeTickBufferNoDedupWhenDisabled(t *testing.T) {
	buf := NewReOrderBuffer(false)

	msg1 := newReorderBufferTestMessage(t, 1, 10, message.MessageTypeInsert)
	msg2 := newReorderBufferTestMessage(t, 2, 10, message.MessageTypeInsert)

	pushResult, err := buf.Push(msg1)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	pushResult, err = buf.Push(msg2)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	assert.Equal(t, 2, buf.Len())

	result := buf.PopUtilTimeTick(10)
	require.Len(t, result, 2)
}

func TestReOrderByTimeTickBufferDoesNotDeduplicateTimeTick(t *testing.T) {
	buf := NewReOrderBuffer(true)

	msg1 := newReorderBufferTestMessage(t, 1, 10, message.MessageTypeTimeTick)
	msg2 := newReorderBufferTestMessage(t, 2, 10, message.MessageTypeTimeTick)

	pushResult, err := buf.Push(msg1)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	pushResult, err = buf.Push(msg2)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	assert.Equal(t, 2, buf.Len())

	result := buf.PopUtilTimeTick(10)
	require.Len(t, result, 2)
}

func TestReOrderByTimeTickBufferClearsSeenTimeTicksAfterPop(t *testing.T) {
	buf := NewReOrderBuffer(true)

	pushResult, err := buf.Push(newReorderBufferTestMessage(t, 1, 10, message.MessageTypeInsert))
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	require.Len(t, buf.PopUtilTimeTick(10), 1)

	pushResult, err = buf.Push(newReorderBufferTestMessage(t, 2, 10, message.MessageTypeInsert))
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	assert.Equal(t, 1, buf.Len())
}

func newReorderBufferTestMessage(t *testing.T, id int64, timetick uint64, msgType message.MessageType) *mock_message.MockImmutableMessage {
	return newReorderBufferTestMessageOfVersion(t, id, timetick, msgType, message.VersionV2)
}

// newReorderBufferTestMessageOfVersion builds the fixture at a chosen message
// version. VersionOld is what an upgraded 2.x topic replays, and it is exempt
// from the timetick dedup.
func newReorderBufferTestMessageOfVersion(
	t *testing.T, id int64, timetick uint64, msgType message.MessageType, version message.Version,
) *mock_message.MockImmutableMessage {
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().MessageID().Return(newReorderBufferTestMessageID(id)).Maybe()
	msg.EXPECT().TimeTick().Return(timetick).Maybe()
	msg.EXPECT().MessageType().Return(msgType).Maybe()
	msg.EXPECT().Version().Return(version).Maybe()
	return msg
}

// A 2.x insert request splits into several v0 InsertMsgs that all carry
// BeginTs() -- the MINIMUM row timestamp -- so they share a timetick with
// different message IDs. They never passed the timetick interceptor, so the
// uniqueness invariant the drop rule relies on does not hold for them, and
// dropping the later ones would silently lose rows on upgrade replay.
func TestReOrderByTimeTickBufferKeepsDistinctOldVersionMessages(t *testing.T) {
	buf := NewReOrderBuffer(true)

	msg1 := newReorderBufferTestMessageOfVersion(t, 1, 10, message.MessageTypeInsert, message.VersionOld)
	msg2 := newReorderBufferTestMessageOfVersion(t, 2, 10, message.MessageTypeInsert, message.VersionOld)

	pushResult, err := buf.Push(msg1)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	pushResult, err = buf.Push(msg2)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped, "a v0 message must never be dropped for a repeated timetick")
	assert.Equal(t, 2, buf.Len())

	result := buf.PopUtilTimeTick(10)
	require.Len(t, result, 2, "both v0 messages must reach the consumer")

	// A post-upgrade message that happens to reuse that timetick is still
	// deduped: the v0 exemption must not poison the seen set.
	msg3 := newReorderBufferTestMessage(t, 3, 11, message.MessageTypeInsert)
	msg4 := newReorderBufferTestMessage(t, 4, 11, message.MessageTypeInsert)
	pushResult, err = buf.Push(msg3)
	require.NoError(t, err)
	require.False(t, pushResult.Dropped)
	pushResult, err = buf.Push(msg4)
	require.NoError(t, err)
	require.True(t, pushResult.Dropped)
}

type reorderBufferTestMessageID int64

func newReorderBufferTestMessageID(id int64) message.MessageID {
	return reorderBufferTestMessageID(id)
}

func (id reorderBufferTestMessageID) WALName() message.WALName {
	return message.WALNameTest
}

func (id reorderBufferTestMessageID) LT(other message.MessageID) bool {
	return id < other.(reorderBufferTestMessageID)
}

func (id reorderBufferTestMessageID) LTE(other message.MessageID) bool {
	return id <= other.(reorderBufferTestMessageID)
}

func (id reorderBufferTestMessageID) EQ(other message.MessageID) bool {
	return id == other.(reorderBufferTestMessageID)
}

func (id reorderBufferTestMessageID) Marshal() string {
	return strconv.FormatInt(int64(id), 10)
}

func (id reorderBufferTestMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		Id:      id.Marshal(),
		WALName: commonpb.WALName(id.WALName()),
	}
}

func (id reorderBufferTestMessageID) String() string {
	return id.Marshal()
}
