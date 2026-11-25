package utility

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func requireStreamingCode(t *testing.T, err error, code streamingpb.StreamingCode, text string) {
	t.Helper()
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.Equal(t, code, streamingErr.Code)
	require.Contains(t, err.Error(), text)
}

func newBodyTestInsert() message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 10}).
		WithBody(&msgpb.InsertRequest{CollectionID: 10, NumRows: 3}).
		MustBuildMutable()
}

func newBodyTestDelete() message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 10, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			CollectionID: 10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}},
		}).
		MustBuildMutable()
}

func corruptBody(msg message.MutableMessage) message.MutableMessage {
	return message.NewMutableMessageBeforeAppend([]byte{0xff}, msg.Properties().ToRawMap())
}

func TestDecodeInsertBody(t *testing.T) {
	ctx := context.Background()

	body, err := DecodeInsertBody(ctx, newBodyTestInsert())
	require.NoError(t, err)
	require.Equal(t, uint64(3), body.GetNumRows())

	_, err = DecodeInsertBody(ctx, newBodyTestDelete())
	requireStreamingCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, "decode insert message failed")

	_, err = DecodeInsertBody(ctx, corruptBody(newBodyTestInsert()))
	requireStreamingCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, "decode insert body failed")

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = DecodeInsertBody(canceled, newBodyTestInsert())
	require.ErrorIs(t, err, context.Canceled)
}

func TestDecodeDeleteBody(t *testing.T) {
	ctx := context.Background()

	body, err := DecodeDeleteBody(ctx, newBodyTestDelete())
	require.NoError(t, err)
	require.Equal(t, []int64{7}, body.GetPrimaryKeys().GetIntId().GetData())

	_, err = DecodeDeleteBody(ctx, newBodyTestInsert())
	requireStreamingCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, "decode delete message failed")

	_, err = DecodeDeleteBody(ctx, corruptBody(newBodyTestDelete()))
	requireStreamingCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, "decode delete body failed")
}

func TestUpdateBodyAppendScope(t *testing.T) {
	msg := message.NewUpdateMessageBuilderV2().WithVChannel("p1_10v0").
		WithHeader(&message.UpdateMessageHeader{CollectionId: 10}).
		WithBody(&message.UpdateMessageBody{PartitionId: 1}).MustBuildMutable()
	update := message.MustAsMutableUpdateMessageV2(msg)
	parent := t.Context()
	ctx, first, err := DecodeUpdateBody(parent, update)
	require.NoError(t, err)
	downstream, again, err := DecodeUpdateBody(ctx, update)
	require.NoError(t, err)
	require.Same(t, ctx, downstream)
	require.Same(t, first, again)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = DecodeUpdateBody(canceled, update)
	require.ErrorIs(t, err, context.Canceled)

	update.OverwriteBody(&message.UpdateMessageBody{PartitionId: 2})
	_, next, err := DecodeUpdateBody(parent, update)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.GetPartitionId())
	require.EqualValues(t, 1, first.GetPartitionId())

	invalid := message.MustAsMutableUpdateMessageV2(corruptBody(msg))
	unchanged, body, err := DecodeUpdateBody(parent, invalid)
	require.ErrorIs(t, err, message.ErrMalformedBody)
	require.Nil(t, body)
	require.Equal(t, parent, unchanged)
}

type failingBody struct{ err error }

func (f failingBody) Body(context.Context) (*msgpb.InsertRequest, error) {
	return nil, f.err
}

// A payload that can not be read, such as a decryption failure, must stay retriable.
func TestDecodeBodyKeepsUnreadablePayloadRetriable(t *testing.T) {
	_, err := decodeBody[*msgpb.InsertRequest](context.Background(), "insert", failingBody{err: errors.New("kms unavailable")})
	requireStreamingCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_INNER, "decode insert payload failed: kms unavailable")

	_, err = decodeBody[*msgpb.InsertRequest](context.Background(), "insert", failingBody{err: context.DeadlineExceeded})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
