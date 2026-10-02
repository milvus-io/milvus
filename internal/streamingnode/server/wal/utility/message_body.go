package utility

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// DecodeInsertBody decodes the body of an insert message on the append path.
// A message that is not a valid insert is rejected as unrecoverable, a payload
// that can not be read right now stays retriable. msg must not be nil.
func DecodeInsertBody(ctx context.Context, msg message.MutableMessage) (*msgpb.InsertRequest, error) {
	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	if err != nil {
		return nil, status.NewUnrecoverableError("decode insert message failed: %v", err)
	}
	return decodeBody(ctx, "insert", insertMsg)
}

// DecodeDeleteBody decodes the body of a delete message on the append path,
// with the same error classification as DecodeInsertBody. msg must not be nil.
func DecodeDeleteBody(ctx context.Context, msg message.MutableMessage) (*msgpb.DeleteRequest, error) {
	deleteMsg, err := message.AsMutableDeleteMessageV1(msg)
	if err != nil {
		return nil, status.NewUnrecoverableError("decode delete message failed: %v", err)
	}
	return decodeBody(ctx, "delete", deleteMsg)
}

func decodeBody[B proto.Message](ctx context.Context, kind string, msg interface {
	Body(ctx context.Context) (B, error)
},
) (B, error) {
	body, err := msg.Body(ctx)
	switch {
	case err == nil:
		return body, nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return body, err
	case errors.Is(err, message.ErrMalformedBody):
		return body, status.NewUnrecoverableError("decode %s body failed: %v", kind, err)
	default:
		return body, status.NewInner("decode %s payload failed: %v", kind, err)
	}
}
