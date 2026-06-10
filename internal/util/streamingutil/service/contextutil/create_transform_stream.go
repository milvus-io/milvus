package contextutil

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

const (
	createTransformStreamKey = "create-transform-stream"
)

func WithCreateTransformStream(ctx context.Context, req *streamingpb.CreateTransformStreamRequest) context.Context {
	bytes, err := proto.Marshal(req)
	if err != nil {
		panic(fmt.Sprintf("unreachable: marshal create transform stream request should never failed, %+v", req))
	}
	msg := base64.StdEncoding.EncodeToString(bytes)
	return metadata.AppendToOutgoingContext(ctx, createTransformStreamKey, msg)
}

func GetCreateTransformStream(ctx context.Context) (*streamingpb.CreateTransformStreamRequest, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("create transform stream metadata not found from incoming context")
	}
	msg := md.Get(createTransformStreamKey)
	if len(msg) == 0 {
		return nil, errors.New("create transform stream metadata not found")
	}

	bytes, err := base64.StdEncoding.DecodeString(msg[0])
	if err != nil {
		return nil, errors.Wrap(err, "decode create transform stream metadata failed")
	}

	req := &streamingpb.CreateTransformStreamRequest{}
	if err := proto.Unmarshal(bytes, req); err != nil {
		return nil, errors.Wrap(err, "unmarshal create transform stream request failed")
	}
	return req, nil
}
