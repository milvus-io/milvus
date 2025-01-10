package contextutil

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

const (
	createConsumerKey = "create-consumer"
)

// WithCreateConsumer attaches create consumer request to context.
func WithCreateConsumer(ctx context.Context, req *streamingpb.CreateConsumerRequest) context.Context {
	bytes, err := proto.Marshal(req)
	if err != nil {
		panic(fmt.Sprintf("unreachable: marshal create consumer request should never failed, %+v", req))
	}
	// use base64 encoding to transfer binary to text.
	msg := base64.StdEncoding.EncodeToString(bytes)
	return metadata.AppendToOutgoingContext(ctx, createConsumerKey, msg)
}

// GetCreateConsumer gets create consumer request from context.
func GetCreateConsumer(ctx context.Context) (*streamingpb.CreateConsumerRequest, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("create consumer metadata not found from incoming context")
	}
	msg := md.Get(createConsumerKey)
	if len(msg) == 0 {
		return nil, errors.New("create consumer metadata not found")
	}

	bytes, err := base64.StdEncoding.DecodeString(msg[0])
	if err != nil {
		return nil, errors.Wrap(err, "decode create consumer metadata failed")
	}

	req := &streamingpb.CreateConsumerRequest{}
	if err := proto.Unmarshal(bytes, req); err != nil {
		return nil, errors.Wrap(err, "unmarshal create consumer request failed")
	}
	return req, nil
}
