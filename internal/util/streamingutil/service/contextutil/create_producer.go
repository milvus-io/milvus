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
	createProducerKey = "create-producer"
)

// WithCreateProducer attaches create producer request to context.
func WithCreateProducer(ctx context.Context, req *streamingpb.CreateProducerRequest) context.Context {
	bytes, err := proto.Marshal(req)
	if err != nil {
		panic(fmt.Sprintf("unreachable: marshal create producer request failed, %+v", err))
	}
	// use base64 encoding to transfer binary to text.
	msg := base64.StdEncoding.EncodeToString(bytes)
	return metadata.AppendToOutgoingContext(ctx, createProducerKey, msg)
}

// GetCreateProducer gets create producer request from context.
func GetCreateProducer(ctx context.Context) (*streamingpb.CreateProducerRequest, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("create producer metadata not found from incoming context")
	}
	msg := md.Get(createProducerKey)
	if len(msg) == 0 {
		return nil, errors.New("create consumer metadata not found")
	}

	bytes, err := base64.StdEncoding.DecodeString(msg[0])
	if err != nil {
		return nil, errors.Wrap(err, "decode create consumer metadata failed")
	}

	req := &streamingpb.CreateProducerRequest{}
	if err := proto.Unmarshal(bytes, req); err != nil {
		return nil, errors.Wrap(err, "unmarshal create producer request failed")
	}
	return req, nil
}
