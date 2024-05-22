package contextutil

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestWithCreateProducer(t *testing.T) {
	req := &logpb.CreateProducerRequest{
		ChannelName: "test",
		Term:        1,
	}
	ctx := WithCreateProducer(context.Background(), req)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, md)

	ctx = metadata.NewIncomingContext(context.Background(), md)
	req2, err := GetCreateProducer(ctx)
	assert.Nil(t, err)
	assert.Equal(t, req.ChannelName, req2.ChannelName)
	assert.Equal(t, req.Term, req2.Term)

	// panic case.
	assert.Panics(t, func() { WithCreateProducer(context.Background(), nil) })
}

func TestGetCreateProducer(t *testing.T) {
	// empty context.
	req, err := GetCreateProducer(context.Background())
	assert.Error(t, err)
	assert.Nil(t, req)

	// key not exist.
	md := metadata.New(map[string]string{})
	req, err = GetCreateProducer(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Nil(t, req)

	// invalid value.
	md = metadata.New(map[string]string{
		createProducerKey: "invalid",
	})
	req, err = GetCreateProducer(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Nil(t, req)

	// unmarshal error.
	md = metadata.New(map[string]string{
		createProducerKey: base64.StdEncoding.EncodeToString([]byte("invalid")),
	})
	req, err = GetCreateProducer(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Nil(t, req)

	// normal case is tested on TestWithCreateProducer.
}
