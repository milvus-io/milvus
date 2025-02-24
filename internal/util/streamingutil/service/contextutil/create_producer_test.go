package contextutil

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestWithCreateProducer(t *testing.T) {
	req := &streamingpb.CreateProducerRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name: "test",
			Term: 1,
		},
	}
	ctx := WithCreateProducer(context.Background(), req)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, md)

	ctx = metadata.NewIncomingContext(context.Background(), md)
	req2, err := GetCreateProducer(ctx)
	assert.Nil(t, err)
	assert.Equal(t, req.Pchannel.Name, req2.Pchannel.Name)
	assert.Equal(t, req.Pchannel.Term, req2.Pchannel.Term)

	// panic case.
	assert.NotPanics(t, func() { WithCreateProducer(context.Background(), nil) })
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
