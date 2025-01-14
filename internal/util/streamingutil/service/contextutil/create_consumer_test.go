package contextutil

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

func TestWithCreateConsumer(t *testing.T) {
	req := &streamingpb.CreateConsumerRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name: "test",
			Term: 1,
		},
	}
	ctx := WithCreateConsumer(context.Background(), req)

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, md)

	ctx = metadata.NewIncomingContext(context.Background(), md)
	req2, err := GetCreateConsumer(ctx)
	assert.Nil(t, err)
	assert.Equal(t, req.Pchannel.Name, req2.Pchannel.Name)
	assert.Equal(t, req.Pchannel.Term, req2.Pchannel.Term)

	// panic case.
	assert.NotPanics(t, func() { WithCreateConsumer(context.Background(), nil) })
}

func TestGetCreateConsumer(t *testing.T) {
	// empty context.
	req, err := GetCreateConsumer(context.Background())
	assert.Error(t, err)
	assert.Nil(t, req)

	// key not exist.
	md := metadata.New(map[string]string{})
	req, err = GetCreateConsumer(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Nil(t, req)

	// invalid value.
	md = metadata.New(map[string]string{
		createConsumerKey: "invalid",
	})
	req, err = GetCreateConsumer(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Nil(t, req)

	// unmarshal error.
	md = metadata.New(map[string]string{
		createConsumerKey: base64.StdEncoding.EncodeToString([]byte("invalid")),
	})
	req, err = GetCreateConsumer(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Nil(t, req)

	// normal case is tested on TestWithCreateConsumer.
}
