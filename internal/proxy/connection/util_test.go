package connection

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func Test_getIdentifierFromContext(t *testing.T) {
	t.Run("metadata not found", func(t *testing.T) {
		ctx := context.TODO()
		_, err := GetIdentifierFromContext(ctx)
		assert.Error(t, err)
	})

	t.Run("no identifier", func(t *testing.T) {
		md := metadata.New(map[string]string{})
		ctx := metadata.NewIncomingContext(context.TODO(), md)
		_, err := GetIdentifierFromContext(ctx)
		assert.Error(t, err)
	})

	t.Run("invalid identifier", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"identifier": "i-am-not-invalid-identifier",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)
		_, err := GetIdentifierFromContext(ctx)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"identifier": "20230518",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)
		identifier, err := GetIdentifierFromContext(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(20230518), identifier)
	})
}

func TestKeepActiveInterceptor(t *testing.T) {
	md := metadata.New(map[string]string{
		"identifier": "20230518",
	})
	ctx := metadata.NewIncomingContext(context.TODO(), md)

	rpcCalled := false
	rpcChan := make(chan struct{}, 1)
	var handler grpc.UnaryHandler = func(ctx context.Context, req interface{}) (interface{}, error) {
		rpcCalled = true
		rpcChan <- struct{}{}
		return "not-important", nil
	}

	got, err := KeepActiveInterceptor(ctx, nil, nil, handler)
	<-rpcChan
	assert.True(t, rpcCalled)
	assert.NoError(t, err)
	assert.Equal(t, "not-important", got)
}
