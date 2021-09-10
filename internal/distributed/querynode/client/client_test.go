package grpcquerynodeclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNilClient(t *testing.T) {
	client, err := NewClient(context.Background(), "test")
	assert.Nil(t, err)
	err = client.Stop()
	assert.Nil(t, err)
}
