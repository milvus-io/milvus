package contextutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestWithClusterID(t *testing.T) {
	ctx := WithClusterID(context.Background(), "test-cluster-id")

	md, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, md)

	ctx = metadata.NewIncomingContext(context.Background(), md)
	clusterID, err := GetClusterID(ctx)
	assert.Nil(t, err)
	assert.Equal(t, "test-cluster-id", clusterID)

	// panic case.
	assert.NotPanics(t, func() { WithClusterID(context.Background(), "") })
}

func TestGetClusterID(t *testing.T) {
	// empty context.
	clusterID, err := GetClusterID(context.Background())
	assert.Error(t, err)
	assert.Empty(t, clusterID)

	// key not exist.
	md := metadata.New(map[string]string{})
	clusterID, err = GetClusterID(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Empty(t, clusterID)

	// invalid value.
	md = metadata.New(map[string]string{
		clusterIDKey: "",
	})
	clusterID, err = GetClusterID(metadata.NewIncomingContext(context.Background(), md))
	assert.Error(t, err)
	assert.Empty(t, clusterID)
}
