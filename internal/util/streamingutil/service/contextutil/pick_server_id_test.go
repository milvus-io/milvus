package contextutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithPickServerID(t *testing.T) {
	ctx := context.Background()
	ctx = WithPickServerID(ctx, 1)
	serverID, ok := GetPickServerID(ctx)
	assert.True(t, ok)
	assert.EqualValues(t, 1, serverID)
}

func TestGetPickServerID(t *testing.T) {
	ctx := context.Background()
	serverID, ok := GetPickServerID(ctx)
	assert.False(t, ok)
	assert.EqualValues(t, -1, serverID)

	// normal case is tested in TestWithPickServerID
}
