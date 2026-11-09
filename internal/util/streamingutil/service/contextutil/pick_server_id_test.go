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
	assert.EqualValues(t, UnspecifiedServerID, serverID)

	// normal case is tested in TestWithPickServerID
}

func TestPickResultIsIndependentFromParent(t *testing.T) {
	parent := WithPickServerID(t.Context(), 7)
	SetPickedServerID(parent, 7)
	id, _ := GetPickServerID(parent)
	child := WithPickServerID(parent, id)
	assert.Equal(t, UnspecifiedServerID, GetPickedServerID(child))
	SetPickedServerID(child, 9)
	assert.EqualValues(t, 7, GetPickedServerID(parent))
	assert.EqualValues(t, 9, GetPickedServerID(child))
	id, ok := GetPickServerID(child)
	assert.True(t, ok)
	assert.EqualValues(t, 7, id) // observation does not change an explicit route

	unspecified := WithPickServerID(parent, UnspecifiedServerID)
	_, ok = GetPickServerID(unspecified)
	assert.False(t, ok)
	SetPickedServerID(unspecified, 9)
	id, ok = GetPickServerID(unspecified)
	assert.True(t, ok)
	assert.EqualValues(t, 9, id)
	SetPickedServerID(unspecified, UnspecifiedServerID)
	_, ok = GetPickServerID(unspecified)
	assert.False(t, ok)

	SetPickedServerID(t.Context(), 9) // legacy callers without routing state
	assert.Equal(t, UnspecifiedServerID, GetPickedServerID(t.Context()))
}
