package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamingNodeStatus(t *testing.T) {
	s := StreamingNodeStatus{Err: ErrStopping}
	assert.False(t, s.IsHealthy())

	s = StreamingNodeStatus{Err: ErrNotAlive}
	assert.False(t, s.IsHealthy())
}
