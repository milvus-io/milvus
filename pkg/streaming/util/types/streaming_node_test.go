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

	info := StreamingNodeInfo{
		ServerID: 1,
		Address:  "localhost:8080",
	}
	pb := NewProtoFromStreamingNodeInfo(info)
	info2 := NewStreamingNodeInfoFromProto(pb)
	assert.Equal(t, info.ServerID, info2.ServerID)
	assert.Equal(t, info.Address, info2.Address)
}
