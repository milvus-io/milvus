package flowgraph

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_flowGraphMaxQueueLength(t *testing.T) {
	Params.Init()
	length := Params.FlowGraphMaxQueueLength()
	assert.Equal(t, length, int32(1024))
}

func TestParamTable_flowGraphMaxParallelism(t *testing.T) {
	Params.Init()
	maxParallelism := Params.FlowGraphMaxParallelism()
	assert.Equal(t, maxParallelism, int32(1024))
}
