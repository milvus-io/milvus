package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_QueryNodeID(t *testing.T) {
	Params.InitParamTable()
	id := Params.QueryNodeID()
	assert.Equal(t, id, 0)
}

func TestParamTable_TopicStart(t *testing.T) {
	Params.InitParamTable()
	topicStart := Params.TopicStart()
	assert.Equal(t, topicStart, 0)
}

func TestParamTable_TopicEnd(t *testing.T) {
	Params.InitParamTable()
	topicEnd := Params.TopicEnd()
	assert.Equal(t, topicEnd, 128)
}
