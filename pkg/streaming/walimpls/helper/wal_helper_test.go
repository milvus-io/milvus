package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
)

func TestWALHelper(t *testing.T) {
	h := NewWALHelper(&walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name: "test",
			Term: 1,
		},
	})
	assert.NotNil(t, h.Channel())
	assert.Equal(t, h.Channel().Name, "test")
	assert.NotNil(t, h.Log())
}
