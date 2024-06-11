package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

func TestWALHelper(t *testing.T) {
	h := NewWALHelper(&walimpls.OpenOption{
		Channel: &logpb.PChannelInfo{
			Name:          "test",
			Term:          1,
			ServerID:      1,
			VChannelInfos: []*logpb.VChannelInfo{},
		},
	})
	assert.NotNil(t, h.Channel())
	assert.Equal(t, h.Channel().Name, "test")
	assert.NotNil(t, h.Log())
}
