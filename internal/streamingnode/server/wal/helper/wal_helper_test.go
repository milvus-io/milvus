package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
)

func TestWALHelper(t *testing.T) {
	h := NewWALHelper(&walimpls.OpenOption{
		Channel: &streamingpb.PChannelInfo{
			Name:          "test",
			Term:          1,
			ServerID:      1,
			VChannelInfos: []*streamingpb.VChannelInfo{},
		},
	})
	assert.NotNil(t, h.Channel())
	assert.Equal(t, h.Channel().Name, "test")
	assert.NotNil(t, h.Log())
}
