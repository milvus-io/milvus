package typeconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

func TestPChannelInfo(t *testing.T) {
	info := types.PChannelInfo{Name: "pchannel", Term: 1}
	pbInfo := NewProtoFromPChannelInfo(info)

	info2 := NewPChannelInfoFromProto(pbInfo)
	assert.Equal(t, info.Name, info2.Name)
	assert.Equal(t, info.Term, info2.Term)

	assert.Panics(t, func() {
		NewProtoFromPChannelInfo(types.PChannelInfo{Name: "", Term: 1})
	})
	assert.Panics(t, func() {
		NewProtoFromPChannelInfo(types.PChannelInfo{Name: "c", Term: -1})
	})

	assert.Panics(t, func() {
		NewPChannelInfoFromProto(&streamingpb.PChannelInfo{Name: "", Term: 1})
	})

	assert.Panics(t, func() {
		NewPChannelInfoFromProto(&streamingpb.PChannelInfo{Name: "c", Term: -1})
	})
}
