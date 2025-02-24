package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestPChannelInfo(t *testing.T) {
	info := PChannelInfo{Name: "pchannel", Term: 1}
	pbInfo := NewProtoFromPChannelInfo(info)

	info2 := NewPChannelInfoFromProto(pbInfo)
	assert.Equal(t, info.Name, info2.Name)
	assert.Equal(t, info.Term, info2.Term)

	assert.Panics(t, func() {
		NewProtoFromPChannelInfo(PChannelInfo{Name: "", Term: 1})
	})
	assert.Panics(t, func() {
		NewProtoFromPChannelInfo(PChannelInfo{Name: "c", Term: -1})
	})

	assert.Panics(t, func() {
		NewPChannelInfoFromProto(&streamingpb.PChannelInfo{Name: "", Term: 1})
	})

	assert.Panics(t, func() {
		NewPChannelInfoFromProto(&streamingpb.PChannelInfo{Name: "c", Term: -1})
	})
}
