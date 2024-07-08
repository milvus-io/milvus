package typeconverter

import (
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// NewPChannelInfoFromProto converts protobuf PChannelInfo to PChannelInfo
func NewPChannelInfoFromProto(pchannel *streamingpb.PChannelInfo) types.PChannelInfo {
	if pchannel.GetName() == "" {
		panic("pchannel name is empty")
	}
	if pchannel.GetTerm() <= 0 {
		panic("pchannel term is empty or negetive")
	}
	return types.PChannelInfo{
		Name: pchannel.GetName(),
		Term: pchannel.GetTerm(),
	}
}

// NewProtoFromPChannelInfo converts PChannelInfo to protobuf PChannelInfo
func NewProtoFromPChannelInfo(pchannel types.PChannelInfo) *streamingpb.PChannelInfo {
	if pchannel.Name == "" {
		panic("pchannel name is empty")
	}
	if pchannel.Term <= 0 {
		panic("pchannel term is empty or negetive")
	}
	return &streamingpb.PChannelInfo{
		Name: pchannel.Name,
		Term: pchannel.Term,
	}
}
