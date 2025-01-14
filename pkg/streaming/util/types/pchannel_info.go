package types

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

const (
	InitialTerm int64 = -1
)

// NewPChannelInfoFromProto converts protobuf PChannelInfo to PChannelInfo
func NewPChannelInfoFromProto(pchannel *streamingpb.PChannelInfo) PChannelInfo {
	if pchannel.GetName() == "" {
		panic("pchannel name is empty")
	}
	if pchannel.GetTerm() <= 0 {
		panic("pchannel term is empty or negetive")
	}
	return PChannelInfo{
		Name: pchannel.GetName(),
		Term: pchannel.GetTerm(),
	}
}

// NewProtoFromPChannelInfo converts PChannelInfo to protobuf PChannelInfo
func NewProtoFromPChannelInfo(pchannel PChannelInfo) *streamingpb.PChannelInfo {
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

// PChannelInfo is the struct for pchannel info.
type PChannelInfo struct {
	Name string // name of pchannel.
	Term int64  // term of pchannel.
}

func (c *PChannelInfo) String() string {
	return fmt.Sprintf("%s@%d", c.Name, c.Term)
}

type PChannelInfoAssigned struct {
	Channel PChannelInfo
	Node    StreamingNodeInfo
}
