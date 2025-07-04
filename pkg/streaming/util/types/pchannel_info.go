package types

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	InitialTerm  int64      = -1
	AccessModeRW AccessMode = AccessMode(streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE) // It's the default option.
	AccessModeRO AccessMode = AccessMode(streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY)
)

type AccessMode streamingpb.PChannelAccessMode

func (m AccessMode) String() string {
	switch m {
	case AccessModeRO:
		return "ro"
	case AccessModeRW:
		return "rw"
	default:
		panic("undefined access mode")
	}
}

// NewPChannelInfoFromProto converts protobuf PChannelInfo to PChannelInfo
func NewPChannelInfoFromProto(pchannel *streamingpb.PChannelInfo) PChannelInfo {
	if pchannel.GetName() == "" {
		panic("pchannel name is empty")
	}
	if pchannel.GetTerm() <= 0 {
		panic("pchannel term is empty or negetive")
	}
	accessMode := AccessMode(pchannel.GetAccessMode())
	_ = accessMode.String() // assertion.
	return PChannelInfo{
		Name:       pchannel.GetName(),
		Term:       pchannel.GetTerm(),
		AccessMode: accessMode,
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
		Name:       pchannel.Name,
		Term:       pchannel.Term,
		AccessMode: streamingpb.PChannelAccessMode(pchannel.AccessMode),
	}
}

// ChannelID is the unique identifier of a pchannel.
type ChannelID struct {
	Name string
	// TODO: add replica id in future.
}

func (id ChannelID) IsZero() bool {
	return id.Name == ""
}

func (id ChannelID) String() string {
	return id.Name
}

// LT is used to make a ChannelID sortable.
func (id1 ChannelID) LT(id2 ChannelID) bool {
	return id1.Name < id2.Name
}

// PChannelInfo is the struct for pchannel info.
type PChannelInfo struct {
	Name       string     // name of pchannel.
	Term       int64      // term of pchannel.
	AccessMode AccessMode // Access mode, if AccessModeRO, the wal impls should be read-only, the append operation will panics.
	// If accessMode is AccessModeRW, the wal impls should be read-write,
	// and it will fence the old rw wal impls or wait the old rw wal impls close.
}

func (c PChannelInfo) ChannelID() ChannelID {
	return ChannelID{Name: c.Name}
}

func (c PChannelInfo) String() string {
	return fmt.Sprintf("%s:%s@%d", c.Name, c.AccessMode, c.Term)
}

// PChannelInfoAssigned is a pair that represent a channel assignment of channel
type PChannelInfoAssigned struct {
	Channel PChannelInfo
	Node    StreamingNodeInfo
}

func (c PChannelInfoAssigned) String() string {
	return fmt.Sprintf("%s>%s", c.Channel, c.Node)
}
