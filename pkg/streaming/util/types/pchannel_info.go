package types

const (
	InitialTerm int64 = -1
)

// PChannelInfo is the struct for pchannel info.
type PChannelInfo struct {
	Name string // name of pchannel.
	Term int64  // term of pchannel.
}

type PChannelInfoAssigned struct {
	Channel PChannelInfo
	Node    StreamingNodeInfo
}
