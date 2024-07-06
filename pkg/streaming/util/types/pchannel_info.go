package types

import "fmt"

const (
	InitialTerm int64 = -1
)

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
