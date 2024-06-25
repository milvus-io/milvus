package types

const (
	InitialTerm int64 = -1
)

// PChannelInfo is the struct for pchannel info.
type PChannelInfo struct {
	Name     string // name of pchannel.
	Term     int64  // term of pchannel.
	ServerID int64  // assigned streaming node server id of pchannel.
}
