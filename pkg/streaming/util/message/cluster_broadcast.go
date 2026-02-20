package message

// ClusterChannels describes the physical channel topology of the cluster.
// Channels is the raw pchannel name list.
// ControlChannel is the control channel name (e.g. "pchannel0_vcchan").
//
// WithClusterLevelBroadcast uses this to build the broadcast channel list,
// substituting the control channel for the pchannel it resides on.
type ClusterChannels struct {
	Channels       []string
	ControlChannel string
}
