package proxynode

// This file lists all the parameter proxynode node needs to start,
// not used, just for me to record.

type InitParams struct {
	nodeID UniqueID

	proxyServiceAddress string
	masterAddress       string
	pulsarAddress       string

	searchBufSize      int
	searchChannelNames []string

	searchResultBufSize      int
	searchResultChannelNames []string
	subTopicName             string

	// TODO: this variable dynamic changes, how?
	queryNodeNum int

	insertBufSize      int
	insertChannelNames []string

	timeTickBufSize      int
	timeTickChannelNames []string

	defaultPartitionName string
	maxFieldNum          int
	maxNameLength        int
	maxDimension         int
}
