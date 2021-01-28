package grpcproxynode

const (
	StartParamsKey                     = "START_PARAMS"
	MasterPort                         = "master.port"
	MasterHost                         = "master.address"
	PulsarPort                         = "pulsar.port"
	PulsarHost                         = "pulsar.address"
	IndexServerPort                    = "indexBuilder.port"
	IndexServerHost                    = "indexBuilder.address"
	QueryNodeIDList                    = "nodeID.queryNodeIDList"
	TimeTickInterval                   = "proxyNode.timeTickInterval"
	SubName                            = "msgChannel.subNamePrefix.proxySubNamePrefix"
	TimeTickChannelNames               = "msgChannel.chanNamePrefix.proxyTimeTick"
	MsgStreamInsertBufSize             = "proxyNode.msgStream.insert.bufSize"
	MsgStreamSearchBufSize             = "proxyNode.msgStream.search.bufSize"
	MsgStreamSearchResultBufSize       = "proxyNode.msgStream.searchResult.recvBufSize"
	MsgStreamSearchResultPulsarBufSize = "proxyNode.msgStream.searchResult.pulsarBufSize"
	MsgStreamTimeTickBufSize           = "proxyNode.msgStream.timeTick.bufSize"
	MaxNameLength                      = "proxyNode.maxNameLength"
	MaxFieldNum                        = "proxyNode.maxFieldNum"
	MaxDimension                       = "proxyNode.MaxDimension"
	DefaultPartitionTag                = "common.defaultPartitionTag"
)
