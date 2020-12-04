package querynode

import (
	"log"
	"os"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (p *ParamTable) Init() {
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/query_node.yaml")
	if err != nil {
		panic(err)
	}

	queryNodeIDStr := os.Getenv("QUERY_NODE_ID")
	if queryNodeIDStr == "" {
		queryNodeIDList := p.QueryNodeIDList()
		if len(queryNodeIDList) <= 0 {
			queryNodeIDStr = "0"
		} else {
			queryNodeIDStr = strconv.Itoa(int(queryNodeIDList[0]))
		}
	}
	p.Save("_queryNodeID", queryNodeIDStr)
}

func (p *ParamTable) pulsarAddress() (string, error) {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return url, nil
}

func (p *ParamTable) QueryNodeID() UniqueID {
	queryNodeID, err := p.Load("_queryNodeID")
	if err != nil {
		panic(err)
	}
	id, err := strconv.Atoi(queryNodeID)
	if err != nil {
		panic(err)
	}
	return UniqueID(id)
}

func (p *ParamTable) insertChannelRange() []int {
	insertChannelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	return paramtable.ConvertRangeToIntRange(insertChannelRange, ",")
}

// advanced params
// stats
func (p *ParamTable) statsPublishInterval() int {
	return p.ParseInt("queryNode.stats.publishInterval")
}

// dataSync:
func (p *ParamTable) flowGraphMaxQueueLength() int32 {
	return p.ParseInt32("queryNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) flowGraphMaxParallelism() int32 {
	return p.ParseInt32("queryNode.dataSync.flowGraph.maxParallelism")
}

// msgStream
func (p *ParamTable) insertReceiveBufSize() int64 {
	return p.ParseInt64("queryNode.msgStream.insert.recvBufSize")
}

func (p *ParamTable) insertPulsarBufSize() int64 {
	return p.ParseInt64("queryNode.msgStream.insert.pulsarBufSize")
}

func (p *ParamTable) searchReceiveBufSize() int64 {
	return p.ParseInt64("queryNode.msgStream.search.recvBufSize")
}

func (p *ParamTable) searchPulsarBufSize() int64 {
	return p.ParseInt64("queryNode.msgStream.search.pulsarBufSize")
}

func (p *ParamTable) searchResultReceiveBufSize() int64 {
	return p.ParseInt64("queryNode.msgStream.searchResult.recvBufSize")
}

func (p *ParamTable) statsReceiveBufSize() int64 {
	return p.ParseInt64("queryNode.msgStream.stats.recvBufSize")
}

func (p *ParamTable) etcdAddress() string {
	etcdAddress, err := p.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	return etcdAddress
}

func (p *ParamTable) metaRootPath() string {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	return rootPath + "/" + subPath
}

func (p *ParamTable) gracefulTime() int64 {
	return p.ParseInt64("queryNode.gracefulTime")
}

func (p *ParamTable) insertChannelNames() []string {

	prefix, err := p.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		log.Fatal(err)
	}
	prefix += "-"
	channelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(channelRange, ",")

	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	sep := len(channelIDs) / p.queryNodeNum()
	index := p.sliceIndex()
	if index == -1 {
		panic("queryNodeID not Match with Config")
	}
	start := index * sep
	return ret[start : start+sep]
}

func (p *ParamTable) searchChannelNames() []string {
	prefix, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Fatal(err)
	}
	prefix += "-"
	channelRange, err := p.Load("msgChannel.channelRange.search")
	if err != nil {
		panic(err)
	}

	channelIDs := paramtable.ConvertRangeToIntSlice(channelRange, ",")

	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	return ret
}

func (p *ParamTable) searchResultChannelNames() []string {
	prefix, err := p.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		log.Fatal(err)
	}
	prefix += "-"
	channelRange, err := p.Load("msgChannel.channelRange.searchResult")
	if err != nil {
		panic(err)
	}

	channelIDs := paramtable.ConvertRangeToIntSlice(channelRange, ",")

	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	return ret
}

func (p *ParamTable) msgChannelSubName() string {
	// TODO: subName = namePrefix + "-" + queryNodeID, queryNodeID is assigned by master
	name, err := p.Load("msgChannel.subNamePrefix.queryNodeSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	queryNodeIDStr, err := p.Load("_QueryNodeID")
	if err != nil {
		panic(err)
	}
	return name + "-" + queryNodeIDStr
}

func (p *ParamTable) statsChannelName() string {
	channels, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	return channels
}

func (p *ParamTable) sliceIndex() int {
	queryNodeID := p.QueryNodeID()
	queryNodeIDList := p.QueryNodeIDList()
	for i := 0; i < len(queryNodeIDList); i++ {
		if queryNodeID == queryNodeIDList[i] {
			return i
		}
	}
	return -1
}

func (p *ParamTable) queryNodeNum() int {
	return len(p.QueryNodeIDList())
}
