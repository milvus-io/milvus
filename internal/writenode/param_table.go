package writenode

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
	err := p.LoadYaml("advanced/write_node.yaml")
	if err != nil {
		panic(err)
	}

	writeNodeIDStr := os.Getenv("WRITE_NODE_ID")
	if writeNodeIDStr == "" {
		writeNodeIDList := p.WriteNodeIDList()
		if len(writeNodeIDList) <= 0 {
			writeNodeIDStr = "0"
		} else {
			writeNodeIDStr = strconv.Itoa(int(writeNodeIDList[0]))
		}
	}
	p.Save("_writeNodeID", writeNodeIDStr)
}

func (p *ParamTable) pulsarAddress() (string, error) {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return url, nil
}

func (p *ParamTable) WriteNodeID() UniqueID {
	writeNodeID, err := p.Load("_writeNodeID")
	if err != nil {
		panic(err)
	}
	id, err := strconv.Atoi(writeNodeID)
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
	return p.ParseInt("writeNode.stats.publishInterval")
}

// dataSync:
func (p *ParamTable) flowGraphMaxQueueLength() int32 {
	return p.ParseInt32("writeNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) flowGraphMaxParallelism() int32 {
	return p.ParseInt32("writeNode.dataSync.flowGraph.maxParallelism")
}

// msgStream
func (p *ParamTable) insertReceiveBufSize() int64 {
	return p.ParseInt64("writeNode.msgStream.insert.recvBufSize")
}

func (p *ParamTable) insertPulsarBufSize() int64 {
	return p.ParseInt64("writeNode.msgStream.insert.pulsarBufSize")
}

func (p *ParamTable) searchReceiveBufSize() int64 {
	return p.ParseInt64("writeNode.msgStream.search.recvBufSize")
}

func (p *ParamTable) searchPulsarBufSize() int64 {
	return p.ParseInt64("writeNode.msgStream.search.pulsarBufSize")
}

func (p *ParamTable) searchResultReceiveBufSize() int64 {
	return p.ParseInt64("writeNode.msgStream.searchResult.recvBufSize")
}

func (p *ParamTable) statsReceiveBufSize() int64 {
	return p.ParseInt64("writeNode.msgStream.stats.recvBufSize")
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
	gracefulTime, err := p.Load("writeNode.gracefulTime")
	if err != nil {
		panic(err)
	}
	time, err := strconv.Atoi(gracefulTime)
	if err != nil {
		panic(err)
	}
	return int64(time)
}

func (p *ParamTable) insertChannelNames() []string {
	prefix, err := p.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}

	channelIDs := paramtable.ConvertRangeToIntSlice(channelRange, ",")

	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	sep := len(channelIDs) / p.writeNodeNum()
	index := p.sliceIndex()
	if index == -1 {
		panic("writeNodeID not Match with Config")
	}
	start := index * sep
	return ret[start : start+sep]
}

func (p *ParamTable) searchChannelNames() []string {
	prefix, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Fatal(err)
	}
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
	// TODO: subName = namePrefix + "-" + writeNodeID, writeNodeID is assigned by master
	name, err := p.Load("msgChannel.subNamePrefix.writeNodeSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	writeNodeIDStr, err := p.Load("_WriteNodeID")
	if err != nil {
		panic(err)
	}
	return name + "-" + writeNodeIDStr
}

func (p *ParamTable) writeNodeTimeTickChannelName() string {
	channels, err := p.Load("msgChannel.chanNamePrefix.writeNodeTimeTick")
	if err != nil {
		panic(err)
	}
	return channels
}

func (p *ParamTable) sliceIndex() int {
	writeNodeID := p.WriteNodeID()
	writeNodeIDList := p.WriteNodeIDList()
	for i := 0; i < len(writeNodeIDList); i++ {
		if writeNodeID == writeNodeIDList[i] {
			return i
		}
	}
	return -1
}

func (p *ParamTable) writeNodeNum() int {
	return len(p.WriteNodeIDList())
}
