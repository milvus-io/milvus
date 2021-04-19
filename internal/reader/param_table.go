package reader

import (
	"log"
	"strconv"
	"strings"

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
	err = p.LoadYaml("advanced/channel.yaml")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) pulsarAddress() (string, error) {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return url, nil
}

func (p *ParamTable) queryNodeID() int {
	queryNodeID, err := p.Load("reader.clientid")
	if err != nil {
		panic(err)
	}
	id, err := strconv.Atoi(queryNodeID)
	if err != nil {
		panic(err)
	}
	return id
}

// TODO: func (p *ParamTable) DmChannelRange() []int {
func (p *ParamTable) topicStart() int {
	topicStart, err := p.Load("reader.topicstart")
	if err != nil {
		panic(err)
	}
	topicStartNum, err := strconv.Atoi(topicStart)
	if err != nil {
		panic(err)
	}
	return topicStartNum
}

func (p *ParamTable) topicEnd() int {
	topicEnd, err := p.Load("reader.topicend")
	if err != nil {
		panic(err)
	}
	topicEndNum, err := strconv.Atoi(topicEnd)
	if err != nil {
		panic(err)
	}
	return topicEndNum
}

// advanced params
// stats
func (p *ParamTable) statsPublishInterval() int {
	timeInterval, err := p.Load("reader.stats.publishInterval")
	if err != nil {
		panic(err)
	}
	interval, err := strconv.Atoi(timeInterval)
	if err != nil {
		panic(err)
	}
	return interval
}

// dataSync:
func (p *ParamTable) flowGraphMaxQueueLength() int32 {
	queueLength, err := p.Load("reader.dataSync.flowGraph.maxQueueLength")
	if err != nil {
		panic(err)
	}
	length, err := strconv.Atoi(queueLength)
	if err != nil {
		panic(err)
	}
	return int32(length)
}

func (p *ParamTable) flowGraphMaxParallelism() int32 {
	maxParallelism, err := p.Load("reader.dataSync.flowGraph.maxParallelism")
	if err != nil {
		panic(err)
	}
	maxPara, err := strconv.Atoi(maxParallelism)
	if err != nil {
		panic(err)
	}
	return int32(maxPara)
}

// msgStream
// TODO: func (p *ParamTable) insertStreamBufSize() int64
func (p *ParamTable) dmReceiveBufSize() int64 {
	revBufSize, err := p.Load("reader.msgStream.dm.recvBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) dmPulsarBufSize() int64 {
	pulsarBufSize, err := p.Load("reader.msgStream.dm.pulsarBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(pulsarBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) searchReceiveBufSize() int64 {
	revBufSize, err := p.Load("reader.msgStream.search.recvBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) searchPulsarBufSize() int64 {
	pulsarBufSize, err := p.Load("reader.msgStream.search.pulsarBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(pulsarBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) searchResultReceiveBufSize() int64 {
	revBufSize, err := p.Load("reader.msgStream.searchResult.recvBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) statsReceiveBufSize() int64 {
	revBufSize, err := p.Load("reader.msgStream.stats.recvBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) etcdAddress() string {
	etcdAddress, err := p.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	return etcdAddress
}

func (p *ParamTable) etcdRootPath() string {
	etcdRootPath, err := p.Load("etcd.rootpath")
	if err != nil {
		panic(err)
	}
	return etcdRootPath
}

func (p *ParamTable) insertChannelNames() []string {
	ch, err := p.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}

	chanRange := strings.Split(channelRange, ",")
	if len(chanRange) != 2 {
		panic("Illegal channel range num")
	}
	channelBegin, err := strconv.Atoi(chanRange[0])
	if err != nil {
		panic(err)
	}
	channelEnd, err := strconv.Atoi(chanRange[1])
	if err != nil {
		panic(err)
	}
	if channelBegin < 0 || channelEnd < 0 {
		panic("Illegal channel range value")
	}
	if channelBegin > channelEnd {
		panic("Illegal channel range value")
	}

	channels := make([]string, channelEnd-channelBegin)
	for i := 0; i < channelEnd-channelBegin; i++ {
		channels[i] = ch + "-" + strconv.Itoa(channelBegin+i)
	}
	return channels
}

func (p *ParamTable) searchChannelNames() []string {
	ch, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := p.Load("msgChannel.channelRange.search")
	if err != nil {
		panic(err)
	}

	chanRange := strings.Split(channelRange, ",")
	if len(chanRange) != 2 {
		panic("Illegal channel range num")
	}
	channelBegin, err := strconv.Atoi(chanRange[0])
	if err != nil {
		panic(err)
	}
	channelEnd, err := strconv.Atoi(chanRange[1])
	if err != nil {
		panic(err)
	}
	if channelBegin < 0 || channelEnd < 0 {
		panic("Illegal channel range value")
	}
	if channelBegin > channelEnd {
		panic("Illegal channel range value")
	}

	channels := make([]string, channelEnd-channelBegin)
	for i := 0; i < channelEnd-channelBegin; i++ {
		channels[i] = ch + "-" + strconv.Itoa(channelBegin+i)
	}
	return channels
}

func (p *ParamTable) searchResultChannelNames() []string {
	ch, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := p.Load("msgChannel.channelRange.search")
	if err != nil {
		panic(err)
	}

	chanRange := strings.Split(channelRange, ",")
	if len(chanRange) != 2 {
		panic("Illegal channel range num")
	}
	channelBegin, err := strconv.Atoi(chanRange[0])
	if err != nil {
		panic(err)
	}
	channelEnd, err := strconv.Atoi(chanRange[1])
	if err != nil {
		panic(err)
	}
	if channelBegin < 0 || channelEnd < 0 {
		panic("Illegal channel range value")
	}
	if channelBegin > channelEnd {
		panic("Illegal channel range value")
	}

	channels := make([]string, channelEnd-channelBegin)
	for i := 0; i < channelEnd-channelBegin; i++ {
		channels[i] = ch + "-" + strconv.Itoa(channelBegin+i)
	}
	return channels
}
