package proxy

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (pt *ParamTable) Init() {
	pt.BaseTable.Init()
	err := pt.LoadYaml("milvus.yaml")
	if err != nil {
		panic(err)
	}
	err = pt.LoadYaml("advanced/proxy.yaml")
	if err != nil {
		panic(err)
	}
	err = pt.LoadYaml("advanced/channel.yaml")
	if err != nil {
		panic(err)
	}

	proxyIDStr := os.Getenv("PROXY_ID")
	if proxyIDStr == "" {
		proxyIDStr = "2"
	}
	pt.Save("_proxyID", proxyIDStr)
}

func (pt *ParamTable) NetWorkAddress() string {
	addr, err := pt.Load("proxy.network.address")
	if err != nil {
		panic(err)
	}
	if ip := net.ParseIP(addr); ip == nil {
		panic("invalid ip proxy.network.address")
	}
	port, err := pt.Load("proxy.network.port")
	if err != nil {
		panic(err)
	}
	_, err = strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	return addr + ":" + port
}

func (pt *ParamTable) MasterAddress() string {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	return ret
}

func (pt *ParamTable) PulsarAddress() string {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return ret
}

func (pt *ParamTable) ProxyNum() int {
	ret := pt.ProxyIDList()
	return len(ret)
}

func (pt *ParamTable) ProxyIDList() []UniqueID {
	proxyIDStr, err := pt.Load("nodeID.proxyIDList")
	if err != nil {
		panic(err)
	}
	var ret []UniqueID
	proxyIDs := strings.Split(proxyIDStr, ",")
	for _, i := range proxyIDs {
		v, err := strconv.Atoi(i)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
		ret = append(ret, UniqueID(v))
	}
	return ret
}

func (pt *ParamTable) ProxyID() UniqueID {
	proxyID, err := pt.Load("_proxyID")
	if err != nil {
		panic(err)
	}
	ID, err := strconv.Atoi(proxyID)
	if err != nil {
		panic(err)
	}
	return UniqueID(ID)
}

func (pt *ParamTable) TimeTickInterval() time.Duration {
	internalStr, err := pt.Load("proxy.timeTickInterval")
	if err != nil {
		panic(err)
	}
	interval, err := strconv.Atoi(internalStr)
	if err != nil {
		panic(err)
	}
	return time.Duration(interval) * time.Millisecond
}

func (pt *ParamTable) convertRangeToSlice(rangeStr, sep string) []int {
	channelIDs := strings.Split(rangeStr, sep)
	startStr := channelIDs[0]
	endStr := channelIDs[1]
	start, err := strconv.Atoi(startStr)
	if err != nil {
		panic(err)
	}
	end, err := strconv.Atoi(endStr)
	if err != nil {
		panic(err)
	}
	var ret []int
	for i := start; i <= end; i++ {
		ret = append(ret, i)
	}
	return ret
}

func (pt *ParamTable) sliceIndex() int {
	proxyID := pt.ProxyID()
	proxyIDList := pt.ProxyIDList()
	for i := 0; i < len(proxyIDList); i++ {
		if proxyID == proxyIDList[i] {
			return i
		}
	}
	return -1
}

func (pt *ParamTable) InsertChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	iRangeStr, err := pt.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	channelIDs := pt.convertRangeToSlice(iRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}

	proxyNum := pt.ProxyNum()
	sep := len(channelIDs) / proxyNum
	index := pt.sliceIndex()
	if index == -1 {
		panic("ProxyID not Match with Config")
	}
	start := index * sep
	return ret[start : start+sep]
}

func (pt *ParamTable) DeleteChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.delete")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	dRangeStr, err := pt.Load("msgChannel.channelRange.delete")
	if err != nil {
		panic(err)
	}
	channelIDs := pt.convertRangeToSlice(dRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	proxyNum := pt.ProxyNum()
	sep := len(channelIDs) / proxyNum
	index := pt.sliceIndex()
	if index == -1 {
		panic("ProxyID not Match with Config")
	}
	start := index * sep
	return ret[start : start+sep]
}

func (pt *ParamTable) K2SChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.k2s")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	k2sRangeStr, err := pt.Load("msgChannel.channelRange.k2s")
	if err != nil {
		panic(err)
	}
	channelIDs := pt.convertRangeToSlice(k2sRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	proxyNum := pt.ProxyNum()
	sep := len(channelIDs) / proxyNum
	index := pt.sliceIndex()
	if index == -1 {
		panic("ProxyID not Match with Config")
	}
	start := index * sep
	return ret[start : start+sep]
}

func (pt *ParamTable) SearchChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		panic(err)
	}
	prefix += "-0"
	return []string{prefix}
}

func (pt *ParamTable) SearchResultChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	sRangeStr, err := pt.Load("msgChannel.channelRange.searchResult")
	if err != nil {
		panic(err)
	}
	channelIDs := pt.convertRangeToSlice(sRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	proxyNum := pt.ProxyNum()
	sep := len(channelIDs) / proxyNum
	index := pt.sliceIndex()
	if index == -1 {
		panic("ProxyID not Match with Config")
	}
	start := index * sep

	return ret[start : start+sep]
}

func (pt *ParamTable) ProxySubName() string {
	prefix, err := pt.Load("msgChannel.subNamePrefix.proxySubNamePrefix")
	if err != nil {
		panic(err)
	}
	proxyIDStr, err := pt.Load("_proxyID")
	if err != nil {
		panic(err)
	}
	return prefix + "-" + proxyIDStr
}

func (pt *ParamTable) ProxyTimeTickChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		panic(err)
	}
	prefix += "-0"
	return []string{prefix}
}

func (pt *ParamTable) DataDefinitionChannelNames() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.dataDefinition")
	if err != nil {
		panic(err)
	}
	prefix += "-0"
	return []string{prefix}
}

func (pt *ParamTable) parseInt64(key string) int64 {
	valueStr, err := pt.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return int64(value)
}

func (pt *ParamTable) MsgStreamInsertBufSize() int64 {
	return pt.parseInt64("proxy.msgStream.insert.bufSize")
}

func (pt *ParamTable) MsgStreamSearchBufSize() int64 {
	return pt.parseInt64("proxy.msgStream.search.bufSize")
}

func (pt *ParamTable) MsgStreamSearchResultBufSize() int64 {
	return pt.parseInt64("proxy.msgStream.searchResult.recvBufSize")
}

func (pt *ParamTable) MsgStreamSearchResultPulsarBufSize() int64 {
	return pt.parseInt64("proxy.msgStream.searchResult.pulsarBufSize")
}

func (pt *ParamTable) MsgStreamTimeTickBufSize() int64 {
	return pt.parseInt64("proxy.msgStream.timeTick.bufSize")
}

func (pt *ParamTable) insertChannelNames() []string {
	ch, err := pt.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := pt.Load("msgChannel.channelRange.insert")
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

func (pt *ParamTable) searchChannelNames() []string {
	ch, err := pt.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := pt.Load("msgChannel.channelRange.search")
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

func (pt *ParamTable) searchResultChannelNames() []string {
	ch, err := pt.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Fatal(err)
	}
	channelRange, err := pt.Load("msgChannel.channelRange.search")
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
