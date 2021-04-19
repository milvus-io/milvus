package querynode

import (
	"log"
	"os"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	PulsarAddress string
	ETCDAddress   string
	MetaRootPath  string

	QueryNodeID  UniqueID
	QueryNodeNum int

	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32

	// dm
	InsertChannelNames   []string
	InsertChannelRange   []int
	InsertReceiveBufSize int64
	InsertPulsarBufSize  int64

	// dd
	DDChannelNames   []string
	DDReceiveBufSize int64
	DDPulsarBufSize  int64

	// search
	SearchChannelNames         []string
	SearchResultChannelNames   []string
	SearchReceiveBufSize       int64
	SearchPulsarBufSize        int64
	SearchResultReceiveBufSize int64

	// stats
	StatsPublishInterval int
	StatsChannelName     string
	StatsReceiveBufSize  int64

	GracefulTime        int64
	MsgChannelSubName   string
	DefaultPartitionTag string
	SliceIndex          int
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

	err = p.LoadYaml("advanced/common.yaml")
	if err != nil {
		panic(err)
	}
	err = p.Save("_queryNodeID", queryNodeIDStr)
	if err != nil {
		panic(err)
	}

	p.initPulsarAddress()
	p.initETCDAddress()
	p.initMetaRootPath()

	p.initQueryNodeID()
	p.initQueryNodeNum()

	p.initGracefulTime()
	p.initMsgChannelSubName()
	p.initDefaultPartitionTag()
	p.initSliceIndex()

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()

	p.initInsertChannelNames()
	p.initInsertChannelRange()
	p.initInsertReceiveBufSize()
	p.initInsertPulsarBufSize()

	p.initDDChannelNames()
	p.initDDReceiveBufSize()
	p.initDDPulsarBufSize()

	p.initSearchChannelNames()
	p.initSearchResultChannelNames()
	p.initSearchReceiveBufSize()
	p.initSearchPulsarBufSize()
	p.initSearchResultReceiveBufSize()

	p.initStatsPublishInterval()
	p.initStatsChannelName()
	p.initStatsReceiveBufSize()
}

func (p *ParamTable) initPulsarAddress() {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *ParamTable) initQueryNodeID() {
	queryNodeID, err := p.Load("_queryNodeID")
	if err != nil {
		panic(err)
	}
	id, err := strconv.Atoi(queryNodeID)
	if err != nil {
		panic(err)
	}
	p.QueryNodeID = UniqueID(id)
}

func (p *ParamTable) initInsertChannelRange() {
	insertChannelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	p.InsertChannelRange = paramtable.ConvertRangeToIntRange(insertChannelRange, ",")
}

// advanced params
// stats
func (p *ParamTable) initStatsPublishInterval() {
	p.StatsPublishInterval = p.ParseInt("queryNode.stats.publishInterval")
}

// dataSync:
func (p *ParamTable) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.ParseInt32("queryNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.ParseInt32("queryNode.dataSync.flowGraph.maxParallelism")
}

// msgStream
func (p *ParamTable) initInsertReceiveBufSize() {
	p.InsertReceiveBufSize = p.ParseInt64("queryNode.msgStream.insert.recvBufSize")
}

func (p *ParamTable) initInsertPulsarBufSize() {
	p.InsertPulsarBufSize = p.ParseInt64("queryNode.msgStream.insert.pulsarBufSize")
}

func (p *ParamTable) initDDReceiveBufSize() {
	revBufSize, err := p.Load("queryNode.msgStream.dataDefinition.recvBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	p.DDReceiveBufSize = int64(bufSize)
}

func (p *ParamTable) initDDPulsarBufSize() {
	pulsarBufSize, err := p.Load("queryNode.msgStream.dataDefinition.pulsarBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(pulsarBufSize)
	if err != nil {
		panic(err)
	}
	p.DDPulsarBufSize = int64(bufSize)
}

func (p *ParamTable) initSearchReceiveBufSize() {
	p.SearchReceiveBufSize = p.ParseInt64("queryNode.msgStream.search.recvBufSize")
}

func (p *ParamTable) initSearchPulsarBufSize() {
	p.SearchPulsarBufSize = p.ParseInt64("queryNode.msgStream.search.pulsarBufSize")
}

func (p *ParamTable) initSearchResultReceiveBufSize() {
	p.SearchResultReceiveBufSize = p.ParseInt64("queryNode.msgStream.searchResult.recvBufSize")
}

func (p *ParamTable) initStatsReceiveBufSize() {
	p.StatsReceiveBufSize = p.ParseInt64("queryNode.msgStream.stats.recvBufSize")
}

func (p *ParamTable) initETCDAddress() {
	ETCDAddress, err := p.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	p.ETCDAddress = ETCDAddress
}

func (p *ParamTable) initMetaRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initGracefulTime() {
	p.GracefulTime = p.ParseInt64("queryNode.gracefulTime")
}

func (p *ParamTable) initInsertChannelNames() {

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
	sep := len(channelIDs) / p.QueryNodeNum
	index := p.SliceIndex
	if index == -1 {
		panic("queryNodeID not Match with Config")
	}
	start := index * sep
	p.InsertChannelNames = ret[start : start+sep]
}

func (p *ParamTable) initSearchChannelNames() {
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
	p.SearchChannelNames = ret
}

func (p *ParamTable) initSearchResultChannelNames() {
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
	p.SearchResultChannelNames = ret
}

func (p *ParamTable) initMsgChannelSubName() {
	// TODO: subName = namePrefix + "-" + queryNodeID, queryNodeID is assigned by master
	name, err := p.Load("msgChannel.subNamePrefix.queryNodeSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	queryNodeIDStr, err := p.Load("_QueryNodeID")
	if err != nil {
		panic(err)
	}
	p.MsgChannelSubName = name + "-" + queryNodeIDStr
}

func (p *ParamTable) initStatsChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	p.StatsChannelName = channels
}

func (p *ParamTable) initDDChannelNames() {
	prefix, err := p.Load("msgChannel.chanNamePrefix.dataDefinition")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	iRangeStr, err := p.Load("msgChannel.channelRange.dataDefinition")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(iRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	p.DDChannelNames = ret
}

func (p *ParamTable) initDefaultPartitionTag() {
	defaultTag, err := p.Load("common.defaultPartitionTag")
	if err != nil {
		panic(err)
	}

	p.DefaultPartitionTag = defaultTag
}

func (p *ParamTable) initSliceIndex() {
	queryNodeID := p.QueryNodeID
	queryNodeIDList := p.QueryNodeIDList()
	for i := 0; i < len(queryNodeIDList); i++ {
		if queryNodeID == queryNodeIDList[i] {
			p.SliceIndex = i
			return
		}
	}
	p.SliceIndex = -1
}

func (p *ParamTable) initQueryNodeNum() {
	p.QueryNodeNum = len(p.QueryNodeIDList())
}
