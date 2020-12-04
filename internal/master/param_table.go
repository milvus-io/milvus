package master

import (
	"log"
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int

	EtcdAddress   string
	MetaRootPath  string
	KvRootPath    string
	PulsarAddress string

	// nodeID
	ProxyIDList     []typeutil.UniqueID
	WriteNodeIDList []typeutil.UniqueID

	TopicNum                    int
	QueryNodeNum                int
	SoftTimeTickBarrierInterval typeutil.Timestamp

	// segment
	SegmentSize           float64
	SegmentSizeFactor     float64
	DefaultRecordSize     int64
	MinSegIDAssignCnt     int64
	MaxSegIDAssignCnt     int64
	SegIDAssignExpiration int64

	// msgChannel
	ProxyTimeTickChannelNames     []string
	WriteNodeTimeTickChannelNames []string
	DDChannelNames                []string
	InsertChannelNames            []string
	K2SChannelNames               []string
	QueryNodeStatsChannelName     string
	MsgChannelSubName             string

	MaxPartitionNum     int64
	DefaultPartitionTag string
}

var Params ParamTable

func (p *ParamTable) Init() {
	// load yaml
	p.BaseTable.Init()

	err := p.LoadYaml("advanced/master.yaml")
	if err != nil {
		panic(err)
	}

	// set members
	p.initAddress()
	p.initPort()

	p.initEtcdAddress()
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initPulsarAddress()

	p.initProxyIDList()
	p.initWriteNodeIDList()

	p.initTopicNum()
	p.initQueryNodeNum()
	p.initSoftTimeTickBarrierInterval()

	p.initSegmentSize()
	p.initSegmentSizeFactor()
	p.initDefaultRecordSize()
	p.initMinSegIDAssignCnt()
	p.initMaxSegIDAssignCnt()
	p.initSegIDAssignExpiration()

	p.initProxyTimeTickChannelNames()
	p.initWriteNodeTimeTickChannelNames()
	p.initInsertChannelNames()
	p.initDDChannelNames()
	p.initK2SChannelNames()
	p.initQueryNodeStatsChannelName()
	p.initMsgChannelSubName()
	p.initMaxPartitionNum()
	p.initDefaultPartitionTag()
}

func (p *ParamTable) initAddress() {
	masterAddress, err := p.Load("master.address")
	if err != nil {
		panic(err)
	}
	p.Address = masterAddress
}

func (p *ParamTable) initPort() {
	p.Port = p.ParseInt("master.port")
}

func (p *ParamTable) initEtcdAddress() {
	addr, err := p.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	p.EtcdAddress = addr
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
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

func (p *ParamTable) initKvRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initTopicNum() {
	iRangeStr, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	rangeSlice := paramtable.ConvertRangeToIntRange(iRangeStr, ",")
	p.TopicNum = rangeSlice[1] - rangeSlice[0]
}

func (p *ParamTable) initSegmentSize() {
	p.SegmentSize = p.ParseFloat("master.segment.size")
}

func (p *ParamTable) initSegmentSizeFactor() {
	p.SegmentSizeFactor = p.ParseFloat("master.segment.sizeFactor")
}

func (p *ParamTable) initDefaultRecordSize() {
	p.DefaultRecordSize = p.ParseInt64("master.segment.defaultSizePerRecord")
}

func (p *ParamTable) initMinSegIDAssignCnt() {
	p.MinSegIDAssignCnt = p.ParseInt64("master.segment.minIDAssignCnt")
}

func (p *ParamTable) initMaxSegIDAssignCnt() {
	p.MaxSegIDAssignCnt = p.ParseInt64("master.segment.maxIDAssignCnt")
}

func (p *ParamTable) initSegIDAssignExpiration() {
	p.SegIDAssignExpiration = p.ParseInt64("master.segment.IDAssignExpiration")
}

func (p *ParamTable) initQueryNodeNum() {
	p.QueryNodeNum = len(p.QueryNodeIDList())
}

func (p *ParamTable) initQueryNodeStatsChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	p.QueryNodeStatsChannelName = channels
}

func (p *ParamTable) initProxyIDList() {
	p.ProxyIDList = p.BaseTable.ProxyIDList()
}

func (p *ParamTable) initProxyTimeTickChannelNames() {
	ch, err := p.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		log.Panic(err)
	}
	id, err := p.Load("nodeID.proxyIDList")
	if err != nil {
		log.Panicf("load proxy id list error, %s", err.Error())
	}
	ids := strings.Split(id, ",")
	channels := make([]string, 0, len(ids))
	for _, i := range ids {
		_, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
		channels = append(channels, ch+"-"+i)
	}
	p.ProxyTimeTickChannelNames = channels
}

func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.masterSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	p.MsgChannelSubName = name
}

func (p *ParamTable) initSoftTimeTickBarrierInterval() {
	t, err := p.Load("master.timeSync.softTimeTickBarrierInterval")
	if err != nil {
		log.Panic(err)
	}
	v, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		log.Panic(err)
	}
	p.SoftTimeTickBarrierInterval = tsoutil.ComposeTS(v, 0)
}

func (p *ParamTable) initWriteNodeIDList() {
	p.WriteNodeIDList = p.BaseTable.WriteNodeIDList()
}

func (p *ParamTable) initWriteNodeTimeTickChannelNames() {
	ch, err := p.Load("msgChannel.chanNamePrefix.writeNodeTimeTick")
	if err != nil {
		log.Fatal(err)
	}
	id, err := p.Load("nodeID.writeNodeIDList")
	if err != nil {
		log.Panicf("load write node id list error, %s", err.Error())
	}
	ids := strings.Split(id, ",")
	channels := make([]string, 0, len(ids))
	for _, i := range ids {
		_, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load write node id list error, %s", err.Error())
		}
		channels = append(channels, ch+"-"+i)
	}
	p.WriteNodeTimeTickChannelNames = channels
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

func (p *ParamTable) initInsertChannelNames() {
	prefix, err := p.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	iRangeStr, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(iRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	p.InsertChannelNames = ret
}

func (p *ParamTable) initK2SChannelNames() {
	prefix, err := p.Load("msgChannel.chanNamePrefix.k2s")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	iRangeStr, err := p.Load("msgChannel.channelRange.k2s")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(iRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	p.K2SChannelNames = ret
}

func (p *ParamTable) initMaxPartitionNum() {
	str, err := p.Load("master.maxPartitionNum")
	if err != nil {
		panic(err)
	}
	maxPartitionNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxPartitionNum = maxPartitionNum
}

func (p *ParamTable) initDefaultPartitionTag() {
	defaultTag, err := p.Load("common.defaultPartitionTag")
	if err != nil {
		panic(err)
	}

	p.DefaultPartitionTag = defaultTag
}
