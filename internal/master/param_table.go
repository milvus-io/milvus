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
	EtcdRootPath  string
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
	InsertChannelNames            []string
	K2SChannelNames               []string
	QueryNodeStatsChannelName     string
	MsgChannelSubName             string
}

var Params ParamTable

func (p *ParamTable) Init() {
	// load yaml
	p.BaseTable.Init()
	err := p.LoadYaml("milvus.yaml")
	if err != nil {
		panic(err)
	}
	err = p.LoadYaml("advanced/channel.yaml")
	if err != nil {
		panic(err)
	}
	err = p.LoadYaml("advanced/master.yaml")
	if err != nil {
		panic(err)
	}

	// set members
	p.initAddress()
	p.initPort()

	p.initEtcdAddress()
	p.initEtcdRootPath()
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
	p.initK2SChannelNames()
	p.initQueryNodeStatsChannelName()
	p.initMsgChannelSubName()
}

func (p *ParamTable) initAddress() {
	masterAddress, err := p.Load("master.address")
	if err != nil {
		panic(err)
	}
	p.Address = masterAddress
}

func (p *ParamTable) initPort() {
	masterPort, err := p.Load("master.port")
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(masterPort)
	if err != nil {
		panic(err)
	}
	p.Port = port
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

func (p *ParamTable) initEtcdRootPath() {
	path, err := p.Load("etcd.rootpath")
	if err != nil {
		panic(err)
	}
	p.EtcdRootPath = path
}

func (p *ParamTable) initTopicNum() {
	insertChannelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}

	channelRange := strings.Split(insertChannelRange, ",")
	if len(channelRange) != 2 {
		panic("Illegal channel range num")
	}
	channelBegin, err := strconv.Atoi(channelRange[0])
	if err != nil {
		panic(err)
	}
	channelEnd, err := strconv.Atoi(channelRange[1])
	if err != nil {
		panic(err)
	}
	if channelBegin < 0 || channelEnd < 0 {
		panic("Illegal channel range value")
	}
	if channelBegin > channelEnd {
		panic("Illegal channel range value")
	}
	p.TopicNum = channelEnd
}

func (p *ParamTable) initSegmentSize() {
	threshold, err := p.Load("master.segment.size")
	if err != nil {
		panic(err)
	}
	segmentThreshold, err := strconv.ParseFloat(threshold, 64)
	if err != nil {
		panic(err)
	}
	p.SegmentSize = segmentThreshold
}

func (p *ParamTable) initSegmentSizeFactor() {
	segFactor, err := p.Load("master.segment.sizeFactor")
	if err != nil {
		panic(err)
	}
	factor, err := strconv.ParseFloat(segFactor, 64)
	if err != nil {
		panic(err)
	}
	p.SegmentSizeFactor = factor
}

func (p *ParamTable) initDefaultRecordSize() {
	size, err := p.Load("master.segment.defaultSizePerRecord")
	if err != nil {
		panic(err)
	}
	res, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		panic(err)
	}
	p.DefaultRecordSize = res
}

func (p *ParamTable) initMinSegIDAssignCnt() {
	size, err := p.Load("master.segment.minIDAssignCnt")
	if err != nil {
		panic(err)
	}
	res, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MinSegIDAssignCnt = res
}

func (p *ParamTable) initMaxSegIDAssignCnt() {
	size, err := p.Load("master.segment.maxIDAssignCnt")
	if err != nil {
		panic(err)
	}
	res, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		panic(err)
	}
	p.MaxSegIDAssignCnt = res
}

func (p *ParamTable) initSegIDAssignExpiration() {
	duration, err := p.Load("master.segment.IDAssignExpiration")
	if err != nil {
		panic(err)
	}
	res, err := strconv.ParseInt(duration, 10, 64)
	if err != nil {
		panic(err)
	}
	p.SegIDAssignExpiration = res
}

func (p *ParamTable) initQueryNodeNum() {
	id, err := p.Load("nodeID.queryNodeIDList")
	if err != nil {
		panic(err)
	}
	ids := strings.Split(id, ",")
	for _, i := range ids {
		_, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
	}
	p.QueryNodeNum = len(ids)
}

func (p *ParamTable) initQueryNodeStatsChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	p.QueryNodeStatsChannelName = channels
}

func (p *ParamTable) initProxyIDList() {
	id, err := p.Load("nodeID.proxyIDList")
	if err != nil {
		log.Panicf("load proxy id list error, %s", err.Error())
	}
	ids := strings.Split(id, ",")
	idList := make([]typeutil.UniqueID, 0, len(ids))
	for _, i := range ids {
		v, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
		idList = append(idList, typeutil.UniqueID(v))
	}
	p.ProxyIDList = idList
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
	id, err := p.Load("nodeID.writeNodeIDList")
	if err != nil {
		log.Panic(err)
	}
	ids := strings.Split(id, ",")
	idlist := make([]typeutil.UniqueID, 0, len(ids))
	for _, i := range ids {
		v, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
		idlist = append(idlist, typeutil.UniqueID(v))
	}
	p.WriteNodeIDList = idlist
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

func (p *ParamTable) initInsertChannelNames() {
	ch, err := p.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		log.Fatal(err)
	}
	id, err := p.Load("nodeID.queryNodeIDList")
	if err != nil {
		log.Panicf("load query node id list error, %s", err.Error())
	}
	ids := strings.Split(id, ",")
	channels := make([]string, 0, len(ids))
	for _, i := range ids {
		_, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load query node id list error, %s", err.Error())
		}
		channels = append(channels, ch+"-"+i)
	}
	p.InsertChannelNames = channels
}

func (p *ParamTable) initK2SChannelNames() {
	ch, err := p.Load("msgChannel.chanNamePrefix.k2s")
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
	p.K2SChannelNames = channels
}
