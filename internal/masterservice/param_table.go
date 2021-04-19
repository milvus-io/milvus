package masterservice

import (
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params ParamTable

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int
	NodeID  uint64

	PulsarAddress             string
	EtcdAddress               string
	MetaRootPath              string
	KvRootPath                string
	ProxyTimeTickChannel      string //get from proxy client
	MsgChannelSubName         string
	TimeTickChannel           string
	DdChannel                 string
	StatisticsChannel         string
	DataServiceSegmentChannel string // get from data service, data service create segment, or data node flush segment

	MaxPartitionNum      int64
	DefaultPartitionName string
	DefaultIndexName     string

	Timeout int
}

func (p *ParamTable) Init() {
	// load yaml
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/master.yaml")
	if err != nil {
		panic(err)
	}

	p.initAddress()
	p.initPort()
	p.initNodeID()

	p.initPulsarAddress()
	p.initEtcdAddress()
	p.initMetaRootPath()
	p.initKvRootPath()

	p.initMsgChannelSubName()
	p.initTimeTickChannel()
	p.initDdChannelName()
	p.initStatisticsChannelName()

	p.initMaxPartitionNum()
	p.initDefaultPartitionName()
	p.initDefaultIndexName()

	p.initTimeout()
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

func (p *ParamTable) initNodeID() {
	p.NodeID = uint64(p.ParseInt64("master.nodeID"))
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *ParamTable) initEtcdAddress() {
	addr, err := p.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	p.EtcdAddress = addr
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

func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.masterSubNamePrefix")
	if err != nil {
		panic(err)
	}
	p.MsgChannelSubName = name
}

func (p *ParamTable) initTimeTickChannel() {
	channel, err := p.Load("msgChannel.chanNamePrefix.masterTimeTick")
	if err != nil {
		panic(err)
	}
	p.TimeTickChannel = channel
}

func (p *ParamTable) initDdChannelName() {
	channel, err := p.Load("msgChannel.chanNamePrefix.dataDefinition")
	if err != nil {
		panic(err)
	}
	p.DdChannel = channel
}

func (p *ParamTable) initStatisticsChannelName() {
	channel, err := p.Load("msgChannel.chanNamePrefix.masterStatistics")
	if err != nil {
		panic(err)
	}
	p.StatisticsChannel = channel
}

func (p *ParamTable) initMaxPartitionNum() {
	p.MaxPartitionNum = p.ParseInt64("master.maxPartitionNum")
}

func (p *ParamTable) initDefaultPartitionName() {
	name, err := p.Load("common.defaultPartitionName")
	if err != nil {
		panic(err)
	}
	p.DefaultPartitionName = name
}

func (p *ParamTable) initDefaultIndexName() {
	name, err := p.Load("common.defaultIndexName")
	if err != nil {
		panic(err)
	}
	p.DefaultIndexName = name
}

func (p *ParamTable) initTimeout() {
	p.Timeout = p.ParseInt("master.timeout")
}
