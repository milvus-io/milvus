package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int
	NodeID  int64

	MasterAddress string

	EtcdAddress   string
	MetaRootPath  string
	KvRootPath    string
	PulsarAddress string

	// segment
	SegmentSize           float64
	SegmentSizeFactor     float64
	DefaultRecordSize     int64
	SegIDAssignExpiration int64

	InsertChannelPrefixName       string
	InsertChannelNumPerCollection int64
	StatisticsChannelName         string
	TimeTickChannelName           string
	DataNodeNum                   int
	SegmentChannelName            string // todo init
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
	p.NodeID = 1 // todo

	p.initEtcdAddress()
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initPulsarAddress()

	p.initSegmentSize()
	p.initSegmentSizeFactor()
	p.initDefaultRecordSize()
	p.initSegIDAssignExpiration()
	p.initInsertChannelPrefixName()
	p.initInsertChannelNumPerCollection()
	p.initStatisticsChannelName()
	p.initTimeTickChannelName()
	p.initDataNodeNum()
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
func (p *ParamTable) initSegmentSize() {
	p.SegmentSize = p.ParseFloat("master.segment.size")
}

func (p *ParamTable) initSegmentSizeFactor() {
	p.SegmentSizeFactor = p.ParseFloat("master.segment.sizeFactor")
}

func (p *ParamTable) initDefaultRecordSize() {
	p.DefaultRecordSize = p.ParseInt64("master.segment.defaultSizePerRecord")
}

// TODO read from config/env
func (p *ParamTable) initSegIDAssignExpiration() {
	p.SegIDAssignExpiration = 3000 //ms
}

func (p *ParamTable) initInsertChannelPrefixName() {
	p.InsertChannelPrefixName = "insert-channel-"
}

func (p *ParamTable) initInsertChannelNumPerCollection() {
	p.InsertChannelNumPerCollection = 4
}

func (p *ParamTable) initStatisticsChannelName() {
	p.StatisticsChannelName = "dataservice-statistics-channel"
}

func (p *ParamTable) initTimeTickChannelName() {
	p.TimeTickChannelName = "dataservice-timetick-channel"
}

func (p *ParamTable) initDataNodeNum() {
	p.DataNodeNum = 2
}
