package dataservice

import (
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int
	NodeID  int64

	EtcdAddress   string
	MetaRootPath  string
	KvRootPath    string
	PulsarAddress string

	// segment
	SegmentSize           float64
	SegmentSizeFactor     float64
	DefaultRecordSize     int64
	SegIDAssignExpiration int64

	InsertChannelPrefixName     string
	InsertChannelNum            int64
	StatisticsChannelName       string
	TimeTickChannelName         string
	DataNodeNum                 int
	SegmentInfoChannelName      string
	DataServiceSubscriptionName string
	K2SChannelNames             []string

	SegmentFlushMetaPath string
}

var Params ParamTable
var once sync.Once

func (p *ParamTable) Init() {
	once.Do(func() {
		// load yaml
		p.BaseTable.Init()

		if err := p.LoadYaml("advanced/data_service.yaml"); err != nil {
			panic(err)
		}

		// set members
		p.initAddress()
		p.initPort()
		p.initNodeID()

		p.initEtcdAddress()
		p.initMetaRootPath()
		p.initKvRootPath()
		p.initPulsarAddress()

		p.initSegmentSize()
		p.initSegmentSizeFactor()
		p.initDefaultRecordSize()
		p.initSegIDAssignExpiration()
		p.initInsertChannelPrefixName()
		p.initInsertChannelNum()
		p.initStatisticsChannelName()
		p.initTimeTickChannelName()
		p.initDataNodeNum()
		p.initSegmentInfoChannelName()
		p.initDataServiceSubscriptionName()
		p.initK2SChannelNames()
		p.initSegmentFlushMetaPath()
	})
}

func (p *ParamTable) initAddress() {
	dataserviceAddress, err := p.Load("dataservice.address")
	if err != nil {
		panic(err)
	}
	p.Address = dataserviceAddress
}

func (p *ParamTable) initPort() {
	p.Port = p.ParseInt("dataservice.port")
}

func (p *ParamTable) initNodeID() {
	p.NodeID = p.ParseInt64("dataservice.nodeID")
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
	p.SegmentSize = p.ParseFloat("dataservice.segment.size")
}

func (p *ParamTable) initSegmentSizeFactor() {
	p.SegmentSizeFactor = p.ParseFloat("dataservice.segment.sizeFactor")
}

func (p *ParamTable) initDefaultRecordSize() {
	p.DefaultRecordSize = p.ParseInt64("dataservice.segment.defaultSizePerRecord")
}

func (p *ParamTable) initSegIDAssignExpiration() {
	p.SegIDAssignExpiration = p.ParseInt64("dataservice.segment.IDAssignExpiration") //ms
}

func (p *ParamTable) initInsertChannelPrefixName() {
	var err error
	p.InsertChannelPrefixName, err = p.Load("msgChannel.chanNamePrefix.dataServiceInsertChannel")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initInsertChannelNum() {
	p.InsertChannelNum = p.ParseInt64("dataservice.insertChannelNum")
}

func (p *ParamTable) initStatisticsChannelName() {
	var err error
	p.StatisticsChannelName, err = p.Load("msgChannel.chanNamePrefix.dataServiceStatistic")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initTimeTickChannelName() {
	var err error
	p.TimeTickChannelName, err = p.Load("msgChannel.chanNamePrefix.dataServiceTimeTick")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initDataNodeNum() {
	p.DataNodeNum = p.ParseInt("dataservice.dataNodeNum")
}

func (p *ParamTable) initSegmentInfoChannelName() {
	var err error
	p.SegmentInfoChannelName, err = p.Load("msgChannel.chanNamePrefix.dataServiceSegmentInfo")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initDataServiceSubscriptionName() {
	var err error
	p.DataServiceSubscriptionName, err = p.Load("msgChannel.subNamePrefix.dataServiceSubNamePrefix")
	if err != nil {
		panic(err)
	}
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

func (p *ParamTable) initSegmentFlushMetaPath() {
	subPath, err := p.Load("etcd.segFlushMetaSubPath")
	if err != nil {
		panic(err)
	}
	p.SegmentFlushMetaPath = subPath
}
