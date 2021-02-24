package masterservice

import (
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	NodeID uint64

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

	Log log.Config
}

func (p *ParamTable) Init() {
	once.Do(func() {
		// load yaml
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/master.yaml")
		if err != nil {
			panic(err)
		}

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

		p.initLogCfg()
	})
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

func (p *ParamTable) initLogCfg() {
	p.Log = log.Config{}
	format, err := p.Load("log.format")
	if err != nil {
		panic(err)
	}
	p.Log.Format = format
	level, err := p.Load("log.level")
	if err != nil {
		panic(err)
	}
	p.Log.Level = level
	devStr, err := p.Load("log.dev")
	if err != nil {
		panic(err)
	}
	dev, err := strconv.ParseBool(devStr)
	if err != nil {
		panic(err)
	}
	p.Log.Development = dev
	p.Log.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.Log.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.Log.File.MaxDays = p.ParseInt("log.file.maxAge")
	rootPath, err := p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	p.Log.File.Filename = path.Join(rootPath, fmt.Sprintf("masterservice-%d.log", p.NodeID))
}
