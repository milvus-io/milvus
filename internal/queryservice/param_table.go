package queryservice

import (
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type ParamTable struct {
	paramtable.BaseTable

	NodeID uint64

	Address        string
	QueryServiceID UniqueID

	// stats
	StatsChannelName string

	// timetick
	TimeTickChannelName string

	Log log.Config
}

var Params ParamTable
var once sync.Once

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/query_node.yaml")
		if err != nil {
			panic(err)
		}

		err = p.LoadYaml("advanced/query_service.yaml")
		if err != nil {
			panic(err)
		}

		err = p.LoadYaml("milvus.yaml")
		if err != nil {
			panic(err)
		}

		p.initNodeID()
		p.initLogCfg()

		p.initStatsChannelName()
		p.initTimeTickChannelName()
		p.initQueryServiceAddress()
	})
}

func (p *ParamTable) initNodeID() {
	p.NodeID = uint64(p.ParseInt64("queryService.nodeID"))
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
	if len(rootPath) != 0 {
		p.Log.File.Filename = path.Join(rootPath, fmt.Sprintf("queryService-%d.log", p.NodeID))
	} else {
		p.Log.File.Filename = ""
	}
}

func (p *ParamTable) initStatsChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	p.StatsChannelName = channels
}

func (p *ParamTable) initTimeTickChannelName() {
	timeTickChannelName, err := p.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		panic(err)
	}
	p.TimeTickChannelName = timeTickChannelName

}

func (p *ParamTable) initQueryServiceAddress() {
	url, err := p.Load("_QueryServiceAddress")
	if err != nil {
		panic(err)
	}
	p.Address = url
}
