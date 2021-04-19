package queryservice

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type ParamTable struct {
	paramtable.BaseTable

	Address        string
	Port           int
	QueryServiceID UniqueID

	// stats
	StatsChannelName string

	// timetick
	TimeTickChannelName string
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

		err = p.LoadYaml("milvus.yaml")
		if err != nil {
			panic(err)
		}

		p.initStatsChannelName()
		p.initTimeTickChannelName()
		p.initQueryServiceAddress()
		p.initPort()
	})
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

func (p *ParamTable) initPort() {
	p.Port = p.ParseInt("queryService.port")
}
