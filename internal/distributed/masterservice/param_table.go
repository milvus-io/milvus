package grpcmasterservice

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	Address string // ip:port
	Port    int

	ProxyServiceAddress string
	IndexServiceAddress string
	QueryServiceAddress string
	DataServiceAddress  string
}

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/master.yaml")
		if err != nil {
			panic(err)
		}
		p.initAddress()
		p.initPort()
		p.initProxyServiceAddress()
		p.initIndexServiceAddress()
		p.initQueryServiceAddress()
		p.initDataServiceAddress()

	})
}

func (p *ParamTable) initAddress() {
	ret, err := p.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	p.Address = ret
}

func (p *ParamTable) initPort() {
	p.Port = p.ParseInt("master.port")
}

func (p *ParamTable) initProxyServiceAddress() {
	ret, err := p.Load("_PROXY_SERVICE_ADDRESS")
	if err != nil {
		panic(err)
	}
	p.ProxyServiceAddress = ret
}

func (p *ParamTable) initIndexServiceAddress() {
	ret, err := p.Load("IndexServiceAddress")
	if err != nil {
		panic(err)
	}
	p.IndexServiceAddress = ret
}

func (p *ParamTable) initQueryServiceAddress() {
	ret, err := p.Load("_QueryServiceAddress")
	if err != nil {
		panic(err)
	}
	p.QueryServiceAddress = ret
}

func (p *ParamTable) initDataServiceAddress() {
	ret, err := p.Load("_DataServiceAddress")
	if err != nil {
		panic(err)
	}
	p.DataServiceAddress = ret
}
