package proxy

import (
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (p *ParamTable) InitParamTable() {
	p.Init()
}

func (p *ParamTable) MasterAddress() string {
	masterAddress, err := p.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	return masterAddress
}

func (p *ParamTable) PulsarAddress() string {
	pulsarAddress, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return pulsarAddress
}
