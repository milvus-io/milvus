package proxyservice

import (
	"log"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (pt *ParamTable) Init() {
	pt.BaseTable.Init()
}

func (pt *ParamTable) PulsarAddress() string {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return ret
}

func (pt *ParamTable) MasterAddress() string {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	return ret
}

func (pt *ParamTable) NodeTimeTickChannel() []string {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		log.Panic(err)
	}
	prefix += "-0"
	return []string{prefix}
}

func (pt *ParamTable) ServiceTimeTickChannel() string {
	ch, err := pt.Load("msgChannel.chanNamePrefix.proxyServiceTimeTick")
	if err != nil {
		log.Panic(err)
	}
	return ch
}

func (pt *ParamTable) DataServiceAddress() string {
	// NOT USED NOW
	return "TODO: read from config"
}
