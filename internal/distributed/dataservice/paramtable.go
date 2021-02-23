package grpcdataserviceclient

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	Port          int
	MasterAddress string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initPort()
		pt.initParams()
		pt.LoadFromEnv()
	})
}

func (pt *ParamTable) initParams() {
	pt.initMasterAddress()
}

func (pt *ParamTable) LoadFromEnv() {

}

func (pt *ParamTable) initPort() {
	pt.Port = pt.ParseInt("dataservice.port")
}

func (pt *ParamTable) initMasterAddress() {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	pt.MasterAddress = ret
}
