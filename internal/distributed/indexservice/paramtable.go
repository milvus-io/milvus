package grpcindexservice

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	ServiceAddress string
	ServicePort    int
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initParams()
	})
}

func (pt *ParamTable) initParams() {
	pt.initServicePort()
	pt.initServiceAddress()
}

func (pt *ParamTable) initServicePort() {
	pt.ServicePort = pt.ParseInt("indexService.port")
}

func (pt *ParamTable) initServiceAddress() {
	ret, err := pt.Load("IndexServiceAddress")
	if err != nil {
		panic(err)
	}
	pt.ServiceAddress = ret
}
