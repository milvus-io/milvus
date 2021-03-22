package grpcindexnode

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	IndexServerAddress string

	IP      string
	Port    int
	Address string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initParams()
	})
}

func (pt *ParamTable) LoadFromArgs() {

}

func (pt *ParamTable) LoadFromEnv() {
	Params.IP = funcutil.GetLocalIP()
}

func (pt *ParamTable) initParams() {
	pt.initPort()
	pt.initIndexServerAddress()
}

// todo remove and use load from env
func (pt *ParamTable) initIndexServerAddress() {
	ret, err := pt.Load("IndexServiceAddress")
	if err != nil {
		panic(err)
	}
	pt.IndexServerAddress = ret
}

func (pt *ParamTable) initPort() {
	port := pt.ParseInt("indexNode.port")
	pt.Port = port
}
