package grpcdatanode

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	IP   string
	Port int

	MasterAddress      string
	DataServiceAddress string
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initMasterAddress()
		pt.initDataServiceAddress()
		pt.initPort()
	})
}

func (pt *ParamTable) LoadFromArgs() {

}

func (pt *ParamTable) LoadFromEnv() {
	Params.IP = funcutil.GetLocalIP()
}

func (pt *ParamTable) initPort() {
	port := pt.ParseInt("dataNode.port")
	pt.Port = port
}

func (pt *ParamTable) initMasterAddress() {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	pt.MasterAddress = ret
}

func (pt *ParamTable) initDataServiceAddress() {
	ret, err := pt.Load("_DataServiceAddress")
	if err != nil {
		panic(err)
	}
	pt.DataServiceAddress = ret
}
