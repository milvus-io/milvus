package dataservice

import (
	"os"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	MasterAddress string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initParams()
		pt.LoadFromEnv()
	})
}

func (pt *ParamTable) initParams() {
	pt.initMasterAddress()
}

func (pt *ParamTable) LoadFromEnv() {
	masterAddress := os.Getenv("MASTER_ADDRESS")
	if masterAddress != "" {
		pt.MasterAddress = masterAddress
	}
}

func (pt *ParamTable) initMasterAddress() {
	masterHost, err := pt.Load("master.address")
	if err != nil {
		panic(err)
	}
	port, err := pt.Load("master.port")
	if err != nil {
		panic(err)
	}
	pt.MasterAddress = masterHost + ":" + port
}
