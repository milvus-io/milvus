package grpcindexservice

import (
	"net"
	"strconv"
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
	pt.ServicePort = pt.ParseInt("indexServer.port")
}

func (pt *ParamTable) initServiceAddress() {
	addr, err := pt.Load("indexServer.address")
	if err != nil {
		panic(err)
	}

	hostName, _ := net.LookupHost(addr)
	if len(hostName) <= 0 {
		if ip := net.ParseIP(addr); ip == nil {
			panic("invalid ip proxyService.address")
		}
	}

	port, err := pt.Load("indexServer.port")
	if err != nil {
		panic(err)
	}
	_, err = strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	pt.ServiceAddress = addr + ":" + port
}
