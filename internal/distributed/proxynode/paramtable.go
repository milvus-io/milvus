package grpcproxynode

import (
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	ProxyServiceAddress string
	ProxyServicePort    int

	IndexServerAddress string
	MasterAddress      string

	DataServiceAddress  string
	QueryServiceAddress string

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

// todo
func (pt *ParamTable) LoadFromArgs() {

}

//todo
func (pt *ParamTable) LoadFromEnv() {

	masterAddress := os.Getenv("MASTER_ADDRESS")
	if masterAddress != "" {
		pt.MasterAddress = masterAddress
	}

	proxyServiceAddress := os.Getenv("PROXY_SERVICE_ADDRESS")
	if proxyServiceAddress != "" {
		pt.ProxyServiceAddress = proxyServiceAddress
	}

	indexServiceAddress := os.Getenv("INDEX_SERVICE_ADDRESS")
	if indexServiceAddress != "" {
		pt.IndexServerAddress = indexServiceAddress
	}

	queryServiceAddress := os.Getenv("QUERY_SERVICE_ADDRESS")
	if queryServiceAddress != "" {
		pt.QueryServiceAddress = queryServiceAddress
	}

	dataServiceAddress := os.Getenv("DATA_SERVICE_ADDRESS")
	if dataServiceAddress != "" {
		pt.DataServiceAddress = dataServiceAddress
	}

}

func (pt *ParamTable) initParams() {
	pt.initPoxyServicePort()

	pt.initProxyServiceAddress()
	pt.initMasterAddress()
	pt.initIndexServerAddress()
	pt.initDataServiceAddress()
	pt.initQueryServiceAddress()
}

func (pt *ParamTable) initPoxyServicePort() {
	pt.ProxyServicePort = pt.ParseInt("proxyService.port")
}

func (pt *ParamTable) initProxyServiceAddress() {
	addr, err := pt.Load("proxyService.address")
	if err != nil {
		panic(err)
	}

	hostName, _ := net.LookupHost(addr)
	if len(hostName) <= 0 {
		if ip := net.ParseIP(addr); ip == nil {
			panic("invalid ip proxyService.address")
		}
	}

	port, err := pt.Load("proxyService.port")
	if err != nil {
		panic(err)
	}
	_, err = strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	pt.ProxyServiceAddress = addr + ":" + port
}

// todo remove and use load from env
func (pt *ParamTable) initIndexServerAddress() {
	addr, err := pt.Load("indexServer.address")
	if err != nil {
		panic(err)
	}

	hostName, _ := net.LookupHost(addr)
	if len(hostName) <= 0 {
		if ip := net.ParseIP(addr); ip == nil {
			panic("invalid ip indexServer.address")
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

	pt.IndexServerAddress = addr + ":" + port
}

// todo remove and use load from env
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

// todo remove and use load from env
func (pt *ParamTable) initDataServiceAddress() {
	addr, err := pt.Load("dataService.address")
	if err != nil {
		panic(err)
	}

	port, err := pt.Load("dataService.port")
	if err != nil {
		panic(err)
	}
	pt.DataServiceAddress = addr + ":" + port
}

// todo remove and use load from env
func (pt *ParamTable) initQueryServiceAddress() {
	addr, err := pt.Load("queryService.address")
	if err != nil {
		panic(err)
	}

	port, err := pt.Load("queryService.port")
	if err != nil {
		panic(err)
	}
	pt.QueryServiceAddress = addr + ":" + port
}
