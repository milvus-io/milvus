package grpcindexnode

import (
	"net"
	"os"
	"strconv"
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
	indexServiceAddress := os.Getenv("INDEX_SERVICE_ADDRESS")
	if indexServiceAddress != "" {
		pt.IndexServerAddress = indexServiceAddress
	}

	Params.IP = funcutil.GetLocalIP()
	host := os.Getenv("INDEX_NODE_HOST")
	if len(host) > 0 {
		Params.IP = host
	}
}

func (pt *ParamTable) initParams() {
	pt.initPort()
	pt.initIndexServerAddress()
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

func (pt *ParamTable) initPort() {
	port := pt.ParseInt("indexNode.port")
	pt.Port = port
}
