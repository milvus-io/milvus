package grpcproxyservice

import (
	"net"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (pt *ParamTable) Init() {
	pt.BaseTable.Init()
}

func (pt *ParamTable) NetworkPort() int {
	return pt.ParseInt("proxyService.port")
}

func (pt *ParamTable) NetworkAddress() string {
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
	return addr + ":" + port
}
