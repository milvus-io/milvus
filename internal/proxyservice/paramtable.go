package proxyservice

import (
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	PulsarAddress           string
	MasterAddress           string
	NodeTimeTickChannel     []string
	ServiceTimeTickChannel  string
	DataServiceAddress      string
	InsertChannelPrefixName string
	InsertChannelNum        int64
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()

		if err := pt.LoadYaml("advanced/data_service.yaml"); err != nil {
			panic(err)
		}

		pt.initPulsarAddress()
		pt.initMasterAddress()
		pt.initNodeTimeTickChannel()
		pt.initServiceTimeTickChannel()
		pt.initDataServiceAddress()
		pt.initInsertChannelPrefixName()
		pt.initInsertChannelNum()
	})
}

func (pt *ParamTable) initPulsarAddress() {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	pt.PulsarAddress = ret
}

func (pt *ParamTable) initMasterAddress() {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	pt.MasterAddress = ret
}

func (pt *ParamTable) initNodeTimeTickChannel() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		log.Panic(err)
	}
	prefix += "-0"
	pt.NodeTimeTickChannel = []string{prefix}
}

func (pt *ParamTable) initServiceTimeTickChannel() {
	ch, err := pt.Load("msgChannel.chanNamePrefix.proxyServiceTimeTick")
	if err != nil {
		log.Panic(err)
	}
	pt.ServiceTimeTickChannel = ch
}

func (pt *ParamTable) initDataServiceAddress() {
	// NOT USED NOW
	pt.DataServiceAddress = "TODO: read from config"
}

func (pt *ParamTable) initInsertChannelNum() {
	pt.InsertChannelNum = pt.ParseInt64("dataservice.insertChannelNum")
}

func (pt *ParamTable) initInsertChannelPrefixName() {
	var err error
	pt.InsertChannelPrefixName, err = pt.Load("msgChannel.chanNamePrefix.dataServiceInsertChannel")
	if err != nil {
		panic(err)
	}
}
