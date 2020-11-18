package paramtable

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (p *ParamTable) InitParamTable() {
	p.Init()
}

func (p *ParamTable) Address() string {
	masterAddress, _ := p.Load("master.address")
	return masterAddress
}

func (p *ParamTable) Port() int {
	masterPort, _ := p.Load("master.port")
	port, err := strconv.Atoi(masterPort)
	if err != nil {
		panic(err)
	}
	return port
}

func (p *ParamTable) PulsarToic() string {
	pulsarTopic, _ := p.Load("master.pulsartopic")
	return pulsarTopic
}

func (p *ParamTable) SegmentThreshold() float64 {
	threshole, _ := p.Load("master.segmentthreshold")
	segmentThreshole, err := strconv.ParseFloat(threshole, 32)
	if err != nil {
		panic(err)
	}
	return segmentThreshole
}
