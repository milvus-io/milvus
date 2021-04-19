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
	threshold, _ := p.Load("master.segmentThreshold")
	segmentThreshold, err := strconv.ParseFloat(threshold, 32)
	if err != nil {
		panic(err)
	}
	return segmentThreshold
}

func (p *ParamTable) DefaultRecordSize() int64 {
	size, _ := p.Load("master.defaultSizePerRecord")
	res, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func (p *ParamTable) MinimumAssignSize() int64 {
	size, _ := p.Load("master.minimumAssignSize")
	res, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func (p *ParamTable) SegmentExpireDuration() int64 {
	duration, _ := p.Load("master.segmentExpireDuration")
	res, err := strconv.ParseInt(duration, 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func (p *ParamTable) QueryNodeNum() (int, error) {
	num, _ := p.Load("master.querynodenum")
	return strconv.Atoi(num)
}

func (p *ParamTable) StatsChannels() string {
	channels, _ := p.Load("master.statsChannels")
	return channels
}
