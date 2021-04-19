package flowgraph

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable
}

var Params ParamTable

func (p *ParamTable) Init() {
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/flow_graph.yaml")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) FlowGraphMaxQueueLength() int32 {
	queueLength, err := p.Load("flowGraph.maxQueueLength")
	if err != nil {
		panic(err)
	}
	length, err := strconv.Atoi(queueLength)
	if err != nil {
		panic(err)
	}
	return int32(length)
}

func (p *ParamTable) FlowGraphMaxParallelism() int32 {
	maxParallelism, err := p.Load("flowGraph.maxParallelism")
	if err != nil {
		panic(err)
	}
	maxPara, err := strconv.Atoi(maxParallelism)
	if err != nil {
		panic(err)
	}
	return int32(maxPara)
}
