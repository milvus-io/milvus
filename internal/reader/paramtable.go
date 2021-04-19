package reader

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

func (p *ParamTable) TopicStart() int {
	topicStart, _ := p.Load("reader.topicstart")
	topicStartNum, err := strconv.Atoi(topicStart)
	if err != nil {
		panic(err)
	}
	return topicStartNum
}

func (p *ParamTable) TopicEnd() int {
	topicEnd, _ := p.Load("reader.topicend")
	topicEndNum, err := strconv.Atoi(topicEnd)
	if err != nil {
		panic(err)
	}
	return topicEndNum
}
