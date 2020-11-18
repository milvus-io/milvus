package reader

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
	err := p.LoadYaml("advanced/reader.yaml")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) PulsarAddress() (string, error) {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	return "pulsar://" + url, nil
}

func (p *ParamTable) QueryNodeID() int {
	queryNodeID, err := p.Load("reader.clientid")
	if err != nil {
		panic(err)
	}
	id, err := strconv.Atoi(queryNodeID)
	if err != nil {
		panic(err)
	}
	return id
}

func (p *ParamTable) TopicStart() int {
	topicStart, err := p.Load("reader.topicstart")
	if err != nil {
		panic(err)
	}
	topicStartNum, err := strconv.Atoi(topicStart)
	if err != nil {
		panic(err)
	}
	return topicStartNum
}

func (p *ParamTable) TopicEnd() int {
	topicEnd, err := p.Load("reader.topicend")
	if err != nil {
		panic(err)
	}
	topicEndNum, err := strconv.Atoi(topicEnd)
	if err != nil {
		panic(err)
	}
	return topicEndNum
}

// private advanced params
func (p *ParamTable) statsServiceTimeInterval() int {
	timeInterval, err := p.Load("service.statsServiceTimeInterval")
	if err != nil {
		panic(err)
	}
	interval, err := strconv.Atoi(timeInterval)
	if err != nil {
		panic(err)
	}
	return interval
}

func (p *ParamTable) statsMsgStreamReceiveBufSize() int64 {
	revBufSize, err := p.Load("msgStream.receiveBufSize.statsMsgStream")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) dmMsgStreamReceiveBufSize() int64 {
	revBufSize, err := p.Load("msgStream.receiveBufSize.dmMsgStream")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) searchMsgStreamReceiveBufSize() int64 {
	revBufSize, err := p.Load("msgStream.receiveBufSize.searchMsgStream")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) searchResultMsgStreamReceiveBufSize() int64 {
	revBufSize, err := p.Load("msgStream.receiveBufSize.searchResultMsgStream")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) searchPulsarBufSize() int64 {
	pulsarBufSize, err := p.Load("msgStream.pulsarBufSize.search")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(pulsarBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}

func (p *ParamTable) dmPulsarBufSize() int64 {
	pulsarBufSize, err := p.Load("msgStream.pulsarBufSize.dm")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(pulsarBufSize)
	if err != nil {
		panic(err)
	}
	return int64(bufSize)
}
