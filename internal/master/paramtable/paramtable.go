package paramtable

import (
	"log"
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
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

func (p *ParamTable) ProxyIDList() []typeutil.UniqueID {
	id, err := p.Load("master.proxyidlist")
	if err != nil {
		log.Panicf("load proxy id list error, %s", err.Error())
	}
	ids := strings.Split(id, ",")
	idlist := make([]typeutil.UniqueID, 0, len(ids))
	for _, i := range ids {
		v, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
		idlist = append(idlist, typeutil.UniqueID(v))
	}
	return idlist
}

func (p *ParamTable) ProxyTimeSyncChannels() []string {
	chs, err := p.Load("master.proxyTimeSyncChannels")
	if err != nil {
		log.Panic(err)
	}
	return strings.Split(chs, ",")
}

func (p *ParamTable) ProxyTimeSyncSubName() string {
	name, err := p.Load("master.proxyTimeSyncSubName")
	if err != nil {
		log.Panic(err)
	}
	return name
}

func (p *ParamTable) SoftTimeTickBarrierInterval() typeutil.Timestamp {
	t, err := p.Load("master.softTimeTickBarrierInterval")
	if err != nil {
		log.Panic(err)
	}
	v, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		log.Panic(err)
	}
	return tsoutil.ComposeTS(v, 0)
}

func (p *ParamTable) WriteIDList() []typeutil.UniqueID {
	id, err := p.Load("master.writeidlist")
	if err != nil {
		log.Panic(err)
	}
	ids := strings.Split(id, ",")
	idlist := make([]typeutil.UniqueID, 0, len(ids))
	for _, i := range ids {
		v, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			log.Panicf("load proxy id list error, %s", err.Error())
		}
		idlist = append(idlist, typeutil.UniqueID(v))
	}
	return idlist
}

func (p *ParamTable) WriteTimeSyncChannels() []string {
	chs, err := p.Load("master.writeTimeSyncChannels")
	if err != nil {
		log.Fatal(err)
	}
	return strings.Split(chs, ",")
}

func (p *ParamTable) WriteTimeSyncSubName() string {
	name, err := p.Load("master.writeTimeSyncSubName")
	if err != nil {
		log.Fatal(err)
	}
	return name
}

func (p *ParamTable) DMTimeSyncChannels() []string {
	chs, err := p.Load("master.dmTimeSyncChannels")
	if err != nil {
		log.Fatal(err)
	}
	return strings.Split(chs, ",")
}

func (p *ParamTable) K2STimeSyncChannels() []string {
	chs, err := p.Load("master.k2sTimeSyncChannels")
	if err != nil {
		log.Fatal(err)
	}
	return strings.Split(chs, ",")
}
