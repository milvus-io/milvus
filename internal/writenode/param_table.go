package writenode

import (
	"log"
	"os"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	PulsarAddress string

	WriteNodeID                  UniqueID
	WriteNodeNum                 int
	WriteNodeTimeTickChannelName string

	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32

	// dm
	InsertChannelNames   []string
	InsertChannelRange   []int
	InsertReceiveBufSize int64
	InsertPulsarBufSize  int64

	// dd
	DDChannelNames   []string
	DDReceiveBufSize int64
	DDPulsarBufSize  int64

	MsgChannelSubName   string
	DefaultPartitionTag string
	SliceIndex          int
}

var Params ParamTable

func (p *ParamTable) Init() {
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/write_node.yaml")
	if err != nil {
		panic(err)
	}

	writeNodeIDStr := os.Getenv("WRITE_NODE_ID")
	if writeNodeIDStr == "" {
		writeNodeIDList := p.WriteNodeIDList()
		if len(writeNodeIDList) <= 0 {
			writeNodeIDStr = "0"
		} else {
			writeNodeIDStr = strconv.Itoa(int(writeNodeIDList[0]))
		}
	}
	err = p.Save("_writeNodeID", writeNodeIDStr)
	if err != nil {
		panic(err)
	}

	p.initPulsarAddress()

	p.initWriteNodeID()
	p.initWriteNodeNum()
	p.initWriteNodeTimeTickChannelName()

	p.initMsgChannelSubName()
	p.initDefaultPartitionTag()
	p.initSliceIndex()

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()

	p.initInsertChannelNames()
	p.initInsertChannelRange()
	p.initInsertReceiveBufSize()
	p.initInsertPulsarBufSize()

	p.initDDChannelNames()
	p.initDDReceiveBufSize()
	p.initDDPulsarBufSize()
}

func (p *ParamTable) initWriteNodeID() {
	writeNodeID, err := p.Load("_writeNodeID")
	if err != nil {
		panic(err)
	}
	id, err := strconv.Atoi(writeNodeID)
	if err != nil {
		panic(err)
	}
	p.WriteNodeID = UniqueID(id)
}

func (p *ParamTable) initPulsarAddress() {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *ParamTable) initInsertChannelRange() {
	insertChannelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	p.InsertChannelRange = paramtable.ConvertRangeToIntRange(insertChannelRange, ",")
}

// advanced params
// dataSync:
func (p *ParamTable) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.ParseInt32("writeNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.ParseInt32("writeNode.dataSync.flowGraph.maxParallelism")
}

// msgStream
func (p *ParamTable) initInsertReceiveBufSize() {
	p.InsertReceiveBufSize = p.ParseInt64("writeNode.msgStream.insert.recvBufSize")
}

func (p *ParamTable) initInsertPulsarBufSize() {
	p.InsertPulsarBufSize = p.ParseInt64("writeNode.msgStream.insert.pulsarBufSize")
}

func (p *ParamTable) initDDReceiveBufSize() {
	revBufSize, err := p.Load("writeNode.msgStream.dataDefinition.recvBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(revBufSize)
	if err != nil {
		panic(err)
	}
	p.DDReceiveBufSize = int64(bufSize)
}

func (p *ParamTable) initDDPulsarBufSize() {
	pulsarBufSize, err := p.Load("writeNode.msgStream.dataDefinition.pulsarBufSize")
	if err != nil {
		panic(err)
	}
	bufSize, err := strconv.Atoi(pulsarBufSize)
	if err != nil {
		panic(err)
	}
	p.DDPulsarBufSize = int64(bufSize)
}

func (p *ParamTable) initInsertChannelNames() {

	prefix, err := p.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		log.Fatal(err)
	}
	prefix += "-"
	channelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(channelRange, ",")

	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	sep := len(channelIDs) / p.WriteNodeNum
	index := p.SliceIndex
	if index == -1 {
		panic("writeNodeID not Match with Config")
	}
	start := index * sep
	p.InsertChannelNames = ret[start : start+sep]
}

func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.writeNodeSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	writeNodeIDStr, err := p.Load("_writeNodeID")
	if err != nil {
		panic(err)
	}
	p.MsgChannelSubName = name + "-" + writeNodeIDStr
}

func (p *ParamTable) initDDChannelNames() {
	prefix, err := p.Load("msgChannel.chanNamePrefix.dataDefinition")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	iRangeStr, err := p.Load("msgChannel.channelRange.dataDefinition")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(iRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	p.DDChannelNames = ret
}

func (p *ParamTable) initDefaultPartitionTag() {
	defaultTag, err := p.Load("common.defaultPartitionTag")
	if err != nil {
		panic(err)
	}

	p.DefaultPartitionTag = defaultTag
}

func (p *ParamTable) initWriteNodeTimeTickChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.writeNodeTimeTick")
	if err != nil {
		panic(err)
	}
	p.WriteNodeTimeTickChannelName = channels
}

func (p *ParamTable) initSliceIndex() {
	writeNodeID := p.WriteNodeID
	writeNodeIDList := p.WriteNodeIDList()
	for i := 0; i < len(writeNodeIDList); i++ {
		if writeNodeID == writeNodeIDList[i] {
			p.SliceIndex = i
			return
		}
	}
	p.SliceIndex = -1
}

func (p *ParamTable) initWriteNodeNum() {
	p.WriteNodeNum = len(p.WriteNodeIDList())
}
