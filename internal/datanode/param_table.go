package datanode

import (
	"log"
	"os"
	"path"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	// === PRIVATE Configs ===
	dataNodeIDList []UniqueID

	paramtable.BaseTable

	// === DataNode Internal Components Configs ===
	NodeID                  UniqueID
	IP                      string // GOOSE TODO load from config file
	Port                    int64
	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32
	FlushInsertBufferSize   int32
	FlushDdBufferSize       int32
	InsertBinlogRootPath    string
	DdBinlogRootPath        string

	// === DataNode External Components Configs ===
	// --- External Client Address ---
	MasterAddress  string
	ServiceAddress string // GOOSE TODO: init from config file

	// --- Pulsar ---
	PulsarAddress string

	// - insert channel -
	InsertChannelNames   []string
	InsertChannelRange   []int
	InsertReceiveBufSize int64
	InsertPulsarBufSize  int64

	// - dd channel -
	DDChannelNames []string // GOOSE TODO, set after Init
	// DDReceiveBufSize int64
	// DDPulsarBufSize  int64

	// - seg statistics channel -
	SegmentStatisticsChannelName string // GOOSE TODO, set after init
	// SegmentStatisticsBufSize        int64
	// SegmentStatisticsUpdateInterval int //  GOOSE TODO remove

	// - timetick channel -
	TimeTickChannelName string // GOOSE TODO: set after init

	// - complete flush channel -
	CompleteFlushChannelName string // GOOSE TODO: set after init

	// - channel subname -
	MsgChannelSubName string

	DefaultPartitionName string

	// --- ETCD ---
	EtcdAddress         string
	MetaRootPath        string
	SegFlushMetaSubPath string
	DDLFlushMetaSubPath string

	// --- MinIO ---
	MinioAddress         string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	MinioBucketName      string
}

var Params ParamTable

func (p *ParamTable) Init() {
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/data_node.yaml")
	if err != nil {
		panic(err)
	}

	// === DataNode Internal Components Configs ===
	p.initNodeID()
	p.initIP()
	p.initPort()
	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()
	p.initFlushInsertBufferSize()
	p.initFlushDdBufferSize()
	p.initInsertBinlogRootPath()
	p.initDdBinlogRootPath()

	// === DataNode External Components Configs ===
	// --- Master ---
	p.initMasterAddress()

	// --- Pulsar ---
	p.initPulsarAddress()

	// - insert channel -
	p.initInsertChannelNames()
	p.initInsertChannelRange()
	p.initInsertReceiveBufSize()
	p.initInsertPulsarBufSize()

	// - dd channel -
	// p.initDDChannelNames()
	// p.initDDReceiveBufSize()
	// p.initDDPulsarBufSize()

	// - seg statistics channel -
	// p.initSegmentStatisticsChannelName()
	// p.initSegmentStatisticsBufSize()
	// p.initSegmentStatisticsUpdateInterval()

	// - timetick channel -
	// p.initTimeTickChannelName()

	// - flush completed channel -
	// p.initCompleteFlushChannelName()

	// - channel subname -
	p.initMsgChannelSubName()

	// --- ETCD ---
	p.initEtcdAddress()
	p.initMetaRootPath()
	p.initSegFlushMetaSubPath()
	p.initDDLFlushMetaSubPath()

	// --- MinIO ---
	p.initMinioAddress()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSL()
	p.initMinioBucketName()

	p.initDefaultPartitionName()
	// p.initSliceIndex()

}

// ==== DataNode internal components configs ====
func (p *ParamTable) initNodeID() {
	p.dataNodeIDList = p.DataNodeIDList()
	dataNodeIDStr := os.Getenv("DATA_NODE_ID")
	if dataNodeIDStr == "" {
		if len(p.dataNodeIDList) <= 0 {
			dataNodeIDStr = "0"
		} else {
			dataNodeIDStr = strconv.Itoa(int(p.dataNodeIDList[0]))
		}
	}
	err := p.Save("_dataNodeID", dataNodeIDStr)
	if err != nil {
		panic(err)
	}

	p.NodeID = p.ParseInt64("_dataNodeID")
}

func (p *ParamTable) initIP() {
	addr, err := p.Load("dataNode.address")
	if err != nil {
		panic(err)
	}
	p.IP = addr
}

func (p *ParamTable) initPort() {
	port := p.ParseInt64("dataNode.port")
	p.Port = port
}

// ---- flowgraph configs ----
func (p *ParamTable) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.ParseInt32("dataNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.ParseInt32("dataNode.dataSync.flowGraph.maxParallelism")
}

// ---- flush configs ----
func (p *ParamTable) initFlushInsertBufferSize() {
	p.FlushInsertBufferSize = p.ParseInt32("datanode.flush.insertBufSize")
}

func (p *ParamTable) initFlushDdBufferSize() {
	p.FlushDdBufferSize = p.ParseInt32("datanode.flush.ddBufSize")
}

func (p *ParamTable) initInsertBinlogRootPath() {
	// GOOSE TODO: rootPath change to  TenentID
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	p.InsertBinlogRootPath = path.Join(rootPath, "insert_log")
}

func (p *ParamTable) initDdBinlogRootPath() {
	// GOOSE TODO: rootPath change to  TenentID
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	p.DdBinlogRootPath = path.Join(rootPath, "data_definition_log")
}

// ===== DataNode External components configs ====
// ---- Master ----
func (p *ParamTable) initMasterAddress() {
	addr, err := p.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	p.MasterAddress = addr
}

// ---- Pulsar ----
func (p *ParamTable) initPulsarAddress() {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

// - insert channel -
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
	sep := len(channelIDs) / len(p.dataNodeIDList)
	index := p.sliceIndex()
	if index == -1 {
		panic("dataNodeID not Match with Config")
	}
	start := index * sep
	p.InsertChannelNames = ret[start : start+sep]
}

func (p *ParamTable) initInsertChannelRange() {
	insertChannelRange, err := p.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	p.InsertChannelRange = paramtable.ConvertRangeToIntRange(insertChannelRange, ",")
}

func (p *ParamTable) initInsertReceiveBufSize() {
	p.InsertReceiveBufSize = p.ParseInt64("dataNode.msgStream.insert.recvBufSize")
}

func (p *ParamTable) initInsertPulsarBufSize() {
	p.InsertPulsarBufSize = p.ParseInt64("dataNode.msgStream.insert.pulsarBufSize")
}

// - dd channel - GOOSE TODO: remove
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

// func (p *ParamTable) initDDReceiveBufSize() {
//     revBufSize, err := p.Load("dataNode.msgStream.dataDefinition.recvBufSize")
//     if err != nil {
//         panic(err)
//     }
//     bufSize, err := strconv.Atoi(revBufSize)
//     if err != nil {
//         panic(err)
//     }
//     p.DDReceiveBufSize = int64(bufSize)
// }

// func (p *ParamTable) initDDPulsarBufSize() {
//     pulsarBufSize, err := p.Load("dataNode.msgStream.dataDefinition.pulsarBufSize")
//     if err != nil {
//         panic(err)
//     }
//     bufSize, err := strconv.Atoi(pulsarBufSize)
//     if err != nil {
//         panic(err)
//     }
//     p.DDPulsarBufSize = int64(bufSize)
// }

// - seg statistics channel - GOOSE TODO: remove
func (p *ParamTable) initSegmentStatisticsChannelName() {

	channelName, err := p.Load("msgChannel.chanNamePrefix.dataNodeSegStatistics")
	if err != nil {
		panic(err)
	}

	p.SegmentStatisticsChannelName = channelName
}

// func (p *ParamTable) initSegmentStatisticsBufSize() {
//     p.SegmentStatisticsBufSize = p.ParseInt64("dataNode.msgStream.segStatistics.recvBufSize")
// }
//
// func (p *ParamTable) initSegmentStatisticsUpdateInterval() {
//     p.SegmentStatisticsUpdateInterval = p.ParseInt("dataNode.msgStream.segStatistics.updateInterval")
// }

// - flush completed channel - GOOSE TODO: remove
func (p *ParamTable) initCompleteFlushChannelName() {
	p.CompleteFlushChannelName = "flush-completed"
}

// - Timetick channel - GOOSE TODO: remove
func (p *ParamTable) initTimeTickChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.dataNodeTimeTick")
	if err != nil {
		panic(err)
	}
	p.TimeTickChannelName = channels + "-" + strconv.FormatInt(p.NodeID, 10)
}

// - msg channel subname -
func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	p.MsgChannelSubName = name + "-" + strconv.FormatInt(p.NodeID, 10)
}

func (p *ParamTable) initDefaultPartitionName() {
	defaultTag, err := p.Load("common.defaultPartitionTag")
	if err != nil {
		panic(err)
	}

	p.DefaultPartitionName = defaultTag
}

// --- ETCD ---
func (p *ParamTable) initEtcdAddress() {
	addr, err := p.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	p.EtcdAddress = addr
}

func (p *ParamTable) initMetaRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *ParamTable) initSegFlushMetaSubPath() {
	subPath, err := p.Load("etcd.segFlushMetaSubPath")
	if err != nil {
		panic(err)
	}
	p.SegFlushMetaSubPath = subPath
}

func (p *ParamTable) initDDLFlushMetaSubPath() {
	subPath, err := p.Load("etcd.ddlFlushMetaSubPath")
	if err != nil {
		panic(err)
	}
	p.DDLFlushMetaSubPath = subPath
}

func (p *ParamTable) initMinioAddress() {
	endpoint, err := p.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioAddress = endpoint
}

func (p *ParamTable) initMinioAccessKeyID() {
	keyID, err := p.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = keyID
}

func (p *ParamTable) initMinioSecretAccessKey() {
	key, err := p.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *ParamTable) initMinioUseSSL() {
	usessl, err := p.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	p.MinioUseSSL, _ = strconv.ParseBool(usessl)
}

func (p *ParamTable) initMinioBucketName() {
	bucketName, err := p.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *ParamTable) sliceIndex() int {
	dataNodeID := p.NodeID
	dataNodeIDList := p.dataNodeIDList
	for i := 0; i < len(dataNodeIDList); i++ {
		if dataNodeID == dataNodeIDList[i] {
			return i
		}
	}
	return -1
}
