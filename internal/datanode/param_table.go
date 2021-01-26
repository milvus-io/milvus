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
	InsertChannelNames []string

	// - dd channel -
	DDChannelNames []string

	// - seg statistics channel -
	SegmentStatisticsChannelName string

	// - timetick channel -
	TimeTickChannelName string

	// - complete flush channel -
	CompleteFlushChannelName string

	// - channel subname -
	MsgChannelSubName string

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
	p.initServiceAddress()

	// --- Pulsar ---
	p.initPulsarAddress()

	// - insert channel -
	p.initInsertChannelNames()

	// - dd channel -
	p.initDDChannelNames()

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

func (p *ParamTable) initServiceAddress() {
	addr, err := p.Load("dataService.address")
	if err != nil {
		panic(err)
	}

	port, err := p.Load("dataService.port")
	if err != nil {
		panic(err)
	}
	p.ServiceAddress = addr + ":" + port
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
	p.InsertChannelNames = make([]string, 0)
}

func (p *ParamTable) initDDChannelNames() {
	p.DDChannelNames = make([]string, 0)
}

// - msg channel subname -
func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
	if err != nil {
		log.Panic(err)
	}
	p.MsgChannelSubName = name + "-" + strconv.FormatInt(p.NodeID, 10)
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
