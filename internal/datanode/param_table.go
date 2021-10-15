// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// ParamTable in DataNode contains all configs for DataNode
type ParamTable struct {
	paramtable.BaseTable

	// ID of the current DataNode
	NodeID UniqueID

	// IP of the current DataNode
	IP string

	// Port of the current DataNode
	Port                    int
	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32
	FlushInsertBufferSize   int64
	InsertBinlogRootPath    string
	StatsBinlogRootPath     string
	DeleteBinlogRootPath    string
	Alias                   string // Different datanode in one machine

	// Pulsar address
	PulsarAddress string

	// Rocksmq path
	RocksmqPath string

	// Cluster channels
	ClusterChannelPrefix string

	// Segment statistics channel
	SegmentStatisticsChannelName string

	// Timetick channel
	TimeTickChannelName string

	// Channel subscribition name -
	MsgChannelSubName string

	// --- ETCD ---
	EtcdEndpoints       []string
	MetaRootPath        string
	ChannelWatchSubPath string

	// MinIO

	MinioAddress         string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	MinioBucketName      string

	CreatedTime time.Time
	UpdatedTime time.Time
}

// Params is global var in DataNode
var Params ParamTable
var once sync.Once

// InitAlias init this DataNode alias
func (p *ParamTable) InitAlias(alias string) {
	p.Alias = alias
}

// InitOnce call params Init only once
func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}

// Init initializes DataNode configs
func (p *ParamTable) Init() {
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/data_node.yaml")
	if err != nil {
		panic(err)
	}

	p.initFlowGraphMaxQueueLength()
	p.initFlowGraphMaxParallelism()
	p.initFlushInsertBufferSize()
	p.initInsertBinlogRootPath()
	p.initStatsBinlogRootPath()
	p.initDeleteBinlogRootPath()

	p.initPulsarAddress()
	p.initRocksmqPath()

	// Must init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initSegmentStatisticsChannelName()
	p.initTimeTickChannelName()

	p.initEtcdEndpoints()
	p.initMetaRootPath()
	p.initChannelWatchPath()

	p.initMinioAddress()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSL()
	p.initMinioBucketName()

	p.initRoleName()
}

func (p *ParamTable) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.ParseInt32("dataNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.ParseInt32("dataNode.dataSync.flowGraph.maxParallelism")
}

func (p *ParamTable) initFlushInsertBufferSize() {
	p.FlushInsertBufferSize = p.ParseInt64("_DATANODE_INSERTBUFSIZE")
}

func (p *ParamTable) initInsertBinlogRootPath() {
	// GOOSE TODO: rootPath change to  TenentID
	rootPath, err := p.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.InsertBinlogRootPath = path.Join(rootPath, "insert_log")
}

func (p *ParamTable) initStatsBinlogRootPath() {
	rootPath, err := p.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.StatsBinlogRootPath = path.Join(rootPath, "stats_log")
}

func (p *ParamTable) initDeleteBinlogRootPath() {
	rootPath, err := p.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.DeleteBinlogRootPath = path.Join(rootPath, "delta_log")
}

func (p *ParamTable) initPulsarAddress() {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *ParamTable) initRocksmqPath() {
	path, err := p.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

func (p *ParamTable) initClusterMsgChannelPrefix() {
	name, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = name
}

func (p *ParamTable) initSegmentStatisticsChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordStatistic")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SegmentStatisticsChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initTimeTickChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initMsgChannelSubName() {
	config, err := p.Load("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config, strconv.FormatInt(p.NodeID, 10)}
	p.MsgChannelSubName = strings.Join(s, "-")
}

func (p *ParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
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

func (p *ParamTable) initChannelWatchPath() {
	p.ChannelWatchSubPath = "channelwatch"
}

// --- MinIO ---
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

func (p *ParamTable) initRoleName() {
	p.RoleName = "datanode"
}
