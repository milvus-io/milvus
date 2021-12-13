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

package datacoord

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// ParamTable is a derived struct of paramtable.BaseTable. It achieves Composition by
// embedding paramtable.BaseTable. It is used to quickly and easily access the system configuration.
type ParamTable struct {
	paramtable.BaseTable

	NodeID int64

	IP      string
	Port    int
	Address string

	// --- ETCD ---
	EtcdEndpoints           []string
	MetaRootPath            string
	KvRootPath              string
	SegmentBinlogSubPath    string
	CollectionBinlogSubPath string
	ChannelWatchSubPath     string

	// --- MinIO ---
	MinioAddress         string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	MinioBucketName      string
	MinioRootPath        string

	// --- Pulsar ---
	PulsarAddress string

	// --- Rocksmq ---
	RocksmqPath string

	FlushStreamPosSubPath string
	StatsStreamPosSubPath string

	// --- SEGMENTS ---
	SegmentMaxSize          float64
	SegmentSealProportion   float64
	SegAssignmentExpiration int64

	// --- Channels ---
	ClusterChannelPrefix      string
	InsertChannelPrefixName   string
	TimeTickChannelName       string
	SegmentInfoChannelName    string
	DataCoordSubscriptionName string

	CreatedTime time.Time
	UpdatedTime time.Time

	EnableCompaction        bool
	EnableGarbageCollection bool

	CompactionRetentionDuration int64
	EnableAutoCompaction        bool

	// Garbage Collection
	GCInterval         time.Duration
	GCMissingTolerance time.Duration
	GCDropTolerance    time.Duration
}

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// Init is an override method of BaseTable's Init. It mainly calls the
// Init of BaseTable and do some other initialization.
func (p *ParamTable) Init() {
	// load yaml
	p.BaseTable.Init()

	// set members
	p.initEtcdEndpoints()
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initSegmentBinlogSubPath()
	p.initCollectionBinlogSubPath()
	p.initChannelWatchPrefix()

	p.initPulsarAddress()
	p.initRocksmqPath()

	p.initSegmentMaxSize()
	p.initSegmentSealProportion()
	p.initSegAssignmentExpiration()

	// Has to init global msgchannel prefix before other channel names
	p.initClusterMsgChannelPrefix()
	p.initInsertChannelPrefixName()
	p.initTimeTickChannelName()
	p.initSegmentInfoChannelName()
	p.initDataCoordSubscriptionName()
	p.initRoleName()

	p.initFlushStreamPosSubPath()
	p.initStatsStreamPosSubPath()

	p.initEnableCompaction()

	p.initMinioAddress()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSL()
	p.initMinioBucketName()
	p.initMinioRootPath()

	p.initCompactionRetentionDuration()
	p.initEnableAutoCompaction()

	p.initEnableGarbageCollection()
	p.initGCInterval()
	p.initGCMissingTolerance()
	p.initGCDropTolerance()
}

// InitOnce ensures param table is a singleton
func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}

func (p *ParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *ParamTable) initRocksmqPath() {
	path, err := p.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
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
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initKvRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initSegmentBinlogSubPath() {
	subPath, err := p.Load("etcd.segmentBinlogSubPath")
	if err != nil {
		panic(err)
	}
	p.SegmentBinlogSubPath = subPath
}

func (p *ParamTable) initCollectionBinlogSubPath() {
	subPath, err := p.Load("etcd.collectionBinlogSubPath")
	if err != nil {
		panic(err)
	}
	p.CollectionBinlogSubPath = subPath
}

func (p *ParamTable) initSegmentMaxSize() {
	p.SegmentMaxSize = p.ParseFloatWithDefault("dataCoord.segment.maxSize", 512.0)
}

func (p *ParamTable) initSegmentSealProportion() {
	p.SegmentSealProportion = p.ParseFloatWithDefault("dataCoord.segment.sealProportion", 0.75)
}

func (p *ParamTable) initSegAssignmentExpiration() {
	p.SegAssignmentExpiration = p.ParseInt64WithDefault("dataCoord.segment.assignmentExpiration", 2000)
}

func (p *ParamTable) initClusterMsgChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *ParamTable) initInsertChannelPrefixName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordInsertChannel")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.InsertChannelPrefixName = strings.Join(s, "-")
}

func (p *ParamTable) initTimeTickChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initSegmentInfoChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.dataCoordSegmentInfo")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SegmentInfoChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initDataCoordSubscriptionName() {
	config, err := p.Load("msgChannel.subNamePrefix.dataCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.DataCoordSubscriptionName = strings.Join(s, "-")
}

func (p *ParamTable) initRoleName() {
	p.RoleName = "datacoord"
}

func (p *ParamTable) initFlushStreamPosSubPath() {
	subPath, err := p.Load("etcd.flushStreamPosSubPath")
	if err != nil {
		panic(err)
	}
	p.FlushStreamPosSubPath = subPath
}

func (p *ParamTable) initStatsStreamPosSubPath() {
	subPath, err := p.Load("etcd.statsStreamPosSubPath")
	if err != nil {
		panic(err)
	}
	p.StatsStreamPosSubPath = subPath
}

func (p *ParamTable) initChannelWatchPrefix() {
	// WARN: this value should not be put to milvus.yaml. It's a default value for channel watch path.
	// This will be removed after we reconstruct our config module.
	p.ChannelWatchSubPath = "channelwatch"
}

func (p *ParamTable) initEnableCompaction() {
	p.EnableCompaction = p.ParseBool("dataCoord.enableCompaction", false)
}

// -- GC --

func (p *ParamTable) initEnableGarbageCollection() {
	p.EnableGarbageCollection = p.ParseBool("dataCoord.enableGarbageCollection", false)
}

func (p *ParamTable) initGCInterval() {
	p.GCInterval = time.Duration(p.ParseInt64WithDefault("dataCoord.gc.interval", 60*60)) * time.Second
}

func (p *ParamTable) initGCMissingTolerance() {
	p.GCMissingTolerance = time.Duration(p.ParseInt64WithDefault("dataCoord.gc.missingTolerance", 24*60*60)) * time.Second
}

func (p *ParamTable) initGCDropTolerance() {
	p.GCDropTolerance = time.Duration(p.ParseInt64WithDefault("dataCoord.gc.dropTolerance", 24*60*60)) * time.Second
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
	keyID, err := p.Load("_MinioAccessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = keyID
}

func (p *ParamTable) initMinioSecretAccessKey() {
	key, err := p.Load("_MinioSecretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *ParamTable) initMinioUseSSL() {
	usessl, err := p.Load("_MinioUseSSL")
	if err != nil {
		panic(err)
	}
	p.MinioUseSSL, _ = strconv.ParseBool(usessl)
}

func (p *ParamTable) initMinioBucketName() {
	bucketName, err := p.Load("_MinioBucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}

func (p *ParamTable) initMinioRootPath() {
	rootPath, err := p.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.MinioRootPath = rootPath
}

func (p *ParamTable) initCompactionRetentionDuration() {
	p.CompactionRetentionDuration = p.ParseInt64WithDefault("dataCoord.compaction.retentionDuration", 432000)
}

func (p *ParamTable) initEnableAutoCompaction() {
	p.EnableAutoCompaction = p.ParseBool("dataCoord.compaction.enableAutoCompaction", false)
}
