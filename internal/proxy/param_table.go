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

package proxy

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

const (
	// SuggestPulsarMaxMessageSize defines the maximum size of Pulsar message.
	SuggestPulsarMaxMessageSize = 5 * 1024 * 1024
)

// ParamTable is a derived struct of paramtable.BaseTable. It achieves Composition by
// embedding paramtable.BaseTable. It is used to quickly and easily access the system configuration.
type ParamTable struct {
	paramtable.BaseTable

	// NetworkPort & IP are not used
	NetworkPort int
	IP          string

	NetworkAddress string

	Alias string

	EtcdEndpoints []string
	MetaRootPath  string
	PulsarAddress string

	RocksmqPath string // not used in Proxy

	ProxyID                  UniqueID
	TimeTickInterval         time.Duration
	MsgStreamTimeTickBufSize int64
	MaxNameLength            int64
	MaxFieldNum              int64
	MaxShardNum              int32
	MaxDimension             int64
	DefaultPartitionName     string
	DefaultIndexName         string
	BufFlagExpireTime        time.Duration
	BufFlagCleanupInterval   time.Duration

	// --- Channels ---
	ClusterChannelPrefix      string
	ProxyTimeTickChannelNames []string
	ProxySubName              string

	// required from query coord
	SearchResultChannelNames   []string
	RetrieveResultChannelNames []string

	MaxTaskNum int64

	PulsarMaxMessageSize int

	CreatedTime time.Time
	UpdatedTime time.Time
}

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// InitOnce ensures that Init is only called once.
func (pt *ParamTable) InitOnce() {
	once.Do(func() {
		pt.Init()
	})
}

// Init is an override method of BaseTable's Init. It mainly calls the
// Init of BaseTable and do some other initialization.
func (pt *ParamTable) Init() {
	pt.BaseTable.Init()
	pt.initEtcdEndpoints()
	pt.initMetaRootPath()
	pt.initPulsarAddress()
	pt.initRocksmqPath()
	pt.initTimeTickInterval()
	// Has to init global msgchannel prefix before other channel names
	pt.initClusterMsgChannelPrefix()
	pt.initProxySubName()
	pt.initProxyTimeTickChannelNames()
	pt.initMsgStreamTimeTickBufSize()
	pt.initMaxNameLength()
	pt.initMaxFieldNum()
	pt.initMaxShardNum()
	pt.initMaxDimension()
	pt.initDefaultPartitionName()
	pt.initDefaultIndexName()

	pt.initPulsarMaxMessageSize()

	pt.initMaxTaskNum()
	pt.initBufFlagExpireTime()
	pt.initBufFlagCleanupInterval()

	pt.initRoleName()
}

// InitAlias initialize Alias member.
func (pt *ParamTable) InitAlias(alias string) {
	pt.Alias = alias
}

func (pt *ParamTable) initPulsarAddress() {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	pt.PulsarAddress = ret
}

func (pt *ParamTable) initRocksmqPath() {
	path, err := pt.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	pt.RocksmqPath = path
}

func (pt *ParamTable) initTimeTickInterval() {
	interval := pt.ParseIntWithDefault("proxy.timeTickInterval", 200)
	pt.TimeTickInterval = time.Duration(interval) * time.Millisecond
}

func (pt *ParamTable) initClusterMsgChannelPrefix() {
	config, err := pt.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	pt.ClusterChannelPrefix = config
}

func (pt *ParamTable) initProxySubName() {
	config, err := pt.Load("msgChannel.subNamePrefix.proxySubNamePrefix")
	if err != nil {
		panic(err)
	}
	s := []string{pt.ClusterChannelPrefix, config, strconv.FormatInt(pt.ProxyID, 10)}
	pt.ProxySubName = strings.Join(s, "-")
}

func (pt *ParamTable) initProxyTimeTickChannelNames() {
	config, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{pt.ClusterChannelPrefix, config, "0"}
	prefix := strings.Join(s, "-")
	pt.ProxyTimeTickChannelNames = []string{prefix}
}

func (pt *ParamTable) initMsgStreamTimeTickBufSize() {
	pt.MsgStreamTimeTickBufSize = pt.ParseInt64WithDefault("proxy.msgStream.timeTick.bufSize", 512)
}

func (pt *ParamTable) initMaxNameLength() {
	str := pt.LoadWithDefault("proxy.maxNameLength", "255")
	maxNameLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxNameLength = maxNameLength
}

func (pt *ParamTable) initMaxShardNum() {
	str := pt.LoadWithDefault("proxy.maxShardNum", "256")
	maxShardNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxShardNum = int32(maxShardNum)
}

func (pt *ParamTable) initMaxFieldNum() {
	str := pt.LoadWithDefault("proxy.maxFieldNum", "64")
	maxFieldNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxFieldNum = maxFieldNum
}

func (pt *ParamTable) initMaxDimension() {
	str := pt.LoadWithDefault("proxy.maxDimension", "32768")
	maxDimension, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxDimension = maxDimension
}

func (pt *ParamTable) initDefaultPartitionName() {
	name := pt.LoadWithDefault("common.defaultPartitionName", "_default")
	pt.DefaultPartitionName = name
}

func (pt *ParamTable) initDefaultIndexName() {
	name := pt.LoadWithDefault("common.defaultIndexName", "_default_idx")
	pt.DefaultIndexName = name
}

func (pt *ParamTable) initPulsarMaxMessageSize() {
	// pulsarHost, err := pt.Load("pulsar.address")
	// if err != nil {
	// 	panic(err)
	// }

	// pulsarRestPort, err := pt.Load("pulsar.rest-port")
	// if err != nil {
	// 	panic(err)
	// }

	// protocol := "http"
	// url := "/admin/v2/brokers/configuration/runtime"
	// runtimeConfig, err := GetPulsarConfig(protocol, pulsarHost, pulsarRestPort, url)
	// if err != nil {
	// 	panic(err)
	// }
	// maxMessageSizeStr := fmt.Sprintf("%v", runtimeConfig[PulsarMaxMessageSizeKey])
	// pt.PulsarMaxMessageSize, err = strconv.Atoi(maxMessageSizeStr)
	// if err != nil {
	// 	panic(err)
	// }

	maxMessageSizeStr, err := pt.Load("pulsar.maxMessageSize")
	if err != nil {
		pt.PulsarMaxMessageSize = SuggestPulsarMaxMessageSize
	} else {
		maxMessageSize, err := strconv.Atoi(maxMessageSizeStr)
		if err != nil {
			pt.PulsarMaxMessageSize = SuggestPulsarMaxMessageSize
		} else {
			pt.PulsarMaxMessageSize = maxMessageSize
		}
	}
}

func (pt *ParamTable) initRoleName() {
	pt.RoleName = "proxy"
}

func (pt *ParamTable) initEtcdEndpoints() {
	endpoints, err := pt.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	pt.EtcdEndpoints = strings.Split(endpoints, ",")
}

func (pt *ParamTable) initMetaRootPath() {
	rootPath, err := pt.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := pt.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	pt.MetaRootPath = path.Join(rootPath, subPath)
}

func (pt *ParamTable) initMaxTaskNum() {
	pt.MaxTaskNum = pt.ParseInt64WithDefault("proxy.maxTaskNum", 1024)
}

func (pt *ParamTable) initBufFlagExpireTime() {
	expireTime := pt.ParseInt64WithDefault("proxy.bufFlagExpireTime", 3600)
	pt.BufFlagExpireTime = time.Duration(expireTime) * time.Second
}

func (pt *ParamTable) initBufFlagCleanupInterval() {
	interval := pt.ParseInt64WithDefault("proxy.bufFlagCleanupInterval", 600)
	pt.BufFlagCleanupInterval = time.Duration(interval) * time.Second
}
