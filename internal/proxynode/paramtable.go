// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxynode

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

const (
	StartParamsKey                 = "START_PARAMS"
	PulsarMaxMessageSizeKey        = "maxMessageSize"
	SuggestPulsarMaxMessageSizeKey = 5 * 1024 * 1024
)

type ParamTable struct {
	paramtable.BaseTable

	NetworkPort    int
	IP             string
	NetworkAddress string

	EtcdEndpoints []string
	MetaRootPath  string
	MasterAddress string
	PulsarAddress string

	ProxyID                    UniqueID
	TimeTickInterval           time.Duration
	K2SChannelNames            []string
	SearchChannelNames         []string
	SearchResultChannelNames   []string
	RetrieveChannelNames       []string
	RetrieveResultChannelNames []string
	ProxySubName               string
	ProxyTimeTickChannelNames  []string
	MsgStreamTimeTickBufSize   int64
	MaxNameLength              int64
	MaxFieldNum                int64
	MaxDimension               int64
	DefaultPartitionName       string
	DefaultIndexName           string

	PulsarMaxMessageSize int
	Log                  log.Config
	RoleName             string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		err := pt.LoadYaml("advanced/proxy_node.yaml")
		if err != nil {
			panic(err)
		}
		pt.initParams()
	})
}

func (pt *ParamTable) initParams() {
	pt.initLogCfg()
	pt.initEtcdEndpoints()
	pt.initMetaRootPath()
	pt.initPulsarAddress()
	pt.initTimeTickInterval()
	pt.initK2SChannelNames()
	pt.initProxySubName()
	pt.initProxyTimeTickChannelNames()
	pt.initMsgStreamTimeTickBufSize()
	pt.initMaxNameLength()
	pt.initMaxFieldNum()
	pt.initMaxDimension()
	pt.initDefaultPartitionName()
	pt.initDefaultIndexName()

	pt.initPulsarMaxMessageSize()
	pt.initRoleName()
}

func (pt *ParamTable) initPulsarAddress() {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	pt.PulsarAddress = ret
}

func (pt *ParamTable) initTimeTickInterval() {
	intervalStr, err := pt.Load("proxyNode.timeTickInterval")
	if err != nil {
		panic(err)
	}
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		panic(err)
	}
	pt.TimeTickInterval = time.Duration(interval) * time.Millisecond
}

func (pt *ParamTable) initK2SChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.k2s")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	k2sRangeStr, err := pt.Load("msgChannel.channelRange.k2s")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(k2sRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	pt.K2SChannelNames = ret
}

func (pt *ParamTable) initProxySubName() {
	prefix, err := pt.Load("msgChannel.subNamePrefix.proxySubNamePrefix")
	if err != nil {
		panic(err)
	}
	pt.ProxySubName = prefix + "-" + strconv.Itoa(int(pt.ProxyID))
}

func (pt *ParamTable) initProxyTimeTickChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		panic(err)
	}
	prefix += "-0"
	pt.ProxyTimeTickChannelNames = []string{prefix}
}

func (pt *ParamTable) initMsgStreamTimeTickBufSize() {
	pt.MsgStreamTimeTickBufSize = pt.ParseInt64("proxyNode.msgStream.timeTick.bufSize")
}

func (pt *ParamTable) initMaxNameLength() {
	str, err := pt.Load("proxyNode.maxNameLength")
	if err != nil {
		panic(err)
	}
	maxNameLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxNameLength = maxNameLength
}

func (pt *ParamTable) initMaxFieldNum() {
	str, err := pt.Load("proxyNode.maxFieldNum")
	if err != nil {
		panic(err)
	}
	maxFieldNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxFieldNum = maxFieldNum
}

func (pt *ParamTable) initMaxDimension() {
	str, err := pt.Load("proxyNode.maxDimension")
	if err != nil {
		panic(err)
	}
	maxDimension, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxDimension = maxDimension
}

func (pt *ParamTable) initDefaultPartitionName() {
	name, err := pt.Load("common.defaultPartitionName")
	if err != nil {
		panic(err)
	}
	pt.DefaultPartitionName = name
}

func (pt *ParamTable) initDefaultIndexName() {
	name, err := pt.Load("common.defaultIndexName")
	if err != nil {
		panic(err)
	}
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
		pt.PulsarMaxMessageSize = SuggestPulsarMaxMessageSizeKey
	} else {
		maxMessageSize, err := strconv.Atoi(maxMessageSizeStr)
		if err != nil {
			pt.PulsarMaxMessageSize = SuggestPulsarMaxMessageSizeKey
		} else {
			pt.PulsarMaxMessageSize = maxMessageSize
		}
	}
}

func (pt *ParamTable) initLogCfg() {
	pt.Log = log.Config{}
	format, err := pt.Load("log.format")
	if err != nil {
		panic(err)
	}
	pt.Log.Format = format
	level, err := pt.Load("log.level")
	if err != nil {
		panic(err)
	}
	pt.Log.Level = level
	devStr, err := pt.Load("log.dev")
	if err != nil {
		panic(err)
	}
	dev, err := strconv.ParseBool(devStr)
	if err != nil {
		panic(err)
	}
	pt.Log.Development = dev
	pt.Log.File.MaxSize = pt.ParseInt("log.file.maxSize")
	pt.Log.File.MaxBackups = pt.ParseInt("log.file.maxBackups")
	pt.Log.File.MaxDays = pt.ParseInt("log.file.maxAge")
	rootPath, err := pt.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if len(rootPath) != 0 {
		pt.Log.File.Filename = path.Join(rootPath, fmt.Sprintf("proxynode-%d.log", pt.ProxyID))
	} else {
		pt.Log.File.Filename = ""
	}
}

func (pt *ParamTable) initRoleName() {
	pt.RoleName = fmt.Sprintf("%s-%d", "ProxyNode", pt.ProxyID)
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
