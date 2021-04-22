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
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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

	MasterAddress string
	PulsarAddress string

	QueryNodeNum                       int
	QueryNodeIDList                    []UniqueID
	ProxyID                            UniqueID
	TimeTickInterval                   time.Duration
	K2SChannelNames                    []string
	SearchChannelNames                 []string
	SearchResultChannelNames           []string
	ProxySubName                       string
	ProxyTimeTickChannelNames          []string
	MsgStreamInsertBufSize             int64
	MsgStreamSearchBufSize             int64
	MsgStreamSearchResultBufSize       int64
	MsgStreamSearchResultPulsarBufSize int64
	MsgStreamTimeTickBufSize           int64
	MaxNameLength                      int64
	MaxFieldNum                        int64
	MaxDimension                       int64
	DefaultPartitionName               string
	DefaultIndexName                   string

	PulsarMaxMessageSize int
	Log                  log.Config
	RoleName             string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) LoadConfigFromInitParams(initParams *internalpb.InitParams) error {
	pt.ProxyID = initParams.NodeID

	config := viper.New()
	config.SetConfigType("yaml")
	save := func() error {
		for _, key := range config.AllKeys() {
			val := config.Get(key)
			str, err := cast.ToStringE(val)
			if err != nil {
				switch val := val.(type) {
				case []interface{}:
					str = str[:0]
					for _, v := range val {
						ss, err := cast.ToStringE(v)
						if err != nil {
							log.Warn("proxynode", zap.String("error", err.Error()))
						}
						if len(str) == 0 {
							str = ss
						} else {
							str = str + "," + ss
						}
					}

				default:
					log.Debug("proxynode", zap.String("error", "Undefined config type, key="+key))
				}
			}
			err = pt.Save(key, str)
			if err != nil {
				panic(err)
			}
		}
		return nil
	}

	for _, pair := range initParams.StartParams {
		if strings.HasPrefix(pair.Key, StartParamsKey) {
			err := config.ReadConfig(bytes.NewBuffer([]byte(pair.Value)))
			if err != nil {
				return err
			}
			err = save()
			if err != nil {
				return err
			}
		}
	}

	pt.initParams()

	return nil
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initLogCfg()
		// err := pt.LoadYaml("advanced/proxy_node.yaml")
		// if err != nil {
		// 	panic(err)
		// }
		// pt.initParams()
	})
}

func (pt *ParamTable) initParams() {
	pt.initPulsarAddress()
	pt.initQueryNodeIDList()
	pt.initQueryNodeNum()
	pt.initTimeTickInterval()
	pt.initK2SChannelNames()
	pt.initProxySubName()
	pt.initProxyTimeTickChannelNames()
	pt.initMsgStreamInsertBufSize()
	pt.initMsgStreamSearchBufSize()
	pt.initMsgStreamSearchResultBufSize()
	pt.initMsgStreamSearchResultPulsarBufSize()
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

func (pt *ParamTable) initQueryNodeNum() {
	pt.QueryNodeNum = len(pt.QueryNodeIDList)
}

func (pt *ParamTable) initQueryNodeIDList() []UniqueID {
	queryNodeIDStr, err := pt.Load("nodeID.queryNodeIDList")
	if err != nil {
		panic(err)
	}
	var ret []UniqueID
	queryNodeIDs := strings.Split(queryNodeIDStr, ",")
	for _, i := range queryNodeIDs {
		v, err := strconv.Atoi(i)
		if err != nil {
			log.Error("proxynode", zap.String("load proxynode id list error", err.Error()))
		}
		ret = append(ret, UniqueID(v))
	}
	pt.QueryNodeIDList = ret
	return ret
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

func (pt *ParamTable) initMsgStreamInsertBufSize() {
	pt.MsgStreamInsertBufSize = pt.ParseInt64("proxyNode.msgStream.insert.bufSize")
}

func (pt *ParamTable) initMsgStreamSearchBufSize() {
	pt.MsgStreamSearchBufSize = pt.ParseInt64("proxyNode.msgStream.search.bufSize")
}

func (pt *ParamTable) initMsgStreamSearchResultBufSize() {
	pt.MsgStreamSearchResultBufSize = pt.ParseInt64("proxyNode.msgStream.searchResult.recvBufSize")
}

func (pt *ParamTable) initMsgStreamSearchResultPulsarBufSize() {
	pt.MsgStreamSearchResultPulsarBufSize = pt.ParseInt64("proxyNode.msgStream.searchResult.pulsarBufSize")
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
