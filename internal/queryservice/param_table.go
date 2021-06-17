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

package queryservice

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type ParamTable struct {
	paramtable.BaseTable

	NodeID uint64

	Address        string
	Port           int
	QueryServiceID UniqueID

	// stats
	StatsChannelName string

	// timetick
	TimeTickChannelName string

	Log      log.Config
	RoleName string

	// search
	SearchChannelPrefix       string
	SearchResultChannelPrefix string

	// --- ETCD ---
	EtcdEndpoints []string
	MetaRootPath  string
}

var Params ParamTable
var once sync.Once

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/query_node.yaml")
		if err != nil {
			panic(err)
		}

		err = p.LoadYaml("advanced/query_service.yaml")
		if err != nil {
			panic(err)
		}

		err = p.LoadYaml("milvus.yaml")
		if err != nil {
			panic(err)
		}

		p.initNodeID()
		p.initLogCfg()

		p.initStatsChannelName()
		p.initTimeTickChannelName()
		p.initQueryServiceAddress()
		p.initRoleName()
		p.initSearchChannelPrefix()
		p.initSearchResultChannelPrefix()

		// --- ETCD ---
		p.initEtcdEndpoints()
		p.initMetaRootPath()
	})
}

func (p *ParamTable) initNodeID() {
	p.NodeID = uint64(p.ParseInt64("queryService.nodeID"))
}

func (p *ParamTable) initLogCfg() {
	p.Log = log.Config{}
	format, err := p.Load("log.format")
	if err != nil {
		panic(err)
	}
	p.Log.Format = format
	level, err := p.Load("log.level")
	if err != nil {
		panic(err)
	}
	p.Log.Level = level
	devStr, err := p.Load("log.dev")
	if err != nil {
		panic(err)
	}
	dev, err := strconv.ParseBool(devStr)
	if err != nil {
		panic(err)
	}
	p.Log.Development = dev
	p.Log.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.Log.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.Log.File.MaxDays = p.ParseInt("log.file.maxAge")
	rootPath, err := p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if len(rootPath) != 0 {
		p.Log.File.Filename = path.Join(rootPath, fmt.Sprintf("queryService-%d.log", p.NodeID))
	} else {
		p.Log.File.Filename = ""
	}
}

func (p *ParamTable) initStatsChannelName() {
	channels, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	p.StatsChannelName = channels
}

func (p *ParamTable) initTimeTickChannelName() {
	timeTickChannelName, err := p.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		panic(err)
	}
	p.TimeTickChannelName = timeTickChannelName

}

func (p *ParamTable) initQueryServiceAddress() {
	url, err := p.Load("_QueryServiceAddress")
	if err != nil {
		panic(err)
	}
	p.Address = url
}

func (p *ParamTable) initRoleName() {
	p.RoleName = fmt.Sprintf("%s-%d", "QueryService", p.NodeID)
}

func (p *ParamTable) initSearchChannelPrefix() {
	channelName, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Error(err.Error())
	}

	p.SearchChannelPrefix = channelName
}

func (p *ParamTable) initSearchResultChannelPrefix() {
	channelName, err := p.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		log.Error(err.Error())
	}

	p.SearchResultChannelPrefix = channelName
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
