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

package paramtable

import (
	"os"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/internal/log"
)

var Params BaseParamTable
var once sync.Once

type BaseParamTable struct {
	BaseTable

	// --- ETCD ---
	EtcdEndpoints []string
	MetaRootPath  string
	KvRootPath    string

	// --- Embed ETCD ---
	UseEmbedEtcd   bool
	EtcdConfigPath string
	EtcdDataDir    string

	initOnce sync.Once

	LogConfig *log.Config
}

func (p *BaseParamTable) Init() {
	p.initOnce.Do(func() {
		p.BaseTable.Init()
		p.LoadCfgToMemory()
	})
}

func (p *BaseParamTable) LoadCfgToMemory() {
	p.initEtcdConf()
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initLogCfg()
}

func (p *BaseParamTable) initEtcdConf() {
	p.initUseEmbedEtcd()
	if p.UseEmbedEtcd {
		p.initConfigPath()
		p.initEtcdDataDir()
	} else {
		p.initEtcdEndpoints()
	}
}

func (p *BaseParamTable) initUseEmbedEtcd() {
	p.UseEmbedEtcd = p.ParseBool("etcd.use.embed", false)
	if p.UseEmbedEtcd && (os.Getenv(metricsinfo.DeployModeEnvKey) != metricsinfo.StandaloneDeployMode) {
		panic("embedded etcd can not be used under distributed mode")
	}
}

func (p *BaseParamTable) initConfigPath() {
	addr, err := p.LoadWithDefault("etcd.config.path", "")
	if err != nil {
		panic(err)
	}
	p.EtcdConfigPath = addr
}

func (p *BaseParamTable) initEtcdDataDir() {
	addr, err := p.LoadWithDefault("etcd.data.dir", "default.etcd")
	if err != nil {
		panic(err)
	}
	p.EtcdDataDir = addr
}

func (p *BaseParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
}

func (p *BaseParamTable) initMetaRootPath() {
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

func (p *BaseParamTable) initKvRootPath() {
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

func (p *BaseParamTable) initLogCfg() {
	p.LogConfig = &log.Config{}
	format, err := p.Load("log.format")
	if err != nil {
		panic(err)
	}
	p.LogConfig.Format = format
	level, err := p.Load("log.level")
	if err != nil {
		panic(err)
	}
	p.LogConfig.Level = level
	p.LogConfig.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.LogConfig.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.LogConfig.File.MaxDays = p.ParseInt("log.file.maxAge")
	p.LogConfig.File.RootPath, err = p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
}
