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

package paramtable

import (
	"os"
	"path"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

// BaseParamTable is a derived struct of BaseTable. It achieves Composition by
// embedding BaseTable. It is used to quickly and easily access the system configuration.
type BaseParamTable struct {
	BaseTable
	once sync.Once

	// --- ETCD ---
	EtcdEndpoints []string
	MetaRootPath  string
	KvRootPath    string

	// --- Embed ETCD ---
	UseEmbedEtcd   bool
	EtcdConfigPath string
	EtcdDataDir    string
}

// Init is an override method of BaseTable's Init. It mainly calls the
// Init of BaseTable and do some other initialization.
func (p *BaseParamTable) Init() {
	p.once.Do(func() {
		p.BaseTable.Init()
		p.LoadCfgToMemory()
	})
}

// LoadCfgToMemory loads configurations from file into memory.
func (p *BaseParamTable) LoadCfgToMemory() {
	p.initEtcdConf()
	p.initMetaRootPath()
	p.initKvRootPath()
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
	addr := p.LoadWithDefault("etcd.config.path", "")
	p.EtcdConfigPath = addr
}

func (p *BaseParamTable) initEtcdDataDir() {
	addr := p.LoadWithDefault("etcd.data.dir", "default.etcd")
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
	p.MetaRootPath = path.Join(rootPath, subPath)
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
	p.KvRootPath = path.Join(rootPath, subPath)
}
