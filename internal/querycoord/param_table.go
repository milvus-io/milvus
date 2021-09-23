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

package querycoord

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

	Address      string
	Port         int
	QueryCoordID UniqueID

	// stats
	StatsChannelName string

	// timetick
	TimeTickChannelName string

	Log      log.Config
	RoleName string

	// channels
	ClusterChannelPrefix      string
	SearchChannelPrefix       string
	SearchResultChannelPrefix string

	// --- ETCD ---
	EtcdEndpoints []string
	MetaRootPath  string
	KvRootPath    string

	//--- Minio ---
	MinioEndPoint        string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSLStr       bool
	MinioBucketName      string
}

var Params ParamTable
var once sync.Once

func (p *ParamTable) InitOnce() {
	once.Do(func() {
		p.Init()
	})
}
func (p *ParamTable) Init() {
	p.BaseTable.Init()
	err := p.LoadYaml("advanced/query_node.yaml")
	if err != nil {
		panic(err)
	}

	err = p.LoadYaml("milvus.yaml")
	if err != nil {
		panic(err)
	}

	p.initLogCfg()

	p.initQueryCoordAddress()
	p.initRoleName()

	// --- Channels ---
	p.initClusterMsgChannelPrefix()
	p.initSearchChannelPrefix()
	p.initSearchResultChannelPrefix()
	p.initStatsChannelName()
	p.initTimeTickChannelName()

	// --- ETCD ---
	p.initEtcdEndpoints()
	p.initMetaRootPath()
	p.initKvRootPath()

	//--- Minio ----
	p.initMinioEndPoint()
	p.initMinioAccessKeyID()
	p.initMinioSecretAccessKey()
	p.initMinioUseSSLStr()
	p.initMinioBucketName()
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
	p.Log.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.Log.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.Log.File.MaxDays = p.ParseInt("log.file.maxAge")
	rootPath, err := p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if len(rootPath) != 0 {
		p.Log.File.Filename = path.Join(rootPath, fmt.Sprintf("queryCoord-%d.log", p.NodeID))
	} else {
		p.Log.File.Filename = ""
	}
}

func (p *ParamTable) initQueryCoordAddress() {
	url, err := p.Load("_QueryCoordAddress")
	if err != nil {
		panic(err)
	}
	p.Address = url
}

func (p *ParamTable) initRoleName() {
	p.RoleName = fmt.Sprintf("%s-%d", "QueryCoord", p.NodeID)
}

func (p *ParamTable) initClusterMsgChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.cluster")
	if err != nil {
		panic(err)
	}
	p.ClusterChannelPrefix = config
}

func (p *ParamTable) initSearchChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		log.Error(err.Error())
	}

	s := []string{p.ClusterChannelPrefix, config}
	p.SearchChannelPrefix = strings.Join(s, "-")
}

func (p *ParamTable) initSearchResultChannelPrefix() {
	config, err := p.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		log.Error(err.Error())
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.SearchResultChannelPrefix = strings.Join(s, "-")
}

func (p *ParamTable) initStatsChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.queryNodeStats")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.StatsChannelName = strings.Join(s, "-")
}

func (p *ParamTable) initTimeTickChannelName() {
	config, err := p.Load("msgChannel.chanNamePrefix.queryTimeTick")
	if err != nil {
		panic(err)
	}
	s := []string{p.ClusterChannelPrefix, config}
	p.TimeTickChannelName = strings.Join(s, "-")
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

func (p *ParamTable) initKvRootPath() {
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

func (p *ParamTable) initMinioEndPoint() {
	url, err := p.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioEndPoint = url
}

func (p *ParamTable) initMinioAccessKeyID() {
	id, err := p.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = id
}

func (p *ParamTable) initMinioSecretAccessKey() {
	key, err := p.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *ParamTable) initMinioUseSSLStr() {
	ssl, err := p.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	sslBoolean, err := strconv.ParseBool(ssl)
	if err != nil {
		panic(err)
	}
	p.MinioUseSSLStr = sslBoolean
}

func (p *ParamTable) initMinioBucketName() {
	bucketName, err := p.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
}
