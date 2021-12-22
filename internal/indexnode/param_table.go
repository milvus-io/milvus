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

package indexnode

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// ParamTable is used to record configuration items.
type ParamTable struct {
	paramtable.BaseTable

	IP      string
	Address string
	Port    int

	NodeID int64
	Alias  string

	EtcdEndpoints        []string
	MetaRootPath         string
	IndexStorageRootPath string

	MinIOAddress         string
	MinIOAccessKeyID     string
	MinIOSecretAccessKey string
	MinIOUseSSL          bool
	MinioBucketName      string

	SimdType string

	CreatedTime time.Time
	UpdatedTime time.Time
}

// Params is a package scoped variable of type ParamTable.
var Params ParamTable
var once sync.Once

// InitAlias initializes an alias for the IndexNode role.
func (pt *ParamTable) InitAlias(alias string) {
	pt.Alias = alias
}

// Init is used to initialize configuration items.
func (pt *ParamTable) Init() {
	pt.BaseTable.Init()
	pt.initParams()
}

// InitOnce is used to initialize configuration items, and it will only be called once.
func (pt *ParamTable) InitOnce() {
	once.Do(func() {
		pt.Init()
	})
}

func (pt *ParamTable) initParams() {
	pt.initMinIOAddress()
	pt.initMinIOAccessKeyID()
	pt.initMinIOSecretAccessKey()
	pt.initMinIOUseSSL()
	pt.initMinioBucketName()
	pt.initEtcdEndpoints()
	pt.initMetaRootPath()
	pt.initIndexStorageRootPath()
	pt.initRoleName()
	pt.initKnowhereSimdType()
}

// initMinIOAddress load minio address from BaseTable.
func (pt *ParamTable) initMinIOAddress() {
	ret, err := pt.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	pt.MinIOAddress = ret
}

// initMinIOAccessKeyID load access key for minio from BaseTable.
func (pt *ParamTable) initMinIOAccessKeyID() {
	ret, err := pt.Load("_MinioAccessKeyID")
	if err != nil {
		panic(err)
	}
	pt.MinIOAccessKeyID = ret
}

func (pt *ParamTable) initMinIOSecretAccessKey() {
	ret, err := pt.Load("_MinioSecretAccessKey")
	if err != nil {
		panic(err)
	}
	pt.MinIOSecretAccessKey = ret
}

func (pt *ParamTable) initMinIOUseSSL() {
	ret, err := pt.Load("_MinioUseSSL")
	if err != nil {
		panic(err)
	}
	pt.MinIOUseSSL, err = strconv.ParseBool(ret)
	if err != nil {
		panic(err)
	}
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

func (pt *ParamTable) initIndexStorageRootPath() {
	rootPath, err := pt.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	pt.IndexStorageRootPath = path.Join(rootPath, "index_files")
}

func (pt *ParamTable) initMinioBucketName() {
	bucketName, err := pt.Load("_MinioBucketName")
	if err != nil {
		panic(err)
	}
	pt.MinioBucketName = bucketName
}

func (pt *ParamTable) initRoleName() {
	pt.RoleName = "indexnode"
}

func (pt *ParamTable) initKnowhereSimdType() {
	simdType := pt.LoadWithDefault("knowhere.simdType", "auto")
	pt.SimdType = simdType
	log.Debug("initialize the knowhere simd type", zap.String("simd_type", pt.SimdType))
}
