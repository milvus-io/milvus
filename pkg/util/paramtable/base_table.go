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
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	config "github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UniqueID is type alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

const (
	DefaultGlogConf             = "glog.conf"
	DefaultMinioHost            = "localhost"
	DefaultMinioPort            = "9000"
	DefaultMinioAccessKey       = "minioadmin"
	DefaultMinioSecretAccessKey = "minioadmin"
	DefaultMinioUseSSL          = "false"
	DefaultMinioBucketName      = "a-bucket"
	DefaultMinioUseIAM          = "false"
	DefaultMinioCloudProvider   = "aws"
	DefaultMinioIAMEndpoint     = ""
	DefaultEtcdEndpoints        = "localhost:2379"

	DefaultLogFormat                         = "text"
	DefaultLogLevelForBase                   = "debug"
	DefaultRootPath                          = ""
	DefaultMinioLogLevel                     = "error"
	DefaultKnowhereThreadPoolNumRatioInBuild = 1
	DefaultMinioRegion                       = ""
	DefaultMinioUseVirtualHost               = "false"
)

// Const of Global Config List
func globalConfigPrefixs() []string {
	return []string{"metastore", "localStorage", "etcd", "minio", "pulsar", "kafka", "rocksmq", "log", "grpc", "common", "quotaAndLimits"}
}

var defaultYaml = []string{"milvus.yaml", "default.yaml", "user.yaml"}

// BaseTable the basics of paramtable
type BaseTable struct {
	once sync.Once
	mgr  *config.Manager

	configDir string
	YamlFiles []string
}

// NewBaseTableFromYamlOnly only used in migration tool.
// Maybe we shouldn't limit the configDir internally.
func NewBaseTableFromYamlOnly(yaml string) *BaseTable {
	mgr, _ := config.Init(config.WithFilesSource(&config.FileInfo{
		Files:           []string{yaml},
		RefreshInterval: 10 * time.Second,
	}))
	gp := &BaseTable{mgr: mgr, YamlFiles: []string{yaml}}
	return gp
}

// GlobalInitWithYaml initializes the param table with the given yaml.
// We will update the global DefaultYaml variable directly, once and for all.
// GlobalInitWithYaml shall be called at the very beginning before initiating the base table.
// GlobalInitWithYaml should be called only in standalone and embedded Milvus.
func (gp *BaseTable) GlobalInitWithYaml(yaml string) {
	gp.once.Do(func() {
		defaultYaml = []string{yaml}
	})
}

func (gp *BaseTable) UpdateSourceOptions(opts ...config.Option) {
	gp.mgr.UpdateSourceOptions(opts...)
}

// init initializes the param table.
// if refreshInterval greater than 0 will auto refresh config from source
func (gp *BaseTable) init(refreshInterval int) {
	formatter := func(key string) string {
		ret := strings.ToLower(key)
		ret = strings.TrimPrefix(ret, "milvus.")
		ret = strings.ReplaceAll(ret, "/", "")
		ret = strings.ReplaceAll(ret, "_", "")
		ret = strings.ReplaceAll(ret, ".", "")
		return ret
	}
	if len(gp.YamlFiles) == 0 {
		gp.YamlFiles = defaultYaml
	}
	var err error
	gp.mgr, err = config.Init(config.WithEnvSource(formatter))
	if err != nil {
		return
	}
	gp.initConfigsFromLocal(refreshInterval)
	gp.initConfigsFromRemote(refreshInterval)
}

func (gp *BaseTable) initConfigsFromLocal(refreshInterval int) {
	gp.configDir = gp.initConfPath()
	err := gp.mgr.AddSource(config.NewFileSource(&config.FileInfo{
		Files: lo.Map(gp.YamlFiles, func(file string, _ int) string {
			return path.Join(gp.configDir, file)
		}),
		RefreshInterval: time.Duration(refreshInterval) * time.Second,
	}))
	if err != nil {
		log.Warn("init baseTable with file failed", zap.Strings("configFile", gp.YamlFiles), zap.Error(err))
		return
	}
}

func (gp *BaseTable) initConfigsFromRemote(refreshInterval int) {
	etcdConfig := EtcdConfig{}
	etcdConfig.Init(gp)
	etcdConfig.Endpoints.PanicIfEmpty = false
	etcdConfig.RootPath.PanicIfEmpty = false
	if etcdConfig.Endpoints.GetValue() == "" {
		return
	}
	if etcdConfig.UseEmbedEtcd.GetAsBool() && !etcd.HasServer() {
		return
	}
	info := &config.EtcdInfo{
		UseEmbed:        etcdConfig.UseEmbedEtcd.GetAsBool(),
		UseSSL:          etcdConfig.EtcdUseSSL.GetAsBool(),
		Endpoints:       etcdConfig.Endpoints.GetAsStrings(),
		CertFile:        etcdConfig.EtcdTLSCert.GetValue(),
		KeyFile:         etcdConfig.EtcdTLSKey.GetValue(),
		CaCertFile:      etcdConfig.EtcdTLSCACert.GetValue(),
		MinVersion:      etcdConfig.EtcdTLSMinVersion.GetValue(),
		KeyPrefix:       etcdConfig.RootPath.GetValue(),
		RefreshInterval: time.Duration(refreshInterval) * time.Second,
	}

	s, err := config.NewEtcdSource(info)
	if err != nil {
		log.Info("init with etcd failed", zap.Error(err))
		return
	}
	gp.mgr.AddSource(s)
	s.SetEventHandler(gp.mgr)
}

// GetConfigDir returns the config directory
func (gp *BaseTable) GetConfigDir() string {
	return gp.configDir
}

func (gp *BaseTable) initConfPath() string {
	// check if user set conf dir through env
	configDir, err := gp.mgr.GetConfig("MILVUSCONF")
	if err != nil {
		runPath, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		configDir = runPath + "/configs"
		if _, err := os.Stat(configDir); err != nil {
			_, fpath, _, _ := runtime.Caller(0)
			configDir = path.Dir(fpath) + "/../../../configs"
		}
	}
	return configDir
}

func (gp *BaseTable) FileConfigs() map[string]string {
	return gp.mgr.FileConfigs()
}

// Load loads an object with @key.
func (gp *BaseTable) Load(key string) (string, error) {
	return gp.mgr.GetConfig(key)
}

func (gp *BaseTable) Get(key string) string {
	return gp.GetWithDefault(key, "")
}

// GetWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (gp *BaseTable) GetWithDefault(key, defaultValue string) string {
	if gp.mgr == nil {
		return defaultValue
	}

	str, err := gp.mgr.GetConfig(key)
	if err != nil {
		return defaultValue
	}
	return str
}

// Remove Config by key
func (gp *BaseTable) Remove(key string) error {
	gp.mgr.DeleteConfig(key)
	return nil
}

// Update Config
func (gp *BaseTable) Save(key, value string) error {
	gp.mgr.SetConfig(key, value)
	return nil
}

// Reset Config to default value
func (gp *BaseTable) Reset(key string) error {
	gp.mgr.ResetConfig(key)
	return nil
}
