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
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/fips"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	DefaultLogFormat                                     = "text"
	DefaultLogLevelForBase                               = "debug"
	DefaultRootPath                                      = ""
	DefaultMinioLogLevel                                 = "fatal"
	DefaultKnowhereThreadPoolNumRatioInBuild             = 1
	DefaultKnowhereThreadPoolNumRatioInBuildOfStandalone = 0.75
	DefaultMinioRegion                                   = ""
	DefaultMinioUseVirtualHost                           = "false"
	DefaultMinioRequestTimeout                           = "10000"
	DefaultMinioMaxConnections                           = "100"
)

// Const of Global Config List
func globalConfigPrefixs() []string {
	return []string{"metastore", "localStorage", "etcd", "tikv", "minio", "pulsar", "kafka", "rocksmq", "log", "grpc", "common", "quotaAndLimits", "trace", "queryView"}
}

// Later files override earlier ones. Deployment and user settings take precedence
// over the optional test configuration shipped in the image.
func defaultConfigFiles(useTestConfig bool) []string {
	files := []string{"milvus.yaml"}
	if useTestConfig {
		files = append(files, config.TestConfigFile)
	}
	return append(files, "default.yaml", "user.yaml")
}

// BaseTable the basics of paramtable
type BaseTable struct {
	once   sync.Once
	mgr    *config.Manager
	config *baseTableConfig

	// etcdClient is the single etcd client created when remote config is
	// enabled. It is injected into the config etcd source and shared with other
	// etcd users (e.g. the version gate confirmator); it lives for the process
	// lifetime and is never closed by the source.
	etcdClient *clientv3.Client
}

type baseTableConfig struct {
	configDir       string
	refreshInterval time.Duration
	skipRemote      bool
	skipEnv         bool
	useTestConfig   bool
	yamlFiles       []string
}

type Option func(*baseTableConfig)

func Files(files []string) Option {
	return func(bt *baseTableConfig) {
		bt.yamlFiles = files
	}
}

func Interval(interval time.Duration) Option {
	return func(bt *baseTableConfig) {
		bt.refreshInterval = interval
	}
}

func SkipRemote(skip bool) Option {
	return func(bt *baseTableConfig) {
		bt.skipRemote = skip
	}
}

func SkipEnv(skip bool) Option {
	return func(bt *baseTableConfig) {
		bt.skipEnv = skip
	}
}

// NewBaseTableFromYamlOnly loads a single file from the configuration directory.
func NewBaseTableFromYamlOnly(yaml string) *BaseTable {
	return NewBaseTable(Files([]string{yaml}), SkipRemote(true), SkipEnv(true))
}

// LoadRequiredYAML loads one local configuration file and returns its
// read or parse error to callers that must not fall back to built-in defaults.
func LoadRequiredYAML(name string) (*BaseTable, error) {
	dir := initConfPath()
	manager := config.NewManager()
	source := config.NewFileSource(&config.FileInfo{Files: []string{path.Join(dir, name)}})
	if err := manager.AddSource(source); err != nil {
		manager.Close()
		return nil, err
	}
	return &BaseTable{mgr: manager, config: &baseTableConfig{configDir: dir}}, nil
}

func NewBaseTable(opts ...Option) *BaseTable {
	useTestConfig := os.Getenv("MILVUS_ENABLE_TEST_CONFIG") == "true"
	defaultConfig := &baseTableConfig{
		configDir:       initConfPath(),
		yamlFiles:       defaultConfigFiles(useTestConfig),
		useTestConfig:   useTestConfig,
		refreshInterval: defaultRefreshInterval,
		skipRemote:      false,
		skipEnv:         false,
	}
	for _, opt := range opts {
		opt(defaultConfig)
	}
	bt := &BaseTable{config: defaultConfig}
	bt.init()
	return bt
}

// init initializes the param table.
// if refreshInterval greater than 0 will auto refresh config from source
func (bt *BaseTable) init() {
	formatter := func(key string) string {
		ret := strings.ToLower(key)
		ret = strings.TrimPrefix(ret, "milvus.")
		ret = strings.ReplaceAll(ret, "/", "")
		ret = strings.ReplaceAll(ret, "_", "")
		ret = strings.ReplaceAll(ret, ".", "")
		return ret
	}

	var err error
	bt.mgr, err = config.Init()
	if err != nil {
		mlog.Error(context.TODO(), "failed to initialize config manager", mlog.Err(err))
		panic(err)
	}

	// Establish the initial file-backed group spellings before importing
	// environment overrides. A spelling learned only after an unsegmented
	// value exists must fail closed, as it does for later source refreshes.
	// Source priority still makes environment values override file values.
	bt.initConfigsFromLocal()
	if !bt.config.skipEnv {
		err := bt.mgr.AddSource(config.NewEnvSource(formatter))
		if err != nil {
			mlog.Warn(context.TODO(), "init baseTable with env failed", mlog.Err(err))
			return
		}
	}
	if !bt.config.skipRemote {
		bt.initConfigsFromRemote()
	}
}

func (bt *BaseTable) initConfigsFromLocal() {
	refreshInterval := bt.config.refreshInterval
	var files []string
	for _, file := range bt.config.yamlFiles {
		_, err := os.Stat(path.Join(bt.config.configDir, file))
		// not found
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			mlog.Warn(context.TODO(), "failed to check file", mlog.String("file", file), mlog.Err(err))
			panic(err)
		}
		files = append(files, path.Join(bt.config.configDir, file))
	}

	err := bt.mgr.AddSource(config.NewFileSource(&config.FileInfo{
		Files:           files,
		RefreshInterval: refreshInterval,
	}))
	if err != nil {
		mlog.Warn(context.TODO(), "init baseTable with file failed", mlog.Strings("configFile", bt.config.yamlFiles),
			mlog.String("error", config.RedactedValue))
		if bt.config.useTestConfig && slices.Contains(files, path.Join(bt.config.configDir, config.TestConfigFile)) {
			// The files publish as one source. A failure in any layer drops the test overrides.
			// Parser errors can include configuration values, so keep the startup diagnostic safe.
			panic("failed to load local configuration including _test.yaml")
		}
	}
}

// Tests replace these to observe a rejected FIPS check.
var (
	requireGoFIPS = fips.RequireGoModule
	exitProcess   = os.Exit
)

func (bt *BaseTable) initConfigsFromRemote() {
	// Package-level paramtable.Get() calls reach this point before the startup
	// check in cmd/milvus runs, and the etcd connection opened here is the
	// process's first. A MILVUS_FIPS_ENABLED=ON build must not open it outside
	// Go FIPS 140-3 mode.
	if err := requireGoFIPS(); err != nil {
		fmt.Fprintln(os.Stderr, "Milvus cannot start:", err)
		exitProcess(1)
		return
	}
	refreshInterval := bt.config.refreshInterval
	etcdConfig := EtcdConfig{}
	etcdConfig.Init(bt)
	etcdConfig.Endpoints.PanicIfEmpty = false
	etcdConfig.RootPath.PanicIfEmpty = false
	if etcdConfig.Endpoints.GetValue() == "" {
		return
	}
	if etcdConfig.UseEmbedEtcd.GetAsBool() && !etcd.HasServer() {
		return
	}
	etcdCli, err := etcd.CreateEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdEnableAuth.GetAsBool(),
		etcdConfig.EtcdAuthUserName.GetValue(),
		etcdConfig.EtcdAuthPassword.GetValue(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue(),
		etcd.WithDialTimeout(etcdConfig.DialTimeout.GetAsDuration(time.Millisecond)))
	if err != nil {
		// TLS and dial errors can contain protected paths, endpoints, or
		// transport settings. Keep those details in the in-process error only.
		mlog.Warn(context.TODO(), "init with etcd client failed", mlog.String("error", config.RedactedValue))
		return
	}
	bt.etcdClient = etcdCli

	info := &config.EtcdInfo{
		KeyPrefix:       etcdConfig.RootPath.GetValue(),
		RefreshInterval: refreshInterval,
	}
	s, err := config.NewEtcdSource(etcdCli, info)
	if err != nil {
		mlog.Info(context.TODO(), "init with etcd failed", mlog.Err(err))
		return
	}
	bt.mgr.AddSource(s)
	s.SetEventHandler(bt.mgr)
}

// GetConfigDir returns the config directory
func (bt *BaseTable) GetConfigDir() string {
	return bt.config.configDir
}

func initConfPath() string {
	// check if user set conf dir through env
	configDir := os.Getenv("MILVUSCONF")
	if len(configDir) != 0 {
		return configDir
	}
	runPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	configDir = runPath + "/configs"
	if _, err := os.Stat(configDir); err != nil {
		_, fpath, _, _ := runtime.Caller(0)
		configDir = path.Dir(fpath) + "/../../../configs"
	}
	return configDir
}

func (bt *BaseTable) FileConfigs() map[string]string {
	return bt.mgr.FileConfigs()
}

func (bt *BaseTable) UpdateSourceOptions(opts ...config.Option) {
	bt.mgr.UpdateSourceOptions(opts...)
}

// Load loads an object with @key.
func (bt *BaseTable) Load(key string) (string, error) {
	_, v, err := bt.mgr.GetConfig(key)
	return v, err
}

func (bt *BaseTable) Get(key string) string {
	return bt.GetWithDefault(key, "")
}

// GetWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (bt *BaseTable) GetWithDefault(key, defaultValue string) string {
	if bt.mgr == nil {
		return defaultValue
	}

	_, str, err := bt.mgr.GetConfig(key)
	if err != nil {
		return defaultValue
	}
	return str
}

// Remove Config by key
func (bt *BaseTable) Remove(key string) error {
	normalizedKey := strings.ToLower(strings.TrimPrefix(key, "milvus."))
	bt.mgr.DeleteConfig(key)
	bt.mgr.EvictCachedValue(key)
	// Fire runtime config update event so watchers (e.g. tracing) can react immediately.
	bt.mgr.OnEvent(&config.Event{
		EventSource: config.RuntimeSource,
		EventType:   config.DeleteType,
		Key:         normalizedKey,
		Value:       "",
		HasUpdated:  true,
	})
	return nil
}

// Update Config
func (bt *BaseTable) Save(key, value string) error {
	normalizedKey := strings.ToLower(strings.TrimPrefix(key, "milvus."))
	bt.mgr.SetConfig(key, value)
	bt.mgr.EvictCachedValue(key)
	// Fire runtime config update event so watchers (e.g. tracing) can react immediately.
	bt.mgr.OnEvent(&config.Event{
		EventSource: config.RuntimeSource,
		EventType:   config.UpdateType,
		Key:         normalizedKey,
		Value:       value,
		HasUpdated:  true,
	})
	return nil
}

func (bt *BaseTable) SaveGroup(group map[string]string) error {
	for key, value := range group {
		normalizedKey := strings.ToLower(strings.TrimPrefix(key, "milvus."))
		bt.mgr.SetMapConfig(key, value)
		// Fire runtime config update event so watchers (e.g. tracing) can react immediately.
		bt.mgr.OnEvent(&config.Event{
			EventSource: config.RuntimeSource,
			EventType:   config.UpdateType,
			Key:         normalizedKey,
			Value:       value,
			HasUpdated:  true,
		})
	}
	return nil
}

// Reset Config to default value
func (bt *BaseTable) Reset(key string) error {
	normalizedKey := strings.ToLower(strings.TrimPrefix(key, "milvus."))
	bt.mgr.ResetConfig(key)
	bt.mgr.EvictCachedValue(key)
	// Fire runtime config update event so watchers can refresh their derived state.
	// If the key is not found in any source after reset, emit a DELETE event.
	if _, v, err := bt.mgr.GetConfig(key); err == nil {
		bt.mgr.OnEvent(&config.Event{
			EventSource: config.RuntimeSource,
			EventType:   config.UpdateType,
			Key:         normalizedKey,
			Value:       v,
			HasUpdated:  true,
		})
	} else {
		bt.mgr.OnEvent(&config.Event{
			EventSource: config.RuntimeSource,
			EventType:   config.DeleteType,
			Key:         normalizedKey,
			Value:       "",
			HasUpdated:  true,
		})
	}
	return nil
}

func (bt *BaseTable) Manager() *config.Manager {
	return bt.mgr
}
