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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
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
	return []string{"metastore", "localStorage", "etcd", "tikv", "minio", "pulsar", "kafka", "rocksmq", "log", "grpc", "common", "quotaAndLimits", "trace"}
}

// support read "milvus.yaml", "_test.yaml", "default.yaml", "user.yaml" as this order.
// order: milvus.yaml < _test.yaml < default.yaml < user.yaml, do not change the order below.
// Use _test.yaml only for test related purpose.
var defaultYaml = []string{defaultPrimaryConfigName, "_test.yaml", "default.yaml", "user.yaml"}

const (
	// defaultPrimaryConfigName is the primary configuration file of a stock
	// milvus binary.
	defaultPrimaryConfigName = "milvus.yaml"

	// MilvusPrimaryConfigEnvKey names the environment variable that replaces
	// the primary configuration file name for the process. See
	// PrimaryConfigName. Like MilvusConfigRefreshIntervalEnvKey it is read
	// directly with os.Getenv, before any config source exists; the env
	// source will also ingest it as an ordinary key (primaryconfig), which
	// nothing reads and which is harmless.
	MilvusPrimaryConfigEnvKey = "MILVUS_PRIMARY_CONFIG"
)

// primaryConfigName is the link-time primary configuration file name. A
// deployment form whose configuration contract names a different primary
// file bakes it in when it builds its binary:
//
//	go build -ldflags "-X github.com/milvus-io/milvus/pkg/v3/util/paramtable.primaryConfigName=kite.yaml"
//
// It is a variable and not a function call on purpose: the global paramtable
// is initialized by package-level initializers - internal/proxy, datacoord,
// rootcoord and the coordinator clients all declare `var Params =
// paramtable.Get()`, and Get calls Init - which run before any main or init
// function a form could write. Nothing a form calls at run time is early
// enough, so the name has to be decided before the process starts: at link
// time here, or in the environment (MilvusPrimaryConfigEnvKey), which takes
// precedence so an operator can override a build.
var primaryConfigName = defaultPrimaryConfigName

// resolvePrimaryConfigName returns the primary configuration file name for
// this process and whether it was set explicitly (link time or environment)
// rather than left at the default.
//
// It runs for EVERY table NewBaseTable builds, including one whose file list
// the Files option then replaces (NewBaseTableFromYamlOnly, the embedded
// build): the name is a process-wide declaration, and a bad one is refused
// at the first table whichever that is, rather than only at the first table
// that would have read the file. A name that coincides with another entry
// of the list (user.yaml, say) is accepted and loads that file twice, which
// is redundant but not wrong.
//
// The name is checked, because the failure mode of a bad one is not local:
// the file source rejects a present file with a non-yaml extension (and a
// directory, which is what an empty name resolves to), and that rejection
// drops EVERY local yaml source with only a warning logged. A bare file name
// with a .yaml/.yml extension is the whole contract; anything else is a
// wiring mistake and stops the process here, at the first paramtable, rather
// than at the first parameter read that comes back as a compiled-in default.
func resolvePrimaryConfigName() (name string, explicit bool) {
	name = primaryConfigName
	explicit = name != defaultPrimaryConfigName
	if fromEnv := os.Getenv(MilvusPrimaryConfigEnvKey); fromEnv != "" {
		name = fromEnv
		explicit = true
	}
	if name == "" || name != filepath.Base(name) {
		panic(fmt.Sprintf("paramtable: primary config name %q must be a bare file name", name))
	}
	if ext := filepath.Ext(name); ext != ".yaml" && ext != ".yml" {
		panic(fmt.Sprintf("paramtable: primary config name %q must end in .yaml or .yml", name))
	}
	return name, explicit
}

// PrimaryConfigName reports the primary configuration file name every
// paramtable of this process reads in milvus.yaml's position: the link-time
// primaryConfigName, overridden by MilvusPrimaryConfigEnvKey when set. The
// rest of the file list (_test.yaml, default.yaml, user.yaml) keeps its
// meaning whatever the primary is. A missing primary file is skipped exactly
// as a missing milvus.yaml is - the table then runs on the compiled-in
// defaults - with a warning when the name was set explicitly, since for a
// form that declared its own file a missing one is a packaging mistake.
//
// It panics on a name that is not a bare .yaml/.yml file name; see
// resolvePrimaryConfigName.
func PrimaryConfigName() string {
	name, _ := resolvePrimaryConfigName()
	return name
}

// defaultYamlFiles is the file list a table built without the Files option
// reads: the resolved primary name in place of milvus.yaml, then the rest of
// defaultYaml unchanged.
func defaultYamlFiles() []string {
	name, _ := resolvePrimaryConfigName()
	files := make([]string, 0, len(defaultYaml))
	files = append(files, name)
	files = append(files, defaultYaml[1:]...)
	return files
}

// BaseTable the basics of paramtable
type BaseTable struct {
	once   sync.Once
	mgr    *config.Manager
	config *baseTableConfig
}

type baseTableConfig struct {
	configDir       string
	refreshInterval time.Duration
	skipRemote      bool
	skipEnv         bool
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

// NewBaseTableFromYamlOnly only used in migration tool.
// Maybe we shouldn't limit the configDir internally.
func NewBaseTableFromYamlOnly(yaml string) *BaseTable {
	return NewBaseTable(Files([]string{yaml}), SkipRemote(true), SkipEnv(true))
}

func NewBaseTable(opts ...Option) *BaseTable {
	defaultConfig := &baseTableConfig{
		configDir:       initConfPath(),
		yamlFiles:       defaultYamlFiles(),
		refreshInterval: 5 * time.Second,
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

	if !bt.config.skipEnv {
		err := bt.mgr.AddSource(config.NewEnvSource(formatter))
		if err != nil {
			mlog.Warn(context.TODO(), "init baseTable with env failed", mlog.Err(err))
			return
		}
	}
	bt.initConfigsFromLocal()
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
			bt.warnIfExplicitPrimaryMissing(file)
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
		mlog.Warn(context.TODO(), "init baseTable with file failed", mlog.Strings("configFile", bt.config.yamlFiles), mlog.Err(err))
		return
	}
}

// warnIfExplicitPrimaryMissing logs when the file that is missing is a
// primary configuration file somebody set on purpose. A missing milvus.yaml
// is ordinary - tests and embedded builds run without one - but a form that
// named its own primary file and does not ship it is running on compiled-in
// defaults without knowing, and that is worth one line at start-up.
func (bt *BaseTable) warnIfExplicitPrimaryMissing(file string) {
	if len(bt.config.yamlFiles) == 0 || bt.config.yamlFiles[0] != file {
		return
	}
	if name, explicit := resolvePrimaryConfigName(); explicit && name == file {
		mlog.Warn(context.TODO(), "primary config file was set explicitly but is not present in the config directory, running on compiled-in defaults",
			mlog.String("file", file), mlog.String("configDir", bt.config.configDir))
	}
}

func (bt *BaseTable) initConfigsFromRemote() {
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
	info := &config.EtcdInfo{
		UseEmbed:        etcdConfig.UseEmbedEtcd.GetAsBool(),
		EnableAuth:      etcdConfig.EtcdEnableAuth.GetAsBool(),
		UserName:        etcdConfig.EtcdAuthUserName.GetValue(),
		PassWord:        etcdConfig.EtcdAuthPassword.GetValue(),
		UseSSL:          etcdConfig.EtcdUseSSL.GetAsBool(),
		Endpoints:       etcdConfig.Endpoints.GetAsStrings(),
		CertFile:        etcdConfig.EtcdTLSCert.GetValue(),
		KeyFile:         etcdConfig.EtcdTLSKey.GetValue(),
		CaCertFile:      etcdConfig.EtcdTLSCACert.GetValue(),
		MinVersion:      etcdConfig.EtcdTLSMinVersion.GetValue(),
		KeyPrefix:       etcdConfig.RootPath.GetValue(),
		DialTimeout:     etcdConfig.DialTimeout.GetAsDuration(time.Millisecond),
		RefreshInterval: refreshInterval,
	}

	s, err := config.NewEtcdSource(info)
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
