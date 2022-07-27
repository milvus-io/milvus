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
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	config "github.com/milvus-io/milvus/internal/config"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// UniqueID is type alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

const (
	DefaultMilvusYaml           = "milvus.yaml"
	DefaultEasyloggingYaml      = "easylogging.yaml"
	DefaultMinioHost            = "localhost"
	DefaultMinioPort            = "9000"
	DefaultMinioAccessKey       = "minioadmin"
	DefaultMinioSecretAccessKey = "minioadmin"
	DefaultMinioUseSSL          = "false"
	DefaultMinioBucketName      = "a-bucket"
	DefaultMinioUseIAM          = "false"
	DefaultMinioIAMEndpoint     = ""
	DefaultEtcdEndpoints        = "localhost:2379"
	DefaultInsertBufferSize     = "16777216"
	DefaultEnvPrefix            = "milvus"
)

var defaultYaml = DefaultMilvusYaml

// Base abstracts BaseTable
// TODO: it's never used, consider to substitute BaseTable or to remove it
type Base interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) ([]string, []string, error)
	LoadYaml(fileName string) error
	Remove(key string) error
	Save(key, value string) error
	Init()
}

// BaseTable the basics of paramtable
type BaseTable struct {
	once sync.Once
	mgr  *config.Manager
	// params    *memkv.MemoryKV

	configDir string

	RoleName   string
	Log        log.Config
	LogCfgFunc func(log.Config)
}

// GlobalInitWithYaml initializes the param table with the given yaml.
// We will update the global DefaultYaml variable directly, once and for all.
// GlobalInitWithYaml shall be called at the very beginning before initiating the base table.
// GlobalInitWithYaml should be called only in standalone and embedded Milvus.
func (gp *BaseTable) GlobalInitWithYaml(yaml string) {
	gp.once.Do(func() {
		defaultYaml = yaml
		gp.Init()
	})
}

// Init initializes the param table.
func (gp *BaseTable) Init() {
	var err error
	formatter := func(key string) string {
		ret := strings.ToLower(key)
		ret = strings.TrimPrefix(ret, "milvus.")
		ret = strings.ReplaceAll(ret, "/", "")
		ret = strings.ReplaceAll(ret, "_", "")
		ret = strings.ReplaceAll(ret, ".", "")
		return ret
	}
	gp.mgr, err = config.Init(config.WithEnvSource(formatter))
	if err != nil {
		return
	}

	gp.configDir = gp.initConfPath()
	configFilePath := gp.configDir + "/" + defaultYaml
	gp.mgr, err = config.Init(config.WithEnvSource(formatter), config.WithFilesSource(configFilePath))
	if err != nil {
		log.Warn("init baseTable with file failed", zap.String("configFile", configFilePath), zap.Error(err))
		return
	}
	defer gp.InitLogCfg()

	endpoints, err := gp.mgr.GetConfig("etcd.endpoints")
	if err != nil {
		log.Info("cannot find etcd.endpoints")
		return
	}
	rootPath, err := gp.mgr.GetConfig("etcd.rootPath")
	if err != nil {
		log.Info("cannot find etcd.rootPath")
		return
	}
	gp.mgr, err = config.Init(config.WithEnvSource(formatter),
		config.WithFilesSource(configFilePath),
		config.WithEtcdSource(&config.EtcdInfo{
			Endpoints:       strings.Split(endpoints, ","),
			KeyPrefix:       rootPath,
			RefreshMode:     config.ModeInterval,
			RefreshInterval: 10 * time.Second,
		}))
	if err != nil {
		log.Info("init with etcd failed", zap.Error(err))
		return
	}
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
		configDir = runPath + "/configs/"
		if _, err := os.Stat(configDir); err != nil {
			_, fpath, _, _ := runtime.Caller(0)
			configDir = path.Dir(fpath) + "/../../../configs/"
		}
	}
	return configDir
}

// Load loads an object with @key.
func (gp *BaseTable) Load(key string) (string, error) {
	return gp.mgr.GetConfig(key)
}

// LoadWithPriority loads an object with multiple @keys, return the first successful value.
// If all keys not exist, return error.
// This is to be compatible with old configuration file.
func (gp *BaseTable) LoadWithPriority(keys []string) (string, error) {
	for _, key := range keys {
		if str, err := gp.mgr.GetConfig(key); err == nil {
			return str, nil
		}
	}
	return "", fmt.Errorf("invalid keys: %v", keys)
}

// LoadWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (gp *BaseTable) LoadWithDefault(key, defaultValue string) string {
	str, err := gp.mgr.GetConfig(key)
	if err != nil {
		return defaultValue
	}
	return str
}

// LoadWithDefault2 loads an object with multiple @keys, return the first successful value.
// If all keys not exist, return @defaultValue.
// This is to be compatible with old configuration file.
func (gp *BaseTable) LoadWithDefault2(keys []string, defaultValue string) string {
	for _, key := range keys {
		str, err := gp.mgr.GetConfig(key)
		if err == nil {
			return str
		}
	}
	return defaultValue
}

func (gp *BaseTable) Get(key string) string {
	value, err := gp.mgr.GetConfig(key)
	if err != nil {
		return ""
	}
	return value
}

func (gp *BaseTable) GetByPattern(pattern string) map[string]string {
	return gp.mgr.GetConfigsByPattern(pattern)
}

// For compatible reason, only visiable for Test
func (gp *BaseTable) Remove(key string) error {
	gp.mgr.DeleteConfig(key)
	return nil
}

// For compatible reason, only visiable for Test
func (gp *BaseTable) Save(key, value string) error {
	gp.mgr.SetConfig(key, value)
	return nil
}

func (gp *BaseTable) ParseBool(key string, defaultValue bool) bool {
	valueStr := gp.LoadWithDefault(key, strconv.FormatBool(defaultValue))
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseFloat(key string) float64 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseFloatWithDefault(key string, defaultValue float64) float64 {
	valueStr := gp.LoadWithDefault(key, fmt.Sprintf("%f", defaultValue))
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt64(key string) int64 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt64WithDefault(key string, defaultValue int64) int64 {
	valueStr := gp.LoadWithDefault(key, strconv.FormatInt(defaultValue, 10))
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt32(key string) int32 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseInt(valueStr, 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(value)
}

func (gp *BaseTable) ParseInt32WithDefault(key string, defaultValue int32) int32 {
	valueStr := gp.LoadWithDefault(key, strconv.FormatInt(int64(defaultValue), 10))
	value, err := strconv.ParseInt(valueStr, 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(value)
}

func (gp *BaseTable) ParseInt(key string) int {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseIntWithDefault(key string, defaultValue int) int {
	valueStr := gp.LoadWithDefault(key, strconv.FormatInt(int64(defaultValue), 10))
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

// package methods
// ConvertRangeToIntRange converts a range of strings to a range of ints.
func ConvertRangeToIntRange(rangeStr, sep string) []int {
	items := strings.Split(rangeStr, sep)
	if len(items) != 2 {
		panic("Illegal range ")
	}

	startStr := items[0]
	endStr := items[1]
	start, err := strconv.Atoi(startStr)
	if err != nil {
		panic(err)
	}
	end, err := strconv.Atoi(endStr)
	if err != nil {
		panic(err)
	}

	if start < 0 || end < 0 {
		panic("Illegal range value")
	}
	if start > end {
		panic("Illegal range value, start > end")
	}
	return []int{start, end}
}

// ConvertRangeToIntSlice convert given @rangeStr & @sep to a slice of ints.
func ConvertRangeToIntSlice(rangeStr, sep string) []int {
	rangeSlice := ConvertRangeToIntRange(rangeStr, sep)
	start, end := rangeSlice[0], rangeSlice[1]
	var ret []int
	for i := start; i < end; i++ {
		ret = append(ret, i)
	}
	return ret
}

// InitLogCfg init log of the base table
func (gp *BaseTable) InitLogCfg() {
	gp.Log = log.Config{}
	format, err := gp.Load("log.format")
	if err != nil {
		panic(err)
	}
	gp.Log.Format = format
	level, err := gp.Load("log.level")
	if err != nil {
		panic(err)
	}
	gp.Log.Level = level
	gp.Log.File.MaxSize = gp.ParseInt("log.file.maxSize")
	gp.Log.File.MaxBackups = gp.ParseInt("log.file.maxBackups")
	gp.Log.File.MaxDays = gp.ParseInt("log.file.maxAge")
}

// SetLogConfig set log config of the base table
func (gp *BaseTable) SetLogConfig() {
	gp.LogCfgFunc = func(cfg log.Config) {
		var err error
		grpclog, err := gp.Load("grpc.log.level")
		if err != nil {
			cfg.GrpcLevel = DefaultLogLevel
		} else {
			cfg.GrpcLevel = strings.ToUpper(grpclog)
		}
		logutil.SetupLogger(&cfg)
		defer log.Sync()
	}
}

// SetLogger sets the logger file by given id
func (gp *BaseTable) SetLogger(id UniqueID) {
	rootPath, err := gp.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if rootPath != "" {
		if id < 0 {
			gp.Log.File.Filename = path.Join(rootPath, gp.RoleName+".log")
		} else {
			gp.Log.File.Filename = path.Join(rootPath, gp.RoleName+"-"+strconv.FormatInt(id, 10)+".log")
		}
	} else {
		gp.Log.File.Filename = ""
	}

	if gp.LogCfgFunc != nil {
		gp.LogCfgFunc(gp.Log)
	}
}

// func (gp *BaseTable) loadKafkaConfig() {
// 	brokerList := os.Getenv("KAFKA_BROKER_LIST")
// 	if brokerList == "" {
// 		brokerList = gp.Get("kafka.brokerList")
// 	}
// 	gp.Save("_KafkaBrokerList", brokerList)
// }

// func (gp *BaseTable) loadPulsarConfig() {
// 	pulsarAddress := os.Getenv("PULSAR_ADDRESS")
// 	if pulsarAddress == "" {
// 		pulsarHost := gp.Get("pulsar.address")
// 		port := gp.Get("pulsar.port")
// 		if len(pulsarHost) != 0 && len(port) != 0 {
// 			pulsarAddress = "pulsar://" + pulsarHost + ":" + port
// 		}
// 	}
// 	gp.Save("_PulsarAddress", pulsarAddress)

// 	// parse pulsar address to find the host
// 	pulsarURL, err := url.ParseRequestURI(pulsarAddress)
// 	if err != nil {
// 		gp.Save("_PulsarWebAddress", "")
// 		log.Info("failed to parse pulsar config, assume pulsar not used", zap.Error(err))
// 		return
// 	}
// 	webport := gp.LoadWithDefault("pulsar.webport", "80")
// 	pulsarWebAddress := "http://" + pulsarURL.Hostname() + ":" + webport
// 	gp.Save("_PulsarWebAddress", pulsarWebAddress)
// 	log.Info("Pulsar config", zap.String("pulsar url", pulsarAddress), zap.String("pulsar web url", pulsarWebAddress))
// }

// func (gp *BaseTable) loadRocksMQConfig() {
// 	rocksmqPath := os.Getenv("ROCKSMQ_PATH")
// 	if rocksmqPath == "" {
// 		rocksmqPath = gp.Get("rocksmq.path")
// 	}
// 	gp.Save("_RocksmqPath", rocksmqPath)
// }

// func (gp *BaseTable) loadMQConfig() {
// 	gp.loadPulsarConfig()
// 	gp.loadKafkaConfig()
// 	gp.loadRocksMQConfig()
// }

// func (gp *BaseTable) loadEtcdConfig() {
// 	etcdEndpoints := os.Getenv("ETCD_ENDPOINTS")
// 	if etcdEndpoints == "" {
// 		etcdEndpoints = gp.LoadWithDefault("etcd.endpoints", DefaultEtcdEndpoints)
// 	}
// 	gp.Save("_EtcdEndpoints", etcdEndpoints)
// }

// func (gp *BaseTable) loadMinioConfig() {
// 	minioAddress := os.Getenv("MINIO_ADDRESS")
// 	if minioAddress == "" {
// 		minioHost := gp.LoadWithDefault("minio.address", DefaultMinioHost)
// 		port := gp.LoadWithDefault("minio.port", DefaultMinioPort)
// 		minioAddress = minioHost + ":" + port
// 	}
// 	gp.Save("_MinioAddress", minioAddress)

// 	minioAccessKey := os.Getenv("MINIO_ACCESS_KEY")
// 	if minioAccessKey == "" {
// 		minioAccessKey = gp.LoadWithDefault("minio.accessKeyID", DefaultMinioAccessKey)
// 	}
// 	gp.Save("_MinioAccessKeyID", minioAccessKey)

// 	minioSecretKey := os.Getenv("MINIO_SECRET_KEY")
// 	if minioSecretKey == "" {
// 		minioSecretKey = gp.LoadWithDefault("minio.secretAccessKey", DefaultMinioSecretAccessKey)
// 	}
// 	gp.Save("_MinioSecretAccessKey", minioSecretKey)

// 	minioUseSSL := os.Getenv("MINIO_USE_SSL")
// 	if minioUseSSL == "" {
// 		minioUseSSL = gp.LoadWithDefault("minio.useSSL", DefaultMinioUseSSL)
// 	}
// 	gp.Save("_MinioUseSSL", minioUseSSL)

// 	minioBucketName := os.Getenv("MINIO_BUCKET_NAME")
// 	if minioBucketName == "" {
// 		minioBucketName = gp.LoadWithDefault("minio.bucketName", DefaultMinioBucketName)
// 	}
// 	gp.Save("_MinioBucketName", minioBucketName)

// 	minioUseIAM := os.Getenv("MINIO_USE_IAM")
// 	if minioUseIAM == "" {
// 		minioUseIAM = gp.LoadWithDefault("minio.useIAM", DefaultMinioUseIAM)
// 	}
// 	gp.Save("_MinioUseIAM", minioUseIAM)

// 	minioIAMEndpoint := os.Getenv("MINIO_IAM_ENDPOINT")
// 	if minioIAMEndpoint == "" {
// 		minioIAMEndpoint = gp.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint)
// 	}
// 	gp.Save("_MinioIAMEndpoint", minioIAMEndpoint)
// }

// func (gp *BaseTable) loadDataNodeConfig() {
// 	insertBufferFlushSize := os.Getenv("DATA_NODE_IBUFSIZE")
// 	if insertBufferFlushSize == "" {
// 		insertBufferFlushSize = gp.LoadWithDefault("datanode.flush.insertBufSize", DefaultInsertBufferSize)
// 	}
// 	gp.Save("_DATANODE_INSERTBUFSIZE", insertBufferFlushSize)
// }

// func (gp *BaseTable) loadOtherEnvs() {
// 	// try to load environment start with ENV_PREFIX
// 	for _, e := range os.Environ() {
// 		parts := strings.SplitN(e, "=", 2)
// 		if strings.Contains(parts[0], DefaultEnvPrefix) {
// 			parts := strings.SplitN(e, "=", 2)
// 			// remove the ENV PREFIX and use the rest as key
// 			keyParts := strings.SplitAfterN(parts[0], ".", 2)
// 			// mem kv throw no errors
// 			gp.Save(keyParts[1], parts[1])
// 		}
// 	}
// }
