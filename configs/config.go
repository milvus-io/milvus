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

// This file is not used for now.
package configs

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/BurntSushi/toml"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"go.uber.org/zap"
)

const MinToleranceTime = 3600

type Config struct {
	Etcd                      Etcd             `toml:"etcd" json:"etcd"`
	Minio                     Minio            `toml:"minio" json:"minio"`
	Pulsar                    Pulsar           `toml:"pulsar" json:"pulsar"`
	RocksMq                   RocksMq          `toml:"rocksmq" json:"rocksmq"`
	Grpc                      Grpc             `toml:"grpc" json:"grpc"`
	FlowGraph                 FlowGraph        `toml:"flowgraph" json:"flowgrph"`
	RootCoord                 RootCoord        `toml:"rootcoord" json:"rootcoord"`
	Proxy                     Proxy            `toml:"proxy" json:"proxy"`
	QueryCoord                QueryCoord       `toml:"querycoord" json:"querycoord"`
	QueryNode                 QueryNode        `toml:"querynode" json:"querynode"`
	IndexCoord                IndexCoord       `toml:"indexcoord" json:"indexcoord"`
	IndexNode                 IndexNode        `toml:"indexnode" json:"indexnode"`
	DataCoord                 DataCoord        `toml:"datacoord" json:"datacoord"`
	DataNode                  DataNode         `toml:"datanode" json:"datanode"`
	TimeTickInterval          uint64           `toml:"timetick-interval" json:"timetick-interval"`
	TimeTickBufferSize        uint32           `toml:"timetick-buffer-size" json:"timetick-buffer-size"`
	NameLengthLimit           uint32           `toml:"name-length-limit" json:"name-length-limit"`
	FieldCountLimit           uint32           `toml:"field-count-limit" json:"field-count-limit"`
	DimensionLimit            uint32           `toml:"dimension-limit" json:"dimension-limit"`
	ShardCountLimit           uint32           `toml:"shard-count-limit" json:"shard-count-limit"`
	DMLChannelCount           uint32           `toml:"dml-channel-count" json:"dml-channel-count"`
	PartitionCountLimit       uint32           `toml:"partition-count-limit" json:"partition-count-limit"`
	EnableIndexMinSegmentSize uint32           `toml:"enable-index-min-segment-size" json:"enable-index-min-segment-size"`
	SIMDType                  string           `toml:"simd-type" json:"simd-type"`
	Compaction                Compaction       `toml:"compaction" json:"compaction"`
	Log                       Log              `toml:"log" json:"log"`
	LocalStorage              LocalStorage     `toml:"localstorage" json:"localstorage"`
	SkipQueryChannelRecover   bool             `toml:"skip-query-channel-recover" json:"skip-query-channel-recover"`
	Metrics                   Metrics          `toml:"" json:"metrics"`
	GarbageCollector          GarbageCollector `toml:"gc" json:"gc"`
}

type Etcd struct {
	Endpoints  []string `toml:"endpoints" json:"endpoints"`
	UseEmbed   bool     `toml:"use-embed" json:"use-embed"`
	ConfigPath string   `toml:"config-path" json:"config-path"`
	DataPath   string   `toml:"data-path" json:"data-path"`
}

type Minio struct {
	Address         string `toml:"address" json:"address"`
	Port            int    `toml:"port" json:"port"`
	AccessKeyID     string `toml:"access-key-id" json:"access-key-id"`
	SecretAccessKey string `toml:"secret-access-key" json:"secret-access-key"`
	UseSSL          bool   `toml:"use-ssl" json:"use-ssl"`
	BucketName      string `toml:"bucket-name" json:"bucket-name"`
	RootPath        string `toml:"root-path" json:"root-path"`
}

type Pulsar struct {
	Address        string `toml:"address" json:"address"`
	Port           int    `toml:"port" json:"port"`
	MaxMessageSize uint64 `toml:"max-message-size" json:"max-message-size"`
}

type RocksMq struct {
	Path              string `toml:"path" json:"path"`
	PageSize          uint32 `toml:"page-size" json:"page-size"`
	RetentionDuration uint32 `toml:"retention-duration" json:"retention-duration"`
	RetentionSize     uint32 `toml:"retention-size" json:"retention-size"`
}

type Grpc struct {
	ServerMaxReceiveSize uint64 `toml:"server-max-receive-size" json:"server-max-receive-size"`
	ServerMaxSendSize    uint64 `toml:"server-max-send-size" json:"server-max-send-size"`
	ClientMaxReceiveSize uint64 `toml:"client-max-receive-size" json:"client-max-receive-size"`
	ClientMaxSendSize    uint64 `toml:"client-max-send-size" json:"client-max-send-size"`
}

type RootCoord struct {
	Address string `toml:"address" json:"address"`
	Port    int    `toml:"port" json:"port"`
}

type Proxy struct {
	Port         int    `toml:"port" json:"port"`
	MaxTaskCount uint32 `toml:"max-task-count" json:"max-task-count"`
}

type QueryCoord struct {
	Address                        string `toml:"address" json:"address"`
	Port                           int    `toml:"port" json:"port"`
	AutoHandOff                    bool   `toml:"auto-handoff" json:"auto-handoff"`
	AutoBalance                    bool   `toml:"auto-balance" json:"auto-balance"`
	BalanceInterval                uint32 `toml:"balance-interval" json:"balance-interval"`
	MemoryUsageLimitRatio          uint32 `toml:"memory-usage-limit-ratio" json:"memory-usage-limit-ratio"`
	AutoBalanceMemoryUsageGapRatio uint32 `toml:"auto-balance-memory-usage-gap-ratio" json:"auto-balance-memory-usage-gap-ratio"`
}

type FlowGraph struct {
	QueueLengthLimit uint32 `toml:"queue-length-limit" json:"queue-length-limit"`
	ParallelismLimit uint32 `toml:"parallelism-limit" json:"parallelism-limit"`
}

type QueryNode struct {
	Port                 int    `toml:"port" json:"port"`
	GracefulTime         uint32 `toml:"graceful-time" json:"graceful-time"`
	StatsPublishInterval uint32 `toml:"stats-publish-interval" json:"stats-publish-interval"`
	SegcoreChunkRows     uint32 `toml:"segcore-chunk-rows" json:"segcore-chunk-rows"`
}

type IndexCoord struct {
	Address string `toml:"address" json:"address"`
	Port    int    `toml:"port" json:"port"`
}

type IndexNode struct {
	Port int `toml:"port" json:"port"`
}

type DataCoord struct {
	Address string `toml:"address" json:"address"`
	Port    int    `toml:"port" json:"port"`
}

type GarbageCollector struct {
	Interval             uint32 `toml:"interval" json:"interval"`
	MissedFileTolerance  uint32 `toml:"missed-files-tolerance" json:"missed-files-tolerance"`
	DroppedFileTolerance uint32 `toml:"dropped-files-tolerance" json:"dropped-files-tolerance"`
}

type Compaction struct {
	EnableCompaction bool `toml:"enable-compaction" json:"enable-compaction"`
}

type DataNode struct {
	Port                  int    `toml:"port" json:"port"`
	InsertBufferSizeLimit uint32 `toml:"insert-buffer-size-limit" json:"insert-buffer-size-limit"`
}

type LocalStorage struct {
	Enable bool   `toml:"enable" json:"enable"`
	Path   string `toml:"path" json:"path"`
}

type Log struct {
	Level  string     `toml:"level" json:"level"`
	Format string     `toml:"format" json:"format"`
	File   LogFileCfg `toml:"file" json:"file"`
}

type LogFileCfg struct {
	RootPath   string `toml:"root-path" json:"root-path"`
	MaxSize    uint32 `toml:"max-size" json:"max-size"`
	MaxAge     uint32 `toml:"max-age" json:"max-age"`
	MaxBackups uint32 `toml:"max-backups" json:"max-backups"`
}

type Metrics struct {
	GitCommit    string `toml:"" json:"git-commit-key"`
	DeployMode   string `toml:"" json:"deploy-mode"`
	GitBuildTags string `toml:"" json:"git-build-tags"`
	BuildTime    string `toml:"" json:"build-time"`
	GoVersion    string `toml:"" json:"go-version"`
}

var defaultCfg = Config{
	Etcd: Etcd{
		Endpoints: []string{"localhost:2379"},
		UseEmbed:  false,
	},
	Minio: Minio{
		Address:         "localhost",
		Port:            9000,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UseSSL:          false,
		BucketName:      "a-bucket",
		RootPath:        "files",
	},
	Pulsar: Pulsar{
		Address:        "localhost",
		Port:           6650,
		MaxMessageSize: 5242880,
	},
	RocksMq: RocksMq{
		Path:              "/var/lib/milvus/rdb_data",
		PageSize:          2147483648,
		RetentionDuration: 10080,
		RetentionSize:     8192,
	},
	Grpc: Grpc{
		ServerMaxReceiveSize: 2147483647,
		ServerMaxSendSize:    2147483647,
		ClientMaxReceiveSize: 104857600,
		ClientMaxSendSize:    104857600,
	},
	FlowGraph: FlowGraph{
		QueueLengthLimit: 1024,
		ParallelismLimit: 1024,
	},
	RootCoord: RootCoord{
		Address: "localhost",
		Port:    53100,
	},
	Proxy: Proxy{
		Port:         53100,
		MaxTaskCount: 1024,
	},
	QueryCoord: QueryCoord{
		Address:                        "localhost",
		Port:                           19531,
		AutoHandOff:                    true,
		AutoBalance:                    true,
		BalanceInterval:                60,
		MemoryUsageLimitRatio:          90,
		AutoBalanceMemoryUsageGapRatio: 30,
	},
	QueryNode: QueryNode{
		Port:                 21123,
		GracefulTime:         0,
		StatsPublishInterval: 1000,
		SegcoreChunkRows:     32768,
	},
	IndexCoord: IndexCoord{
		Address: "localhost",
		Port:    31000,
	},
	IndexNode: IndexNode{
		Port: 21121,
	},
	DataCoord: DataCoord{
		Address: "localhost",
		Port:    13333,
	},
	DataNode: DataNode{
		Port:                  21124,
		InsertBufferSizeLimit: 16777216,
	},
	TimeTickInterval:          200,
	TimeTickBufferSize:        512,
	NameLengthLimit:           255,
	FieldCountLimit:           256,
	DimensionLimit:            32768,
	ShardCountLimit:           256,
	DMLChannelCount:           256,
	PartitionCountLimit:       4096,
	EnableIndexMinSegmentSize: 1024,
	SIMDType:                  "auto",
	Compaction: Compaction{
		EnableCompaction: true,
	},
	Log: Log{
		Level:  "debug",
		Format: "text",
		File: LogFileCfg{
			RootPath:   "",
			MaxSize:    300,
			MaxAge:     10,
			MaxBackups: 20,
		},
	},
	LocalStorage: LocalStorage{
		Enable: true,
		Path:   "/var/lib/milvus/data/",
	},
	SkipQueryChannelRecover: false,
	Metrics: Metrics{
		GitCommit:    "",
		DeployMode:   "CLUSTER",
		GitBuildTags: "",
		GoVersion:    "",
		BuildTime:    "",
	},
	GarbageCollector: GarbageCollector{
		Interval:             3600,
		MissedFileTolerance:  86400,
		DroppedFileTolerance: 86400,
	},
}

var globalConfig atomic.Value

func init() {
	cfg := defaultCfg
	SetGlobalConfig(&cfg)
}

func SetGlobalConfig(cfg *Config) {
	globalConfig.Store(cfg)
}

func GetGlobalConfig() *Config {
	return globalConfig.Load().(*Config)
}

func InitializeConfig(path string, enforceEnvCfg func(c *Config), enforceCmdCfg func(c *Config)) {
	cfg := GetGlobalConfig()
	if path != "" {
		err := cfg.Load(path)
		if err != nil {
			if _, ok := err.(*ErrUndecodedConfig); ok {
				log.Warn(err.Error())
			} else {
				log.Fatal("failed to load config", zap.String("filePath", path), zap.Error(err))
			}
		}
	}
	if enforceEnvCfg != nil {
		enforceEnvCfg(cfg)
	}
	if enforceCmdCfg != nil {
		enforceCmdCfg(cfg)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatal("config is not valid, please check the config file, environment variables and command options", zap.Error(err))
	}
}

func (c *Config) Load(path string) error {
	meta, err := toml.DecodeFile(path, c)

	undecodedKeys := meta.Undecoded()
	if len(undecodedKeys) > 0 && err == nil {
		undecoded := make([]string, 0, len(undecodedKeys))
		for _, k := range undecodedKeys {
			undecoded = append(undecoded, k.String())
		}
		return &ErrUndecodedConfig{ConfigFile: path, Undecoded: undecoded}
	}

	return err
}

func (c *Config) Validate() error {
	if c.Etcd.UseEmbed && c.Metrics.DeployMode != metricsinfo.StandaloneDeployMode {
		return fmt.Errorf("can not use embed etcd in mode %s", c.Metrics.DeployMode)
	}
	if c.GarbageCollector.MissedFileTolerance < MinToleranceTime {
		return fmt.Errorf("gc: missed file tolerance time can not be less than %d", MinToleranceTime)
	}
	if c.GarbageCollector.DroppedFileTolerance < MinToleranceTime {
		return fmt.Errorf("gc: dropped file tolerance time can not be less than %d", MinToleranceTime)
	}
	return nil
}

type ErrUndecodedConfig struct {
	ConfigFile string
	Undecoded  []string
}

func (e *ErrUndecodedConfig) Error() string {
	return fmt.Sprintf("config file %s contains invalid configuration options: %s", e.ConfigFile, strings.Join(e.Undecoded, ","))
}
