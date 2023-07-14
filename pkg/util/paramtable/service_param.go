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
	"encoding/json"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
)

const (
	// SuggestPulsarMaxMessageSize defines the maximum size of Pulsar message.
	SuggestPulsarMaxMessageSize = 5 * 1024 * 1024
	defaultEtcdLogLevel         = "info"
	defaultEtcdLogPath          = "stdout"
	KafkaProducerConfigPrefix   = "kafka.producer."
	KafkaConsumerConfigPrefix   = "kafka.consumer."
)

// ServiceParam is used to quickly and easily access all basic service configurations.
type ServiceParam struct {
	BaseTable

	LocalStorageCfg LocalStorageConfig
	MetaStoreCfg    MetaStoreConfig
	EtcdCfg         EtcdConfig
	DBCfg           MetaDBConfig
	MQCfg           MQConfig
	PulsarCfg       PulsarConfig
	KafkaCfg        KafkaConfig
	RocksmqCfg      RocksmqConfig
	NatsmqCfg       NatsmqConfig
	MinioCfg        MinioConfig
}

func (p *ServiceParam) init() {
	p.BaseTable.init(2)

	p.LocalStorageCfg.Init(&p.BaseTable)
	p.MetaStoreCfg.Init(&p.BaseTable)
	p.EtcdCfg.Init(&p.BaseTable)
	p.DBCfg.Init(&p.BaseTable)
	p.MQCfg.Init(&p.BaseTable)
	p.PulsarCfg.Init(&p.BaseTable)
	p.KafkaCfg.Init(&p.BaseTable)
	p.RocksmqCfg.Init(&p.BaseTable)
	p.NatsmqCfg.Init(&p.BaseTable)
	p.MinioCfg.Init(&p.BaseTable)
}

func (p *ServiceParam) RocksmqEnable() bool {
	return p.RocksmqCfg.Path.GetValue() != ""
}

// NatsmqEnable checks if NATS messaging queue is enabled.
func (p *ServiceParam) NatsmqEnable() bool {
	return p.NatsmqCfg.ServerStoreDir.GetValue() != ""
}

func (p *ServiceParam) PulsarEnable() bool {
	return p.PulsarCfg.Address.GetValue() != ""
}

func (p *ServiceParam) KafkaEnable() bool {
	return p.KafkaCfg.Address.GetValue() != ""
}

// /////////////////////////////////////////////////////////////////////////////
// --- etcd ---
type EtcdConfig struct {
	// --- ETCD ---
	Endpoints         ParamItem          `refreshable:"false"`
	RootPath          ParamItem          `refreshable:"false"`
	MetaSubPath       ParamItem          `refreshable:"false"`
	KvSubPath         ParamItem          `refreshable:"false"`
	MetaRootPath      CompositeParamItem `refreshable:"false"`
	KvRootPath        CompositeParamItem `refreshable:"false"`
	EtcdLogLevel      ParamItem          `refreshable:"false"`
	EtcdLogPath       ParamItem          `refreshable:"false"`
	EtcdUseSSL        ParamItem          `refreshable:"false"`
	EtcdTLSCert       ParamItem          `refreshable:"false"`
	EtcdTLSKey        ParamItem          `refreshable:"false"`
	EtcdTLSCACert     ParamItem          `refreshable:"false"`
	EtcdTLSMinVersion ParamItem          `refreshable:"false"`

	// --- Embed ETCD ---
	UseEmbedEtcd ParamItem `refreshable:"false"`
	ConfigPath   ParamItem `refreshable:"false"`
	DataDir      ParamItem `refreshable:"false"`
}

func (p *EtcdConfig) Init(base *BaseTable) {
	p.Endpoints = ParamItem{
		Key:          "etcd.endpoints",
		Version:      "2.0.0",
		DefaultValue: "localhost:2379",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.Endpoints.Init(base.mgr)

	p.UseEmbedEtcd = ParamItem{
		Key:          "etcd.use.embed",
		DefaultValue: "false",
		Version:      "2.1.0",
		Doc:          "Whether to enable embedded Etcd (an in-process EtcdServer).",
		Export:       true,
	}
	p.UseEmbedEtcd.Init(base.mgr)

	if p.UseEmbedEtcd.GetAsBool() && (os.Getenv(metricsinfo.DeployModeEnvKey) != metricsinfo.StandaloneDeployMode) {
		panic("embedded etcd can not be used under distributed mode")
	}

	p.ConfigPath = ParamItem{
		Key:     "etcd.config.path",
		Version: "2.1.0",
		Export:  false,
	}
	p.ConfigPath.Init(base.mgr)

	p.DataDir = ParamItem{
		Key:          "etcd.data.dir",
		DefaultValue: "default.etcd",
		Version:      "2.1.0",
		Doc:          `Embedded Etcd only. please adjust in embedded Milvus: /tmp/milvus/etcdData/`,
		Export:       true,
	}
	p.DataDir.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:          "etcd.rootPath",
		Version:      "2.0.0",
		DefaultValue: "by-dev",
		PanicIfEmpty: true,
		Doc:          "The root path where data is stored in etcd",
		Export:       true,
	}
	p.RootPath.Init(base.mgr)

	p.MetaSubPath = ParamItem{
		Key:          "etcd.metaSubPath",
		Version:      "2.0.0",
		DefaultValue: "meta",
		PanicIfEmpty: true,
		Doc:          "metaRootPath = rootPath + '/' + metaSubPath",
		Export:       true,
	}
	p.MetaSubPath.Init(base.mgr)

	p.MetaRootPath = CompositeParamItem{
		Items: []*ParamItem{&p.RootPath, &p.MetaSubPath},
		Format: func(kvs map[string]string) string {
			return path.Join(kvs[p.RootPath.Key], kvs[p.MetaSubPath.Key])
		},
	}

	p.KvSubPath = ParamItem{
		Key:          "etcd.kvSubPath",
		Version:      "2.0.0",
		DefaultValue: "kv",
		PanicIfEmpty: true,
		Doc:          "kvRootPath = rootPath + '/' + kvSubPath",
		Export:       true,
	}
	p.KvSubPath.Init(base.mgr)

	p.KvRootPath = CompositeParamItem{
		Items: []*ParamItem{&p.RootPath, &p.KvSubPath},
		Format: func(kvs map[string]string) string {
			return path.Join(kvs[p.RootPath.Key], kvs[p.KvSubPath.Key])
		},
	}

	p.EtcdLogLevel = ParamItem{
		Key:          "etcd.log.level",
		DefaultValue: defaultEtcdLogLevel,
		Version:      "2.0.0",
		Doc:          "Only supports debug, info, warn, error, panic, or fatal. Default 'info'.",
		Export:       true,
	}
	p.EtcdLogLevel.Init(base.mgr)

	p.EtcdLogPath = ParamItem{
		Key:          "etcd.log.path",
		DefaultValue: defaultEtcdLogPath,
		Version:      "2.0.0",
		Doc: `path is one of:
 - "default" as os.Stderr,
 - "stderr" as os.Stderr,
 - "stdout" as os.Stdout,
 - file path to append server logs to.
please adjust in embedded Milvus: /tmp/milvus/logs/etcd.log`,
		Export: true,
	}
	p.EtcdLogPath.Init(base.mgr)

	p.EtcdUseSSL = ParamItem{
		Key:          "etcd.ssl.enabled",
		DefaultValue: "false",
		Version:      "2.0.0",
		Doc:          "Whether to support ETCD secure connection mode",
		Export:       true,
	}
	p.EtcdUseSSL.Init(base.mgr)

	p.EtcdTLSCert = ParamItem{
		Key:     "etcd.ssl.tlsCert",
		Version: "2.0.0",
		Doc:     "path to your cert file",
		Export:  true,
	}
	p.EtcdTLSCert.Init(base.mgr)

	p.EtcdTLSKey = ParamItem{
		Key:     "etcd.ssl.tlsKey",
		Version: "2.0.0",
		Doc:     "path to your key file",
		Export:  true,
	}
	p.EtcdTLSKey.Init(base.mgr)

	p.EtcdTLSCACert = ParamItem{
		Key:     "etcd.ssl.tlsCACert",
		Version: "2.0.0",
		Doc:     "path to your CACert file",
		Export:  true,
	}
	p.EtcdTLSCACert.Init(base.mgr)

	p.EtcdTLSMinVersion = ParamItem{
		Key:          "etcd.ssl.tlsMinVersion",
		DefaultValue: "1.3",
		Version:      "2.0.0",
		Doc: `TLS min version
Optional values: 1.0, 1.1, 1.2, 1.3。
We recommend using version 1.2 and above.`,
		Export: true,
	}
	p.EtcdTLSMinVersion.Init(base.mgr)
}

type LocalStorageConfig struct {
	Path ParamItem `refreshable:"false"`
}

func (p *LocalStorageConfig) Init(base *BaseTable) {
	p.Path = ParamItem{
		Key:          "localStorage.path",
		Version:      "2.0.0",
		DefaultValue: "/var/lib/milvus/data",
		Doc:          "please adjust in embedded Milvus: /tmp/milvus/data/",
		Export:       true,
	}
	p.Path.Init(base.mgr)
}

type MetaStoreConfig struct {
	MetaStoreType ParamItem `refreshable:"false"`
}

func (p *MetaStoreConfig) Init(base *BaseTable) {
	p.MetaStoreType = ParamItem{
		Key:          "metastore.type",
		Version:      "2.2.0",
		DefaultValue: util.MetaStoreTypeEtcd,
		Doc: `Default value: etcd
Valid values: [etcd, mysql]`,
		Export: true,
	}
	p.MetaStoreType.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- meta db ---
type MetaDBConfig struct {
	Username     ParamItem `refreshable:"false"`
	Password     ParamItem `refreshable:"false"`
	Address      ParamItem `refreshable:"false"`
	Port         ParamItem `refreshable:"false"`
	DBName       ParamItem `refreshable:"false"`
	MaxOpenConns ParamItem `refreshable:"false"`
	MaxIdleConns ParamItem `refreshable:"false"`
}

func (p *MetaDBConfig) Init(base *BaseTable) {
	p.Username = ParamItem{
		Key:          "mysql.username",
		Version:      "2.2.0",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.Username.Init(base.mgr)

	p.Password = ParamItem{
		Key:          "mysql.password",
		Version:      "2.2.0",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.Password.Init(base.mgr)

	p.Address = ParamItem{
		Key:          "mysql.address",
		Version:      "2.2.0",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.Address.Init(base.mgr)

	p.Port = ParamItem{
		Key:          "mysql.port",
		Version:      "2.2.0",
		DefaultValue: "3306",
		Export:       true,
	}
	p.Port.Init(base.mgr)

	p.DBName = ParamItem{
		Key:          "mysql.dbName",
		Version:      "2.2.0",
		PanicIfEmpty: true,
		Export:       true,
	}
	p.DBName.Init(base.mgr)

	p.MaxOpenConns = ParamItem{
		Key:          "mysql.maxOpenConns",
		Version:      "2.2.0",
		DefaultValue: "20",
		Export:       true,
	}
	p.MaxOpenConns.Init(base.mgr)

	p.MaxIdleConns = ParamItem{
		Key:          "mysql.maxIdleConns",
		Version:      "2.2.0",
		DefaultValue: "5",
		Export:       true,
	}
	p.MaxIdleConns.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- mq ---

// MQConfig represents the configuration settings for the message queue.
type MQConfig struct {
	Type ParamItem `refreshable:"false"`
}

// Init initializes the MQConfig object with a BaseTable.
func (p *MQConfig) Init(base *BaseTable) {
	p.Type = ParamItem{
		Key:          "mq.type",
		Version:      "2.3.0",
		DefaultValue: "",
		Doc: `Default value: "default"
Valid values: [default, pulsar, kafka, rocksmq, natsmq]`,
		Export: true,
	}
	p.Type.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- pulsar ---
type PulsarConfig struct {
	Address        ParamItem `refreshable:"false"`
	Port           ParamItem `refreshable:"false"`
	WebAddress     ParamItem `refreshable:"false"`
	WebPort        ParamItem `refreshable:"false"`
	MaxMessageSize ParamItem `refreshable:"true"`

	// support auth
	AuthPlugin ParamItem `refreshable:"false"`
	AuthParams ParamItem `refreshable:"false"`

	// support tenant
	Tenant    ParamItem `refreshable:"false"`
	Namespace ParamItem `refreshable:"false"`
}

func (p *PulsarConfig) Init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "pulsar.port",
		Version:      "2.0.0",
		DefaultValue: "6650",
		Doc:          "Port of Pulsar",
		Export:       true,
	}
	p.Port.Init(base.mgr)

	// due to implicit rule of MQ priority，the default address should be empty
	p.Address = ParamItem{
		Key:          "pulsar.address",
		Version:      "2.0.0",
		DefaultValue: "",
		Formatter: func(addr string) string {
			if addr == "" {
				return ""
			}
			if strings.Contains(addr, ":") {
				return addr
			}
			port, _ := p.Port.get()
			return "pulsar://" + addr + ":" + port
		},
		Doc:    "Address of pulsar",
		Export: true,
	}
	p.Address.Init(base.mgr)

	p.WebPort = ParamItem{
		Key:          "pulsar.webport",
		Version:      "2.0.0",
		DefaultValue: "80",
		Doc:          "Web port of pulsar, if you connect direcly without proxy, should use 8080",
		Export:       true,
	}
	p.WebPort.Init(base.mgr)

	p.WebAddress = ParamItem{
		Key:          "pulsar.webaddress",
		Version:      "2.0.0",
		DefaultValue: "",
		Formatter: func(add string) string {
			pulsarURL, err := url.ParseRequestURI(p.Address.GetValue())
			if err != nil {
				log.Info("failed to parse pulsar config, assume pulsar not used", zap.Error(err))
				return ""
			}
			return "http://" + pulsarURL.Hostname() + ":" + p.WebPort.GetValue()
		},
	}
	p.WebAddress.Init(base.mgr)

	p.MaxMessageSize = ParamItem{
		Key:          "pulsar.maxMessageSize",
		Version:      "2.0.0",
		DefaultValue: strconv.Itoa(SuggestPulsarMaxMessageSize),
		Doc:          "5 * 1024 * 1024 Bytes, Maximum size of each message in pulsar.",
		Export:       true,
	}
	p.MaxMessageSize.Init(base.mgr)

	p.Tenant = ParamItem{
		Key:          "pulsar.tenant",
		Version:      "2.2.0",
		DefaultValue: "public",
		Export:       true,
	}
	p.Tenant.Init(base.mgr)

	p.Namespace = ParamItem{
		Key:          "pulsar.namespace",
		Version:      "2.2.0",
		DefaultValue: "default",
		Export:       true,
	}
	p.Namespace.Init(base.mgr)

	p.AuthPlugin = ParamItem{
		Key:     "pulsar.authPlugin",
		Version: "2.2.0",
	}
	p.AuthPlugin.Init(base.mgr)

	p.AuthParams = ParamItem{
		Key:     "pulsar.authParams",
		Version: "2.2.0",
		Formatter: func(authParams string) string {
			jsonMap := make(map[string]string)
			params := strings.Split(authParams, ",")
			for _, param := range params {
				kv := strings.Split(param, ":")
				if len(kv) == 2 {
					jsonMap[kv[0]] = kv[1]
				}
			}

			jsonData, _ := json.Marshal(&jsonMap)
			return string(jsonData)
		},
	}
	p.AuthParams.Init(base.mgr)
}

// --- kafka ---
type KafkaConfig struct {
	Address             ParamItem  `refreshable:"false"`
	SaslUsername        ParamItem  `refreshable:"false"`
	SaslPassword        ParamItem  `refreshable:"false"`
	SaslMechanisms      ParamItem  `refreshable:"false"`
	SecurityProtocol    ParamItem  `refreshable:"false"`
	ConsumerExtraConfig ParamGroup `refreshable:"false"`
	ProducerExtraConfig ParamGroup `refreshable:"false"`
}

func (k *KafkaConfig) Init(base *BaseTable) {
	// due to implicit rule of MQ priority，the default address should be empty
	k.Address = ParamItem{
		Key:          "kafka.brokerList",
		DefaultValue: "",
		Version:      "2.1.0",
		Export:       true,
	}
	k.Address.Init(base.mgr)

	k.SaslUsername = ParamItem{
		Key:          "kafka.saslUsername",
		DefaultValue: "",
		Version:      "2.1.0",
		Export:       true,
	}
	k.SaslUsername.Init(base.mgr)

	k.SaslPassword = ParamItem{
		Key:          "kafka.saslPassword",
		DefaultValue: "",
		Version:      "2.1.0",
		Export:       true,
	}
	k.SaslPassword.Init(base.mgr)

	k.SaslMechanisms = ParamItem{
		Key:          "kafka.saslMechanisms",
		DefaultValue: "PLAIN",
		Version:      "2.1.0",
		Export:       true,
	}
	k.SaslMechanisms.Init(base.mgr)

	k.SecurityProtocol = ParamItem{
		Key:          "kafka.securityProtocol",
		DefaultValue: "SASL_SSL",
		Version:      "2.1.0",
		Export:       true,
	}
	k.SecurityProtocol.Init(base.mgr)

	k.ConsumerExtraConfig = ParamGroup{
		KeyPrefix: "kafka.consumer.",
		Version:   "2.2.0",
	}
	k.ConsumerExtraConfig.Init(base.mgr)

	k.ProducerExtraConfig = ParamGroup{
		KeyPrefix: "kafka.producer.",
		Version:   "2.2.0",
	}
	k.ProducerExtraConfig.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- rocksmq ---
type RocksmqConfig struct {
	Path          ParamItem `refreshable:"false"`
	LRUCacheRatio ParamItem `refreshable:"false"`
	PageSize      ParamItem `refreshable:"false"`
	// RetentionTimeInMinutes is the time of retention
	RetentionTimeInMinutes ParamItem `refreshable:"false"`
	// RetentionSizeInMB is the size of retention
	RetentionSizeInMB ParamItem `refreshable:"false"`
	// CompactionInterval is the Interval we trigger compaction,
	CompactionInterval ParamItem `refreshable:"false"`
	// TickerTimeInSeconds is the time of expired check, default 10 minutes
	TickerTimeInSeconds ParamItem `refreshable:"false"`
}

func (r *RocksmqConfig) Init(base *BaseTable) {
	r.Path = ParamItem{
		Key:     "rocksmq.path",
		Version: "2.0.0",
		Doc: `The path where the message is stored in rocksmq
please adjust in embedded Milvus: /tmp/milvus/rdb_data`,
		Export: true,
	}
	r.Path.Init(base.mgr)

	r.LRUCacheRatio = ParamItem{
		Key:          "rocksmq.lrucacheratio",
		DefaultValue: "0.0.6",
		Version:      "2.0.0",
		Doc:          "rocksdb cache memory ratio",
		Export:       true,
	}
	r.LRUCacheRatio.Init(base.mgr)

	r.PageSize = ParamItem{
		Key:          "rocksmq.rocksmqPageSize",
		DefaultValue: strconv.FormatInt(64<<20, 10),
		Version:      "2.0.0",
		Doc:          "64 MB, 64 * 1024 * 1024 bytes, The size of each page of messages in rocksmq",
		Export:       true,
	}
	r.PageSize.Init(base.mgr)

	r.RetentionTimeInMinutes = ParamItem{
		Key:          "rocksmq.retentionTimeInMinutes",
		DefaultValue: "4320",
		Version:      "2.0.0",
		Doc:          "3 days, 3 * 24 * 60 minutes, The retention time of the message in rocksmq.",
		Export:       true,
	}
	r.RetentionTimeInMinutes.Init(base.mgr)

	r.RetentionSizeInMB = ParamItem{
		Key:          "rocksmq.retentionSizeInMB",
		DefaultValue: "7200",
		Version:      "2.0.0",
		Doc:          "8 GB, 8 * 1024 MB, The retention size of the message in rocksmq.",
		Export:       true,
	}
	r.RetentionSizeInMB.Init(base.mgr)

	r.CompactionInterval = ParamItem{
		Key:          "rocksmq.compactionInterval",
		DefaultValue: "86400",
		Version:      "2.0.0",
		Doc:          "1 day, trigger rocksdb compaction every day to remove deleted data",
		Export:       true,
	}
	r.CompactionInterval.Init(base.mgr)

	r.TickerTimeInSeconds = ParamItem{
		Key:          "rocksmq.timtickerInterval",
		DefaultValue: "600",
		Version:      "2.2.2",
	}
	r.TickerTimeInSeconds.Init(base.mgr)
}

// NatsmqConfig describes the configuration options for the Nats message queue
type NatsmqConfig struct {
	ServerPort                ParamItem `refreshable:"false"`
	ServerStoreDir            ParamItem `refreshable:"false"`
	ServerMaxFileStore        ParamItem `refreshable:"false"`
	ServerMaxPayload          ParamItem `refreshable:"false"`
	ServerMaxPending          ParamItem `refreshable:"false"`
	ServerInitializeTimeout   ParamItem `refreshable:"false"`
	ServerMonitorDebug        ParamItem `refreshable:"false"`
	ServerMonitorLogTime      ParamItem `refreshable:"false"`
	ServerMonitorLogFile      ParamItem `refreshable:"false"`
	ServerMonitorLogSizeLimit ParamItem `refreshable:"false"`
}

// Init sets up a new NatsmqConfig instance using the provided BaseTable
func (r *NatsmqConfig) Init(base *BaseTable) {
	r.ServerPort = ParamItem{
		Key:          "natsmq.server.port",
		Version:      "2.3.0",
		DefaultValue: "4222",
		Doc:          `Port for nats server listening`,
		Export:       true,
	}
	r.ServerPort.Init(base.mgr)
	r.ServerStoreDir = ParamItem{
		Key:     "natsmq.server.storeDir",
		Version: "2.3.0",
		Doc:     `Directory to use for JetStream storage of nats`,
		Export:  true,
	}
	r.ServerStoreDir.Init(base.mgr)
	r.ServerMaxFileStore = ParamItem{
		Key:          "natsmq.server.maxFileStore",
		Version:      "2.3.0",
		DefaultValue: "17179869184",
		Doc:          `Maximum size of the 'file' storage`,
		Export:       true,
	}
	r.ServerMaxFileStore.Init(base.mgr)
	r.ServerMaxPayload = ParamItem{
		Key:          "natsmq.server.maxPayload",
		Version:      "2.3.0",
		DefaultValue: "8388608",
		Doc:          `Maximum number of bytes in a message payload`,
		Export:       true,
	}
	r.ServerMaxPayload.Init(base.mgr)
	r.ServerMaxPending = ParamItem{
		Key:          "natsmq.server.maxPending",
		Version:      "2.3.0",
		DefaultValue: "67108864",
		Doc:          `Maximum number of bytes buffered for a connection Applies to client connections`,
		Export:       true,
	}
	r.ServerMaxPending.Init(base.mgr)
	r.ServerInitializeTimeout = ParamItem{
		Key:          "natsmq.server.initializeTimeout",
		Version:      "2.3.0",
		DefaultValue: "4000",
		Doc:          `waiting for initialization of natsmq finished`,
		Export:       true,
	}
	r.ServerInitializeTimeout.Init(base.mgr)

	r.ServerMonitorDebug = ParamItem{
		Key:          "natsmq.server.monitor.debug",
		Version:      "2.3.0",
		DefaultValue: "false",
		Doc:          `If true enable debug log messages`,
		Export:       true,
	}
	r.ServerMonitorDebug.Init(base.mgr)
	r.ServerMonitorLogTime = ParamItem{
		Key:          "natsmq.server.monitor.logTime",
		Version:      "2.3.0",
		DefaultValue: "true",
		Doc:          `If set to false, log without timestamps.`,
		Export:       true,
	}
	r.ServerMonitorLogTime.Init(base.mgr)
	r.ServerMonitorLogFile = ParamItem{
		Key:          "natsmq.server.monitor.logFile",
		Version:      "2.3.0",
		DefaultValue: "",
		Doc:          `Log file path relative to..`,
		Export:       true,
	}
	r.ServerMonitorLogFile.Init(base.mgr)
	r.ServerMonitorLogSizeLimit = ParamItem{
		Key:          "natsmq.server.monitor.logSizeLimit",
		Version:      "2.3.0",
		DefaultValue: "0",
		Doc:          `Size in bytes after the log file rolls over to a new one`,
		Export:       true,
	}
	r.ServerMonitorLogSizeLimit.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- minio ---
type MinioConfig struct {
	Address         ParamItem `refreshable:"false"`
	Port            ParamItem `refreshable:"false"`
	AccessKeyID     ParamItem `refreshable:"false"`
	SecretAccessKey ParamItem `refreshable:"false"`
	UseSSL          ParamItem `refreshable:"false"`
	BucketName      ParamItem `refreshable:"false"`
	RootPath        ParamItem `refreshable:"false"`
	UseIAM          ParamItem `refreshable:"false"`
	CloudProvider   ParamItem `refreshable:"false"`
	IAMEndpoint     ParamItem `refreshable:"false"`
}

func (p *MinioConfig) Init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "minio.port",
		DefaultValue: "9000",
		Version:      "2.0.0",
		Doc:          "Port of MinIO/S3",
		Export:       true,
	}
	p.Port.Init(base.mgr)

	p.Address = ParamItem{
		Key:          "minio.address",
		DefaultValue: "",
		Version:      "2.0.0",
		Formatter: func(addr string) string {
			if addr == "" {
				return ""
			}
			if strings.Contains(addr, ":") {
				return addr
			}
			port, _ := p.Port.get()
			return addr + ":" + port
		},
		Doc:    "Address of MinIO/S3",
		Export: true,
	}
	p.Address.Init(base.mgr)

	p.AccessKeyID = ParamItem{
		Key:          "minio.accessKeyID",
		Version:      "2.0.0",
		DefaultValue: "minioadmin",
		PanicIfEmpty: false, // tmp fix, need to be conditional
		Doc:          "accessKeyID of MinIO/S3",
		Export:       true,
	}
	p.AccessKeyID.Init(base.mgr)

	p.SecretAccessKey = ParamItem{
		Key:          "minio.secretAccessKey",
		Version:      "2.0.0",
		DefaultValue: "minioadmin",
		PanicIfEmpty: false, // tmp fix, need to be conditional
		Doc:          "MinIO/S3 encryption string",
		Export:       true,
	}
	p.SecretAccessKey.Init(base.mgr)

	p.UseSSL = ParamItem{
		Key:          "minio.useSSL",
		Version:      "2.0.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
		Doc:          "Access to MinIO/S3 with SSL",
		Export:       true,
	}
	p.UseSSL.Init(base.mgr)

	p.BucketName = ParamItem{
		Key:          "minio.bucketName",
		Version:      "2.0.0",
		DefaultValue: "a-bucket",
		PanicIfEmpty: true,
		Doc:          "Bucket name in MinIO/S3",
		Export:       true,
	}
	p.BucketName.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:          "minio.rootPath",
		Version:      "2.0.0",
		PanicIfEmpty: false,
		Doc:          "The root path where the message is stored in MinIO/S3",
		Export:       true,
	}
	p.RootPath.Init(base.mgr)

	p.UseIAM = ParamItem{
		Key:          "minio.useIAM",
		DefaultValue: DefaultMinioUseIAM,
		Version:      "2.0.0",
		Doc: `Whether to useIAM role to access S3/GCS instead of access/secret keys
For more information, refer to
aws: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html
gcp: https://cloud.google.com/storage/docs/access-control/iam
aliyun (ack): https://www.alibabacloud.com/help/en/container-service-for-kubernetes/latest/use-rrsa-to-enforce-access-control
aliyun (ecs): https://www.alibabacloud.com/help/en/elastic-compute-service/latest/attach-an-instance-ram-role`,
		Export: true,
	}
	p.UseIAM.Init(base.mgr)

	p.CloudProvider = ParamItem{
		Key:          "minio.cloudProvider",
		DefaultValue: DefaultMinioCloudProvider,
		Version:      "2.2.0",
		Doc: `Cloud Provider of S3. Supports: "aws", "gcp", "aliyun".
You can use "aws" for other cloud provider supports S3 API with signature v4, e.g.: minio
You can use "gcp" for other cloud provider supports S3 API with signature v2
You can use "aliyun" for other cloud provider uses virtual host style bucket
When useIAM enabled, only "aws", "gcp", "aliyun" is supported for now`,
		Export: true,
	}
	p.CloudProvider.Init(base.mgr)

	p.IAMEndpoint = ParamItem{
		Key:          "minio.iamEndpoint",
		DefaultValue: DefaultMinioIAMEndpoint,
		Version:      "2.0.0",
		Doc: `Custom endpoint for fetch IAM role credentials. when useIAM is true & cloudProvider is "aws".
Leave it empty if you want to use AWS default endpoint`,
		Export: true,
	}
	p.IAMEndpoint.Init(base.mgr)
}
