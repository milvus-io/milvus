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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"go.uber.org/zap"
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
	PulsarCfg       PulsarConfig
	KafkaCfg        KafkaConfig
	RocksmqCfg      RocksmqConfig
	MinioCfg        MinioConfig
}

func (p *ServiceParam) init() {
	p.BaseTable.init(10)

	p.LocalStorageCfg.Init(&p.BaseTable)
	p.MetaStoreCfg.Init(&p.BaseTable)
	p.EtcdCfg.Init(&p.BaseTable)
	p.DBCfg.Init(&p.BaseTable)
	p.PulsarCfg.Init(&p.BaseTable)
	p.KafkaCfg.Init(&p.BaseTable)
	p.RocksmqCfg.Init(&p.BaseTable)
	p.MinioCfg.Init(&p.BaseTable)
}

func (p *ServiceParam) RocksmqEnable() bool {
	return p.RocksmqCfg.Path.GetValue() != ""
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
		PanicIfEmpty: true,
	}
	p.Endpoints.Init(base.mgr)

	p.UseEmbedEtcd = ParamItem{
		Key:          "etcd.use.embed",
		DefaultValue: "false",
		Version:      "2.1.0",
	}
	p.UseEmbedEtcd.Init(base.mgr)

	if p.UseEmbedEtcd.GetAsBool() && (os.Getenv(metricsinfo.DeployModeEnvKey) != metricsinfo.StandaloneDeployMode) {
		panic("embedded etcd can not be used under distributed mode")
	}

	p.ConfigPath = ParamItem{
		Key:     "etcd.config.path",
		Version: "2.1.0",
	}
	p.ConfigPath.Init(base.mgr)

	p.DataDir = ParamItem{
		Key:          "etcd.data.dir",
		DefaultValue: "default.etcd",
		Version:      "2.1.0",
	}
	p.DataDir.Init(base.mgr)

	p.Endpoints = ParamItem{
		Key:          "etcd.endpoints",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.Endpoints.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:          "etcd.rootPath",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.RootPath.Init(base.mgr)

	p.MetaSubPath = ParamItem{
		Key:          "etcd.metaSubPath",
		Version:      "2.0.0",
		PanicIfEmpty: true,
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
		PanicIfEmpty: true,
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
	}
	p.EtcdLogLevel.Init(base.mgr)

	p.EtcdLogPath = ParamItem{
		Key:          "etcd.log.path",
		DefaultValue: defaultEtcdLogPath,
		Version:      "2.0.0",
	}
	p.EtcdLogPath.Init(base.mgr)

	p.EtcdUseSSL = ParamItem{
		Key:          "etcd.ssl.enabled",
		DefaultValue: "false",
		Version:      "2.0.0",
	}
	p.EtcdUseSSL.Init(base.mgr)

	p.EtcdTLSCert = ParamItem{
		Key:     "etcd.ssl.tlsCert",
		Version: "2.0.0",
	}
	p.EtcdTLSCert.Init(base.mgr)

	p.EtcdTLSKey = ParamItem{
		Key:     "etcd.ssl.tlsKey",
		Version: "2.0.0",
	}
	p.EtcdTLSKey.Init(base.mgr)

	p.EtcdTLSCACert = ParamItem{
		Key:     "etcd.ssl.tlsCACert",
		Version: "2.0.0",
	}
	p.EtcdTLSCACert.Init(base.mgr)

	p.EtcdTLSMinVersion = ParamItem{
		Key:          "etcd.ssl.tlsMinVersion",
		DefaultValue: "1.3",
		Version:      "2.0.0",
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
	LogLevel     ParamItem `refreshable:"false"`
}

func (p *MetaDBConfig) Init(base *BaseTable) {
	p.Username = ParamItem{
		Key:          "mysql.username",
		Version:      "2.2.0",
		PanicIfEmpty: true,
	}
	p.Username.Init(base.mgr)

	p.Password = ParamItem{
		Key:          "mysql.password",
		Version:      "2.2.0",
		PanicIfEmpty: true,
	}
	p.Password.Init(base.mgr)

	p.Address = ParamItem{
		Key:          "mysql.address",
		Version:      "2.2.0",
		PanicIfEmpty: true,
	}
	p.Address.Init(base.mgr)

	p.Port = ParamItem{
		Key:          "mysql.port",
		Version:      "2.2.0",
		DefaultValue: "3306",
	}
	p.Port.Init(base.mgr)

	p.DBName = ParamItem{
		Key:          "mysql.dbName",
		Version:      "2.2.0",
		PanicIfEmpty: true,
	}
	p.DBName.Init(base.mgr)

	p.MaxOpenConns = ParamItem{
		Key:          "mysql.maxOpenConns",
		Version:      "2.2.0",
		DefaultValue: "20",
	}
	p.MaxOpenConns.Init(base.mgr)

	p.MaxIdleConns = ParamItem{
		Key:          "mysql.maxIdleConns",
		Version:      "2.2.0",
		DefaultValue: "5",
	}
	p.MaxIdleConns.Init(base.mgr)

	p.LogLevel = ParamItem{
		Key:          "log.level",
		Version:      "2.0.0",
		DefaultValue: "debug",
	}
	p.LogLevel.Init(base.mgr)
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
	}
	p.Address.Init(base.mgr)

	p.WebPort = ParamItem{
		Key:          "pulsar.webport",
		Version:      "2.0.0",
		DefaultValue: "80",
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
	}
	p.MaxMessageSize.Init(base.mgr)

	p.Tenant = ParamItem{
		Key:          "pulsar.tenant",
		Version:      "2.2.0",
		DefaultValue: "public",
	}
	p.Tenant.Init(base.mgr)

	p.Namespace = ParamItem{
		Key:          "pulsar.namespace",
		Version:      "2.2.0",
		DefaultValue: "default",
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
	}
	k.Address.Init(base.mgr)

	k.SaslUsername = ParamItem{
		Key:          "kafka.saslUsername",
		DefaultValue: "",
		Version:      "2.1.0",
	}
	k.SaslUsername.Init(base.mgr)

	k.SaslPassword = ParamItem{
		Key:          "kafka.saslPassword",
		DefaultValue: "",
		Version:      "2.1.0",
	}
	k.SaslPassword.Init(base.mgr)

	k.SaslMechanisms = ParamItem{
		Key:          "kafka.saslMechanisms",
		DefaultValue: "PLAIN",
		Version:      "2.1.0",
	}
	k.SaslMechanisms.Init(base.mgr)

	k.SecurityProtocol = ParamItem{
		Key:          "kafka.securityProtocol",
		DefaultValue: "SASL_SSL",
		Version:      "2.1.0",
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
	}
	r.Path.Init(base.mgr)

	r.LRUCacheRatio = ParamItem{
		Key:          "rocksmq.lrucacheratio",
		DefaultValue: "0.0.6",
		Version:      "2.0.0",
	}
	r.LRUCacheRatio.Init(base.mgr)

	r.PageSize = ParamItem{
		Key:          "rocksmq.rocksmqPageSize",
		DefaultValue: strconv.FormatInt(256<<20, 10),
		Version:      "2.0.0",
	}
	r.PageSize.Init(base.mgr)

	r.RetentionTimeInMinutes = ParamItem{
		Key:          "rocksmq.retentionTimeInMinutes",
		DefaultValue: "7200",
		Version:      "2.0.0",
	}
	r.RetentionTimeInMinutes.Init(base.mgr)

	r.RetentionSizeInMB = ParamItem{
		Key:          "rocksmq.retentionSizeInMB",
		DefaultValue: "7200",
		Version:      "2.0.0",
	}
	r.RetentionSizeInMB.Init(base.mgr)

	r.CompactionInterval = ParamItem{
		Key:          "rocksmq.compactionInterval",
		DefaultValue: "86400",
		Version:      "2.0.0",
	}
	r.CompactionInterval.Init(base.mgr)

	r.TickerTimeInSeconds = ParamItem{
		Key:          "rocksmq.timtickerInterval",
		DefaultValue: "600",
		Version:      "2.2.2",
	}
	r.TickerTimeInSeconds.Init(base.mgr)
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
	}
	p.Address.Init(base.mgr)

	p.AccessKeyID = ParamItem{
		Key:          "minio.accessKeyID",
		Version:      "2.0.0",
		PanicIfEmpty: false, // tmp fix, need to be conditional
	}
	p.AccessKeyID.Init(base.mgr)

	p.SecretAccessKey = ParamItem{
		Key:          "minio.secretAccessKey",
		Version:      "2.0.0",
		PanicIfEmpty: false, // tmp fix, need to be conditional
	}
	p.SecretAccessKey.Init(base.mgr)

	p.UseSSL = ParamItem{
		Key:          "minio.useSSL",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.UseSSL.Init(base.mgr)

	p.BucketName = ParamItem{
		Key:          "minio.bucketName",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.BucketName.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:          "minio.rootPath",
		Version:      "2.0.0",
		PanicIfEmpty: true,
	}
	p.RootPath.Init(base.mgr)

	p.UseIAM = ParamItem{
		Key:          "minio.useIAM",
		DefaultValue: DefaultMinioUseIAM,
		Version:      "2.0.0",
	}
	p.UseIAM.Init(base.mgr)

	p.CloudProvider = ParamItem{
		Key:          "minio.cloudProvider",
		DefaultValue: DefaultMinioCloudProvider,
		Version:      "2.2.0",
	}
	p.CloudProvider.Init(base.mgr)

	p.IAMEndpoint = ParamItem{
		Key:          "minio.iamEndpoint",
		DefaultValue: DefaultMinioIAMEndpoint,
		Version:      "2.0.0",
	}
	p.IAMEndpoint.Init(base.mgr)
}
