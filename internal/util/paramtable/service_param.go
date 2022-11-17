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

func (p *ServiceParam) Init() {
	p.BaseTable.Init()

	p.LocalStorageCfg.Init(&p.BaseTable)
	p.MetaStoreCfg.Init(&p.BaseTable)
	p.EtcdCfg.Init(&p.BaseTable)
	if p.MetaStoreCfg.MetaStoreType == util.MetaStoreTypeMysql {
		log.Debug("Mysql protocol is used as meta store")
		p.DBCfg.Init(&p.BaseTable)
	}
	p.PulsarCfg.Init(&p.BaseTable)
	p.KafkaCfg.Init(&p.BaseTable)
	p.RocksmqCfg.Init(&p.BaseTable)
	p.MinioCfg.Init(&p.BaseTable)
}

// /////////////////////////////////////////////////////////////////////////////
// --- etcd ---
type EtcdConfig struct {
	// --- ETCD ---
	Endpoints         ParamItem
	RootPath          ParamItem
	MetaSubPath       ParamItem
	KvSubPath         ParamItem
	MetaRootPath      CompositeParamItem
	KvRootPath        CompositeParamItem
	EtcdLogLevel      ParamItem
	EtcdLogPath       ParamItem
	EtcdUseSSL        ParamItem
	EtcdTLSCert       ParamItem
	EtcdTLSKey        ParamItem
	EtcdTLSCACert     ParamItem
	EtcdTLSMinVersion ParamItem

	// --- Embed ETCD ---
	UseEmbedEtcd ParamItem
	ConfigPath   ParamItem
	DataDir      ParamItem
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

	if p.UseEmbedEtcd.GetAsBool() {
		p.ConfigPath = ParamItem{
			Key:          "etcd.config.path",
			DefaultValue: "",
			Version:      "2.1.0",
		}
		p.ConfigPath.Init(base.mgr)

		p.DataDir = ParamItem{
			Key:          "etcd.data.dir",
			DefaultValue: "default.etcd",
			Version:      "2.1.0",
		}
		p.DataDir.Init(base.mgr)
	} else {
		p.Endpoints = ParamItem{
			Key:          "etcd.endpoints",
			Version:      "2.0.0",
			PanicIfEmpty: true,
		}
		p.Endpoints.Init(base.mgr)
	}

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
			return path.Join(kvs["etcd.rootPath"], kvs["etcd.metaSubPath"])
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
			return path.Join(kvs["etcd.rootPath"], kvs["etcd.kvSubPath"])
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
	Path ParamItem
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
	Base *BaseTable

	MetaStoreType string
}

func (p *MetaStoreConfig) Init(base *BaseTable) {
	p.Base = base
	p.LoadCfgToMemory()
}

func (p *MetaStoreConfig) LoadCfgToMemory() {
	p.initMetaStoreType()
}

func (p *MetaStoreConfig) initMetaStoreType() {
	p.MetaStoreType = p.Base.LoadWithDefault("metastore.type", util.MetaStoreTypeEtcd)
}

// /////////////////////////////////////////////////////////////////////////////
// --- meta db ---
type MetaDBConfig struct {
	Base *BaseTable

	Username     string
	Password     string
	Address      string
	Port         int
	DBName       string
	MaxOpenConns int
	MaxIdleConns int
}

func (p *MetaDBConfig) Init(base *BaseTable) {
	p.Base = base
	p.LoadCfgToMemory()
}

func (p *MetaDBConfig) LoadCfgToMemory() {
	p.initUsername()
	p.initPassword()
	p.initAddress()
	p.initPort()
	p.initDbName()
	p.initMaxOpenConns()
	p.initMaxIdleConns()
}

func (p *MetaDBConfig) initUsername() {
	username, err := p.Base.Load("mysql.username")
	if err != nil {
		panic(err)
	}
	p.Username = username
}

func (p *MetaDBConfig) initPassword() {
	password, err := p.Base.Load("mysql.password")
	if err != nil {
		panic(err)
	}
	p.Password = password
}

func (p *MetaDBConfig) initAddress() {
	address, err := p.Base.Load("mysql.address")
	if err != nil {
		panic(err)
	}
	p.Address = address
}

func (p *MetaDBConfig) initPort() {
	port := p.Base.ParseIntWithDefault("mysql.port", 3306)
	p.Port = port
}

func (p *MetaDBConfig) initDbName() {
	dbName, err := p.Base.Load("mysql.dbName")
	if err != nil {
		panic(err)
	}
	p.DBName = dbName
}

func (p *MetaDBConfig) initMaxOpenConns() {
	maxOpenConns := p.Base.ParseIntWithDefault("mysql.maxOpenConns", 20)
	p.MaxOpenConns = maxOpenConns
}

func (p *MetaDBConfig) initMaxIdleConns() {
	maxIdleConns := p.Base.ParseIntWithDefault("mysql.maxIdleConns", 5)
	p.MaxIdleConns = maxIdleConns
}

// /////////////////////////////////////////////////////////////////////////////
// --- pulsar ---
type PulsarConfig struct {
	Address        ParamItem
	Port           ParamItem
	WebAddress     ParamItem
	WebPort        ParamItem
	MaxMessageSize ParamItem

	// support auth
	AuthPlugin ParamItem
	AuthParams ParamItem

	// support tenant
	Tenant    ParamItem
	Namespace ParamItem
}

func (p *PulsarConfig) Init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "pulsar.port",
		Version:      "2.0.0",
		DefaultValue: "6650",
	}
	p.Port.Init(base.mgr)

	p.Address = ParamItem{
		Key:          "pulsar.address",
		Version:      "2.0.0",
		DefaultValue: "localhost",
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

			if len(jsonMap) == 0 {
				return ""
			}
			jsonData, _ := json.Marshal(&jsonMap)
			return string(jsonData)
		},
	}
	p.AuthParams.Init(base.mgr)

}

// --- kafka ---
type KafkaConfig struct {
	Address             ParamItem
	SaslUsername        ParamItem
	SaslPassword        ParamItem
	SaslMechanisms      ParamItem
	SecurityProtocol    ParamItem
	ConsumerExtraConfig ParamGroup
	ProducerExtraConfig ParamGroup
}

func (k *KafkaConfig) Init(base *BaseTable) {
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
	Path ParamItem
}

func (r *RocksmqConfig) Init(base *BaseTable) {
	r.Path = ParamItem{
		Key:          "rocksmq.path",
		DefaultValue: "",
		Version:      "2.0.0",
	}
	r.Path.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- minio ---
type MinioConfig struct {
	Address         ParamItem
	Port            ParamItem
	AccessKeyID     ParamItem
	SecretAccessKey ParamItem
	UseSSL          ParamItem
	BucketName      ParamItem
	RootPath        ParamItem
	UseIAM          ParamItem
	CloudProvider   ParamItem
	IAMEndpoint     ParamItem
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
		PanicIfEmpty: true,
	}
	p.AccessKeyID.Init(base.mgr)

	p.SecretAccessKey = ParamItem{
		Key:          "minio.secretAccessKey",
		Version:      "2.0.0",
		PanicIfEmpty: true,
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
