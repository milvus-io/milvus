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
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

const (
	// SuggestPulsarMaxMessageSize defines the maximum size of Pulsar message.
	SuggestPulsarMaxMessageSize = 5 * 1024 * 1024
	defaultEtcdLogLevel         = "info"
	defaultEtcdLogPath          = "stdout"
)

// ServiceParam is used to quickly and easily access all basic service configurations.
type ServiceParam struct {
	BaseTable

	LocalStorageCfg LocalStorageConfig
	EtcdCfg         EtcdConfig
	PulsarCfg       PulsarConfig
	KafkaCfg        KafkaConfig
	RocksmqCfg      RocksmqConfig
	MinioCfg        MinioConfig
}

func (p *ServiceParam) Init() {
	p.BaseTable.Init()

	p.LocalStorageCfg.init(&p.BaseTable)
	p.EtcdCfg.init(&p.BaseTable)
	p.PulsarCfg.init(&p.BaseTable)
	p.KafkaCfg.init(&p.BaseTable)
	p.RocksmqCfg.init(&p.BaseTable)
	p.MinioCfg.init(&p.BaseTable)
}

///////////////////////////////////////////////////////////////////////////////
// --- etcd ---
type EtcdConfig struct {
	Base *BaseTable

	// --- ETCD ---
	Endpoints         []string
	MetaRootPath      string
	KvRootPath        string
	EtcdLogLevel      string
	EtcdLogPath       string
	EtcdUseSSL        bool
	EtcdTLSCert       string
	EtcdTLSKey        string
	EtcdTLSCACert     string
	EtcdTLSMinVersion string

	// --- Embed ETCD ---
	UseEmbedEtcd bool
	ConfigPath   string
	DataDir      string
}

func (p *EtcdConfig) init(base *BaseTable) {
	p.Base = base
	p.LoadCfgToMemory()
}

func (p *EtcdConfig) LoadCfgToMemory() {
	p.initUseEmbedEtcd()
	if p.UseEmbedEtcd {
		p.initConfigPath()
		p.initDataDir()
	} else {
		p.initEndpoints()
	}
	p.initMetaRootPath()
	p.initKvRootPath()
	p.initEtcdLogLevel()
	p.initEtcdLogPath()
	p.initEtcdUseSSL()
	p.initEtcdTLSCert()
	p.initEtcdTLSKey()
	p.initEtcdTLSCACert()
	p.initEtcdTLSMinVersion()
}

func (p *EtcdConfig) initUseEmbedEtcd() {
	p.UseEmbedEtcd = p.Base.ParseBool("etcd.use.embed", false)
	if p.UseEmbedEtcd && (os.Getenv(metricsinfo.DeployModeEnvKey) != metricsinfo.StandaloneDeployMode) {
		panic("embedded etcd can not be used under distributed mode")
	}
}

func (p *EtcdConfig) initConfigPath() {
	addr := p.Base.LoadWithDefault("etcd.config.path", "")
	p.ConfigPath = addr
}

func (p *EtcdConfig) initDataDir() {
	addr := p.Base.LoadWithDefault("etcd.data.dir", "default.etcd")
	p.DataDir = addr
}

func (p *EtcdConfig) initEndpoints() {
	endpoints, err := p.Base.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.Endpoints = strings.Split(endpoints, ",")
}

func (p *EtcdConfig) initMetaRootPath() {
	rootPath, err := p.Base.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Base.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = path.Join(rootPath, subPath)
}

func (p *EtcdConfig) initKvRootPath() {
	rootPath, err := p.Base.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Base.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = path.Join(rootPath, subPath)
}

func (p *EtcdConfig) initEtcdLogLevel() {
	p.EtcdLogLevel = p.Base.LoadWithDefault("etcd.log.level", defaultEtcdLogLevel)
}

func (p *EtcdConfig) initEtcdLogPath() {
	p.EtcdLogPath = p.Base.LoadWithDefault("etcd.log.path", defaultEtcdLogPath)
}

func (p *EtcdConfig) initEtcdUseSSL() {
	p.EtcdUseSSL = p.Base.ParseBool("etcd.ssl.enabled", false)
}

func (p *EtcdConfig) initEtcdTLSCert() {
	p.EtcdTLSCert = p.Base.LoadWithDefault("etcd.ssl.tlsCert", "")
}

func (p *EtcdConfig) initEtcdTLSKey() {
	p.EtcdTLSKey = p.Base.LoadWithDefault("etcd.ssl.tlsKey", "")
}

func (p *EtcdConfig) initEtcdTLSCACert() {
	p.EtcdTLSCACert = p.Base.LoadWithDefault("etcd.ssl.tlsCACert", "")
}

func (p *EtcdConfig) initEtcdTLSMinVersion() {
	p.EtcdTLSMinVersion = p.Base.LoadWithDefault("etcd.ssl.tlsMinVersion", "1.3")
}

type LocalStorageConfig struct {
	Base *BaseTable

	Path string
}

func (p *LocalStorageConfig) init(base *BaseTable) {
	p.Base = base
	p.initPath()
}

func (p *LocalStorageConfig) initPath() {
	p.Path = p.Base.LoadWithDefault("localStorage.path", "/var/lib/milvus/data")
}

///////////////////////////////////////////////////////////////////////////////
// --- pulsar ---
type PulsarConfig struct {
	Base *BaseTable

	Address        string
	MaxMessageSize int
}

func (p *PulsarConfig) init(base *BaseTable) {
	p.Base = base

	p.initAddress()
	p.initMaxMessageSize()
}

func (p *PulsarConfig) initAddress() {
	addr, err := p.Base.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.Address = addr
}

func (p *PulsarConfig) initMaxMessageSize() {
	maxMessageSizeStr, err := p.Base.Load("pulsar.maxMessageSize")
	if err != nil {
		p.MaxMessageSize = SuggestPulsarMaxMessageSize
	} else {
		maxMessageSize, err := strconv.Atoi(maxMessageSizeStr)
		if err != nil {
			p.MaxMessageSize = SuggestPulsarMaxMessageSize
		} else {
			p.MaxMessageSize = maxMessageSize
		}
	}
}

// --- kafka ---
type KafkaConfig struct {
	Base             *BaseTable
	Address          string
	SaslUsername     string
	SaslPassword     string
	SaslMechanisms   string
	SecurityProtocol string
}

func (k *KafkaConfig) init(base *BaseTable) {
	k.Base = base
	k.initAddress()
	k.initSaslUsername()
	k.initSaslPassword()
	k.initSaslMechanisms()
	k.initSecurityProtocol()
}

func (k *KafkaConfig) initAddress() {
	addr, err := k.Base.Load("_KafkaBrokerList")
	if err != nil {
		panic(err)
	}
	k.Address = addr
}

func (k *KafkaConfig) initSaslUsername() {
	k.SaslUsername = k.Base.LoadWithDefault("kafka.saslUsername", "")
}

func (k *KafkaConfig) initSaslPassword() {
	k.SaslPassword = k.Base.LoadWithDefault("kafka.saslPassword", "")
}

func (k *KafkaConfig) initSaslMechanisms() {
	k.SaslMechanisms = k.Base.LoadWithDefault("kafka.saslMechanisms", "PLAIN")
}

func (k *KafkaConfig) initSecurityProtocol() {
	k.SecurityProtocol = k.Base.LoadWithDefault("kafka.securityProtocol", "SASL_SSL")
}

///////////////////////////////////////////////////////////////////////////////
// --- rocksmq ---
type RocksmqConfig struct {
	Base *BaseTable

	Path string
}

func (p *RocksmqConfig) init(base *BaseTable) {
	p.Base = base

	p.initPath()
}

func (p *RocksmqConfig) initPath() {
	path, err := p.Base.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.Path = path
}

///////////////////////////////////////////////////////////////////////////////
// --- minio ---
type MinioConfig struct {
	Base *BaseTable

	Address         string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
	RootPath        string
	UseIAM          bool
	IAMEndpoint     string
}

func (p *MinioConfig) init(base *BaseTable) {
	p.Base = base

	p.initAddress()
	p.initAccessKeyID()
	p.initSecretAccessKey()
	p.initUseSSL()
	p.initBucketName()
	p.initRootPath()
	p.initUseIAM()
	p.initIAMEndpoint()
}

func (p *MinioConfig) initAddress() {
	endpoint, err := p.Base.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.Address = endpoint
}

func (p *MinioConfig) initAccessKeyID() {
	keyID, err := p.Base.Load("_MinioAccessKeyID")
	if err != nil {
		panic(err)
	}
	p.AccessKeyID = keyID
}

func (p *MinioConfig) initSecretAccessKey() {
	key, err := p.Base.Load("_MinioSecretAccessKey")
	if err != nil {
		panic(err)
	}
	p.SecretAccessKey = key
}

func (p *MinioConfig) initUseSSL() {
	usessl, err := p.Base.Load("_MinioUseSSL")
	if err != nil {
		panic(err)
	}
	p.UseSSL, _ = strconv.ParseBool(usessl)
}

func (p *MinioConfig) initBucketName() {
	bucketName, err := p.Base.Load("_MinioBucketName")
	if err != nil {
		panic(err)
	}
	p.BucketName = bucketName
}

func (p *MinioConfig) initRootPath() {
	rootPath, err := p.Base.Load("minio.rootPath")
	if err != nil {
		panic(err)
	}
	p.RootPath = rootPath
}

func (p *MinioConfig) initUseIAM() {
	useIAM := p.Base.LoadWithDefault("minio.useIAM", DefaultMinioUseIAM)
	p.UseIAM, _ = strconv.ParseBool(useIAM)
}

func (p *MinioConfig) initIAMEndpoint() {
	iamEndpoint := p.Base.LoadWithDefault("minio.iamEndpoint", DefaultMinioIAMEndpoint)
	p.IAMEndpoint = iamEndpoint
}
