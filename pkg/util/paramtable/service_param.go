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

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

const (
	defaultEtcdLogLevel       = "info"
	defaultEtcdLogPath        = "stdout"
	KafkaProducerConfigPrefix = "kafka.producer."
	KafkaConsumerConfigPrefix = "kafka.consumer."
)

// ServiceParam is used to quickly and easily access all basic service configurations.
type ServiceParam struct {
	LocalStorageCfg LocalStorageConfig
	MetaStoreCfg    MetaStoreConfig
	EtcdCfg         EtcdConfig
	TiKVCfg         TiKVConfig
	MQCfg           MQConfig
	PulsarCfg       PulsarConfig
	KafkaCfg        KafkaConfig
	RocksmqCfg      RocksmqConfig
	NatsmqCfg       NatsmqConfig
	MinioCfg        MinioConfig
	ProfileCfg      ProfileConfig
}

func (p *ServiceParam) init(bt *BaseTable) {
	p.LocalStorageCfg.Init(bt)
	p.MetaStoreCfg.Init(bt)
	p.EtcdCfg.Init(bt)
	p.TiKVCfg.Init(bt)
	p.MQCfg.Init(bt)
	p.PulsarCfg.Init(bt)
	p.KafkaCfg.Init(bt)
	p.RocksmqCfg.Init(bt)
	p.NatsmqCfg.Init(bt)
	p.MinioCfg.Init(bt)
	p.ProfileCfg.Init(bt)
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
	RequestTimeout    ParamItem          `refreshable:"false"`

	// --- Embed ETCD ---
	UseEmbedEtcd ParamItem `refreshable:"false"`
	ConfigPath   ParamItem `refreshable:"false"`
	DataDir      ParamItem `refreshable:"false"`

	// --- ETCD Authentication ---
	EtcdEnableAuth   ParamItem `refreshable:"false"`
	EtcdAuthUserName ParamItem `refreshable:"false"`
	EtcdAuthPassword ParamItem `refreshable:"false"`
}

func (p *EtcdConfig) Init(base *BaseTable) {
	p.Endpoints = ParamItem{
		Key:          "etcd.endpoints",
		Version:      "2.0.0",
		DefaultValue: "localhost:2379",
		PanicIfEmpty: true,
		Doc: `Endpoints used to access etcd service. You can change this parameter as the endpoints of your own etcd cluster.
Environment variable: ETCD_ENDPOINTS
etcd preferentially acquires valid address from environment variable ETCD_ENDPOINTS when Milvus is started.`,
		Export: true,
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
		Doc: `Root prefix of the key to where Milvus stores data in etcd.
It is recommended to change this parameter before starting Milvus for the first time.
To share an etcd instance among multiple Milvus instances, consider changing this to a different value for each Milvus instance before you start them.
Set an easy-to-identify root path for Milvus if etcd service already exists.
Changing this for an already running Milvus instance may result in failures to read legacy data.`,
		Export: true,
	}
	p.RootPath.Init(base.mgr)

	p.MetaSubPath = ParamItem{
		Key:          "etcd.metaSubPath",
		Version:      "2.0.0",
		DefaultValue: "meta",
		PanicIfEmpty: true,
		Doc: `Sub-prefix of the key to where Milvus stores metadata-related information in etcd.
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Export: true,
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
		Doc: `Sub-prefix of the key to where Milvus stores timestamps in etcd.
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended not to change this parameter if there is no specific reason.`,
		Export: true,
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

	p.RequestTimeout = ParamItem{
		Key:          "etcd.requestTimeout",
		DefaultValue: "10000",
		Version:      "2.3.4",
		Doc:          `Etcd operation timeout in milliseconds`,
		Export:       true,
	}
	p.RequestTimeout.Init(base.mgr)

	p.EtcdEnableAuth = ParamItem{
		Key:          "etcd.auth.enabled",
		DefaultValue: "false",
		Version:      "2.3.7",
		Doc:          "Whether to enable authentication",
		Export:       true,
	}
	p.EtcdEnableAuth.Init(base.mgr)

	if p.UseEmbedEtcd.GetAsBool() && p.EtcdEnableAuth.GetAsBool() {
		panic("embedded etcd can not enable auth")
	}

	p.EtcdAuthUserName = ParamItem{
		Key:     "etcd.auth.userName",
		Version: "2.3.7",
		Doc:     "username for etcd authentication",
		Export:  true,
	}
	p.EtcdAuthUserName.Init(base.mgr)

	p.EtcdAuthPassword = ParamItem{
		Key:     "etcd.auth.password",
		Version: "2.3.7",
		Doc:     "password for etcd authentication",
		Export:  true,
	}
	p.EtcdAuthPassword.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- tikv ---
type TiKVConfig struct {
	Endpoints        ParamItem          `refreshable:"false"`
	RootPath         ParamItem          `refreshable:"false"`
	MetaSubPath      ParamItem          `refreshable:"false"`
	KvSubPath        ParamItem          `refreshable:"false"`
	MetaRootPath     CompositeParamItem `refreshable:"false"`
	KvRootPath       CompositeParamItem `refreshable:"false"`
	RequestTimeout   ParamItem          `refreshable:"false"`
	SnapshotScanSize ParamItem          `refreshable:"true"`
	TiKVUseSSL       ParamItem          `refreshable:"false"`
	TiKVTLSCert      ParamItem          `refreshable:"false"`
	TiKVTLSKey       ParamItem          `refreshable:"false"`
	TiKVTLSCACert    ParamItem          `refreshable:"false"`
}

func (p *TiKVConfig) Init(base *BaseTable) {
	p.Endpoints = ParamItem{
		Key:          "tikv.endpoints",
		Version:      "2.3.0",
		DefaultValue: "localhost:2389",
		PanicIfEmpty: true,
		Doc:          "Note that the default pd port of tikv is 2379, which conflicts with etcd.",
		Export:       true,
	}
	p.Endpoints.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:          "tikv.rootPath",
		Version:      "2.3.0",
		DefaultValue: "by-dev",
		PanicIfEmpty: true,
		Doc:          "The root path where data is stored in tikv",
		Export:       true,
	}
	p.RootPath.Init(base.mgr)

	p.MetaSubPath = ParamItem{
		Key:          "tikv.metaSubPath",
		Version:      "2.3.0",
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
		Key:          "tikv.kvSubPath",
		Version:      "2.3.0",
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

	p.RequestTimeout = ParamItem{
		Key:          "tikv.requestTimeout",
		Version:      "2.3.0",
		DefaultValue: "10000",
		Doc:          "ms, tikv request timeout",
		Export:       true,
	}
	p.RequestTimeout.Init(base.mgr)

	p.SnapshotScanSize = ParamItem{
		Key:          "tikv.snapshotScanSize",
		Version:      "2.3.0",
		DefaultValue: "256",
		Doc:          "batch size of tikv snapshot scan",
		Export:       true,
	}
	p.SnapshotScanSize.Init(base.mgr)

	p.TiKVUseSSL = ParamItem{
		Key:          "tikv.ssl.enabled",
		DefaultValue: "false",
		Version:      "2.3.0",
		Doc:          "Whether to support TiKV secure connection mode",
		Export:       true,
	}
	p.TiKVUseSSL.Init(base.mgr)

	p.TiKVTLSCert = ParamItem{
		Key:     "tikv.ssl.tlsCert",
		Version: "2.3.0",
		Doc:     "path to your cert file",
		Export:  true,
	}
	p.TiKVTLSCert.Init(base.mgr)

	p.TiKVTLSKey = ParamItem{
		Key:     "tikv.ssl.tlsKey",
		Version: "2.3.0",
		Doc:     "path to your key file",
		Export:  true,
	}
	p.TiKVTLSKey.Init(base.mgr)

	p.TiKVTLSCACert = ParamItem{
		Key:     "tikv.ssl.tlsCACert",
		Version: "2.3.0",
		Doc:     "path to your CACert file",
		Export:  true,
	}
	p.TiKVTLSCACert.Init(base.mgr)
}

type LocalStorageConfig struct {
	Path ParamItem `refreshable:"false"`
}

func (p *LocalStorageConfig) Init(base *BaseTable) {
	p.Path = ParamItem{
		Key:          "localStorage.path",
		Version:      "2.0.0",
		DefaultValue: "/var/lib/milvus/data",
		Doc: `Local path to where vector data are stored during a search or a query to avoid repetitve access to MinIO or S3 service.
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.`,
		Export: true,
	}
	p.Path.Init(base.mgr)
}

type MetaStoreConfig struct {
	MetaStoreType              ParamItem `refreshable:"false"`
	SnapshotTTLSeconds         ParamItem `refreshable:"true"`
	SnapshotReserveTimeSeconds ParamItem `refreshable:"true"`
	PaginationSize             ParamItem `refreshable:"true"`
	ReadConcurrency            ParamItem `refreshable:"true"`
}

func (p *MetaStoreConfig) Init(base *BaseTable) {
	p.MetaStoreType = ParamItem{
		Key:          "metastore.type",
		Version:      "2.2.0",
		DefaultValue: util.MetaStoreTypeEtcd,
		Doc:          `Default value: etcd, Valid values: [etcd, tikv]`,
		Export:       true,
	}
	p.MetaStoreType.Init(base.mgr)

	p.SnapshotTTLSeconds = ParamItem{
		Key:          "metastore.snapshot.ttl",
		Version:      "2.4.14",
		DefaultValue: "86400",
		Doc:          `snapshot ttl in seconds`,
		Export:       true,
	}
	p.SnapshotTTLSeconds.Init(base.mgr)

	p.SnapshotReserveTimeSeconds = ParamItem{
		Key:          "metastore.snapshot.reserveTime",
		Version:      "2.4.14",
		DefaultValue: "3600",
		Doc:          `snapshot reserve time in seconds`,
		Export:       true,
	}
	p.SnapshotReserveTimeSeconds.Init(base.mgr)

	p.PaginationSize = ParamItem{
		Key:          "metastore.paginationSize",
		Version:      "2.5.1",
		DefaultValue: "100000",
		Doc:          `limits the number of results to return from metastore.`,
	}
	p.PaginationSize.Init(base.mgr)

	p.ReadConcurrency = ParamItem{
		Key:          "metastore.readConcurrency",
		Version:      "2.5.1",
		DefaultValue: "32",
		Doc:          `read concurrency for fetching metadata from the metastore.`,
	}
	p.ReadConcurrency.Init(base.mgr)

	// TODO: The initialization operation of metadata storage is called in the initialization phase of every node.
	// There should be a single initialization operation for meta store, then move the metrics registration to there.
	metrics.RegisterMetaType(p.MetaStoreType.GetValue())
}

// /////////////////////////////////////////////////////////////////////////////
// --- mq ---

// MQConfig represents the configuration settings for the message queue.
type MQConfig struct {
	Type              ParamItem `refreshable:"false"`
	EnablePursuitMode ParamItem `refreshable:"true"`
	PursuitLag        ParamItem `refreshable:"true"`
	PursuitBufferSize ParamItem `refreshable:"true"`
	PursuitBufferTime ParamItem `refreshable:"true"`

	MQBufSize         ParamItem `refreshable:"false"`
	ReceiveBufSize    ParamItem `refreshable:"false"`
	IgnoreBadPosition ParamItem `refreshable:"true"`

	// msgdispatcher
	MergeCheckInterval          ParamItem `refreshable:"false"`
	TargetBufSize               ParamItem `refreshable:"false"`
	MaxTolerantLag              ParamItem `refreshable:"true"`
	MaxDispatcherNumPerPchannel ParamItem `refreshable:"true"`
	RetrySleep                  ParamItem `refreshable:"true"`
	RetryTimeout                ParamItem `refreshable:"true"`
}

// Init initializes the MQConfig object with a BaseTable.
func (p *MQConfig) Init(base *BaseTable) {
	p.Type = ParamItem{
		Key:          "mq.type",
		Version:      "2.3.0",
		DefaultValue: "default",
		Doc: `Default value: "default"
Valid values: [default, pulsar, kafka, rocksmq, natsmq]`,
		Export: true,
	}
	p.Type.Init(base.mgr)

	p.MaxTolerantLag = ParamItem{
		Key:          "mq.dispatcher.maxTolerantLag",
		Version:      "2.4.4",
		DefaultValue: "3",
		Doc:          `Default value: "3", the timeout(in seconds) that target sends msgPack`,
		Export:       true,
	}
	p.MaxTolerantLag.Init(base.mgr)

	p.MaxDispatcherNumPerPchannel = ParamItem{
		Key:          "mq.dispatcher.maxDispatcherNumPerPchannel",
		Version:      "2.4.19",
		DefaultValue: "5",
		Doc:          `The maximum number of dispatchers per physical channel, primarily to limit the number of consumers and prevent performance issues(e.g., during recovery when a large number of channels are watched).`,
		Export:       true,
	}
	p.MaxDispatcherNumPerPchannel.Init(base.mgr)

	p.RetrySleep = ParamItem{
		Key:          "mq.dispatcher.retrySleep",
		Version:      "2.4.19",
		DefaultValue: "3",
		Doc:          `register retry sleep time in seconds`,
		Export:       true,
	}
	p.RetrySleep.Init(base.mgr)

	p.RetryTimeout = ParamItem{
		Key:          "mq.dispatcher.retryTimeout",
		Version:      "2.4.19",
		DefaultValue: "60",
		Doc:          `register retry timeout in seconds`,
		Export:       true,
	}
	p.RetryTimeout.Init(base.mgr)

	p.TargetBufSize = ParamItem{
		Key:          "mq.dispatcher.targetBufSize",
		Version:      "2.4.4",
		DefaultValue: "16",
		Doc:          `the lenth of channel buffer for targe`,
		Export:       true,
	}
	p.TargetBufSize.Init(base.mgr)

	p.MergeCheckInterval = ParamItem{
		Key:          "mq.dispatcher.mergeCheckInterval",
		Version:      "2.4.4",
		DefaultValue: "1",
		Doc:          `the interval time(in seconds) for dispatcher to check whether to merge`,
		Export:       true,
	}
	p.MergeCheckInterval.Init(base.mgr)

	p.EnablePursuitMode = ParamItem{
		Key:          "mq.enablePursuitMode",
		Version:      "2.3.0",
		DefaultValue: "true",
		Doc:          `Default value: "true"`,
		Export:       true,
	}
	p.EnablePursuitMode.Init(base.mgr)

	p.PursuitLag = ParamItem{
		Key:          "mq.pursuitLag",
		Version:      "2.3.0",
		DefaultValue: "10",
		Doc:          `time tick lag threshold to enter pursuit mode, in seconds`,
		Export:       true,
	}
	p.PursuitLag.Init(base.mgr)

	p.PursuitBufferSize = ParamItem{
		Key:          "mq.pursuitBufferSize",
		Version:      "2.3.0",
		DefaultValue: "8388608", // 8 MB
		Doc:          `pursuit mode buffer size in bytes`,
		Export:       true,
	}
	p.PursuitBufferSize.Init(base.mgr)

	p.PursuitBufferTime = ParamItem{
		Key:          "mq.pursuitBufferTime",
		Version:      "2.4.12",
		DefaultValue: "60", // 60 s
		Doc:          `pursuit mode buffer time in seconds`,
		Export:       true,
	}
	p.PursuitBufferTime.Init(base.mgr)

	p.MQBufSize = ParamItem{
		Key:          "mq.mqBufSize",
		Version:      "2.3.0",
		DefaultValue: "16",
		Doc:          `MQ client consumer buffer length`,
		Export:       true,
	}
	p.MQBufSize.Init(base.mgr)

	p.ReceiveBufSize = ParamItem{
		Key:          "mq.receiveBufSize",
		Version:      "2.3.0",
		DefaultValue: "16",
		Doc:          "MQ consumer chan buffer length",
	}
	p.ReceiveBufSize.Init(base.mgr)

	p.IgnoreBadPosition = ParamItem{
		Key:          "mq.ignoreBadPosition",
		Version:      "2.3.16",
		DefaultValue: "false",
		Doc:          "A switch for ignoring message queue failing to parse message ID from checkpoint position. Usually caused by switching among different mq implementations. May caused data loss when used by mistake",
	}
	p.IgnoreBadPosition.Init(base.mgr)
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

	// Global request timeout
	RequestTimeout ParamItem `refreshable:"false"`

	// Enable Client side metrics
	EnableClientMetrics ParamItem `refreshable:"false"`
}

func (p *PulsarConfig) Init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "pulsar.port",
		Version:      "2.0.0",
		DefaultValue: "6650",
		Doc:          "Port of Pulsar service.",
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
		Doc: `IP address of Pulsar service.
Environment variable: PULSAR_ADDRESS
pulsar.address and pulsar.port together generate the valid access to Pulsar.
Pulsar preferentially acquires the valid IP address from the environment variable PULSAR_ADDRESS when Milvus is started.
Default value applies when Pulsar is running on the same network with Milvus.`,
		Export: true,
	}
	p.Address.Init(base.mgr)

	p.WebPort = ParamItem{
		Key:          "pulsar.webport",
		Version:      "2.0.0",
		DefaultValue: "80",
		Doc:          "Web port of of Pulsar service. If you connect direcly without proxy, should use 8080.",
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
		DefaultValue: "2097152",
		Doc: `The maximum size of each message in Pulsar. Unit: Byte.
By default, Pulsar can transmit at most 2MB of data in a single message. When the size of inserted data is greater than this value, proxy fragments the data into multiple messages to ensure that they can be transmitted correctly.
If the corresponding parameter in Pulsar remains unchanged, increasing this configuration will cause Milvus to fail, and reducing it produces no advantage.`,
		Export: true,
	}
	p.MaxMessageSize.Init(base.mgr)

	p.Tenant = ParamItem{
		Key:          "pulsar.tenant",
		Version:      "2.2.0",
		DefaultValue: "public",
		Doc: `Pulsar can be provisioned for specific tenants with appropriate capacity allocated to the tenant.
To share a Pulsar instance among multiple Milvus instances, you can change this to an Pulsar tenant rather than the default one for each Milvus instance before you start them. However, if you do not want Pulsar multi-tenancy, you are advised to change msgChannel.chanNamePrefix.cluster to the different value.`,
		Export: true,
	}
	p.Tenant.Init(base.mgr)

	p.Namespace = ParamItem{
		Key:          "pulsar.namespace",
		Version:      "2.2.0",
		DefaultValue: "default",
		Doc:          "A Pulsar namespace is the administrative unit nomenclature within a tenant.",
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

	p.RequestTimeout = ParamItem{
		Key:          "pulsar.requestTimeout",
		Version:      "2.3.0",
		DefaultValue: "60",
		Doc:          "pulsar client global request timeout in seconds",
		Export:       true,
	}
	p.RequestTimeout.Init(base.mgr)

	p.EnableClientMetrics = ParamItem{
		Key:          "pulsar.enableClientMetrics",
		Version:      "2.3.0",
		DefaultValue: "false",
		Doc:          "Whether to register pulsar client metrics into milvus metrics path.",
		Export:       true,
	}
	p.EnableClientMetrics.Init(base.mgr)
}

// --- kafka ---
type KafkaConfig struct {
	Address             ParamItem  `refreshable:"false"`
	SaslUsername        ParamItem  `refreshable:"false"`
	SaslPassword        ParamItem  `refreshable:"false"`
	SaslMechanisms      ParamItem  `refreshable:"false"`
	SecurityProtocol    ParamItem  `refreshable:"false"`
	KafkaUseSSL         ParamItem  `refreshable:"false"`
	KafkaTLSCert        ParamItem  `refreshable:"false"`
	KafkaTLSKey         ParamItem  `refreshable:"false"`
	KafkaTLSCACert      ParamItem  `refreshable:"false"`
	KafkaTLSKeyPassword ParamItem  `refreshable:"false"`
	ConsumerExtraConfig ParamGroup `refreshable:"false"`
	ProducerExtraConfig ParamGroup `refreshable:"false"`
	ReadTimeout         ParamItem  `refreshable:"true"`
}

func (k *KafkaConfig) Init(base *BaseTable) {
	// due to implicit rule of MQ priority，the default address should be empty
	k.Address = ParamItem{
		Key:          "kafka.brokerList",
		DefaultValue: "localhost:9092",
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
		DefaultValue: "",
		Version:      "2.1.0",
		Export:       true,
	}
	k.SaslMechanisms.Init(base.mgr)

	k.SecurityProtocol = ParamItem{
		Key:          "kafka.securityProtocol",
		DefaultValue: "",
		Version:      "2.1.0",
		Export:       true,
	}
	k.SecurityProtocol.Init(base.mgr)

	k.KafkaUseSSL = ParamItem{
		Key:          "kafka.ssl.enabled",
		DefaultValue: "false",
		Version:      "2.3.11",
		Doc:          "whether to enable ssl mode",
		Export:       true,
	}
	k.KafkaUseSSL.Init(base.mgr)

	k.KafkaTLSCert = ParamItem{
		Key:     "kafka.ssl.tlsCert",
		Version: "2.3.11",
		Doc:     "path to client's public key (PEM) used for authentication",
		Export:  true,
	}
	k.KafkaTLSCert.Init(base.mgr)

	k.KafkaTLSKey = ParamItem{
		Key:     "kafka.ssl.tlsKey",
		Version: "2.3.11",
		Doc:     "path to client's private key (PEM) used for authentication",
		Export:  true,
	}
	k.KafkaTLSKey.Init(base.mgr)

	k.KafkaTLSCACert = ParamItem{
		Key:     "kafka.ssl.tlsCaCert",
		Version: "2.3.11",
		Doc:     "file or directory path to CA certificate(s) for verifying the broker's key",
		Export:  true,
	}
	k.KafkaTLSCACert.Init(base.mgr)

	k.KafkaTLSKeyPassword = ParamItem{
		Key:     "kafka.ssl.tlsKeyPassword",
		Version: "2.3.11",
		Doc:     "private key passphrase for use with ssl.key.location and set_ssl_cert(), if any",
		Export:  true,
	}
	k.KafkaTLSKeyPassword.Init(base.mgr)

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

	k.ReadTimeout = ParamItem{
		Key:          "kafka.readTimeout",
		DefaultValue: "10",
		Version:      "2.3.1",
		Export:       true,
	}
	k.ReadTimeout.Init(base.mgr)
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
	// CompressionTypes is compression type of each level
	// len of CompressionTypes means num of rocksdb level.
	// only support {0,7}, 0 means no compress, 7 means zstd
	// default [0,7].
	CompressionTypes ParamItem `refreshable:"false"`
}

func (r *RocksmqConfig) Init(base *BaseTable) {
	r.Path = ParamItem{
		Key:     "rocksmq.path",
		Version: "2.0.0",
		Doc: `Prefix of the key to where Milvus stores data in RocksMQ.
Caution: Changing this parameter after using Milvus for a period of time will affect your access to old data.
It is recommended to change this parameter before starting Milvus for the first time.
Set an easy-to-identify root key prefix for Milvus if etcd service already exists.`,
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
		Doc:          "The maximum size of messages in each page in RocksMQ. Messages in RocksMQ are checked and cleared (when expired) in batch based on this parameters. Unit: Byte.",
		Export:       true,
	}
	r.PageSize.Init(base.mgr)

	r.RetentionTimeInMinutes = ParamItem{
		Key:          "rocksmq.retentionTimeInMinutes",
		DefaultValue: "4320",
		Version:      "2.0.0",
		Doc:          "The maximum retention time of acked messages in RocksMQ. Acked messages in RocksMQ are retained for the specified period of time and then cleared. Unit: Minute.",
		Export:       true,
	}
	r.RetentionTimeInMinutes.Init(base.mgr)

	r.RetentionSizeInMB = ParamItem{
		Key:          "rocksmq.retentionSizeInMB",
		DefaultValue: "7200",
		Version:      "2.0.0",
		Doc:          "The maximum retention size of acked messages of each topic in RocksMQ. Acked messages in each topic are cleared if their size exceed this parameter. Unit: MB.",
		Export:       true,
	}
	r.RetentionSizeInMB.Init(base.mgr)

	r.CompactionInterval = ParamItem{
		Key:          "rocksmq.compactionInterval",
		DefaultValue: "86400",
		Version:      "2.0.0",
		Doc:          "Time interval to trigger rocksdb compaction to remove deleted data. Unit: Second",
		Export:       true,
	}
	r.CompactionInterval.Init(base.mgr)

	r.TickerTimeInSeconds = ParamItem{
		Key:          "rocksmq.timtickerInterval",
		DefaultValue: "600",
		Version:      "2.2.2",
	}
	r.TickerTimeInSeconds.Init(base.mgr)

	r.CompressionTypes = ParamItem{
		Key:          "rocksmq.compressionTypes",
		DefaultValue: "0,0,7,7,7",
		Version:      "2.2.12",
		Doc:          "compaction compression type, only support use 0,7. 0 means not compress, 7 will use zstd. Length of types means num of rocksdb level.",
		Export:       true,
	}
	r.CompressionTypes.Init(base.mgr)
}

// NatsmqConfig describes the configuration options for the Nats message queue
type NatsmqConfig struct {
	ServerPort                ParamItem `refreshable:"false"`
	ServerStoreDir            ParamItem `refreshable:"false"`
	ServerMaxFileStore        ParamItem `refreshable:"false"`
	ServerMaxPayload          ParamItem `refreshable:"false"`
	ServerMaxPending          ParamItem `refreshable:"false"`
	ServerInitializeTimeout   ParamItem `refreshable:"false"`
	ServerMonitorTrace        ParamItem `refreshable:"false"`
	ServerMonitorDebug        ParamItem `refreshable:"false"`
	ServerMonitorLogTime      ParamItem `refreshable:"false"`
	ServerMonitorLogFile      ParamItem `refreshable:"false"`
	ServerMonitorLogSizeLimit ParamItem `refreshable:"false"`
	ServerRetentionMaxAge     ParamItem `refreshable:"true"`
	ServerRetentionMaxBytes   ParamItem `refreshable:"true"`
	ServerRetentionMaxMsgs    ParamItem `refreshable:"true"`
}

// Init sets up a new NatsmqConfig instance using the provided BaseTable
func (r *NatsmqConfig) Init(base *BaseTable) {
	r.ServerPort = ParamItem{
		Key:          "natsmq.server.port",
		Version:      "2.3.0",
		DefaultValue: "4222",
		Doc:          "Listening port of the NATS server.",
		Export:       true,
	}
	r.ServerPort.Init(base.mgr)
	r.ServerStoreDir = ParamItem{
		Key:          "natsmq.server.storeDir",
		DefaultValue: "/var/lib/milvus/nats",
		Version:      "2.3.0",
		Doc:          `Directory to use for JetStream storage of nats`,
		Export:       true,
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
	r.ServerMonitorTrace = ParamItem{
		Key:          "natsmq.server.monitor.trace",
		Version:      "2.3.0",
		DefaultValue: "false",
		Doc:          `If true enable protocol trace log messages`,
		Export:       true,
	}
	r.ServerMonitorTrace.Init(base.mgr)
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
		DefaultValue: "/tmp/milvus/logs/nats.log",
		Doc:          `Log file path relative to .. of milvus binary if use relative path`,
		Export:       true,
	}
	r.ServerMonitorLogFile.Init(base.mgr)
	r.ServerMonitorLogSizeLimit = ParamItem{
		Key:          "natsmq.server.monitor.logSizeLimit",
		Version:      "2.3.0",
		DefaultValue: "536870912",
		Doc:          `Size in bytes after the log file rolls over to a new one`,
		Export:       true,
	}
	r.ServerMonitorLogSizeLimit.Init(base.mgr)

	r.ServerRetentionMaxAge = ParamItem{
		Key:          "natsmq.server.retention.maxAge",
		Version:      "2.3.0",
		DefaultValue: "4320",
		Doc:          `Maximum age of any message in the P-channel`,
		Export:       true,
	}
	r.ServerRetentionMaxAge.Init(base.mgr)
	r.ServerRetentionMaxBytes = ParamItem{
		Key:          "natsmq.server.retention.maxBytes",
		Version:      "2.3.0",
		DefaultValue: "",
		Doc:          `How many bytes the single P-channel may contain. Removing oldest messages if the P-channel exceeds this size`,
		Export:       true,
	}
	r.ServerRetentionMaxBytes.Init(base.mgr)
	r.ServerRetentionMaxMsgs = ParamItem{
		Key:          "natsmq.server.retention.maxMsgs",
		Version:      "2.3.0",
		DefaultValue: "",
		Doc:          `How many message the single P-channel may contain. Removing oldest messages if the P-channel exceeds this limit`,
		Export:       true,
	}
	r.ServerRetentionMaxMsgs.Init(base.mgr)
}

// /////////////////////////////////////////////////////////////////////////////
// --- minio ---
type MinioConfig struct {
	Address            ParamItem `refreshable:"false"`
	Port               ParamItem `refreshable:"false"`
	AccessKeyID        ParamItem `refreshable:"false"`
	SecretAccessKey    ParamItem `refreshable:"false"`
	UseSSL             ParamItem `refreshable:"false"`
	SslCACert          ParamItem `refreshable:"false"`
	BucketName         ParamItem `refreshable:"false"`
	RootPath           ParamItem `refreshable:"false"`
	UseIAM             ParamItem `refreshable:"false"`
	CloudProvider      ParamItem `refreshable:"false"`
	GcpCredentialJSON  ParamItem `refreshable:"false"`
	IAMEndpoint        ParamItem `refreshable:"false"`
	LogLevel           ParamItem `refreshable:"false"`
	Region             ParamItem `refreshable:"false"`
	UseVirtualHost     ParamItem `refreshable:"false"`
	RequestTimeoutMs   ParamItem `refreshable:"false"`
	ListObjectsMaxKeys ParamItem `refreshable:"true"`
}

func (p *MinioConfig) Init(base *BaseTable) {
	p.Port = ParamItem{
		Key:          "minio.port",
		DefaultValue: "9000",
		Version:      "2.0.0",
		Doc:          "Port of MinIO or S3 service.",
		PanicIfEmpty: true,
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
		Doc: `IP address of MinIO or S3 service.
Environment variable: MINIO_ADDRESS
minio.address and minio.port together generate the valid access to MinIO or S3 service.
MinIO preferentially acquires the valid IP address from the environment variable MINIO_ADDRESS when Milvus is started.
Default value applies when MinIO or S3 is running on the same network with Milvus.`,
		Export: true,
	}
	p.Address.Init(base.mgr)

	p.AccessKeyID = ParamItem{
		Key:          "minio.accessKeyID",
		Version:      "2.0.0",
		DefaultValue: "minioadmin",
		PanicIfEmpty: false, // tmp fix, need to be conditional
		Doc: `Access key ID that MinIO or S3 issues to user for authorized access.
Environment variable: MINIO_ACCESS_KEY_ID or minio.accessKeyID
minio.accessKeyID and minio.secretAccessKey together are used for identity authentication to access the MinIO or S3 service.
This configuration must be set identical to the environment variable MINIO_ACCESS_KEY_ID, which is necessary for starting MinIO or S3.
The default value applies to MinIO or S3 service that started with the default docker-compose.yml file.`,
		Export: true,
	}
	p.AccessKeyID.Init(base.mgr)

	p.SecretAccessKey = ParamItem{
		Key:          "minio.secretAccessKey",
		Version:      "2.0.0",
		DefaultValue: "minioadmin",
		PanicIfEmpty: false, // tmp fix, need to be conditional
		Doc: `Secret key used to encrypt the signature string and verify the signature string on server. It must be kept strictly confidential and accessible only to the MinIO or S3 server and users.
Environment variable: MINIO_SECRET_ACCESS_KEY or minio.secretAccessKey
minio.accessKeyID and minio.secretAccessKey together are used for identity authentication to access the MinIO or S3 service.
This configuration must be set identical to the environment variable MINIO_SECRET_ACCESS_KEY, which is necessary for starting MinIO or S3.
The default value applies to MinIO or S3 service that started with the default docker-compose.yml file.`,
		Export: true,
	}
	p.SecretAccessKey.Init(base.mgr)

	p.UseSSL = ParamItem{
		Key:          "minio.useSSL",
		Version:      "2.0.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
		Doc:          "Switch value to control if to access the MinIO or S3 service through SSL.",
		Export:       true,
	}
	p.UseSSL.Init(base.mgr)

	p.SslCACert = ParamItem{
		Key:     "minio.ssl.tlsCACert",
		Version: "2.3.12",
		Doc:     "path to your CACert file",
		Export:  true,
	}
	p.SslCACert.Init(base.mgr)

	p.BucketName = ParamItem{
		Key:          "minio.bucketName",
		Version:      "2.0.0",
		DefaultValue: "a-bucket",
		PanicIfEmpty: true,
		Doc: `Name of the bucket where Milvus stores data in MinIO or S3.
Milvus 2.0.0 does not support storing data in multiple buckets.
Bucket with this name will be created if it does not exist. If the bucket already exists and is accessible, it will be used directly. Otherwise, there will be an error.
To share an MinIO instance among multiple Milvus instances, consider changing this to a different value for each Milvus instance before you start them. For details, see Operation FAQs.
The data will be stored in the local Docker if Docker is used to start the MinIO service locally. Ensure that there is sufficient storage space.
A bucket name is globally unique in one MinIO or S3 instance.`,
		Export: true,
	}
	p.BucketName.Init(base.mgr)

	p.RootPath = ParamItem{
		Key:     "minio.rootPath",
		Version: "2.0.0",
		Formatter: func(rootPath string) string {
			if rootPath == "" {
				return ""
			}
			rootPath = strings.TrimLeft(rootPath, "/")
			return path.Clean(rootPath)
		},
		PanicIfEmpty: false,
		Doc: `Root prefix of the key to where Milvus stores data in MinIO or S3.
It is recommended to change this parameter before starting Milvus for the first time.
To share an MinIO instance among multiple Milvus instances, consider changing this to a different value for each Milvus instance before you start them. For details, see Operation FAQs.
Set an easy-to-identify root key prefix for Milvus if etcd service already exists.
Changing this for an already running Milvus instance may result in failures to read legacy data.`,
		Export: true,
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
		Version:      "2.4.1",
		Doc: `Cloud Provider of S3. Supports: "aws", "gcp", "aliyun".
Cloud Provider of Google Cloud Storage. Supports: "gcpnative".
You can use "aws" for other cloud provider supports S3 API with signature v4, e.g.: minio
You can use "gcp" for other cloud provider supports S3 API with signature v2
You can use "aliyun" for other cloud provider uses virtual host style bucket
You can use "gcpnative" for the Google Cloud Platform provider. Uses service account credentials
for authentication.
When useIAM enabled, only "aws", "gcp", "aliyun" is supported for now`,
		Export: true,
	}
	p.CloudProvider.Init(base.mgr)

	p.GcpCredentialJSON = ParamItem{
		Key:          "minio.gcpCredentialJSON",
		Version:      "2.4.1",
		DefaultValue: "",
		Doc: `The JSON content contains the gcs service account credentials.
Used only for the "gcpnative" cloud provider.`,
		Export: true,
	}
	p.GcpCredentialJSON.Init(base.mgr)

	p.IAMEndpoint = ParamItem{
		Key:          "minio.iamEndpoint",
		DefaultValue: DefaultMinioIAMEndpoint,
		Version:      "2.0.0",
		Doc: `Custom endpoint for fetch IAM role credentials. when useIAM is true & cloudProvider is "aws".
Leave it empty if you want to use AWS default endpoint`,
		Export: true,
	}
	p.IAMEndpoint.Init(base.mgr)
	p.LogLevel = ParamItem{
		Key:          "minio.logLevel",
		DefaultValue: DefaultMinioLogLevel,
		Version:      "2.3.0",
		Doc:          `Log level for aws sdk log. Supported level:  off, fatal, error, warn, info, debug, trace`,
		Export:       true,
	}
	p.LogLevel.Init(base.mgr)
	p.Region = ParamItem{
		Key:          "minio.region",
		DefaultValue: DefaultMinioRegion,
		Version:      "2.3.0",
		Doc:          `Specify minio storage system location region`,
		Export:       true,
	}
	p.Region.Init(base.mgr)

	p.UseVirtualHost = ParamItem{
		Key:          "minio.useVirtualHost",
		Version:      "2.3.0",
		DefaultValue: DefaultMinioUseVirtualHost,
		PanicIfEmpty: false,
		Doc:          "Whether use virtual host mode for bucket",
		Export:       true,
	}
	p.UseVirtualHost.Init(base.mgr)

	p.RequestTimeoutMs = ParamItem{
		Key:          "minio.requestTimeoutMs",
		Version:      "2.3.2",
		DefaultValue: DefaultMinioRequestTimeout,
		Doc:          "minio timeout for request time in milliseconds",
		Export:       true,
	}
	p.RequestTimeoutMs.Init(base.mgr)

	p.ListObjectsMaxKeys = ParamItem{
		Key:          "minio.listObjectsMaxKeys",
		Version:      "2.4.1",
		DefaultValue: "0",
		Doc: `The maximum number of objects requested per batch in minio ListObjects rpc, 
0 means using oss client by default, decrease these configration if ListObjects timeout`,
		Export: true,
	}
	p.ListObjectsMaxKeys.Init(base.mgr)
}

// profile config
type ProfileConfig struct {
	PprofPath ParamItem `refreshable:"false"`
}

func (p *ProfileConfig) Init(base *BaseTable) {
	p.PprofPath = ParamItem{
		Key:          "profile.pprof.path",
		Version:      "2.5.5",
		DefaultValue: "",
		Doc:          "The folder that storing pprof files, by default will use localStoragePath/pprof",
		Formatter: func(v string) string {
			if len(v) == 0 {
				return path.Join(base.Get("localStorage.path"), "pprof")
			}
			return v
		},
		Export: true,
	}
	p.PprofPath.Init(base.mgr)
}
