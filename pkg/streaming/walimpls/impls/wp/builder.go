package wp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"github.com/zilliztech/woodpecker/common/config"
	wpMetrics "github.com/zilliztech/woodpecker/common/metrics"
	wpStorageClient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/woodpecker"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func init() {
	// register the builder to the wal registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(message.WALNameWoodpecker, UnmarshalMessageID)
}

// builderImpl is the builder for woodpecker opener.
type builderImpl struct{}

// Name of the wal builder, should be a lowercase string.
func (b *builderImpl) Name() message.WALName {
	return message.WALNameWoodpecker
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	cfg, err := b.getWpConfig()
	if err != nil {
		return nil, err
	}
	wpMetrics.RegisterClientMetricsWithRegisterer(metrics.GetRegisterer())
	var storageClient wpStorageClient.ObjectStorage
	if cfg.Woodpecker.Storage.IsStorageMinio() {
		storageClient, err = wpStorageClient.NewObjectStorage(context.Background(), cfg)
		if err != nil {
			return nil, err
		}
		log.Ctx(context.Background()).Info("create minio handler finish while building wp opener")
	}
	etcdCli, err := getEtcdClient(context.TODO())
	if err != nil {
		return nil, err
	}
	log.Ctx(context.Background()).Info("create etcd client finish while building wp opener")
	var wpClient woodpecker.Client
	if cfg.Woodpecker.Storage.IsStorageService() {
		wpClient, err = woodpecker.NewClient(context.Background(), cfg, etcdCli, true)
	} else {
		wpMetrics.RegisterServerMetricsWithRegisterer(metrics.GetRegisterer())
		wpClient, err = woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli, storageClient, true)
	}
	if err != nil {
		return nil, err
	}
	log.Ctx(context.Background()).Info("build wp opener finish", zap.String("wpClientInstance", fmt.Sprintf("%p", wpClient)))
	return &openerImpl{
		c: wpClient,
	}, nil
}

func (b *builderImpl) getWpConfig() (*config.Configuration, error) {
	wpConfig, err := config.NewConfiguration()
	if err != nil {
		return nil, err
	}
	err = setCustomWpConfig(wpConfig, &paramtable.Get().WoodpeckerCfg)
	if err != nil {
		return nil, err
	}
	return wpConfig, nil
}

func setCustomWpConfig(wpConfig *config.Configuration, cfg *paramtable.WoodpeckerConfig) error {
	// set the rootPath as the prefix for wp object storage
	wpConfig.Woodpecker.Meta.Prefix = paramtable.Get().WoodpeckerCfg.MetaPrefix.GetValue()
	wpConfig.Etcd.RootPath = paramtable.Get().EtcdCfg.RootPath.GetValue()
	// logClient
	wpConfig.Woodpecker.Client.Auditor.MaxInterval = config.NewDurationSecondsFromInt(int(cfg.AuditorMaxInterval.GetAsDurationByParse().Seconds()))
	wpConfig.Woodpecker.Client.SegmentAppend.MaxRetries = cfg.AppendMaxRetries.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentAppend.QueueSize = cfg.AppendQueueSize.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize = config.NewByteSize(cfg.SegmentRollingMaxSize.GetAsSize())
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = config.NewDurationSecondsFromInt(int(cfg.SegmentRollingMaxTime.GetAsDurationByParse().Seconds()))
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks = cfg.SegmentRollingMaxBlocks.GetAsInt64()

	// quorum configuration
	setQuorumConfig(wpConfig, cfg)

	// logStore
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = config.NewDurationMillisecondsFromInt(int(cfg.SyncMaxInterval.GetAsDurationByParse().Milliseconds()))
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage = config.NewDurationMillisecondsFromInt(int(cfg.SyncMaxIntervalForLocalStorage.GetAsDurationByParse().Milliseconds()))
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries = cfg.SyncMaxEntries.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = config.NewByteSize(cfg.SyncMaxBytes.GetAsSize())
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushRetries = cfg.FlushMaxRetries.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(cfg.FlushMaxSize.GetAsSize())
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = cfg.FlushMaxThreads.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval = config.NewDurationMillisecondsFromInt(int(cfg.RetryInterval.GetAsDurationByParse().Milliseconds()))
	wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes = config.NewByteSize(cfg.CompactionSize.GetAsSize())
	wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads = cfg.CompactionMaxParallelUploads.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads = cfg.CompactionMaxParallelReads.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = config.NewByteSize(cfg.ReaderMaxBatchSize.GetAsSize())
	wpConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads = cfg.ReaderMaxFetchThreads.GetAsInt()
	wpConfig.Woodpecker.Logstore.RetentionPolicy.TTL = int(cfg.RetentionTTL.GetAsDurationByParse().Milliseconds() / 1000) // convert to seconds
	wpConfig.Woodpecker.Logstore.FencePolicy.ConditionWrite = cfg.FencePolicyConditionWrite.GetValue()

	// storage
	wpConfig.Woodpecker.Storage.Type = cfg.StorageType.GetValue()

	// Set RootPath based on configuration
	if cfg.RootPath.GetValue() == "default" {
		// Use LocalStorage.Path as prefix with "wp" subdirectory for default
		wpConfig.Woodpecker.Storage.RootPath = fmt.Sprintf("%s/wp", paramtable.Get().LocalStorageCfg.Path.GetValue())
	} else {
		// Use custom directory as-is
		wpConfig.Woodpecker.Storage.RootPath = cfg.RootPath.GetValue()
	}

	// set bucketName
	wpConfig.Minio.BucketName = paramtable.Get().MinioCfg.BucketName.GetValue()
	wpConfig.Minio.RootPath = fmt.Sprintf("%s/wp", paramtable.Get().MinioCfg.RootPath.GetValue())
	wpConfig.Minio.UseSSL = paramtable.Get().MinioCfg.UseSSL.GetAsBool()
	wpConfig.Minio.UseIAM = paramtable.Get().MinioCfg.UseIAM.GetAsBool()
	addr := paramtable.Get().MinioCfg.Address.GetValue()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		wpConfig.Minio.Address = addr
	} else {
		wpConfig.Minio.Address = host
	}
	wpConfig.Minio.Port = paramtable.Get().MinioCfg.Port.GetAsInt()
	wpConfig.Minio.Region = paramtable.Get().MinioCfg.Region.GetValue()
	wpConfig.Minio.Ssl.TlsCACert = paramtable.Get().MinioCfg.SslCACert.GetValue()
	wpConfig.Minio.SecretAccessKey = paramtable.Get().MinioCfg.SecretAccessKey.GetValue()
	wpConfig.Minio.AccessKeyID = paramtable.Get().MinioCfg.AccessKeyID.GetValue()
	wpConfig.Minio.GcpCredentialJSON = paramtable.Get().MinioCfg.GcpCredentialJSON.GetValue()
	wpConfig.Minio.CloudProvider = paramtable.Get().MinioCfg.CloudProvider.GetValue()
	wpConfig.Minio.ListObjectsMaxKeys = paramtable.Get().MinioCfg.ListObjectsMaxKeys.GetAsInt()
	wpConfig.Minio.CreateBucket = true
	wpConfig.Minio.IamEndpoint = paramtable.Get().MinioCfg.IAMEndpoint.GetValue()
	wpConfig.Minio.UseVirtualHost = paramtable.Get().MinioCfg.UseVirtualHost.GetAsBool()
	wpConfig.Minio.RequestTimeoutMs = config.NewDurationMillisecondsFromInt(paramtable.Get().MinioCfg.RequestTimeoutMs.GetAsInt())
	wpConfig.Minio.LogLevel = paramtable.Get().LogCfg.Level.GetValue()

	// set log
	wpConfig.Log.Level = paramtable.Get().LogCfg.Level.GetValue()
	wpConfig.Log.Format = paramtable.Get().LogCfg.Format.GetValue()
	wpConfig.Log.Stdout = paramtable.Get().LogCfg.Stdout.GetAsBool()
	wpConfig.Log.File.RootPath = paramtable.Get().LogCfg.RootPath.GetValue()
	wpConfig.Log.File.MaxSize = paramtable.Get().LogCfg.MaxSize.GetAsInt()
	wpConfig.Log.File.MaxAge = paramtable.Get().LogCfg.MaxAge.GetAsInt()
	wpConfig.Log.File.MaxBackups = paramtable.Get().LogCfg.MaxBackups.GetAsInt()

	return nil
}

func setQuorumConfig(wpConfig *config.Configuration, cfg *paramtable.WoodpeckerConfig) {
	q := &wpConfig.Woodpecker.Client.Quorum

	// Bind milvus' dynamic config as the runtime source for woodpecker's quorum
	// knobs. milvus is the authoritative config source, so the source always wins
	// (ok=true) over woodpecker's static YAML value. These params are
	// refreshable:"true", and woodpecker calls .Get() per segment when selecting a
	// quorum (woodpecker/quorum/discovery.go), so etcd config changes take effect on
	// the next segment with no restart.
	q.SelectStrategy.AffinityMode.WithSource(func() (string, bool) {
		return cfg.QuorumAffinityMode.GetValue(), true
	})
	q.SelectStrategy.Replicas.WithSource(func() (int, bool) {
		return cfg.QuorumReplicas.GetAsInt(), true
	})
	q.SelectStrategy.Strategy.WithSource(func() (string, bool) {
		return cfg.QuorumStrategy.GetValue(), true
	})

	// JSON-encoded knobs: parse on each read; on empty/parse failure return ok=false
	// so woodpecker falls back to its static (YAML default) value. No logging here
	// because .Get() is called per segment and would spam; format problems are
	// surfaced by the startup validation and change callbacks below instead.
	q.BufferPools.WithSource(func() ([]config.QuorumBufferPool, bool) {
		raw := cfg.QuorumBufferPools.GetValue()
		if raw == "" {
			return nil, false
		}
		var pools []config.QuorumBufferPool
		if err := json.Unmarshal([]byte(raw), &pools); err != nil {
			return nil, false
		}
		return pools, true
	})
	q.SelectStrategy.CustomPlacement.WithSource(func() ([]config.CustomPlacement, bool) {
		raw := cfg.QuorumCustomPlacement.GetValue()
		if raw == "" {
			return nil, false
		}
		var customPlacements []config.CustomPlacement
		if err := json.Unmarshal([]byte(raw), &customPlacements); err != nil {
			return nil, false
		}
		return customPlacements, true
	})

	// Validate the current JSON values once at startup (the change callbacks below
	// only fire on subsequent updates, not on the initial value). Invalid JSON only
	// warns; woodpecker falls back to its static default.
	validateQuorumJSON("woodpecker quorum buffer pools", cfg.QuorumBufferPools.GetValue(), func(b []byte) error {
		var v []config.QuorumBufferPool
		return json.Unmarshal(b, &v)
	})
	validateQuorumJSON("woodpecker quorum custom placement", cfg.QuorumCustomPlacement.GetValue(), func(b []byte) error {
		var v []config.CustomPlacement
		return json.Unmarshal(b, &v)
	})

	// Validate JSON format on config change. A non-nil error is logged once per
	// change by the param framework ("param change callback failed"); it does not
	// veto the change, so on bad JSON woodpecker simply falls back to its static
	// default on the next .Get().
	cfg.QuorumBufferPools.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
		if newValue == "" {
			return nil
		}
		var v []config.QuorumBufferPool
		if err := json.Unmarshal([]byte(newValue), &v); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid quorum buffer pools JSON %q: %v", newValue, err)
		}
		return nil
	})
	cfg.QuorumCustomPlacement.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
		if newValue == "" {
			return nil
		}
		var v []config.CustomPlacement
		if err := json.Unmarshal([]byte(newValue), &v); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid quorum custom placement JSON %q: %v", newValue, err)
		}
		return nil
	})
}

// validateQuorumJSON parses a non-empty raw JSON config once and warns on failure.
func validateQuorumJSON(label, raw string, parse func([]byte) error) {
	if raw == "" {
		return
	}
	if err := parse([]byte(raw)); err != nil {
		log.Ctx(context.Background()).Warn("invalid quorum JSON config at startup, will fall back to static default",
			zap.String("config", label),
			zap.String("json", raw),
			zap.Error(err))
	}
}

func getEtcdClient(ctx context.Context) (*clientv3.Client, error) {
	params := paramtable.Get()
	etcdConfig := &params.EtcdCfg
	log := log.Ctx(ctx)

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
		etcdConfig.ClientOptions()...)
	if err != nil {
		log.Warn("Woodpecker create connection to etcd failed", zap.Error(err))
		return nil, err
	}
	return etcdCli, nil
}
