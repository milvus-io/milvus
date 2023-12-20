package backend

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

type etcdBasedBackend struct {
	cfg     *configs.MilvusConfig
	txn     kv.MetaKv
	etcdCli *clientv3.Client
}

func (b etcdBasedBackend) CleanWithPrefix(prefix string) error {
	return b.txn.RemoveWithPrefix(prefix)
}

func newEtcdBasedBackend(cfg *configs.MilvusConfig) (*etcdBasedBackend, error) {
	config := cfg.EtcdCfg
	etcdInfo := &etcd.EtcdConfig{
		UseEmbed:             config.UseEmbedEtcd.GetAsBool(),
		UseSSL:               config.EtcdUseSSL.GetAsBool(),
		Endpoints:            config.Endpoints.GetAsStrings(),
		CertFile:             config.EtcdTLSCert.GetValue(),
		KeyFile:              config.EtcdTLSKey.GetValue(),
		CaCertFile:           config.EtcdTLSCACert.GetValue(),
		MinVersion:           config.EtcdTLSMinVersion.GetValue(),
		MaxRetries:           config.GrpcMaxRetries.GetAsInt64(),
		PerRetryTimeout:      config.GrpcPerRetryTimeout.GetAsDuration(time.Millisecond),
		DialTimeout:          config.GrpcDialTimeout.GetAsDuration(time.Millisecond),
		DialKeepAliveTime:    config.GrpcDialKeepAliveTime.GetAsDuration(time.Millisecond),
		DialKeepAliveTimeout: config.GrpcDialKeepAliveTimeout.GetAsDuration(time.Millisecond),
	}
	etcdCli, err := etcd.GetEtcdClient(etcdInfo)
	if err != nil {
		return nil, err
	}
	txn := etcdkv.NewEtcdKV(etcdCli, cfg.EtcdCfg.MetaRootPath.GetValue())
	b := &etcdBasedBackend{cfg: cfg, etcdCli: etcdCli, txn: txn}
	return b, nil
}
