package backend

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
)

type etcdBasedBackend struct {
	cfg     *configs.MilvusConfig
	txn     kv.MetaKv
	etcdCli *clientv3.Client
}

func (b etcdBasedBackend) CleanWithPrefix(prefix string) error {
	return b.txn.RemoveWithPrefix(context.TODO(), prefix)
}

func newEtcdBasedBackend(cfg *configs.MilvusConfig) (*etcdBasedBackend, error) {
	etcdCli, err := etcd.CreateEtcdClient(
		cfg.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		cfg.EtcdCfg.EtcdEnableAuth.GetAsBool(),
		cfg.EtcdCfg.EtcdAuthUserName.GetValue(),
		cfg.EtcdCfg.EtcdAuthPassword.GetValue(),
		cfg.EtcdCfg.EtcdUseSSL.GetAsBool(),
		cfg.EtcdCfg.Endpoints.GetAsStrings(),
		cfg.EtcdCfg.EtcdTLSCert.GetValue(),
		cfg.EtcdCfg.EtcdTLSKey.GetValue(),
		cfg.EtcdCfg.EtcdTLSCACert.GetValue(),
		cfg.EtcdCfg.EtcdTLSMinVersion.GetValue())
	if err != nil {
		return nil, err
	}
	txn := etcdkv.NewEtcdKV(etcdCli, cfg.EtcdCfg.MetaRootPath.GetValue())
	b := &etcdBasedBackend{cfg: cfg, etcdCli: etcdCli, txn: txn}
	return b, nil
}
