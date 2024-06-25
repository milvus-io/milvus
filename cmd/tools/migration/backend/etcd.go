package backend

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	etcdConfig := paramtable.GetEtcdCfg(cfg.EtcdCfg)
	etcdCli, err := etcd.CreateEtcdClient(etcdConfig)
	if err != nil {
		return nil, err
	}
	txn := etcdkv.NewEtcdKV(etcdCli, cfg.EtcdCfg.MetaRootPath.GetValue())
	b := &etcdBasedBackend{cfg: cfg, etcdCli: etcdCli, txn: txn}
	return b, nil
}
